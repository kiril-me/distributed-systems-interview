package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
)

// ErrRunNotFound is returned when there is no run with that id. The API turns
// it into a 404.
var ErrRunNotFound = errors.New("run not found")

// ErrNotImplemented is turned into a 501 by the API.
var ErrNotImplemented = errors.New("not implemented")

// Executor owns runs and their steps (docs/decisions.md).
//
// One loop decides: it dispatches ready steps, publishes the outbox and
// applies results. Every write, the loop's and the API's, happens in a
// transaction that holds the run's row lock. The executor has no timeouts: it
// never decides an attempt is lost, the driver reports what the ledger says.
type Executor struct {
	db          *sql.DB
	nc          *nats.Conn
	storage     *storage.Storage
	maxAttempts int

	wake    chan struct{}
	results chan resultEvent
	stop    context.CancelFunc
	done    sync.WaitGroup

	// What the drivers last said about their devices. In memory only: it is
	// about now, and a restart is filled again within StatusInterval.
	mu        sync.Mutex
	listening time.Time             // when status messages started coming in
	devices   map[string]deviceSeen // by device id
	warned    map[string]bool       // job ids already logged as on an offline device
	statusSub *nats.Subscription
}

type deviceSeen struct {
	at     time.Time
	status protocol.DeviceStatus
}

// resultEvent hands one message from the result stream to the loop and waits
// for it to be committed.
type resultEvent struct {
	apply   func(context.Context) error
	applied chan error
}

func NewExecutor(db *sql.DB, nc *nats.Conn, st *storage.Storage) *Executor {
	maxAttempts := 3
	if v, err := strconv.Atoi(os.Getenv("MAX_ATTEMPTS")); err == nil && v > 0 {
		maxAttempts = v
	}
	return &Executor{
		db:          db,
		nc:          nc,
		storage:     st,
		maxAttempts: maxAttempts,
		wake:        make(chan struct{}, 1),
		results:     make(chan resultEvent),
		devices:     map[string]deviceSeen{},
		warned:      map[string]bool{},
	}
}

// Start is called once, when the executor process starts. Nothing needs
// repairing: unpublished outbox rows are published by the loop, and unacked
// results are redelivered by JetStream.
func (e *Executor) Start(ctx context.Context) error {
	if err := e.migrate(ctx); err != nil {
		return err
	}
	js, err := jetstream.New(e.nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	if err := protocol.EnsureStreams(ctx, js); err != nil {
		return err
	}
	kv, err := protocol.RevokedKV(ctx, js)
	if err != nil {
		return err
	}
	cons, err := protocol.ResultConsumer(ctx, js)
	if err != nil {
		return fmt.Errorf("result consumer: %w", err)
	}
	e.mu.Lock()
	e.listening = time.Now()
	e.mu.Unlock()
	if e.statusSub, err = e.nc.Subscribe(protocol.StatusSubject("*"), e.onStatus); err != nil {
		return fmt.Errorf("status: %w", err)
	}

	ctx, e.stop = context.WithCancel(ctx)
	e.done.Add(2)
	go func() { defer e.done.Done(); e.loop(ctx, js, kv) }()
	go func() { defer e.done.Done(); e.consume(ctx, cons) }()
	log.Printf("executor: started, max %d attempts per step", e.maxAttempts)
	return nil
}

// Close is called once, when the executor process stops.
func (e *Executor) Close() {
	if e.statusSub != nil {
		_ = e.statusSub.Unsubscribe()
	}
	if e.stop != nil {
		e.stop()
		e.done.Wait()
	}
}

// onStatus keeps a driver's latest status message.
func (e *Executor) onStatus(msg *nats.Msg) {
	var s protocol.DeviceStatus
	if err := json.Unmarshal(msg.Data, &s); err != nil || s.DeviceID == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.devices[s.DeviceID] = deviceSeen{at: time.Now(), status: s}
}

// seen returns what the drivers last said, and since when the executor has
// been listening.
func (e *Executor) seen() (map[string]deviceSeen, time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]deviceSeen, len(e.devices))
	for k, v := range e.devices {
		out[k] = v
	}
	return out, e.listening
}

// warnOffline logs, once, each attempt that is queued or running on a device
// whose driver has gone quiet. The step is not changed: the device may still
// be working, and the record waits for its ledger (ADR-2). This log line is
// what an alert would be built on.
func (e *Executor) warnOffline(ctx context.Context) error {
	rows, err := e.db.QueryContext(ctx,
		`SELECT job_id, device_id FROM attempts WHERE status IN ($1, $2)`, AttemptDispatched, AttemptStarted)
	if err != nil {
		return err
	}
	var open []openAttempt
	for rows.Next() {
		var a openAttempt
		if err := rows.Scan(&a.jobID, &a.device); err != nil {
			rows.Close()
			return err
		}
		open = append(open, a)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	devices, since := e.seen()
	lastSeen := make(map[string]time.Time, len(devices))
	for id, d := range devices {
		lastSeen[id] = d.at
	}
	now := time.Now()
	warned := map[string]bool{}
	for _, a := range offline(open, lastSeen, since, now) {
		warned[a.jobID] = true
		if !e.warned[a.jobID] {
			heard := "never since the executor started"
			if at, ok := lastSeen[a.device]; ok {
				heard = now.Sub(at).Round(time.Second).String() + " ago"
			}
			log.Printf("executor: ALERT %s waits on %s, offline: last heard %s", a.jobID, a.device, heard)
		}
	}
	e.warned = warned // only the loop touches it
	return nil
}

// poke wakes the loop without waiting for it.
func (e *Executor) poke() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// loop is the one place that decides. After any event it dispatches what is
// ready and publishes what is committed but not yet sent.
func (e *Executor) loop(ctx context.Context, js jetstream.JetStream, kv jetstream.KeyValue) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-e.results:
			ev.applied <- ev.apply(ctx)
		case <-e.wake:
		case <-tick.C:
		}
		if err := e.dispatch(ctx); err != nil {
			log.Printf("executor: dispatch: %v", err)
		}
		if err := e.publish(ctx, js); err != nil {
			log.Printf("executor: publish: %v", err)
		}
		if err := e.revoke(ctx, kv); err != nil {
			log.Printf("executor: revoke: %v", err)
		}
		if err := e.warnOffline(ctx); err != nil {
			log.Printf("executor: offline check: %v", err)
		}
	}
}

// migrate adds what db/init.sql gained after the first deploy, so an existing
// database keeps its runs.
func (e *Executor) migrate(ctx context.Context) error {
	if _, err := e.db.ExecContext(ctx, `
		ALTER TABLE runs ADD COLUMN IF NOT EXISTS idempotency_key TEXT UNIQUE;
		ALTER TABLE attempts ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;`); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// revoke puts every queued attempt of a cancelled run in the REVOKED bucket,
// so its driver reports it instead of running it. An attempt already on its
// device is left alone: it ends as it physically ends. Revoking is best
// effort; a driver that read the bucket just before still runs the job, and
// the record says so.
func (e *Executor) revoke(ctx context.Context, kv jetstream.KeyValue) error {
	rows, err := e.db.QueryContext(ctx,
		`SELECT a.job_id FROM attempts a JOIN runs r ON r.id = a.run_id
		  WHERE r.cancel_requested AND a.status = $1 AND a.revoked_at IS NULL`, AttemptDispatched)
	if err != nil {
		return err
	}
	var jobs []string
	for rows.Next() {
		var job string
		if err := rows.Scan(&job); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, job)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, job := range jobs {
		putCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := kv.Put(putCtx, job, []byte("cancelled"))
		cancel()
		if err != nil {
			return fmt.Errorf("revoke %s: %w", job, err)
		}
		if _, err := e.db.ExecContext(ctx,
			`UPDATE attempts SET revoked_at = now() WHERE job_id = $1`, job); err != nil {
			return err
		}
	}
	return nil
}

// consume reads results one at a time and acks each only after the loop has
// committed it (ADR-1). A result that cannot be applied is redelivered.
func (e *Executor) consume(ctx context.Context, cons jetstream.Consumer) {
	protocol.Receive(ctx, cons, func(msg jetstream.Msg) { e.onResult(ctx, msg) })
}

func (e *Executor) onResult(ctx context.Context, msg jetstream.Msg) {
	var jobID string
	var apply func(context.Context) error
	var err error
	if protocol.IsStarted(msg.Subject()) {
		var s protocol.Started
		err = json.Unmarshal(msg.Data(), &s)
		jobID, apply = s.JobID, func(ctx context.Context) error { return e.applyStarted(ctx, s) }
	} else {
		var res protocol.Result
		err = json.Unmarshal(msg.Data(), &res)
		jobID, apply = res.JobID, func(ctx context.Context) error { return e.applyResult(ctx, res) }
	}
	if err != nil {
		log.Printf("executor: dropping unreadable %s: %v", msg.Subject(), err)
		_ = msg.Term()
		return
	}
	if _, _, _, err := protocol.ParseJobID(jobID); err != nil {
		log.Printf("executor: dropping %s: %v", msg.Subject(), err)
		_ = msg.Term()
		return
	}

	ev := resultEvent{apply: apply, applied: make(chan error, 1)}
	select {
	case e.results <- ev:
	case <-ctx.Done():
		return
	}
	select {
	case err := <-ev.applied:
		if errors.Is(err, ErrRunNotFound) {
			// A real result's run is committed before its command is
			// published, so this one can never apply. Retrying it would hold
			// up every result behind it (MaxAckPending 1).
			log.Printf("executor: dropping %s: %v", jobID, err)
			_ = msg.Term()
			return
		}
		if err != nil {
			log.Printf("executor: apply %s: %v", jobID, err)
			_ = msg.NakWithDelay(time.Second)
			return
		}
		if err := msg.Ack(); err != nil {
			// Redelivered later; applying it again changes nothing.
			log.Printf("executor: ack %s: %v", jobID, err)
		}
	case <-ctx.Done():
	}
}

// runRow is the part of a run that every decision needs.
type runRow struct {
	status          string
	cancelRequested bool
}

// withRun runs fn in a transaction that holds the run's row lock, so the loop
// and the API never act on a stale view of the same run.
func (e *Executor) withRun(ctx context.Context, runID string, fn func(tx *sql.Tx, r runRow) error) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	var r runRow
	err = tx.QueryRowContext(ctx,
		`SELECT status, cancel_requested FROM runs WHERE id = $1 FOR UPDATE`, runID,
	).Scan(&r.status, &r.cancelRequested)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrRunNotFound
	}
	if err != nil {
		return fmt.Errorf("lock run %s: %w", runID, err)
	}
	if err := fn(tx, r); err != nil {
		return err
	}
	return tx.Commit()
}

// dispatch creates an attempt for every step that is ready, in every running
// run. Inserting the attempt is the outbox write (ADR-1).
func (e *Executor) dispatch(ctx context.Context) error {
	rows, err := e.db.QueryContext(ctx,
		`SELECT id FROM runs WHERE status = $1 AND NOT cancel_requested`, RunRunning)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// One run that cannot be dispatched must not hold up the others.
	var errs []error
	for _, id := range ids {
		if err := e.withRun(ctx, id, func(tx *sql.Tx, r runRow) error {
			return dispatchRun(ctx, tx, id, r)
		}); err != nil {
			errs = append(errs, fmt.Errorf("run %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

func dispatchRun(ctx context.Context, tx *sql.Tx, runID string, r runRow) error {
	if r.status != RunRunning || r.cancelRequested {
		return nil
	}
	steps, err := loadSteps(ctx, tx, runID)
	if err != nil {
		return err
	}
	device := map[string]string{}
	for _, s := range steps {
		device[s.Name] = s.DeviceID
	}

	for _, name := range NextSteps(steps) {
		var n int
		if err := tx.QueryRowContext(ctx,
			`SELECT count(*) + 1 FROM attempts WHERE run_id = $1 AND step = $2`, runID, name,
		).Scan(&n); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO attempts (job_id, run_id, step, n, device_id) VALUES ($1, $2, $3, $4, $5)`,
			protocol.JobID(runID, name, n), runID, name, n, device[name]); err != nil {
			return err
		}
		// The new attempt is now the last one, and it has no times or error yet.
		if _, err := tx.ExecContext(ctx,
			`UPDATE steps SET status = $3, started_at = NULL, finished_at = NULL, error = NULL, data_ref = NULL
			  WHERE run_id = $1 AND name = $2`,
			runID, name, StepRunning); err != nil {
			return err
		}
	}
	return nil
}

func loadSteps(ctx context.Context, tx *sql.Tx, runID string) ([]StepState, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT name, device_id, depends_on, status FROM steps WHERE run_id = $1 ORDER BY position`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var steps []StepState
	for rows.Next() {
		var s StepState
		var deps pq.StringArray
		if err := rows.Scan(&s.Name, &s.DeviceID, &deps, &s.Status); err != nil {
			return nil, err
		}
		s.DependsOn = deps
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

// publish sends every committed, unsent command. A crash between the publish
// and the update sends it again; the message id and the driver's ledger check
// make that harmless (ADR-2).
func (e *Executor) publish(ctx context.Context, js jetstream.JetStream) error {
	rows, err := e.db.QueryContext(ctx,
		`SELECT job_id, run_id, step, device_id FROM attempts WHERE published_at IS NULL ORDER BY created_at`)
	if err != nil {
		return err
	}
	var cmds []protocol.Command
	for rows.Next() {
		var c protocol.Command
		if err := rows.Scan(&c.JobID, &c.RunID, &c.Step, &c.DeviceID); err != nil {
			rows.Close()
			return err
		}
		cmds = append(cmds, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, c := range cmds {
		data, err := json.Marshal(c)
		if err != nil {
			return err
		}
		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = js.Publish(pubCtx, protocol.CommandSubject(c.DeviceID), data, jetstream.WithMsgID(c.JobID))
		cancel()
		if err != nil {
			return fmt.Errorf("command %s: %w", c.JobID, err)
		}
		if _, err := e.db.ExecContext(ctx,
			`UPDATE attempts SET published_at = now() WHERE job_id = $1`, c.JobID); err != nil {
			return err
		}
	}
	return nil
}

// applyResult records how an attempt ended and moves its step and run on.
// A result for an attempt that is no longer dispatched is a duplicate.
func (e *Executor) applyResult(ctx context.Context, res protocol.Result) error {
	runID, step, n, err := protocol.ParseJobID(res.JobID)
	if err != nil {
		return err
	}
	return e.withRun(ctx, runID, func(tx *sql.Tx, r runRow) error {
		// A revoked job never reached the instrument: no times, no outcome.
		var started, finished, outcome any
		if attemptStatus(res) != AttemptRevoked {
			started, finished = protocol.InstrumentTime(res.StartedAt), protocol.InstrumentTime(res.FinishedAt)
			outcome = res.Outcome
		}
		updated, err := tx.ExecContext(ctx,
			`UPDATE attempts
			    SET status = $2, execution_id = $3, outcome = $4, error = $5,
			        started_at = $6, finished_at = $7, data_ref = $8, data_sha256 = $9
			  WHERE job_id = $1 AND status IN ($10, $11)`,
			res.JobID, attemptStatus(res), nullIfEmpty(res.ExecutionID), outcome, nullIfEmpty(res.Error),
			started, finished, nullIfEmpty(res.DataRef), nullIfEmpty(res.DataSHA256), AttemptDispatched, AttemptStarted)
		if err != nil {
			return err
		}
		if count, err := updated.RowsAffected(); err != nil || count == 0 {
			return err // nil when it was a duplicate
		}

		var runFailing bool
		if err := tx.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM steps WHERE run_id = $1 AND status = $2)`, runID, StepFailed,
		).Scan(&runFailing); err != nil {
			return err
		}

		// The step shows its last attempt (docs/plan.md, "Step times").
		next := stepAfter(res, n, e.maxAttempts, r.cancelRequested, runFailing)
		if _, err := tx.ExecContext(ctx,
			`UPDATE steps SET status = $3, started_at = $4, finished_at = $5, error = $6, data_ref = $7
			  WHERE run_id = $1 AND name = $2`,
			runID, step, next, started, finished, nullIfEmpty(res.Error), nullIfEmpty(res.DataRef)); err != nil {
			return err
		}
		if next == StepFailed {
			if err := closePending(ctx, tx, runID); err != nil {
				return err
			}
		}
		return settle(ctx, tx, runID)
	})
}

// applyStarted records that an attempt is on its device. It applies only to a
// dispatched attempt: a late or repeated one, after the result, changes nothing.
func (e *Executor) applyStarted(ctx context.Context, s protocol.Started) error {
	runID, step, _, err := protocol.ParseJobID(s.JobID)
	if err != nil {
		return err
	}
	return e.withRun(ctx, runID, func(tx *sql.Tx, _ runRow) error {
		started := protocol.InstrumentTime(s.StartedAt)
		updated, err := tx.ExecContext(ctx,
			`UPDATE attempts SET status = $2, execution_id = $3, started_at = $4
			  WHERE job_id = $1 AND status = $5`,
			s.JobID, AttemptStarted, s.ExecutionID, started, AttemptDispatched)
		if err != nil {
			return err
		}
		if count, err := updated.RowsAffected(); err != nil || count == 0 {
			return err
		}
		// A dispatched attempt is its step's last, so the step shows it.
		_, err = tx.ExecContext(ctx,
			`UPDATE steps SET started_at = $3 WHERE run_id = $1 AND name = $2 AND status = $4`,
			runID, step, started, StepRunning)
		return err
	})
}

// closePending ends every pending step of a run: cancelled if it never ran,
// failed if it is waiting to retry (it has attempts, so it did run).
func closePending(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE steps s
		    SET status = CASE WHEN EXISTS (SELECT 1 FROM attempts a WHERE a.run_id = s.run_id AND a.step = s.name)
		                      THEN $3 ELSE $4 END
		  WHERE s.run_id = $1 AND s.status = $2`,
		runID, StepPending, StepFailed, StepCancelled)
	return err
}

// settle finishes the run once every step is terminal.
func settle(ctx context.Context, tx *sql.Tx, runID string) error {
	rows, err := tx.QueryContext(ctx, `SELECT status FROM steps WHERE run_id = $1`, runID)
	if err != nil {
		return err
	}
	var statuses []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			rows.Close()
			return err
		}
		statuses = append(statuses, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	status, finished := runOutcome(statuses)
	if !finished {
		return nil
	}
	_, err = tx.ExecContext(ctx,
		`UPDATE runs SET status = $2, finished_at = now() WHERE id = $1 AND status IN ($3, $4)`,
		runID, status, RunPending, RunRunning)
	return err
}

// errKeyReused means an idempotency key already names a run of another
// workflow.
var errKeyReused = errors.New("idempotency key already used")

func (e *Executor) CreateRun(ctx context.Context, workflowName string, steps []StepTemplate) (RunView, error) {
	return e.createRun(ctx, workflowName, steps, "")
}

// createRun creates a run. With a key, a run created earlier with the same
// key is returned instead, so a client may retry after a lost answer. The
// unique index decides between two calls racing with one key.
func (e *Executor) createRun(ctx context.Context, workflowName string, steps []StepTemplate, key string) (RunView, error) {
	runID := "run-" + randomHex(6)
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return RunView{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	inserted, err := tx.ExecContext(ctx,
		`INSERT INTO runs (id, workflow_name, idempotency_key) VALUES ($1, $2, $3)
		 ON CONFLICT (idempotency_key) DO NOTHING`, runID, workflowName, nullIfEmpty(key))
	if err != nil {
		return RunView{}, fmt.Errorf("insert run: %w", err)
	}
	if n, err := inserted.RowsAffected(); err != nil {
		return RunView{}, err
	} else if n == 0 {
		var earlier, earlierWorkflow string
		if err := tx.QueryRowContext(ctx,
			`SELECT id, workflow_name FROM runs WHERE idempotency_key = $1`, key,
		).Scan(&earlier, &earlierWorkflow); err != nil {
			return RunView{}, fmt.Errorf("run for key %q: %w", key, err)
		}
		if earlierWorkflow != workflowName {
			return RunView{}, fmt.Errorf("%w by run %s of %q", errKeyReused, earlier, earlierWorkflow)
		}
		return e.GetRun(ctx, earlier)
	}
	for position, st := range steps {
		// A nil slice would be stored as NULL, not as an empty array.
		deps := append([]string{}, st.DependsOn...)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO steps (run_id, name, position, device_id, depends_on) VALUES ($1, $2, $3, $4, $5)`,
			runID, st.Name, position, st.DeviceID, pq.Array(deps)); err != nil {
			return RunView{}, fmt.Errorf("insert step %s: %w", st.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return RunView{}, fmt.Errorf("commit: %w", err)
	}
	return e.GetRun(ctx, runID)
}

func (e *Executor) StartRun(ctx context.Context, runID string) (RunView, error) {
	if _, err := e.startRun(ctx, runID); err != nil {
		return RunView{}, err
	}
	return e.GetRun(ctx, runID)
}

// startRun moves a pending run to running and reports whether this call did.
func (e *Executor) startRun(ctx context.Context, runID string) (bool, error) {
	res, err := e.db.ExecContext(ctx,
		`UPDATE runs SET status = $1, started_at = now() WHERE id = $2 AND status = $3`,
		RunRunning, runID, RunPending)
	if err != nil {
		return false, fmt.Errorf("start run %s: %w", runID, err)
	}
	e.poke()
	n, err := res.RowsAffected()
	return n == 1, err
}

// CancelRun stops new work. Pending steps close (closePending); running steps
// end as they physically end, because ending ctx does not stop an instrument.
func (e *Executor) CancelRun(ctx context.Context, runID string) (RunView, error) {
	if _, err := e.cancelRun(ctx, runID); err != nil {
		return RunView{}, err
	}
	return e.GetRun(ctx, runID)
}

// cancelRun requests the cancel and reports whether this call did: false when
// the run had already finished or a cancel was already requested.
func (e *Executor) cancelRun(ctx context.Context, runID string) (bool, error) {
	var cancelled bool
	err := e.withRun(ctx, runID, func(tx *sql.Tx, r runRow) error {
		if r.status != RunPending && r.status != RunRunning || r.cancelRequested {
			return nil
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE runs SET cancel_requested = true WHERE id = $1`, runID); err != nil {
			return err
		}
		if err := closePending(ctx, tx, runID); err != nil {
			return err
		}
		cancelled = true
		return settle(ctx, tx, runID)
	})
	if err != nil {
		return false, fmt.Errorf("cancel run %s: %w", runID, err)
	}
	e.poke()
	return cancelled, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// GetRun reads the run and its steps from one snapshot, so a commit between
// the two queries can never show a run that disagrees with its steps.
func (e *Executor) GetRun(ctx context.Context, runID string) (RunView, error) {
	var rv RunView
	err := e.snapshot(ctx, func(tx *sql.Tx) (err error) {
		rv, err = readRun(ctx, tx, runID)
		return err
	})
	return rv, err
}

// snapshot runs fn in a read-only transaction that sees one moment.
func (e *Executor) snapshot(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()
	return fn(tx)
}

func readRun(ctx context.Context, tx *sql.Tx, runID string) (RunView, error) {
	var rv RunView
	err := tx.QueryRowContext(ctx,
		`SELECT id, workflow_name, status, created_at, started_at, finished_at FROM runs WHERE id = $1`,
		runID).Scan(&rv.ID, &rv.WorkflowName, &rv.Status, &rv.CreatedAt, &rv.StartedAt, &rv.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return RunView{}, ErrRunNotFound
	}
	if err != nil {
		return RunView{}, fmt.Errorf("get run %s: %w", runID, err)
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT name, device_id, depends_on, status, started_at, finished_at, error, data_ref
		   FROM steps WHERE run_id = $1 ORDER BY position`, runID)
	if err != nil {
		return RunView{}, fmt.Errorf("list steps of %s: %w", runID, err)
	}
	defer rows.Close()
	rv.Steps = []StepView{}
	for rows.Next() {
		var sv StepView
		var deps pq.StringArray
		if err := rows.Scan(&sv.Name, &sv.DeviceID, &deps, &sv.Status,
			&sv.StartedAt, &sv.FinishedAt, &sv.Error, &sv.DataRef); err != nil {
			return RunView{}, fmt.Errorf("scan step of %s: %w", runID, err)
		}
		sv.DependsOn = []string(deps)
		rv.Steps = append(rv.Steps, sv)
	}
	if err := rows.Err(); err != nil {
		return RunView{}, fmt.Errorf("list steps of %s: %w", runID, err)
	}
	return rv, nil
}

func (e *Executor) ListRuns(ctx context.Context) ([]RunView, error) {
	rows, err := e.db.QueryContext(ctx, `SELECT id FROM runs ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan run id: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}

	runs := []RunView{}
	for _, id := range ids {
		rv, err := e.GetRun(ctx, id)
		if err != nil {
			return nil, err
		}
		runs = append(runs, rv)
	}
	return runs, nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return hex.EncodeToString(b)
}
