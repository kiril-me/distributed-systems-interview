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
}

// resultEvent hands one result to the loop and waits for it to be committed.
type resultEvent struct {
	res     protocol.Result
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
	}
}

// Start is called once, when the executor process starts. Nothing needs
// repairing: unpublished outbox rows are published by the loop, and unacked
// results are redelivered by JetStream.
func (e *Executor) Start(ctx context.Context) error {
	js, err := jetstream.New(e.nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	if err := protocol.EnsureStreams(ctx, js); err != nil {
		return err
	}
	cons, err := protocol.ResultConsumer(ctx, js)
	if err != nil {
		return fmt.Errorf("result consumer: %w", err)
	}

	ctx, e.stop = context.WithCancel(ctx)
	e.done.Add(2)
	go func() { defer e.done.Done(); e.loop(ctx, js) }()
	go func() { defer e.done.Done(); e.consume(ctx, cons) }()
	log.Printf("executor: started, max %d attempts per step", e.maxAttempts)
	return nil
}

// Close is called once, when the executor process stops.
func (e *Executor) Close() {
	if e.stop != nil {
		e.stop()
		e.done.Wait()
	}
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
func (e *Executor) loop(ctx context.Context, js jetstream.JetStream) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-e.results:
			ev.applied <- e.applyResult(ctx, ev.res)
		case <-e.wake:
		case <-tick.C:
		}
		if err := e.dispatch(ctx); err != nil {
			log.Printf("executor: dispatch: %v", err)
		}
		if err := e.publish(ctx, js); err != nil {
			log.Printf("executor: publish: %v", err)
		}
	}
}

// consume reads results one at a time and acks each only after the loop has
// committed it (ADR-1). A result that cannot be applied is redelivered.
func (e *Executor) consume(ctx context.Context, cons jetstream.Consumer) {
	protocol.Receive(ctx, cons, func(msg jetstream.Msg) { e.onResult(ctx, msg) })
}

func (e *Executor) onResult(ctx context.Context, msg jetstream.Msg) {
	var res protocol.Result
	if err := json.Unmarshal(msg.Data(), &res); err != nil {
		log.Printf("executor: dropping unreadable result: %v", err)
		_ = msg.Term()
		return
	}
	if _, _, _, err := protocol.ParseJobID(res.JobID); err != nil {
		log.Printf("executor: dropping result: %v", err)
		_ = msg.Term()
		return
	}

	ev := resultEvent{res: res, applied: make(chan error, 1)}
	select {
	case e.results <- ev:
	case <-ctx.Done():
		return
	}
	select {
	case err := <-ev.applied:
		if err != nil {
			log.Printf("executor: apply result %s: %v", res.JobID, err)
			_ = msg.NakWithDelay(time.Second)
			return
		}
		if err := msg.Ack(); err != nil {
			// Redelivered later; applying it again changes nothing.
			log.Printf("executor: ack result %s: %v", res.JobID, err)
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

	for _, id := range ids {
		if err := e.withRun(ctx, id, func(tx *sql.Tx, r runRow) error {
			return dispatchRun(ctx, tx, id, r)
		}); err != nil {
			return fmt.Errorf("run %s: %w", id, err)
		}
	}
	return nil
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
		started := protocol.InstrumentTime(res.StartedAt)
		finished := protocol.InstrumentTime(res.FinishedAt)
		updated, err := tx.ExecContext(ctx,
			`UPDATE attempts
			    SET status = $2, execution_id = $3, outcome = $4, error = $5,
			        started_at = $6, finished_at = $7, data_ref = $8, data_sha256 = $9
			  WHERE job_id = $1 AND status = $10`,
			res.JobID, attemptStatus(res), res.ExecutionID, res.Outcome, nullIfEmpty(res.Error),
			started, finished, nullIfEmpty(res.DataRef), nullIfEmpty(res.DataSHA256), AttemptDispatched)
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

func (e *Executor) CreateRun(ctx context.Context, workflowName string, steps []StepTemplate) (RunView, error) {
	runID := "run-" + randomHex(6)
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return RunView{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO runs (id, workflow_name) VALUES ($1, $2)`, runID, workflowName); err != nil {
		return RunView{}, fmt.Errorf("insert run: %w", err)
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
	if _, err := e.db.ExecContext(ctx,
		`UPDATE runs SET status = $1, started_at = now() WHERE id = $2 AND status = $3`,
		RunRunning, runID, RunPending); err != nil {
		return RunView{}, fmt.Errorf("start run %s: %w", runID, err)
	}
	e.poke()
	return e.GetRun(ctx, runID)
}

// CancelRun stops new work. Pending steps close (closePending); running steps
// end as they physically end, because ending ctx does not stop an instrument.
func (e *Executor) CancelRun(ctx context.Context, runID string) (RunView, error) {
	err := e.withRun(ctx, runID, func(tx *sql.Tx, r runRow) error {
		if r.status != RunPending && r.status != RunRunning {
			return nil
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE runs SET cancel_requested = true WHERE id = $1`, runID); err != nil {
			return err
		}
		if err := closePending(ctx, tx, runID); err != nil {
			return err
		}
		return settle(ctx, tx, runID)
	})
	if err != nil {
		return RunView{}, fmt.Errorf("cancel run %s: %w", runID, err)
	}
	e.poke()
	return e.GetRun(ctx, runID)
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
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return RunView{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	var rv RunView
	err = tx.QueryRowContext(ctx,
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
