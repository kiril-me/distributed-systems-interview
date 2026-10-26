package main

// Integration tests: the real executor against Postgres and an in-process
// JetStream, with fake drivers answering commands. Skipped unless
// TEST_DATABASE_URL is set, e.g.
//
//	TEST_DATABASE_URL=postgres://test@127.0.0.1:55432/postgres?sslmode=disable go test ./services/executor/

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/natstest"
	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// a ──┬──> b (inc) ──┐
//
//	└──> c ────────┴──> d        a, c, d on lh
var workflow = []StepTemplate{
	{Name: "a", DeviceID: "lh"},
	{Name: "b", DeviceID: "inc", DependsOn: []string{"a"}},
	{Name: "c", DeviceID: "lh", DependsOn: []string{"a"}},
	{Name: "d", DeviceID: "lh", DependsOn: []string{"b", "c"}},
}

// freshDB recreates the schema from db/init.sql.
func freshDB(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	raw, err := os.ReadFile("../../db/init.sql")
	if err != nil {
		t.Fatal(err)
	}
	_, schema, _ := strings.Cut(string(raw), `\c executor_db`)
	if _, err := db.Exec(`DROP SCHEMA public CASCADE; CREATE SCHEMA public;` + schema); err != nil {
		t.Fatal(err)
	}
	return db
}

func startExecutor(t *testing.T) (*Executor, jetstream.JetStream) {
	t.Helper()
	e, js, _ := startExecutorBehindProxy(t)
	return e, js
}

// startExecutorBehindProxy connects the executor to NATS through a proxy the
// test can freeze. The fake drivers connect directly.
func startExecutorBehindProxy(t *testing.T) (*Executor, jetstream.JetStream, *natstest.Proxy) {
	t.Helper()
	db := freshDB(t)
	url := natstest.Start(t)
	proxy := natstest.NewProxy(t, url)
	e := NewExecutor(db, natstest.Connect(t, proxy.URL), nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	js, _ := jetstream.New(natstest.Connect(t, url))
	return e, js, proxy
}

// journal is what the fake drivers saw: each command, and which steps had
// already reported completed when it arrived.
type journal struct {
	mu        sync.Mutex
	commands  []protocol.Command
	completed map[string]bool
	before    map[string]map[string]bool // job id -> completed steps at arrival
	results   []protocol.Result
	clock     float64
}

// answer decides how a command ends. It may block.
type answer func(cmd protocol.Command, n int) (outcome string, retryable bool)

// fakeDrivers plays one driver per device: take a command, answer it, publish
// the result twice (a duplicate), then ack.
func fakeDrivers(t *testing.T, js jetstream.JetStream, answer answer, devices ...string) *journal {
	t.Helper()
	j := &journal{completed: map[string]bool{}, before: map[string]map[string]bool{}, clock: 1_700_000_000}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	for _, device := range devices {
		cons, err := protocol.CommandConsumer(ctx, js, device)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				msg, err := cons.Next(jetstream.FetchMaxWait(200 * time.Millisecond))
				if err != nil {
					continue
				}
				var cmd protocol.Command
				_ = json.Unmarshal(msg.Data(), &cmd)
				_, _, n, _ := protocol.ParseJobID(cmd.JobID)

				j.mu.Lock()
				j.commands = append(j.commands, cmd)
				snapshot := map[string]bool{}
				for k := range j.completed {
					snapshot[k] = true
				}
				j.before[cmd.JobID] = snapshot
				j.mu.Unlock()

				outcome, retryable := answer(cmd, n)

				j.mu.Lock()
				j.clock += 1
				res := protocol.Result{JobID: cmd.JobID, ExecutionID: "x-" + cmd.JobID, Outcome: outcome,
					Retryable: retryable, StartedAt: j.clock, FinishedAt: j.clock + 0.5}
				if outcome != protocol.OutcomeCompleted {
					res.Error = "instrument error"
				} else {
					j.completed[cmd.Step] = true
				}
				j.results = append(j.results, res)
				j.mu.Unlock()

				data, _ := json.Marshal(res)
				for _, id := range []string{"1", "2"} {
					if _, err := js.Publish(ctx, protocol.ResultSubject(device), data, jetstream.WithMsgID(cmd.JobID+id)); err != nil {
						return
					}
				}
				_ = msg.Ack()
			}
		}()
	}
	return j
}

func (j *journal) count(step string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, c := range j.commands {
		if c.Step == step {
			n++
		}
	}
	return n
}

func waitFinished(t *testing.T, e *Executor, runID string) RunView {
	t.Helper()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		rv, err := e.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if rv.Status == RunCompleted || rv.Status == RunFailed || rv.Status == RunCancelled {
			return rv
		}
	}
	t.Fatal("run did not finish")
	return RunView{}
}

func statuses(rv RunView) map[string]string {
	out := map[string]string{}
	for _, s := range rv.Steps {
		out[s.Name] = s.Status
	}
	return out
}

func startRun(t *testing.T, e *Executor) string {
	t.Helper()
	rv, err := e.CreateRun(context.Background(), "test", workflow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.StartRun(context.Background(), rv.ID); err != nil {
		t.Fatal(err)
	}
	return rv.ID
}

func TestHappyPathRespectsDependencies(t *testing.T) {
	e, js := startExecutor(t)
	j := fakeDrivers(t, js, func(protocol.Command, int) (string, bool) { return "completed", true }, "lh", "inc")

	rv := waitFinished(t, e, startRun(t, e))
	if rv.Status != RunCompleted {
		t.Fatalf("run %s: %v", rv.Status, statuses(rv))
	}
	for _, s := range workflow {
		if n := j.count(s.Name); n != 1 {
			t.Errorf("%s dispatched %d times", s.Name, n)
		}
		for _, dep := range s.DependsOn {
			if !j.before[protocol.JobID(rv.ID, s.Name, 1)][dep] {
				t.Errorf("%s dispatched before %s completed", s.Name, dep)
			}
		}
	}
	for _, s := range rv.Steps {
		if s.StartedAt == nil || s.FinishedAt == nil || !s.StartedAt.Before(*s.FinishedAt) {
			t.Errorf("%s has no instrument times", s.Name)
		}
	}
}

func TestRetryableFailureRetriesAsNewAttempt(t *testing.T) {
	e, js := startExecutor(t)
	j := fakeDrivers(t, js, func(cmd protocol.Command, n int) (string, bool) {
		if cmd.Step == "b" && n < 3 {
			return "hung", true
		}
		return "completed", true
	}, "lh", "inc")

	rv := waitFinished(t, e, startRun(t, e))
	if rv.Status != RunCompleted || j.count("b") != 3 {
		t.Fatalf("run %s, b dispatched %d times", rv.Status, j.count("b"))
	}
}

// While a retry is on its way, the step shows nothing from the attempt that
// failed: its times and error belong to an attempt that is no longer the last.
func TestRetryClearsPreviousAttemptFromStep(t *testing.T) {
	e, js := startExecutor(t)
	second := make(chan struct{}, 1)
	release := make(chan struct{})
	fakeDrivers(t, js, func(cmd protocol.Command, n int) (string, bool) {
		if cmd.Step == "b" && n == 1 {
			return "failed", true
		}
		if cmd.Step == "b" && n == 2 {
			second <- struct{}{}
			<-release
		}
		return "completed", true
	}, "lh", "inc")

	runID := startRun(t, e)
	<-second // attempt 2 of b is on the instrument
	rv, err := e.GetRun(context.Background(), runID)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range rv.Steps {
		if s.Name == "b" && (s.Status != StepRunning || s.StartedAt != nil || s.FinishedAt != nil || s.Error != nil) {
			t.Fatalf("b shows a previous attempt: %s started=%v finished=%v error=%v", s.Status, s.StartedAt, s.FinishedAt, s.Error)
		}
	}
	waitFinished(t, e, runID)
}

func TestAttemptsUsedUpFailsRunAndCancelsPending(t *testing.T) {
	e, js := startExecutor(t)
	j := fakeDrivers(t, js, func(cmd protocol.Command, n int) (string, bool) {
		if cmd.Step == "b" {
			return "failed", true
		}
		return "completed", true
	}, "lh", "inc")

	rv := waitFinished(t, e, startRun(t, e))
	got := statuses(rv)
	if rv.Status != RunFailed || got["b"] != StepFailed || got["d"] != StepCancelled || got["c"] != StepCompleted {
		t.Fatalf("run %s: %v", rv.Status, got)
	}
	if j.count("b") != 3 || j.count("d") != 0 {
		t.Fatalf("b dispatched %d times, d %d times", j.count("b"), j.count("d"))
	}
}

// Once one step has failed the run will fail, so a retryable failure on
// another step that was still running ends it instead of starting new work.
func TestNoRetryOnceRunIsFailing(t *testing.T) {
	e, js := startExecutor(t)
	j := fakeDrivers(t, js, func(cmd protocol.Command, n int) (string, bool) {
		switch cmd.Step {
		case "c": // lh, non-retryable: fails while b is still running
			return "failed", false
		case "b": // inc, retryable: fails after c has failed
			time.Sleep(300 * time.Millisecond)
			return "failed", true
		}
		return "completed", false
	}, "lh", "inc")

	rv := waitFinished(t, e, startRun(t, e))
	got := statuses(rv)
	if rv.Status != RunFailed || got["b"] != StepFailed || got["c"] != StepFailed || got["d"] != StepCancelled || j.count("b") != 1 {
		t.Fatalf("run %s: %v, b dispatched %d times", rv.Status, got, j.count("b"))
	}
}

func TestNonRetryableFailureFailsAtOnce(t *testing.T) {
	e, js := startExecutor(t)
	j := fakeDrivers(t, js, func(protocol.Command, int) (string, bool) { return "failed", false }, "lh", "inc")

	rv := waitFinished(t, e, startRun(t, e))
	got := statuses(rv)
	if rv.Status != RunFailed || got["a"] != StepFailed || got["b"] != StepCancelled || got["c"] != StepCancelled || got["d"] != StepCancelled {
		t.Fatalf("run %s: %v", rv.Status, got)
	}
	if j.count("a") != 1 {
		t.Fatalf("a dispatched %d times", j.count("a"))
	}
}

func TestCancelLetsRunningStepFinish(t *testing.T) {
	e, js := startExecutor(t)
	release := make(chan struct{})
	arrived := make(chan struct{}, 1)
	fakeDrivers(t, js, func(cmd protocol.Command, n int) (string, bool) {
		arrived <- struct{}{}
		<-release
		return "completed", false
	}, "lh", "inc")

	runID := startRun(t, e)
	<-arrived // a is on the instrument
	rv, err := e.CancelRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := statuses(rv); rv.Status != RunRunning || got["a"] != StepRunning || got["b"] != StepCancelled {
		t.Fatalf("after cancel: run %s: %v", rv.Status, got)
	}
	close(release)

	rv = waitFinished(t, e, runID)
	if got := statuses(rv); rv.Status != RunCancelled || got["a"] != StepCompleted {
		t.Fatalf("run %s: %v", rv.Status, got)
	}
}

// A result published while the executor is cut off must be applied as soon
// as the network is back, not after AckWait (30s) when JetStream redelivers.
func TestResultSentDuringPartitionIsAppliedAfterThaw(t *testing.T) {
	e, js, proxy := startExecutorBehindProxy(t)
	fakeDrivers(t, js, func(cmd protocol.Command, n int) (string, bool) {
		if cmd.Step == "a" { // cut the executor off before a's result goes out
			proxy.Freeze()
			time.AfterFunc(3*time.Second, proxy.Thaw)
		}
		return "completed", true
	}, "lh", "inc")

	start := time.Now()
	rv := waitFinished(t, e, startRun(t, e)) // gives up after 15s
	if rv.Status != RunCompleted || time.Since(start) > 10*time.Second {
		t.Fatalf("run %s after %v", rv.Status, time.Since(start))
	}
}

func TestCancelWhileWaitingToRetryIsFailed(t *testing.T) {
	e, _ := startExecutor(t)
	ctx := context.Background()
	rv, err := e.CreateRun(ctx, "test", workflow)
	if err != nil {
		t.Fatal(err)
	}
	// a ran once and failed, and waits for its retry.
	if _, err := e.db.Exec(`INSERT INTO attempts (job_id, run_id, step, n, device_id, status) VALUES ($1, $2, 'a', 1, 'lh', 'failed')`,
		protocol.JobID(rv.ID, "a", 1), rv.ID); err != nil {
		t.Fatal(err)
	}

	rv, err = e.CancelRun(ctx, rv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := statuses(rv); rv.Status != RunFailed || got["a"] != StepFailed || got["b"] != StepCancelled {
		t.Fatalf("run %s: %v", rv.Status, got)
	}
}

// record is the run, its steps and its attempts as stored, for comparison.
func record(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()
	var run, steps, attempts string
	if err := db.QueryRow(`SELECT
	    (SELECT row_to_json(r) FROM runs r WHERE id = $1)::text,
	    (SELECT json_agg(s ORDER BY position) FROM steps s WHERE run_id = $1)::text,
	    (SELECT json_agg(a ORDER BY step, n) FROM attempts a WHERE run_id = $1)::text`, runID,
	).Scan(&run, &steps, &attempts); err != nil {
		t.Fatal(err)
	}
	return run + "\n" + steps + "\n" + attempts
}

// Applying a result a second time changes nothing: not the attempt, not the
// step, not the run. That includes a retryable failure, which must not start
// another retry.
func TestSameResultTwiceChangesNothing(t *testing.T) {
	e, js := startExecutor(t)
	j := fakeDrivers(t, js, func(cmd protocol.Command, n int) (string, bool) {
		if cmd.Step == "b" && n == 1 {
			return "failed", true
		}
		return "completed", true
	}, "lh", "inc")

	rv := waitFinished(t, e, startRun(t, e))
	if rv.Status != RunCompleted || j.count("b") != 2 {
		t.Fatalf("run %s, b dispatched %d times", rv.Status, j.count("b"))
	}
	before := record(t, e.db, rv.ID)

	j.mu.Lock()
	results := append([]protocol.Result{}, j.results...)
	j.mu.Unlock()
	for _, res := range results {
		if err := e.applyResult(context.Background(), res); err != nil {
			t.Fatalf("apply %s again: %v", res.JobID, err)
		}
	}
	time.Sleep(1500 * time.Millisecond) // a loop tick, in case anything became dispatchable

	if after := record(t, e.db, rv.ID); after != before {
		t.Fatalf("applying %d results again changed the record:\nbefore %s\nafter  %s", len(results), before, after)
	}
}

// A result for a run that does not exist can never apply. It must not hold up
// the results behind it: the consumer has one message in hand at a time.
func TestResultForUnknownRunDoesNotBlockOthers(t *testing.T) {
	e, js := startExecutor(t)
	poison, _ := json.Marshal(protocol.Result{JobID: "run-unknown/a/1", ExecutionID: "x", Outcome: "completed",
		StartedAt: 1, FinishedAt: 2})
	if _, err := js.Publish(context.Background(), protocol.ResultSubject("lh"), poison); err != nil {
		t.Fatal(err)
	}
	fakeDrivers(t, js, func(protocol.Command, int) (string, bool) { return "completed", true }, "lh", "inc")

	if rv := waitFinished(t, e, startRun(t, e)); rv.Status != RunCompleted {
		t.Fatalf("run %s: %v", rv.Status, statuses(rv))
	}
}

// A run that cannot be dispatched does not stop the runs after it. The broken
// run already has an attempt row holding the job id its first step needs.
func TestRunThatCannotDispatchDoesNotBlockOthers(t *testing.T) {
	e, js := startExecutor(t)
	broken, err := e.CreateRun(context.Background(), "test", workflow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Exec(
		`INSERT INTO attempts (job_id, run_id, step, n, device_id, published_at) VALUES ($1, $2, 'b', 1, 'inc', now())`,
		protocol.JobID(broken.ID, "a", 1), broken.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.StartRun(context.Background(), broken.ID); err != nil {
		t.Fatal(err)
	}
	fakeDrivers(t, js, func(protocol.Command, int) (string, bool) { return "completed", true }, "lh", "inc")

	if rv := waitFinished(t, e, startRun(t, e)); rv.Status != RunCompleted {
		t.Fatalf("run %s: %v", rv.Status, statuses(rv))
	}
}

// A database made by an older db/init.sql gains the new columns at start,
// and keeps its runs.
func TestStartMigratesAnOlderDatabase(t *testing.T) {
	db := freshDB(t)
	if _, err := db.Exec(`ALTER TABLE runs DROP COLUMN idempotency_key;
		ALTER TABLE attempts DROP COLUMN revoked_at;
		INSERT INTO runs (id, workflow_name) VALUES ('run-old', 'test')`); err != nil {
		t.Fatal(err)
	}
	for range 2 { // and starting again changes nothing
		e := NewExecutor(db, natstest.Connect(t, natstest.Start(t)), nil)
		if err := e.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		e.Close()
	}
	var columns int
	if err := db.QueryRow(`SELECT count(*) FROM information_schema.columns
		WHERE (table_name, column_name) IN (('runs', 'idempotency_key'), ('attempts', 'revoked_at'))`).Scan(&columns); err != nil || columns != 2 {
		t.Fatalf("%d new columns, %v", columns, err)
	}
	var runs int
	if err := db.QueryRow(`SELECT count(*) FROM runs WHERE id = 'run-old'`).Scan(&runs); err != nil || runs != 1 {
		t.Fatalf("old run: %d, %v", runs, err)
	}
}

// driverOnce plays one driver for one command: it takes the command and hands
// it to the test, which answers by publishing on js.
func driverOnce(t *testing.T, js jetstream.JetStream, device string) protocol.Command {
	t.Helper()
	cons, err := protocol.CommandConsumer(context.Background(), js, device)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("no command for %s: %v", device, err)
	}
	var cmd protocol.Command
	_ = json.Unmarshal(msg.Data(), &cmd)
	_ = msg.Ack()
	return cmd
}

func publishJSON(t *testing.T, js jetstream.JetStream, subject string, v any) {
	t.Helper()
	data, _ := json.Marshal(v)
	if _, err := js.Publish(context.Background(), subject, data); err != nil {
		t.Fatal(err)
	}
}

func attemptStatusOf(t *testing.T, e *Executor, jobID string) string {
	t.Helper()
	var status string
	if err := e.db.QueryRow(`SELECT status FROM attempts WHERE job_id = $1`, jobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A started message moves the attempt from queued to on the device: the step
// stays running, and gets the instrument's start time before its result.
func TestStartedShowsStepOnDevice(t *testing.T) {
	e, js := startExecutor(t)
	rv, err := e.CreateRun(context.Background(), "test", []StepTemplate{{Name: "a", DeviceID: "lh"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.StartRun(context.Background(), rv.ID); err != nil {
		t.Fatal(err)
	}
	cmd := driverOnce(t, js, "lh")
	publishJSON(t, js, protocol.StartedSubject("lh"), protocol.Started{JobID: cmd.JobID, ExecutionID: "x-1", StartedAt: 1_700_000_000})

	waitFor(t, "step a started", func() bool {
		rv, _ := e.GetRun(context.Background(), rv.ID)
		return rv.Steps[0].StartedAt != nil
	})
	if rv, _ := e.GetRun(context.Background(), rv.ID); rv.Steps[0].Status != StepRunning || !rv.Steps[0].StartedAt.Equal(time.Unix(1_700_000_000, 0)) {
		t.Fatalf("step %+v", rv.Steps[0])
	}
	if got := attemptStatusOf(t, e, cmd.JobID); got != AttemptStarted {
		t.Fatalf("attempt %s, want %s", got, AttemptStarted)
	}

	publishJSON(t, js, protocol.ResultSubject("lh"), protocol.Result{JobID: cmd.JobID, ExecutionID: "x-1",
		Outcome: "completed", StartedAt: 1_700_000_000, FinishedAt: 1_700_000_001})
	if rv := waitFinished(t, e, rv.ID); rv.Status != RunCompleted {
		t.Fatalf("run %s: %v", rv.Status, statuses(rv))
	}

	before := record(t, e.db, rv.ID) // a late started changes nothing
	if err := e.applyStarted(context.Background(), protocol.Started{JobID: cmd.JobID, ExecutionID: "x-1", StartedAt: 1_700_000_005}); err != nil {
		t.Fatal(err)
	}
	if after := record(t, e.db, rv.ID); after != before {
		t.Fatalf("late started changed the record:\nbefore %s\nafter  %s", before, after)
	}
}

// Cancel revokes a command still queued for its device, and only that one: an
// attempt already on its device is left to end as it physically ends.
func TestCancelRevokesQueuedCommandOnly(t *testing.T) {
	e, js := startExecutor(t)
	ctx := context.Background()
	kv, err := protocol.RevokedKV(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	isRevoked := func(jobID string) bool { _, err := kv.Get(ctx, jobID); return err == nil }
	wf := []StepTemplate{{Name: "a", DeviceID: "lh"}, {Name: "b", DeviceID: "inc"}}
	rv, err := e.CreateRun(ctx, "test", wf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.StartRun(ctx, rv.ID); err != nil {
		t.Fatal(err)
	}
	onDevice := driverOnce(t, js, "lh") // a starts; b stays queued on inc
	publishJSON(t, js, protocol.StartedSubject("lh"), protocol.Started{JobID: onDevice.JobID, ExecutionID: "x-a", StartedAt: 1_700_000_000})
	waitFor(t, "a started", func() bool { return attemptStatusOf(t, e, onDevice.JobID) == AttemptStarted })
	queued := protocol.JobID(rv.ID, "b", 1)

	if _, err := e.CancelRun(ctx, rv.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "b revoked", func() bool { return isRevoked(queued) })
	if isRevoked(onDevice.JobID) {
		t.Fatal("an attempt on its device was revoked")
	}

	// The drivers answer as the real one would: b was revoked, a ran.
	if cmd := driverOnce(t, js, "inc"); cmd.JobID != queued {
		t.Fatalf("inc got %s", cmd.JobID)
	}
	publishJSON(t, js, protocol.ResultSubject("inc"), protocol.Result{JobID: queued, Outcome: protocol.OutcomeRevoked, Error: "run cancelled before the job started"})
	publishJSON(t, js, protocol.ResultSubject("lh"), protocol.Result{JobID: onDevice.JobID, ExecutionID: "x-a",
		Outcome: "completed", StartedAt: 1_700_000_000, FinishedAt: 1_700_000_001})

	rv = waitFinished(t, e, rv.ID)
	if got := statuses(rv); rv.Status != RunCancelled || got["a"] != StepCompleted || got["b"] != StepCancelled {
		t.Fatalf("run %s: %v", rv.Status, got)
	}
	if rv.Steps[1].StartedAt != nil || attemptStatusOf(t, e, queued) != AttemptRevoked {
		t.Fatalf("b: %+v, attempt %s", rv.Steps[1], attemptStatusOf(t, e, queued))
	}
}

// A revoke that is missed: these fake drivers never read REVOKED, like a
// driver that checked it just before the cancel. The queued command runs when
// the device is free, the run waits for it, and its outcome becomes the
// step's. The record stays true; only the physical work was wasted.
func TestCancelledRunsQueuedCommandStillRuns(t *testing.T) {
	cases := []struct {
		name     string
		wf       []StepTemplate
		wantRun  string
		wantStep map[string]string
	}{
		{"four steps", workflow, RunCancelled,
			map[string]string{"a": StepCompleted, "b": StepCancelled, "c": StepCancelled, "d": StepCancelled}},
		{"one step", []StepTemplate{{Name: "a", DeviceID: "lh"}}, RunCompleted,
			map[string]string{"a": StepCompleted}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e, js := startExecutor(t)
			ctx := context.Background()
			arrived := make(chan string, 16)
			release := make(chan struct{})
			j := fakeDrivers(t, js, func(cmd protocol.Command, n int) (string, bool) {
				arrived <- cmd.JobID
				<-release
				return "completed", false
			}, "lh", "inc")

			first := startRun(t, e)
			<-arrived // the first run's a holds lh

			rv, err := e.CreateRun(ctx, "test", c.wf)
			if err != nil {
				t.Fatal(err)
			}
			second := rv.ID
			if _, err := e.StartRun(ctx, second); err != nil {
				t.Fatal(err)
			}
			queued := protocol.JobID(second, "a", 1)
			for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
				var published bool
				_ = e.db.QueryRow(`SELECT published_at IS NOT NULL FROM attempts WHERE job_id = $1`, queued).Scan(&published)
				if published {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("second run's command was not published")
				}
			}

			rv, err = e.CancelRun(ctx, second)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("right after cancel: run %s, steps %v", rv.Status, statuses(rv))
			close(release)

			waitFinished(t, e, first)
			rv = waitFinished(t, e, second)
			t.Logf("settled: run %s, steps %v, %s executed %d time(s)", rv.Status, statuses(rv), queued, countJob(j, queued))
			if countJob(j, queued) != 1 {
				t.Fatalf("queued command executed %d times", countJob(j, queued))
			}
			got := statuses(rv)
			for name, want := range c.wantStep {
				if got[name] != want {
					t.Errorf("step %s: %s, want %s", name, got[name], want)
				}
			}
			if rv.Status != c.wantRun {
				t.Errorf("run %s, want %s", rv.Status, c.wantRun)
			}
		})
	}
}

func countJob(j *journal, jobID string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, c := range j.commands {
		if c.JobID == jobID {
			n++
		}
	}
	return n
}
