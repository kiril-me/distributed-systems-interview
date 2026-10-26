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
