package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
	"github.com/automata-tech/distributed-systems-interview/services/driver/instrument"
)

// fakeStore is S3 that can be down for the first `down` puts.
type fakeStore struct {
	mu      sync.Mutex
	down    int
	objects map[string][]byte
}

func (s *fakeStore) Put(_ context.Context, key string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down > 0 {
		s.down--
		return "", errors.New("storage down")
	}
	if s.objects == nil {
		s.objects = map[string][]byte{}
	}
	s.objects[key] = data
	return "s3://lab-data/" + key, nil
}

func newInstrument(t *testing.T, ledger string, cfg instrument.Config) *instrument.Instrument {
	t.Helper()
	cfg.DeviceID = "dev-1"
	cfg.LedgerDir = ledger
	if cfg.Duration == 0 {
		cfg.Duration = 20 * time.Millisecond
	}
	if cfg.HangDuration == 0 {
		cfg.HangDuration = 200 * time.Millisecond
	}
	inst, err := instrument.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return inst
}

func newDriver(t *testing.T, inst *instrument.Instrument, store putter) *driver {
	t.Helper()
	return &driver{inst: inst, store: store, spool: t.TempDir(), poll: 10 * time.Millisecond}
}

func command(n int) protocol.Command {
	return protocol.Command{JobID: protocol.JobID("run-1", "read_plate", n), RunID: "run-1", Step: "read_plate", DeviceID: "dev-1"}
}

func executions(inst *instrument.Instrument, jobID string) []instrument.Execution {
	var out []instrument.Execution
	for _, e := range inst.Status().Executions {
		if e.JobID == jobID {
			out = append(out, e)
		}
	}
	return out
}

func TestCompletedReadIsStoredBeforeReport(t *testing.T) {
	inst := newInstrument(t, t.TempDir(), instrument.Config{ProducesData: true, FailuresRetryable: true})
	store := &fakeStore{}
	d := newDriver(t, inst, store)

	res, err := d.execute(context.Background(), command(1))
	if err != nil {
		t.Fatal(err)
	}
	ledger := executions(inst, res.JobID)
	if len(ledger) != 1 {
		t.Fatalf("want 1 execution, got %d", len(ledger))
	}
	if res.Outcome != "completed" || res.Error != "" || res.ExecutionID != ledger[0].ExecutionID {
		t.Fatalf("unexpected result %+v", res)
	}
	if res.StartedAt != ledger[0].StartedAt || res.FinishedAt != *ledger[0].FinishedAt {
		t.Fatal("times must be the ledger's")
	}
	data := store.objects[protocol.DataKey(res.JobID)]
	sum := sha256.Sum256(data)
	if res.DataRef != "s3://lab-data/runs/run-1/read_plate/1.csv" || hex.EncodeToString(sum[:]) != *ledger[0].DataSHA256 || res.DataSHA256 != *ledger[0].DataSHA256 {
		t.Fatalf("stored data does not match the ledger: %+v", res)
	}
}

func TestRedeliveryDoesNotRunAgain(t *testing.T) {
	inst := newInstrument(t, t.TempDir(), instrument.Config{ProducesData: true, FailuresRetryable: true})
	d := newDriver(t, inst, &fakeStore{})

	first, err := d.execute(context.Background(), command(1))
	if err != nil {
		t.Fatal(err)
	}
	second, err := d.execute(context.Background(), command(1))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(executions(inst, first.JobID)); n != 1 {
		t.Fatalf("redelivery ran the job again: %d executions", n)
	}
	if first != second {
		t.Fatalf("redelivery reported something else:\n%+v\n%+v", first, second)
	}
}

func TestFailureCarriesDeviceRetryable(t *testing.T) {
	inst := newInstrument(t, t.TempDir(), instrument.Config{FailPct: 100, FailuresRetryable: false})
	d := newDriver(t, inst, &fakeStore{})

	res, err := d.execute(context.Background(), command(1))
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != "failed" || res.Error == "" || res.Retryable {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestHangIsReadFromLedger(t *testing.T) {
	inst := newInstrument(t, t.TempDir(), instrument.Config{HangPct: 100, FailuresRetryable: true})
	d := newDriver(t, inst, &fakeStore{})

	res, err := d.execute(context.Background(), command(1))
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != "hung" || res.Error == "" || !res.Retryable || res.FinishedAt == 0 {
		t.Fatalf("unexpected result %+v", res)
	}
	if inst.Status().Busy {
		t.Fatal("reported before the instrument was free")
	}
}

func TestRestartDuringJobIsReportedNotRerun(t *testing.T) {
	ledger := t.TempDir()
	cmd := command(1)
	line := `{"execution_id":"e1","job_id":"` + cmd.JobID + `","run_id":"run-1","step_name":"read_plate","started_at":1700000000,"outcome":"running"}` + "\n"
	if err := os.WriteFile(filepath.Join(ledger, "dev-1.jsonl"), []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
	inst := newInstrument(t, ledger, instrument.Config{FailuresRetryable: false}) // a restart
	d := newDriver(t, inst, &fakeStore{})

	res, err := d.execute(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != "hung" || res.ExecutionID != "e1" || len(executions(inst, cmd.JobID)) != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestStorageDownIsRetriedUntilStored(t *testing.T) {
	inst := newInstrument(t, t.TempDir(), instrument.Config{ProducesData: true, FailuresRetryable: true})
	store := &fakeStore{down: 3}
	d := newDriver(t, inst, store)

	res, err := d.execute(context.Background(), command(1))
	if err != nil {
		t.Fatal(err)
	}
	if res.Error != "" || store.objects[protocol.DataKey(res.JobID)] == nil {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestCompletedReadWithoutSpoolIsDataLost(t *testing.T) {
	inst := newInstrument(t, t.TempDir(), instrument.Config{ProducesData: true, FailuresRetryable: true})
	d := newDriver(t, inst, &fakeStore{})
	cmd := command(1)
	if _, err := d.execute(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	d.removeSpool(cmd.JobID) // as if the driver died before the spool was written

	res, err := d.execute(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != "completed" || res.Error == "" || res.DataRef != "" || !res.Retryable {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestBusyInstrumentIsNotAnAttempt(t *testing.T) {
	inst := newInstrument(t, t.TempDir(), instrument.Config{Duration: 300 * time.Millisecond})
	d := newDriver(t, inst, &fakeStore{})
	go func() {
		_, _ = inst.Run(context.Background(), instrument.Job{JobID: "other", RunID: "run-2", StepName: "x"})
	}()
	time.Sleep(50 * time.Millisecond)

	cmd := command(1)
	if _, err := d.execute(context.Background(), cmd); !errors.Is(err, instrument.ErrBusy) {
		t.Fatalf("want ErrBusy, got %v", err)
	}
	if n := len(executions(inst, cmd.JobID)); n != 0 {
		t.Fatalf("busy must not be recorded, got %d executions", n)
	}
}

// A read that takes longer than the driver expected still completed: its data
// must be stored, not reported lost. Only a hang may end the wait for Run.
func TestSlowReadKeepsItsData(t *testing.T) {
	inst := newInstrument(t, t.TempDir(), instrument.Config{ProducesData: true, FailuresRetryable: true, Duration: 300 * time.Millisecond})
	store := &fakeStore{}
	d := newDriver(t, inst, store)

	res, err := d.execute(context.Background(), command(1))
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != "completed" || res.Error != "" || store.objects[protocol.DataKey(res.JobID)] == nil {
		t.Fatalf("completed read lost its data: %+v", res)
	}
}
