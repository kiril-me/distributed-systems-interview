package protocol

import (
	"testing"
	"time"
)

func TestJobIDRoundTrip(t *testing.T) {
	id := JobID("run-abc123", "fill_sample_plate", 2)
	if id != "run-abc123/fill_sample_plate/2" {
		t.Fatalf("JobID = %q", id)
	}
	run, step, n, err := ParseJobID(id)
	if err != nil || run != "run-abc123" || step != "fill_sample_plate" || n != 2 {
		t.Fatalf("ParseJobID(%q) = %q %q %d %v", id, run, step, n, err)
	}
}

func TestParseJobIDRejectsMalformed(t *testing.T) {
	for _, id := range []string{"", "run-1", "run-1/step", "run-1/step/x", "run-1/step/0", "/step/1", "run-1//1"} {
		if _, _, _, err := ParseJobID(id); err == nil {
			t.Errorf("ParseJobID(%q) accepted a malformed id", id)
		}
	}
}

func TestDataKeyIsPerAttempt(t *testing.T) {
	if got := DataKey("run-1/read_plate/3"); got != "runs/run-1/read_plate/3.csv" {
		t.Fatalf("DataKey = %q", got)
	}
}

func TestSubjects(t *testing.T) {
	if CommandSubject("incubator-1") != "lab.cmd.incubator-1" || ResultSubject("incubator-1") != "lab.result.incubator-1" {
		t.Fatal("unexpected subjects")
	}
}

func TestInstrumentTime(t *testing.T) {
	got := InstrumentTime(1700000000.25)
	want := time.Unix(1700000000, 250_000_000)
	if !got.Equal(want) {
		t.Fatalf("InstrumentTime = %v, want %v", got, want)
	}
}
