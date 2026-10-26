package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

func TestAttemptStatus(t *testing.T) {
	cases := []struct {
		res  protocol.Result
		want string
	}{
		{protocol.Result{Outcome: "completed"}, AttemptCompleted},
		{protocol.Result{Outcome: "completed", Error: "data lost after completion"}, AttemptFailed},
		{protocol.Result{Outcome: "failed", Error: "boom"}, AttemptFailed},
		{protocol.Result{Outcome: "hung", Error: "instrument hung"}, AttemptFailed},
		{protocol.Result{Outcome: protocol.OutcomeRevoked, Error: "run cancelled"}, AttemptRevoked},
	}
	for _, c := range cases {
		if got := attemptStatus(c.res); got != c.want {
			t.Errorf("attemptStatus(%+v) = %s, want %s", c.res, got, c.want)
		}
	}
}

func TestStepAfter(t *testing.T) {
	const max = 3
	failed := func(retryable bool) protocol.Result {
		return protocol.Result{Outcome: "failed", Error: "boom", Retryable: retryable}
	}
	cases := []struct {
		name      string
		res       protocol.Result
		n         int
		cancelled bool
		failing   bool
		want      string
	}{
		{"completed", protocol.Result{Outcome: "completed"}, 1, false, false, StepCompleted},
		{"completed after cancel still completed", protocol.Result{Outcome: "completed"}, 1, true, false, StepCompleted},
		{"completed in a failing run still completed", protocol.Result{Outcome: "completed"}, 1, false, true, StepCompleted},
		{"retryable failure retries", failed(true), 1, false, false, StepPending},
		{"retryable failure on last attempt fails", failed(true), max, false, false, StepFailed},
		{"non-retryable failure fails at once", failed(false), 1, false, false, StepFailed},
		{"hung on liquid handler fails at once", protocol.Result{Outcome: "hung", Error: "instrument hung"}, 1, false, false, StepFailed},
		{"no retry after cancel", failed(true), 1, true, false, StepFailed},
		{"no retry once another step failed", failed(true), 1, false, true, StepFailed},
		{"revoked never ran, so cancelled", protocol.Result{Outcome: protocol.OutcomeRevoked, Retryable: true}, 1, true, false, StepCancelled},
		{"revoked in a failing run still cancelled", protocol.Result{Outcome: protocol.OutcomeRevoked}, 2, true, true, StepCancelled},
	}
	for _, c := range cases {
		if got := stepAfter(c.res, c.n, max, c.cancelled, c.failing); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestRunOutcome(t *testing.T) {
	cases := []struct {
		steps    []string
		want     string
		finished bool
	}{
		{[]string{StepCompleted, StepRunning}, "", false},
		{[]string{StepCompleted, StepPending}, "", false},
		{[]string{StepCompleted, StepCompleted}, RunCompleted, true},
		{[]string{StepCompleted, StepCancelled}, RunCancelled, true},
		{[]string{StepCancelled, StepFailed, StepCompleted}, RunFailed, true},
	}
	for _, c := range cases {
		got, finished := runOutcome(c.steps)
		if got != c.want || finished != c.finished {
			t.Errorf("runOutcome(%v) = %q %v, want %q %v", c.steps, got, finished, c.want, c.finished)
		}
	}
}

func TestOffline(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	open := []openAttempt{{"r/a/1", "lh"}, {"r/b/1", "inc"}, {"r/c/1", "pr"}}
	lastSeen := map[string]time.Time{
		"lh":  start.Add(50 * time.Second), // heard 10s ago
		"inc": start.Add(40 * time.Second), // heard 20s ago
	}
	cases := []struct {
		name string
		now  time.Time
		want []string
	}{
		{"just started: nobody counts as offline yet", start.Add(5 * time.Second), nil},
		{"unheard and silent too long", start.Add(60 * time.Second), []string{"r/b/1", "r/c/1"}},
	}
	for _, c := range cases {
		var got []string
		for _, a := range offline(open, lastSeen, start, c.now) {
			got = append(got, a.jobID)
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
