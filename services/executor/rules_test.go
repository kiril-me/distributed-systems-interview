package main

import (
	"testing"

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
		want      string
	}{
		{"completed", protocol.Result{Outcome: "completed"}, 1, false, StepCompleted},
		{"completed after cancel still completed", protocol.Result{Outcome: "completed"}, 1, true, StepCompleted},
		{"retryable failure retries", failed(true), 1, false, StepPending},
		{"retryable failure on last attempt fails", failed(true), max, false, StepFailed},
		{"non-retryable failure fails at once", failed(false), 1, false, StepFailed},
		{"hung on liquid handler fails at once", protocol.Result{Outcome: "hung", Error: "instrument hung"}, 1, false, StepFailed},
		{"no retry after cancel", failed(true), 1, true, StepFailed},
	}
	for _, c := range cases {
		if got := stepAfter(c.res, c.n, max, c.cancelled); got != c.want {
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
