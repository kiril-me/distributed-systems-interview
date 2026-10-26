package main

import "github.com/automata-tech/distributed-systems-interview/services/common/protocol"

// The record's rules, as pure functions (docs/plan.md, "Lifecycles").

const (
	AttemptDispatched = "dispatched"
	AttemptCompleted  = "completed"
	AttemptFailed     = "failed"
)

// attemptStatus: an attempt completed only if the instrument completed it and
// nothing went wrong after (such as lost data).
func attemptStatus(r protocol.Result) string {
	if r.Outcome == protocol.OutcomeCompleted && r.Error == "" {
		return AttemptCompleted
	}
	return AttemptFailed
}

// stepAfter is the step's status once attempt n has ended. A failed or hung
// attempt retries only on a retryable device, within max attempts, and only
// if the run was not cancelled.
func stepAfter(r protocol.Result, n, max int, cancelRequested bool) string {
	if attemptStatus(r) == AttemptCompleted {
		return StepCompleted
	}
	if r.Retryable && n < max && !cancelRequested {
		return StepPending
	}
	return StepFailed
}

// runOutcome derives a run's status from its steps once all are terminal.
// Failed beats cancelled, cancelled beats completed.
func runOutcome(steps []string) (status string, finished bool) {
	var failed, cancelled bool
	for _, s := range steps {
		switch s {
		case StepPending, StepRunning:
			return "", false
		case StepFailed:
			failed = true
		case StepCancelled:
			cancelled = true
		}
	}
	switch {
	case failed:
		return RunFailed, true
	case cancelled:
		return RunCancelled, true
	default:
		return RunCompleted, true
	}
}
