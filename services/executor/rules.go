package main

import (
	"time"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// The record's rules, as pure functions (docs/plan.md, "Lifecycles").

// An attempt is dispatched (queued for its device), then started (the
// instrument's ledger has it), then completed, failed or revoked. started is
// best effort: a result may arrive while the attempt is still dispatched.
const (
	AttemptDispatched = "dispatched"
	AttemptStarted    = "started"
	AttemptCompleted  = "completed"
	AttemptFailed     = "failed"
	AttemptRevoked    = "revoked"
)

// attemptStatus: an attempt completed only if the instrument completed it and
// nothing went wrong after (such as lost data).
func attemptStatus(r protocol.Result) string {
	switch {
	case r.Outcome == protocol.OutcomeRevoked:
		return AttemptRevoked
	case r.Outcome == protocol.OutcomeCompleted && r.Error == "":
		return AttemptCompleted
	}
	return AttemptFailed
}

// stepAfter is the step's status once attempt n has ended. A failed or hung
// attempt retries only on a retryable device, within max attempts, and only
// if the run was not cancelled and no other step has failed (the run will
// fail anyway, so a retry would be physical work for nothing).
func stepAfter(r protocol.Result, n, max int, cancelRequested, runFailing bool) string {
	switch attemptStatus(r) {
	case AttemptCompleted:
		return StepCompleted
	case AttemptRevoked: // it never ran
		return StepCancelled
	}
	if r.Retryable && n < max && !cancelRequested && !runFailing {
		return StepPending
	}
	return StepFailed
}

// DeviceTimeout is how long a device may go unheard before it counts as
// offline: three missed status messages.
const DeviceTimeout = 3 * protocol.StatusInterval

// openAttempt is an attempt that has not ended: queued or on its device.
type openAttempt struct{ jobID, device string }

// offline returns the open attempts whose device has not been heard from for
// longer than DeviceTimeout. A device never heard from counts from since,
// when the executor started listening.
func offline(open []openAttempt, lastSeen map[string]time.Time, since, now time.Time) []openAttempt {
	var out []openAttempt
	for _, a := range open {
		heard, ok := lastSeen[a.device]
		if !ok {
			heard = since
		}
		if now.Sub(heard) > DeviceTimeout {
			out = append(out, a)
		}
	}
	return out
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
