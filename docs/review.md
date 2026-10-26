# Review: ways the record can lie

An independent review of the diff since `692c7a2`, given only `README.md`,
`instrument.go` and the invariants in [plan.md](plan.md). Each finding was
reproduced before it was fixed, unless noted.

## Findings and what was done

| # | Finding | Decision | Evidence |
| --- | --- | --- | --- |
| F3 | The driver stopped waiting for `Run` on a timer (`Duration` + 10s). A read that outlived it (driver frozen, e.g. `docker pause`) completed in the ledger but its data was dropped, so the attempt was reported as "data lost". | Not a false record (data lost maps to failed), but a bug: data lost for nothing. `Run` is now cancelled only when the ledger says the job hung. | `TestSlowReadKeepsItsData` failed before, passes after. On the stack, three 15s `docker pause plate-reader-1` mid-read: each read completed once with its data stored. Side effect: a hang now costs `HANG_DURATION`, not at least 12s. |
| F1 | During a retry the step showed `running` with the failed attempt's times and error. | Fixed: dispatching a new attempt clears the step's times, error and `data_ref`. | `TestRetryClearsPreviousAttemptFromStep` failed before, passes after. |
| — | After one step failed, another step still running could fail retryably and be retried: physical work in a run that will fail. | Fixed: no retry once the run has a failed step. | `TestStepAfter` (unit) and `TestNoRetryOnceRunIsFailing` (integration); with the rule disabled, `b` was dispatched 3 times. |
| F5 | `GetRun` read the run and its steps in two queries, so a commit between them could show a run disagreeing with its steps. | Fixed without reproduction: both queries run in one read-only repeatable-read transaction. | — |
| F4 | Step times are rounded to the microsecond, so they differ from the ledger's floats by up to 0.5 µs. | Rejected. The checker compares to 1 ms (invariant 8 says so). | — |
| F2 | Two runs can show `running` on one device while the ledger has one execution: the second command is queued. | By design. Invariant 5 reworded: `running` means on the device or queued for it. | — |

## Suspected and ruled out

- **Stale ack after a driver partition longer than AckWait:** nats-server 2.10.29 acks by stream sequence, so the old delivery's ack is accepted. A redelivery finds the job in the ledger and reports again; nothing runs twice.
- **Duplicate "data lost" result after the real one was acked:** same message id, so deduped within 2 minutes. Otherwise it arrives after the real result (one ordered consumer, NAK keeps order) and the executor ignores it because the attempt is no longer `dispatched`.
- **Executor republishes a command after the dedupe window or a NATS restart:** the driver finds the job in the ledger and reports without running; the executor ignores the duplicate.
- **Liquid handler runs a step twice:** its failures are not retryable, the job-id ledger check covers redeliveries, and only the one loop dispatches, under the run's row lock.
- **A terminal step is overwritten:** a dispatched attempt implies its step is `running`, and closing pending steps only touches `pending`.
- **A cancelled step that actually ran:** a step with attempts becomes `failed`, not `cancelled`. An attempt still in the outbox after cancel runs, is recorded, and the run waits for it.
- **A run terminal while work is in flight:** the run settles only when every step is terminal; a hang is reported only once the ledger shows `hung`.
- **`ErrBusy`:** unreachable. One command per device at a time, and a restart clears `busy`.
- **Driver restart mid-job:** the instrument marks the job hung on load; the redelivered command reports exactly that.
- **Dependency order or clock skew (invariant 6):** dispatch follows the dependency's committed completion, all containers share the host clock, and the gap is milliseconds.
- **A partial ledger line causing a re-run:** a partial start line means nothing started; a partial end line leaves the start line, so the job is found and marked hung.
- **NATS losing acked publishes:** needs a host or VM crash; `docker kill nats` keeps the page cache.
- **Missing bucket:** the executor creates it at start. If it were missing, the step would stay `running` (stuck, not false).
