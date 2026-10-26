# Plan: a run record that stays true

## Goal

A scientist reads the run record and acts on it. The record must agree with
what the instruments physically did (their ledgers) and with the data in S3,
through crashes, partitions, slow components, restarts and instrument faults.

## Definitions

- **Ledger** — the instrument's append-only log (`instrument.go`). Physical
  truth. It never invents an execution. It can lose an *ending*: a failed
  finish-write followed by a restart turns `completed` into `hung`. Instrument
  disk failure is out of scope.
- **Record** — what the executor stores in Postgres and serves over HTTP and
  GraphQL. It only moves forward: nothing is deleted or rewritten, terminal
  states are final, attempts are append-only.
- **Run** — a set of steps from one workflow DAG.
- **Step** — one node of the DAG, bound to one device.
- **Attempt** — one command for one step. `job_id = <run_id>/<step>/<n>`, used
  everywhere: Postgres, NATS message id, instrument `JobID`, S3 key.
- **Outcome** — belongs to the instrument (`completed`, `failed`, `hung`). The
  executor maps it to attempt and step status.

## Lifecycles

### Step (the five API values, fixed by the README)

```
pending ──> running ──> completed
   │           │  └───> failed
   │           └──────> pending   (retry only; the next dispatch is a new attempt)
   ├──────────────────> cancelled (no attempts yet)
   └──────────────────> failed    (cancelled while waiting to retry)
```

- `pending`: not dispatched yet, or waiting to retry.
- `running`: an attempt was committed and published, and no final report has
  arrived. The work is on the device or on its way there. A plate-reader step
  stays `running` until its data is in S3, because the driver reports only
  after the upload. There is no separate measurement state: lost data is a
  failed attempt with an error.
- `completed`: the last attempt completed on the instrument. For data-producing
  devices the object is also at `data_ref` and its sha256 matches the ledger.
- `failed`: at least one attempt ran, and the last one failed or hung, and no
  retry is allowed or attempts are used up.
- `cancelled`: never dispatched. A step that is cancelled while waiting to
  retry already has failed attempts, so it becomes `failed` (its last attempt's
  outcome) instead. "Cancelled" never hides an execution, and failure beats
  cancel at the step level too.
- Terminal states are final.

### Step times in the HTTP view

`StepView.started_at` / `finished_at` / `error` come from the **last attempt**:
the one that decided the step's status. If the view used the first attempt's
start and the last attempt's finish, retried steps would overlap other steps
on the same device in `/timeline`. Earlier attempts appear only in GraphQL.

### Attempt (internal, exposed through GraphQL)

`dispatched -> completed | failed`. Each attempt carries the instrument's
`execution_id`, `started_at`, `finished_at`, the raw outcome, the error,
`data_ref` and `data_sha256`, all copied from the ledger entry.

### Mapping an outcome to a step

| Instrument outcome | Attempt | Step |
| --- | --- | --- |
| `completed` (+ data stored, if any) | completed | completed |
| `failed`, device retryable, attempts < max | failed | pending -> new attempt |
| `failed`, not retryable or attempts used up | failed | failed |
| `hung` (includes "restarted during the job") | failed | same as `failed` |
| `completed`, but plate data lost before spooling | failed ("data lost") | same as `failed` |

- Retryable is the device's config (`inst.FailuresRetryable()`): liquid
  handler no, incubator and plate reader yes. A hung or lost liquid-handler
  attempt fails the step with no retry.
- `max_attempts` defaults to 3 and can be configured.

### Run

Derived from its steps, in this order:

1. `pending` — not started.
2. `running` — started, and some step is not terminal.
3. `failed` — all steps terminal, and at least one failed.
4. `cancelled` — all steps terminal, at least one cancelled, none failed.
5. `completed` — all steps completed.

So a failure beats a cancel, and a run is never terminal while an attempt is
still physically running.

- **Cancel:** every `pending` step becomes `cancelled`, or `failed` if it is
  waiting to retry. Running steps are left alone and end as whatever they
  physically became: ending `ctx` stops the waiting, not the work.
- **A step ends `failed`** (not retryable, or attempts used up): every
  remaining `pending` step in the **whole run** is closed by the same rule as
  cancel, not only the failed step's descendants. Steps on independent
  branches that are already running finish normally and keep their real
  outcome.
- **Dependencies** must be `completed`, not just finished (`NextSteps`
  already does this).

## Ordering and clocks

Ordering comes from causality, not from timestamps. A step is dispatched only
after its dependencies' completions are committed in Postgres, and a completion
is only reported after the instrument finished. So `dep.finished_at <
step.started_at` holds physically. Instrument times are evidence that the
checker verifies, not something the executor reasons with.

Here every container uses the same host clock. On real hardware, clock skew
between instruments could make that evidence disagree with the true order (see
Limits).

## Topology

```
executor ──(outbox relay)──> stream CMDS     lab.cmd.<device>
                                │  durable consumer driver-<device>
                                │  MaxAckPending=1, explicit ack
                                ▼
                              driver ── inst.Run / inst.Status
                                │  spool -> S3 put
                                ▼
executor <──(durable consumer)── stream RESULTS  lab.result.<device>
```

- **CMDS** — JetStream, file storage, work-queue retention, subject per device.
  Message id = `job_id`. One durable consumer per device with
  `MaxAckPending=1`, so a device handles one command at a time.
  `AckWait` is kept from expiring by `InProgress` while a job runs or waits.
- **RESULTS** — JetStream, file storage. Message id = `job_id` + `:result`. The
  executor acks only after the result's transaction commits.
- Commands for the same device from different runs queue in CMDS, so the
  driver never sends a second job to a busy instrument.

### State lives in

| State | Where |
| --- | --- |
| Runs, steps, attempts, outbox | Postgres (executor only) |
| Commands and results in flight | JetStream file store |
| Physical truth | Instrument ledger (driver volume) |
| Plate data before upload | Driver spool directory: a new named volume per driver in `docker-compose.yml`, separate from `/ledger` |
| Plate data | S3, key `runs/<run_id>/<step>/<n>.csv` |

## Executor

- **One dispatch loop decides.** It calls `NextSteps`, creates attempts, and
  applies results. API writes (`CreateRun`, `StartRun`, `CancelRun`) and the loop
  take the run row with `SELECT … FOR UPDATE`, so they never act on a stale
  view of the same run.
- **Dispatch transaction:** step `pending -> running`, insert attempt `n`
  (`dispatched`), insert outbox row. Commit. Then the relay publishes and marks
  the row published. A crash between commit and publish leads to a republish,
  which the driver deduplicates.
- **Result transaction:** update the attempt `WHERE status = 'dispatched'`.
  If no row was updated, it's a duplicate: ack and drop it. Otherwise map the
  outcome to the step, cascade cancels, recompute the run. Commit, then ack.
- **No timeouts in the executor.** It never decides an attempt is lost. A
  partitioned executor decides nothing; results wait in RESULTS.
- **Startup:** reload, publish any unpublished outbox rows, resume the
  consumer. Nothing else needs repairing because every change was
  transactional.

## Driver

One loop per device, commands handled one at a time:

1. Receive a command (`job_id`, `run_id`, `step`).
2. Look up `job_id` in `inst.Status()`.
   - Found and finished: skip to step 5 (redelivery after a crash or reconnect).
   - Not found: `inst.Run(ctx, Job{JobID: job_id, …})` with a deadline of
     `inst.Duration()` plus a margin.
3. If `Run` returned data: write it to the spool and fsync.
4. Whether `Run` returned or hit the deadline, wait until the ledger entry for
   `job_id` is no longer `running`. A hang resolves to `hung` after
   `HANG_DURATION`, and the device stays ours until then. Send `InProgress` while
   waiting.
5. If the outcome is `completed` and the device produces data:
   - Spool present: verify its sha256 against the ledger, then upload,
     retrying until S3 accepts it. The object is not read back.
   - No spool (crash between `Run` returning and the fsync): report `failed`
     with "data lost after completion".
6. Publish the result, including the ledger execution fields, and wait for
   the JetStream ack.
7. Ack the command and delete the spool file.

- Driver crash = instrument restart. The ledger marks an interrupted job `hung`.
  The unacked command is redelivered, and step 2 finds it.
- `ErrBusy` should not happen, because of `MaxAckPending=1` plus step 4. If it
  does: NAK with a delay. It isn't an attempt because nothing reached the ledger.
- A redelivered message arriving while the same `job_id` is in progress gets
  dropped.

## Invariants (what the checker verifies)

1. Every ledger execution whose `job_id` belongs to a run appears as an
   attempt in the record, with the same execution id, times and outcome.
2. Every attempt with an outcome matches a ledger execution. No phantom
   completions.
3. A completed plate-reader step has a `data_ref` whose object's sha256 equals
   the ledger's `data_sha256`.
4. A non-retryable device ran each step at most once.
5. No device has overlapping executions in its ledger (the instrument
   guarantees this). Two steps `running` on one device do not claim overlap:
   `running` means on the device or queued for it.
6. For every step, `started_at` of the first attempt is later than
   `finished_at` of every dependency's completing attempt (instrument times).
7. Step and run statuses follow the rules above. A `cancelled` step has no
   attempts. A step with attempts is never `cancelled`.
8. A step's `started_at` / `finished_at` in the HTTP view equal its last
   attempt's instrument times, to 1 ms. While a new attempt is running they
   are empty.
9. Attempt numbers are consecutive and stay within `max_attempts`.

Planned: `scripts/check_record.sh` compares `/runs/{id}` with `:7101-7103/status`.

## Failure scenarios and how to trigger them

| Scenario | Trigger | Expected record |
| --- | --- | --- |
| Executor crashes between commit and publish | `chaos.sh kill executor` during dispatch | Relay republishes; driver dedupes; one execution |
| Executor partitioned (loses Postgres + NATS) | `chaos.sh disconnect executor N` | Nothing decided; results drain on reconnect |
| Driver crash = instrument restart | `docker compose restart <driver>` | Ledger `hung`; attempt failed; retry only if retryable |
| Driver crash after Run, before upload | restart plate reader mid-upload | Spool upload on redelivery; or "data lost" -> retry |
| Driver partitioned (loses NATS + S3) | `chaos.sh disconnect <driver> N` | Step stays running; work continues; reports on reconnect, no second run |
| S3 down while the driver holds the data | `chaos.sh storage-down N` | Step stays running until the upload succeeds |
| Instrument hangs | `*_HANG_PCT=…` | Attempt failed after `HANG_DURATION`; retry per device |
| Instrument fails | `*_FAIL_PCT=…` | Retry up to max; liquid handler fails at once; pending steps cancelled |
| Cancel races a dispatch | cancel right after a step becomes ready | Either cancelled with no attempt, or runs to its real outcome |
| NATS restart | `docker compose restart nats` | Clients reconnect; durable streams replay |
| Postgres down | `docker compose stop postgres` | Loop pauses; results not acked; API errors; no wrong state |
| Random mix | `chaos.sh random --seed N --for 120` | All invariants hold |

## Assumptions

- Postgres and the JetStream file store survive a process kill. A single NATS
  node with default fsync does not survive a host crash.
- The ledger survives a restart. It may lose an ending, but never invents an
  execution.
- One executor instance.
- Each device's `Retryable` config doesn't change during a run.
- All containers share one host clock.

## Non-goals

- Authentication; multiple tenants.
- Exactly-once delivery. Delivery is at least once; duplicates are made
  harmless by `job_id`. Physical execution of non-retryable steps is at most
  once.

## Limits

- **A device with no driver, or a driver that never comes back:** its step
  stays `running` forever. That's accurate, but nothing alerts on it.
- **Lost ledger ending:** if the record says `completed` and a later restart
  changes the ledger to `hung`, the record stays `completed` (it only moves
  forward) and the checker flags the mismatch. Caused only by a disk failure,
  which is out of scope.
- **Clock skew** on real hardware can make invariant 6 fail in the evidence
  while the causal order is correct.
- **Run `started_at` / `finished_at`** come from Postgres `now()`. Step times
  come from the instruments. The timeline mixes the two clocks.
- **Data lost between `Run` returning and the spool fsync** costs one retry of
  the plate reader. The attempt says `failed` while the ledger says `completed`.
  The error explains why.

## Next steps

- Message versioning and a contract doc for `lab.cmd.*` / `lab.result.*`.
  For now [protocol.go](../services/common/protocol/protocol.go) and the
  Topology section above are the contract.
