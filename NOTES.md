# Notes

**In short.** The instrument's ledger decides what happened; everything else
is replay that is safe to repeat. One `job_id` per attempt
(`run/step/n`) is the key in Postgres, NATS, the ledger and S3, so a
redelivered command is looked up, not run again. JetStream carries commands
and results both ways, and each side acks only after its next durable step.
The record only moves forward and never claims more than the ledger shows: a
step stays `running` while something is down rather than guessing.
`check_record.sh` checks every finished run against the ledgers and S3.

Details in [docs/plan.md](docs/plan.md) (lifecycles, invariants, failure
table) and [docs/decisions.md](docs/decisions.md) (ADRs). Checker:
[scripts/check_record.sh](scripts/check_record.sh), every finished run against
the instrument ledgers and S3.

## 1. Topology

```
            HTTP + GraphQL :5002
                    │
               ┌────▼─────┐  tx: step running + attempt (outbox)   ┌──────────┐
               │ executor │───────────────────────────────────────▶│ Postgres │
               └──┬────▲──┘                                         └──────────┘
   relay publishes│    │ ack after the result's tx commits
   msg-id=job_id  │    │
          ┌───────▼─┐ ┌┴────────┐
          │  CMDS   │ │ RESULTS │   JetStream, file storage, work queue
          └───────┬─┘ └▲────────┘
  ack after result│    │ msg-id=job_id:result
  is stored;      │    │
  InProgress/10s  │    │
               ┌──▼────┴──┐  job_id in ledger? report it : inst.Run
               │  driver  │──▶ spool + fsync ──▶ S3 ──▶ report
               └────┬─────┘
               instrument ledger
```

`job_id = run/step/attempt` everywhere: Postgres, NATS msg id, instrument
`JobID`, S3 key.

| Stream | Subjects | Consumer | Settings |
| --- | --- | --- | --- |
| `CMDS` | `lab.cmd.<device>` | `driver-<device>`, durable, one per device | explicit ack, AckWait 30s, `MaxAckPending=1` = one command per device across runs |
| `RESULTS` | `lab.result.<device>`, `lab.result.<device>.started` | `executor`, durable | explicit ack, AckWait 30s, `MaxAckPending=1`. one stream, so a start is read before its result |
| KV `REVOKED` | key = `job_id` | — | executor puts, driver reads before `Run` |
| core NATS | `lab.status.<device>` | executor subscription | not stored. a lost one is replaced 5s later |

Both sides read with one long-lived `Messages()` iterator
(`protocol.Receive`), not `Fetch` per call. See E9.

| Message | Sent | Acked | Not acked |
| --- | --- | --- | --- |
| Command | relay, after the attempt row commits | driver, after its result is stored in `RESULTS` | redelivered, ledger check stops a second run |
| `InProgress` | driver, every 10s while a job runs, hangs or waits for S3 | — | — |
| Started | driver, once the ledger has the job. one try, msg id `job_id:started` | executor, after attempt → `started` commits | redelivered. lost = step shows queued until its result |
| Result | driver, after the ledger entry ended and plate data, checked against the ledger's sha256, is in S3 | executor, after the result tx commits | redelivered, applied only while attempt is `dispatched` or `started` |
| Revoke | executor loop, for `dispatched` attempts of a cancel-requested run | — (KV put, retried each tick until `revoked_at` is set) | — |
| Status | driver, every 5s: busy, instrument's current job | — | next one in 5s |

| State | Where |
| --- | --- |
| Runs, steps, attempts, outbox (`published_at IS NULL`) | Postgres, executor only |
| Messages in flight | JetStream file store, single node |
| Physical truth | instrument ledger, driver volume |
| Plate data before upload | driver spool volume |
| Plate data | S3 `runs/<run_id>/<step>/<n>.csv` |
| Jobs no longer wanted | JetStream KV `REVOKED` |
| Device liveness | executor memory, refilled within 5s of a restart |

Attempt: `dispatched` (queued) → `started` (in the ledger, on the device) →
`completed` | `failed` | `revoked` (cancelled while queued, never ran). The
driver checks ledger → `REVOKED` → `Run`, so a job that ran is never reported
revoked. **Deploy the executor before the drivers**: an old executor reads a
`started` message as a result and fails the step (seen in a test, below).

## 2. Why

- **ADR-1** JetStream both ways, outbox, ack only after the next durable step,
  heartbeats. Rejected: core request/reply, lost when either side is down.
  Publishing straight from the executor, crash between commit and publish
  loses the command.
- **ADR-2** job_id everywhere, ledger check before Run, hang read from ledger.
  Rejected: new id per delivery, NATS dedup alone (2 min window), executor
  timeouts (would retry work still on the device).
- **ADR-3** One scheduler, one command per device across runs, failed beats
  cancelled, running step ends as it physically ended. Rejected: device lock
  per run only (`ErrBusy` across runs), cancelling running steps, ordering by
  timestamps.
- **ADR-4** Spool → S3 → report. Rejected: report before upload, data only in
  memory, data through the executor.
- **ADR-7** (proposed, not built) the device queue lives in NATS, and ADR-6
  (`REVOKED`, `started`, deploy order) pays for that. An executor-side device
  lease would keep a queued step `pending`, make cancel a database update and
  remove `REVOKED`. Not done here: the current design is verified and swapping
  it late risks the record. First thing I would change.

## 3. GraphQL

`POST /graphql`, same port, `graph-gophers/graphql-go`. Schema:
[schema.graphql](services/executor/schema.graphql). HTTP API unchanged.

```graphql
type Query {
  run(id: ID!): Run                                              # null if unknown
  runs(status: RunStatus, first: Int = 50, after: ID): RunConnection!   # newest first
  workflows: [Workflow!]!
}
type Mutation { createRun(workflowName: String, idempotencyKey: String): Run!  startRun(id: ID!): Run!  cancelRun(id: ID!): Run! }

Run     { id workflowName status cancelRequested createdAt startedAt finishedAt steps }
Step    { name deviceId dependsOn dependencies status startedAt finishedAt error dataRef attempts }
Attempt { n jobId runId step status outcome executionId startedAt finishedAt error dataRef dataSha256 dispatchedAt }
Device  { id online lastSeenAt busy instrumentJobId current queued }   # Query.devices
```

- Attempts only here. HTTP shows the last attempt. Every physical execution is
  visible with the instrument's job id, execution id and times.
- `status` is ours (dispatched/started/completed/failed/revoked), `outcome` is
  the instrument's (completed/failed/hung). Lost data = outcome completed,
  status failed, with an error. A step is RUNNING whether queued or on the
  device (fixed status values); its last attempt's status says which.
- `devices`: liveness from the drivers' status messages (memory), work from
  the record (snapshot): `current` = started attempts, `queued` = dispatched,
  oldest first. Answers "what is the incubator doing, what is waiting".
- `createRun(idempotencyKey)`: a retry gets the first call's run, a race is
  decided by a unique index. `INVALID_STATE` carries `extensions.status`, so
  a retried `startRun` can tell "already RUNNING" from a refusal.
- No measurement state. A read stays running until the data is in S3.
- `cancelRequested`: a cancelled run stays running until its running steps end.
- `dependencies` next to `dependsOn`, to check order in one query.
- One snapshot per query.
- Following = polling. Subscriptions cut on purpose.
- Errors via `extensions.code`: `NOT_FOUND`, `INVALID_STATE`, `BAD_INPUT`.
- No AsyncAPI. protocol.go + topology in plan.md are the contract for now.

Evidence: `TestSchemaMatchesResolvers` fails on a missing resolver (checked by
renaming one). Integration tests for retries, error codes, paging pass, none
seen failing first. On the stack: create/start, `INVALID_STATE` on second
start, `null` for unknown id, paging. `acceptance.sh` 7/7, `check_record.sh`
652/652.

## 4. What I broke

Each fault checked in the ledger and driver logs, so it landed where intended.
`$C` = `./scripts/chaos.sh`, check = `check_record.sh`.

| # | Hypothesis | Command | Result | Verdict / change |
| --- | --- | --- | --- | --- |
| E1 | Executor crashes between commit and publish (outbox pattern) | `$C kill executor` at 3s | completed, 10/10 | holds. random-time kill, not aimed at the gap |
| E2 | Partitioned executor has no timeouts, so it decides nothing while cut off | `$C disconnect executor 10` | 10/10, but 52s run, ~31s idle after reconnect | record holds, stall → E9 |
| E3 | Driver crash is instrument restart. Hung on liquid handler → failed, no retry | `kill -s KILL liquid-handler-1` mid-step | ledger `hung`, step failed, pending cancelled, 9/9 | holds |
| E4 | Driver crashes after Run, before reporting. hung → treat as failed, retry if allowed | kill plate reader mid-read | attempt 1 `hung`, attempt 2 completed, 10/10 | holds |
| E5 | S3 down while the driver holds the only copy | `$C storage-down 15` across the read | upload retried, one execution, 10/10 | holds |
| E6 | plate data is only in memory → spool to disk before upload | `storage-down 20`, kill plate reader | uploaded from spool after restart, 10/10 | holds |
| E7 | partition = nats + s3 at once. work continues, reports on reconnect | `$C disconnect plate-reader-1 10` | 10/10 | holds |
| E8 | Driver partitioned longer than Ack (at most once) | `$C disconnect liquid-handler-1 45` | one execution per step, 10/10 | holds |
| E9 | ~30s stall after an executor partition, find the cause | sockets, consumer, Postgres sampled each second. idle incubator cut 8s too | result delivered to an abandoned `Fetch` inbox, `ackpend=1` for exactly AckWait, then redelivered. same on the driver. Postgres ruled out | `protocol.Receive`, one iterator. proxy tests red → green. stack: 31s → 4s, run 52s → 24s |
| E10 | failed → retry if allowed, else failed. max 3. rest of pending → cancelled | `CHAOS_SEED=1..4 INC_FAIL_PCT=50 PR_FAIL_PCT=50` | 2 completed, 2 failed as planned | holds |
| E11 | Instrument hangs. hung = physical state unknown → treat as failed | `CHAOS_SEED=1..3 HANG_DURATION=5s INC/PR_HANG_PCT=50` | holds, but a hang cost ≥12s | driver timer → E16 |
| E12 | hung or lost on liquid handler → failed, no retry | `CHAOS_SEED=1..3 LH_FAIL_PCT=25 LH_HANG_PCT=25` | one attempt, other running steps kept their real outcome | holds |
| E13 | Cancel races a dispatch. running step can't be cancelled, ends as what it physically became | cancel at 3s | running steps completed, run `cancelled` after them, 9/9 | holds |
| E14 | NATS restarts | `docker compose restart nats` | 10/10 | holds |
| E15 | Postgres down | stop Postgres 10s | completed in 22s, 10/10 | holds. noisy logs |
| R1–R4 | random mix, all invariants hold | `$C random --seed 7/11/3/7`, R2–R3 with instrument faults | 80/80, 80/80, 60/60, 60/60 | holds. R2 weak, no instrument fault fired |

**Review.** Subagent got only README.md, instrument.go and the invariants,
not our reasoning. Asked for concrete failure sequences, each tied to a
constraint. Reproduce before fixing. Full: [docs/review.md](docs/review.md).

| # | Finding | Decision | Evidence |
| --- | --- | --- | --- |
| F3 / E16 | driver gave up on Run after a timer, slow read lost its data | not a false record (data lost → failed). bug: data lost for nothing. cancel Run only when the ledger says hung | `TestSlowReadKeepsItsData` red → green. 3× 15s `docker pause plate-reader-1` mid-read, data stored, 30/30 |
| F1 / E17 | during retry the step showed the old attempt's times and error | accept. clear times/error on retry dispatch | `TestRetryClearsPreviousAttemptFromStep` red → green, seen on the stack |
| E18 | retry in a failing run | accept, block it | `TestNoRetryOnceRunIsFailing`. rule off → `b` ran 3 times |
| F5 | `GetRun` reads run and steps in two queries | fix, no repro. one read-only tx | — |
| F4 | times rounded to µs | reject. 1ms tolerance in checker | — |
| F2 | two runs `running` on one device | by design. running = on device or queued. revisited in F10: the attempt now says which | — |
| F6 / E19 | result for an unknown run is nak'd forever, `MaxAckPending=1` blocks every result behind it | fix: `Term` on `ErrRunNotFound`, keep nak for the rest (transient). no `MaxDeliver`: a long Postgres outage would drop real results | `TestResultForUnknownRunDoesNotBlockOthers` red → green. stack: published `run-doesnotexist/read_plate/1`, next run stuck 30s+ with `ackpend=1`, `run not found` every 1s. after fix: `dropping result`, acceptance 7/7, 682/682 |
| F7 | one run that fails to dispatch stops dispatch for every run after it | fix: dispatch the rest, join the errors | `TestRunThatCannotDispatchDoesNotBlockOthers` red → green. broken run = pre-inserted attempt row taking its job id, `duplicate key` every tick, healthy run stuck before the fix |
| F8 | `/` in a step name breaks `ParseJobID`, result dropped, step `running` forever. `.` `*` `>` in a device breaks the subject | fix: step names and devices must match `^[A-Za-z0-9_-]+$`, refused at load | `TestValidateWorkflowRefusesUnsafeNames` red → green, `TestShippedWorkflowsLoad` |
| F9 | GraphQL `startRun`/`cancelRun` checked then acted: racing callers all succeeded | fix: act first (conditional update / locked tx), INVALID_STATE for the caller that changed nothing. a cancel while already cancelling is now INVALID_STATE. HTTP unchanged | `TestGraphQLConcurrentMutationsOnlyOneWins`: 10 of 10 succeeded → 1 of 10, ×5 |
| F10 | a step `running` with no start time: on the device, queued, or driver dead? | `started` message, attempt `dispatched` → `started`, step `startedAt` set on the device (ADR-6) | `TestStartIsAnnouncedFromTheLedger`, `TestStartedShowsStepOnDevice` red → green. red showed an old executor fails the step on a `started` → deploy executor first. stack, two runs on liquid-handler-1: `current [A/fill_sample_plate/1] queued [B/fill_sample_plate/1]`, a second later B current, A's next queued |
| F11 | a cancelled run's queued command still runs | KV `REVOKED`, driver checks ledger → revoked → Run (ADR-6) | `TestRevokedJobIsNotRun`, `TestRevokedUnreadableRunsNothing`, `TestCancelRevokesQueuedCommandOnly` red → green. stack: cancel while B queued → attempt `REVOKED`, step cancelled, `startedAt` null, 0 ledger executions for B on all three devices. cancel 1s later, after B started → B ran and is `COMPLETED`, as it physically ended |
| F12 | a driver that never comes back is silent | status every 5s, `ALERT` log after 15s quiet, `devices.online` | `TestOffline`, `TestServeSaysItIsAlive` red → green. stack: `docker pause incubator-1`, run started → `ALERT run-521c…/incubate_samples/1 waits on incubator-1, offline: last heard 16s ago`, once. `online:false`, attempt in `queued`. unpause → online, run completed, 759/759 |
| F13 | a tool retrying `createRun` makes two runs | `idempotencyKey`, unique index, `INVALID_STATE` carries `status` | `TestGraphQLCreateRunIsIdempotent` red → green, 10 racing creates → 1 run, ×5. stack: retry returned the same id, 1 row for the key, other workflow → `BAD_INPUT`, second `startRun` → `{code: INVALID_STATE, status: RUNNING}` |
| F14 | new columns vs the stack's existing database | `ALTER … IF NOT EXISTS` at start, also in init.sql. no `down -v` | `TestStartMigratesAnOlderDatabase`: 0 new columns with migrate off, 2 with it on, old run kept. stack: 0 → 2 columns, 71 runs kept |

**Ruled out:** stale ack after partition > AckWait (server acks by stream
sequence). duplicate "data lost" after the real result (deduped, or ignored,
not `dispatched`). republish after dedup window (ledger check). liquid handler
running twice. terminal step overwritten. cancelled step that ran. run
terminal while work in flight. `ErrBusy`. driver restart mid-job. dependency
order and clock skew. partial ledger line. NATS losing acked publishes (needs
host crash). missing bucket (stuck, not false).

After all fixes: `go test -race ./...` green with `TEST_DATABASE_URL` set
(without it the executor's integration tests skip), `acceptance.sh` 7/7,
`check_record.sh` 759/759 over 77 runs. One flaky test:
`TestCommandSentDuringPartitionArrivesAfterThaw` fails ~1 in 3 under
`-race`, a data race inside the in-process `nats-server` v2.10.29 (99
frames there, none in this code) when both sides create streams at once.
Present before these changes.

## 5. Still not true or not proven

On purpose:
- device with no driver: step stays running forever, true. the executor logs
  `ALERT <job> waits on <device>, offline` once per silence; nothing pages.
  no operator action to close it: a person marking it failed, then the driver
  reporting completed, would contradict the ledger. designed, not built: an
  operator resolution as its own outcome, the late real result kept next to it.
- revoke is best effort: a driver that read `REVOKED` just before the cancel
  runs the job, and the record says so (`TestCancelledRunsQueuedCommandStillRuns`).
- a lost `started` leaves the step looking queued until its result.
- `devices.busy` / `instrumentJobId` are up to 5s old: seen on the stack as
  `busy:false` next to a `current` attempt. `current` (the record) is the
  one to trust.
- HTTP `started_at` on a running step is now filled once it is on the device,
  not only at its result. same shape, earlier value. acceptance 7/7.
- `REVOKED` keys are never deleted. one per cancelled queued attempt; fine
  here, a TTL longer than any queue wait if it grows.
- data lost between Run returning and spool fsync: one re-read. attempt
  failed, ledger completed. on the last attempt the run ends failed. untested
  on the stack.
- ledger loses an ending (completed → hung after restart): record stays
  completed, checker flags it. needs instrument disk failure, out of scope.
- JetStream single node, default fsync. survives service kill, not host crash.
- one executor, no HA.
- step times from instruments, run times from Postgres `now()`. same host
  clock here, skew on real hardware → evidence can break, order can't.

Not proven / not done:
- no executor kill aimed exactly between commit and publish.
- no host crash.
- instrument faults roll per seed, device, step, attempt, not run. all runs in
  a round saw the same faults.
- no timeout on Postgres calls. 2–4s TCP backoff after a partition (E9).
- two concurrent HTTP `startRun` both return 200; only one starts the run. GraphQL fixed (F9), HTTP kept as it is.
- `ListRuns` and GraphQL `runs` read each run with its own queries (~4 per run, up to ~800 for 200). fine at this size, batch with `run_id = ANY($1)` if it grows.
- noisy logs while S3 or Postgres is down.
- no auth, no message versioning, no GraphQL depth limits.

Scale and head-of-line, not measured:
- every result goes through one funnel. `RESULTS` has one consumer with
  `MaxAckPending=1`, and each result is applied inside the one loop.
- each pass of that loop (result, poke or 1s tick) also dispatches over every
  running run (one row-locked tx each), publishes the outbox (up to 5s per
  JetStream publish, serial, first failure stops the rest), puts revokes in KV
  and runs the offline query. a slow NATS or a long outbox delays results for
  every device. cost per pass grows with the number of running runs.
- poison results: an apply that keeps failing with anything other than
  `ErrRunNotFound` is nak'd every 1s for ever and holds every device's results
  behind it. F6 fixed one case, not the class. no `MaxDeliver` on purpose (a
  long Postgres outage would drop real results).
- ordering is not needed for correctness: `applyResult` acts only on a
  `dispatched`/`started` attempt, `applyStarted` only on `dispatched`, both
  under the run's row lock. so results could be applied by a bounded worker
  pool with `MaxAckPending` > 1, with publish, revoke and the offline check
  moved out of the loop. a poison message would then hold one slot, and an
  `ALERT` on delivery count would surface it.
- fine at 3 devices. not designed or tested for tens of devices or hundreds of
  running runs.

## Next steps

- device lease in the executor instead of the queue in NATS (ADR-7).
- results applied in parallel, I/O out of the decision loop (scale note above).
- message versioning + contract doc for `lab.cmd.*` / `lab.result.*`. for now
  protocol.go + topology in plan.md. readers should drop subjects they don't
  know instead of failing the step (removes the deploy-order rule).
- turn the `ALERT` log line into a page, and the operator resolution above.
- timeouts on Postgres calls (E9, 2–4s TCP backoff).
- chaos: executor kill aimed between commit and publish, host crash.
- JetStream cluster, 3 nodes, instead of one.
- instruments without a job id lookup: ADR-5, not built.
- HA executor. out of scope on purpose (decisions.md).

## Time spent

- 26 Oct: ~3h10, about 19:00–22:10, few pauses. this repo, from scratch.
  plus the notes.

## Feedback

- I liked it. It is small enough to finish and still rewards reading
  `instrument.go` closely (ending `ctx` stops the waiting, not the work). The
  ledger side channel turns "the record must be true" into something you can
  check, not argue about.
- Setup took time. My machine did not have enough free disk for the stack,
  so I had to clean up before I could start. A note on how much disk the
  images and volumes need would help.
- This was my first AI-assisted take-home, and it was not obvious how to show
  two things at once: that I use the tools well, and that I understand the
  code, the bottlenecks and the design myself. I settled on decisions agreed
  before any code, evidence for every claim, an independent review, and the
  full conversation in `ai/`. A line in the README on what you want to see
  from an AI-assisted submission would help.
- My first AI session was ad hoc: quick questions to check the code and the
  brief when I had a spare minute. It went further than I asked and started
  solving the exercise. When I sat down to do the work properly, the code it
  left already looked solid, and it took me a while to work out what was
  left for me to do. I put it aside and started this repo clean. A hint that
  agents tend to do this would save the next person that time.

## AI usage

Claude Code for all of it. My pattern: no code until a design decision is
agreed, I decide, it implements with tests first, every claim needs a log
line, a ledger entry or a query. Review by a subagent that got only
README.md, instrument.go and the invariants, not our reasoning.

Conversations in `ai/`:
- Take-home requirements review (26 Oct)
- Ledger and instrument assumptions review (26 Oct). the main one: plan,
  ADRs, implementation, chaos, stall, review, GraphQL
- Test coverage for executor rules (26 Oct)
- NOTES.md drafting (26 Oct)
