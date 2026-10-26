# Decisions

Details live in [plan.md](plan.md).

## ADR-1 Messaging: JetStream both ways, with an outbox

- **Decision:** Commands (`CMDS`, `lab.cmd.<device>`) and results (`RESULTS`, `lab.result.<device>`) are JetStream file streams. The executor writes each command to an outbox in the same transaction as its attempt, and a relay publishes it.
- Each side acks only after its next durable step. The driver acks a command after its result gets a JetStream ack. The executor acks a result after its transaction commits. The driver sends `InProgress` heartbeats while a job runs.
- **Rejected:** Core request/reply, because a message is lost when either side is down or partitioned. Publishing straight from the executor, because a crash between commit and publish loses the command.
- **Consequence:** Delivery is at least once in both directions, so duplicates are normal (ADR-2). Messages survive process kills and partitions. A step stays `running` while anything is down. Durability is one NATS node, so it doesn't survive a host crash.

## ADR-2 Effectively once: one `job_id`, the ledger decides

- **Decision:** `job_id = run_id/step/n` is used as the attempt key, the NATS message id, the instrument `JobID` and the S3 key. It is committed before anything is published.
- Before calling `Run`, the driver looks up `job_id` in `inst.Status()`. If it's already there, the driver reports what the ledger says and doesn't run it again. The driver waits until the ledger entry ends, so a hang is read from the ledger as `hung`. The executor applies a result only while the attempt is still `dispatched`.
- **Rejected:** A new id per delivery, because duplicates can't be detected after a crash. NATS message-id dedup alone, because its window is 2 minutes. Executor timeouts that declare an attempt lost, because they would retry work still on the device.
- **Consequence:** Each attempt runs physically once, even with redelivery, and a non-retryable step runs at most once. The executor never times out, so a driver that never returns leaves its step `running`. Each hang costs `HANG_DURATION`.

## ADR-3 One scheduler, an honest record

- **Decision:** One dispatch loop decides, using `NextSteps`; API writes lock the run row (`FOR UPDATE`). One consumer per device with `MaxAckPending=1` gives one command per device across runs.
- Failed or hung attempts retry as a new attempt if the device is retryable and n < max (3); otherwise the step fails. A failure closes every pending step in the run. Cancel turns pending steps into `cancelled` (`failed` if waiting to retry); running steps end as they physically ended. Failed beats cancelled. A step is dispatched only after its dependencies' completions are committed.
- **Rejected:** Locking a device only within a run (`NextSteps`), because two runs then collide with `ErrBusy`. Cancelling running steps, because ending `ctx` doesn't stop the work. Ordering steps by timestamps.
- **Consequence:** The record never shows a step as cancelled when it ran, and never marks a run terminal while work is still running. A command queued behind another run's job shows as `running`. Clock skew can only affect the evidence, never the order.

## ADR-4 Plate data: spool, then S3, then report

- **Decision:** After `Run` returns, the driver writes the data to its spool volume and fsyncs it. It then uploads to `runs/<run>/<step>/<n>.csv`, retrying until S3 accepts, and checks the sha256 against the ledger.
- Only then does the driver report. It deletes the spool file after the command is acked. If a redelivered job shows `completed` in the ledger but has no spool file, the attempt fails with "data lost", and the step retries.
- **Rejected:** Reporting before uploading, because the record would say completed with no data. Keeping the data only in memory, because a crash loses it. Sending the data through the executor, because it would add another hop and the data still has to reach S3.
- **Consequence:** A step stays `running` while S3 is down, and `data_ref` always points at a verified object. A crash in the few milliseconds between `Run` returning and the fsync costs one re-read; that attempt shows `failed` while the ledger shows `completed`.

## ADR-5 Instruments that cannot look up a job (not built)

- **Context:** ADR-2 rests on the instrument recording our `JobID` and letting the driver look it up. This one does. Real instruments often don't: they run a protocol and report nothing about it afterwards.
- **Decision, if we meet one:** the driver keeps its own write-ahead log. It appends "about to run `job_id`" and fsyncs before calling `Run`, and "ended, outcome X" after. On a redelivery it reads its log instead of the instrument's ledger.
- A log entry with a start and no end means the driver died during the job and nobody knows what happened physically. A retryable device treats it as hung: the attempt fails and retries. A non-retryable device (liquid handler) reports `unknown`: the step fails and is never run again without a person, because a second dispense on a part-dispensed plate is worse than a failed step.
- Where the device has any state to read (a plate barcode, a door sensor, a volume reading), the driver uses it to turn `unknown` into an outcome.
- **Rejected:** Running again on redelivery. That is at-least-once on the physical device, which the liquid handler can't afford.
- **Consequence:** At most once per attempt for non-retryable devices, at the cost of a person deciding the rare unknown. The record says `unknown` rather than guessing.

## ADR-6 Between dispatch and result: queued, on device, revoked, offline

- **Decision:** The driver publishes `started` on `lab.result.<device>.started` once the ledger has the job, so an attempt shows `dispatched` (queued) or `started` (on the device). On cancel, the executor puts queued attempts' job ids in KV `REVOKED`. The driver checks ledger → `REVOKED` → `Run`, and reports a revoked job as `revoked`, so its step becomes `cancelled`. Drivers send a status every 5s on core NATS. The executor keeps the last one in memory, logs an alert for attempts on a device unheard for 15s, and serves both through GraphQL `devices`.
- **Rejected:** Deleting a queued command from `CMDS` by sequence, because a driver may already hold it; the driver-side check is needed anyway. Storing liveness in Postgres, because it is about now and every 5s writes would buy nothing. Failing a step when its device goes quiet, because the device may still be working (ADR-2: the ledger decides).
- **Consequence:** The record tells queued from on the device, and cancel stops queued work before it runs, as a best effort that never makes the record false. An executor older than this reads `started` as a result and fails the step: the executor must be deployed first.

## Out of scope

HA executor, Temporal, graceful shutdown.
