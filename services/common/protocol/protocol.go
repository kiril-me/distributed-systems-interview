// Package protocol is what the executor and the drivers agree on: subjects,
// streams, consumers, message shapes and the job id (docs/decisions.md).
//
//	executor --(outbox)--> CMDS     lab.cmd.<device>     --> driver-<device>
//	executor <------------ RESULTS  lab.result.<device>  <-- driver
package protocol

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	CommandStream = "CMDS"
	ResultStream  = "RESULTS"

	// AckWait is how long a delivered message may go without an ack or a
	// heartbeat before JetStream redelivers it. Heartbeat is well inside it.
	AckWait   = 30 * time.Second
	Heartbeat = 10 * time.Second
)

// OutcomeCompleted is the instrument's word for success (instrument.OutcomeCompleted).
const OutcomeCompleted = "completed"

// OutcomeRevoked is the driver's word for a job it did not run because its
// run was cancelled while the command was queued. The ledger never has it.
const OutcomeRevoked = "revoked"

// RevokedBucket is a key-value bucket of job ids the executor no longer wants
// run. The driver checks it after the ledger and before Run.
const RevokedBucket = "REVOKED"

// StatusInterval is how often a driver says it is alive, on core NATS.
const StatusInterval = 5 * time.Second

func CommandSubject(device string) string { return "lab.cmd." + device }
func ResultSubject(device string) string  { return "lab.result." + device }
func StatusSubject(device string) string  { return "lab.status." + device }

// StartedSubject carries Started messages. It is in the result stream, so the
// executor reads a job's start before its result.
func StartedSubject(device string) string { return ResultSubject(device) + ".started" }

// IsStarted says whether a message on the result stream is a Started.
func IsStarted(subject string) bool { return strings.HasSuffix(subject, ".started") }

// Started says the instrument's ledger has the job: it is on the device, no
// longer queued. Best effort: the Result that follows is what decides.
type Started struct {
	JobID       string  `json:"job_id"`
	ExecutionID string  `json:"execution_id"`
	StartedAt   float64 `json:"started_at"`
}

// DeviceStatus is a driver's heartbeat: the instrument's own view, now.
// Nothing stores it; a missed one is replaced by the next.
type DeviceStatus struct {
	DeviceID     string `json:"device_id"`
	Busy         bool   `json:"busy"`
	CurrentJobID string `json:"current_job_id,omitempty"`
}

// Command asks a driver to run one attempt of one step.
type Command struct {
	JobID    string `json:"job_id"`
	RunID    string `json:"run_id"`
	Step     string `json:"step"`
	DeviceID string `json:"device_id"`
}

// Result is how one attempt ended, as the instrument's ledger has it.
// Outcome is the instrument's own word: completed, failed or hung. Error is
// set when the attempt did not succeed, including a completed reading whose
// data was lost. Times are the instrument's, in Unix seconds.
type Result struct {
	JobID       string  `json:"job_id"`
	ExecutionID string  `json:"execution_id"`
	Outcome     string  `json:"outcome"`
	Error       string  `json:"error,omitempty"`
	Retryable   bool    `json:"retryable"`
	StartedAt   float64 `json:"started_at"`
	FinishedAt  float64 `json:"finished_at"`
	DataRef     string  `json:"data_ref,omitempty"`
	DataSHA256  string  `json:"data_sha256,omitempty"`
}

// JobID names one attempt everywhere: Postgres, NATS message id, instrument
// JobID and S3 key. n counts from 1.
func JobID(runID, step string, n int) string {
	return fmt.Sprintf("%s/%s/%d", runID, step, n)
}

func ParseJobID(id string) (runID, step string, n int, err error) {
	parts := strings.Split(id, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return "", "", 0, fmt.Errorf("malformed job id %q", id)
	}
	n, err = strconv.Atoi(parts[2])
	if err != nil || n < 1 {
		return "", "", 0, fmt.Errorf("malformed job id %q", id)
	}
	return parts[0], parts[1], n, nil
}

// DataKey is where an attempt's data goes in S3. One key per attempt, so a
// retry never overwrites the data of an earlier one.
func DataKey(jobID string) string { return "runs/" + jobID + ".csv" }

// InstrumentTime turns the ledger's Unix seconds into a time, rounded to the
// microsecond (what Postgres keeps).
func InstrumentTime(sec float64) time.Time {
	whole, frac := math.Modf(sec)
	return time.Unix(int64(whole), int64(math.Round(frac*1e6))*1e3)
}

// EnsureStreams creates both streams, or leaves them as they are. Both sides
// call it, so whichever starts first sets them up.
func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	for name, subjects := range map[string]string{
		CommandStream: "lab.cmd.>",
		ResultStream:  "lab.result.>",
	} {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:      name,
			Subjects:  []string{subjects},
			Retention: jetstream.WorkQueuePolicy, // a message is gone once acked
			Storage:   jetstream.FileStorage,
		}); err != nil {
			return fmt.Errorf("stream %s: %w", name, err)
		}
	}
	if _, err := RevokedKV(ctx, js); err != nil {
		return err
	}
	return nil
}

// RevokedKV opens the revoked-jobs bucket, creating it if needed.
func RevokedKV(ctx context.Context, js jetstream.JetStream) (jetstream.KeyValue, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  RevokedBucket,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("bucket %s: %w", RevokedBucket, err)
	}
	return kv, nil
}

// CommandConsumer is one device's queue of commands. MaxAckPending 1 means
// the device has at most one command in hand, across all runs.
func CommandConsumer(ctx context.Context, js jetstream.JetStream, device string) (jetstream.Consumer, error) {
	return js.CreateOrUpdateConsumer(ctx, CommandStream, jetstream.ConsumerConfig{
		Durable:       "driver-" + device,
		FilterSubject: CommandSubject(device),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       AckWait,
		MaxAckPending: 1,
	})
}

// ResultConsumer is the executor's queue of results from every device.
func ResultConsumer(ctx context.Context, js jetstream.JetStream) (jetstream.Consumer, error) {
	return js.CreateOrUpdateConsumer(ctx, ResultStream, jetstream.ConsumerConfig{
		Durable:       "executor",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       AckWait,
		MaxAckPending: 1,
	})
}

// Receive hands a consumer's messages to handle, one at a time, until ctx
// ends.
//
// It keeps one iterator, and so one inbox, for its whole life. Every pull
// request it sends replies to that inbox, so a message delivered late, after a
// partition, still arrives. A Fetch per call abandons its inbox when the call
// returns, and a late delivery to it waits out AckWait before redelivery.
func Receive(ctx context.Context, cons jetstream.Consumer, handle func(jetstream.Msg)) {
	it, err := cons.Messages(jetstream.PullMaxMessages(1))
	for err != nil {
		log.Printf("receive: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		it, err = cons.Messages(jetstream.PullMaxMessages(1))
	}
	stop := context.AfterFunc(ctx, it.Stop) // Next does not take a context
	defer stop()

	for {
		msg, err := it.Next()
		if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			return
		}
		if err != nil {
			log.Printf("receive: %v", err)
			time.Sleep(time.Second)
			continue
		}
		handle(msg)
	}
}
