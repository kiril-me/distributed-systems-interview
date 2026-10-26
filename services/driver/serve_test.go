package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/natstest"
	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
	"github.com/automata-tech/distributed-systems-interview/services/driver/instrument"
)

// nextResult plays the executor: take one result and ack it. Started
// messages on the way are acked and skipped.
func nextResult(t *testing.T, cons jetstream.Consumer, within time.Duration) protocol.Result {
	t.Helper()
	msg, err := cons.Next(jetstream.FetchMaxWait(within))
	if err != nil {
		t.Fatalf("no result: %v", err)
	}
	if protocol.IsStarted(msg.Subject()) {
		_ = msg.Ack()
		return nextResult(t, cons, within)
	}
	var res protocol.Result
	if err := json.Unmarshal(msg.Data(), &res); err != nil {
		t.Fatal(err)
	}
	if err := msg.Ack(); err != nil {
		t.Fatal(err)
	}
	return res
}

func TestServeOverJetStream(t *testing.T) {
	nc := natstest.Connect(t, natstest.Start(t))
	inst := newInstrument(t, t.TempDir(), instrument.Config{FailuresRetryable: false})
	st, err := storage.New("http://unused:8333", "lab-data") // no data device, never called
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- Serve(ctx, inst, nc, st) }()
	t.Cleanup(func() { cancel(); <-served })

	js, _ := jetstream.New(nc)
	if err := protocol.EnsureStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	results, err := protocol.ResultConsumer(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	cmd := protocol.Command{JobID: "run-1/fill/1", RunID: "run-1", Step: "fill", DeviceID: "dev-1"}
	data, _ := json.Marshal(cmd)
	publish := func(msgID string) {
		if _, err := js.Publish(ctx, protocol.CommandSubject("dev-1"), data, jetstream.WithMsgID(msgID)); err != nil {
			t.Fatal(err)
		}
	}

	publish(cmd.JobID)
	first := nextResult(t, results, 5*time.Second)
	if first.JobID != cmd.JobID || first.Outcome != "completed" || first.Retryable {
		t.Fatalf("unexpected result %+v", first)
	}

	// The outbox sends the same command again after the dedupe window. The
	// driver reports it again (JetStream drops that result as a duplicate of
	// the first) and acks it, without running the job a second time.
	publish(cmd.JobID + "-again")
	waitEmpty(t, js, protocol.CommandStream)
	if n := len(executions(inst, cmd.JobID)); n != 1 {
		t.Fatalf("a repeated command ran again: %d executions", n)
	}
}

// waitEmpty waits until every message in the stream has been acked.
func waitEmpty(t *testing.T, js jetstream.JetStream, name string) {
	t.Helper()
	stream, err := js.Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		info, err := stream.Info(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 0 {
			return
		}
	}
	t.Fatalf("stream %s still has unacked messages", name)
}

// A command published while the driver is cut off must reach it as soon as
// the network is back, not after AckWait (30s) when JetStream redelivers.
func TestCommandSentDuringPartitionArrivesAfterThaw(t *testing.T) {
	url := natstest.Start(t)
	proxy := natstest.NewProxy(t, url)
	inst := newInstrument(t, t.TempDir(), instrument.Config{FailuresRetryable: true})
	st, err := storage.New("http://unused:8333", "lab-data")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- Serve(ctx, inst, natstest.Connect(t, proxy.URL), st) }()
	t.Cleanup(func() { cancel(); <-served })

	js, _ := jetstream.New(natstest.Connect(t, url))
	if err := protocol.EnsureStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	results, err := protocol.ResultConsumer(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, js, "driver-dev-1")

	proxy.Freeze()
	cmd := protocol.Command{JobID: "run-1/incubate/1", RunID: "run-1", Step: "incubate", DeviceID: "dev-1"}
	data, _ := json.Marshal(cmd)
	if _, err := js.Publish(ctx, protocol.CommandSubject("dev-1"), data, jetstream.WithMsgID(cmd.JobID)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	proxy.Thaw()

	start := time.Now()
	res := nextResult(t, results, 10*time.Second) // a 30s stall fails here
	if res.JobID != cmd.JobID || time.Since(start) > 5*time.Second {
		t.Fatalf("result %+v after %v", res, time.Since(start))
	}
}

// waitIdle waits until the driver is waiting on its consumer for a command.
func waitIdle(t *testing.T, js jetstream.JetStream, consumer string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		cons, err := js.Consumer(context.Background(), protocol.CommandStream, consumer)
		if err != nil {
			continue
		}
		if info, err := cons.Info(context.Background()); err == nil && info.NumWaiting > 0 {
			return
		}
	}
	t.Fatal("driver never waited for a command")
}

// A driver says it is alive as soon as it serves, and keeps saying it, with
// the instrument's own view of what it is doing.
func TestServeSaysItIsAlive(t *testing.T) {
	nc := natstest.Connect(t, natstest.Start(t))
	statuses, err := nc.SubscribeSync(protocol.StatusSubject("dev-1"))
	if err != nil {
		t.Fatal(err)
	}
	inst := newInstrument(t, t.TempDir(), instrument.Config{})
	st, err := storage.New("http://unused:8333", "lab-data")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- Serve(ctx, inst, nc, st) }()
	t.Cleanup(func() { cancel(); <-served })

	msg, err := statuses.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no status: %v", err)
	}
	var s protocol.DeviceStatus
	if err := json.Unmarshal(msg.Data, &s); err != nil || s.DeviceID != "dev-1" || s.Busy {
		t.Fatalf("status %+v, %v", s, err)
	}
}
