package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
	"github.com/automata-tech/distributed-systems-interview/services/driver/instrument"
)

// Serve is the driver side of the work (docs/decisions.md).
//
// It takes commands for this device one at a time from its JetStream
// consumer, runs each through the driver, publishes the result, and only then
// acks the command (ADR-1). It runs until ctx ends.
func Serve(ctx context.Context, inst *instrument.Instrument, nc *nats.Conn, st *storage.Storage) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	var cons jetstream.Consumer
	var kv jetstream.KeyValue
	for { // NATS may still be starting
		if err = protocol.EnsureStreams(ctx, js); err == nil {
			if cons, err = protocol.CommandConsumer(ctx, js, inst.DeviceID()); err == nil {
				if kv, err = protocol.RevokedKV(ctx, js); err == nil {
					break
				}
			}
		}
		log.Printf("driver %s: set up streams: %v", inst.DeviceID(), err)
		if !sleep(ctx, time.Second) {
			return ctx.Err()
		}
	}

	spool := os.Getenv("SPOOL_DIR")
	if spool == "" {
		spool = "/spool"
	}
	if inst.ProducesData() {
		if err := os.MkdirAll(spool, 0o755); err != nil {
			return err
		}
	}
	d := &driver{inst: inst, store: st, spool: spool, poll: 500 * time.Millisecond}
	d.started = func(ctx context.Context, s protocol.Started) { announce(ctx, js, inst.DeviceID(), s) }
	d.revoked = func(ctx context.Context, jobID string) (bool, error) {
		_, err := kv.Get(ctx, jobID)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	}
	log.Printf("driver %s: serving %s", inst.DeviceID(), protocol.CommandSubject(inst.DeviceID()))
	go sayAlive(ctx, nc, inst)

	protocol.Receive(ctx, cons, func(msg jetstream.Msg) { d.handle(ctx, js, msg) })
	return ctx.Err()
}

// handle runs one command to the end. The command is acked only after its
// result is stored in JetStream; until then a crash brings it back, and the
// ledger check stops it running twice.
func (d *driver) handle(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg) {
	var cmd protocol.Command
	if err := json.Unmarshal(msg.Data(), &cmd); err != nil {
		log.Printf("driver: dropping unreadable command: %v", err)
		_ = msg.Term()
		return
	}
	stop := heartbeat(ctx, msg)
	defer stop()

	res, err := d.execute(ctx, cmd)
	if err != nil {
		log.Printf("driver: %s not decided: %v (will come back)", cmd.JobID, err)
		_ = msg.NakWithDelay(2 * time.Second)
		return
	}
	if err := d.report(ctx, js, res); err != nil {
		return // shutting down; the command comes back
	}
	if err := msg.Ack(); err != nil {
		log.Printf("driver: ack %s: %v (a redelivery will report it again)", cmd.JobID, err)
		return
	}
	d.removeSpool(cmd.JobID)
	log.Printf("driver: %s reported %s", cmd.JobID, res.Outcome)
}

// report publishes the result, retrying until JetStream has stored it.
func (d *driver) report(ctx context.Context, js jetstream.JetStream, res protocol.Result) error {
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}
	for {
		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := js.Publish(pubCtx, protocol.ResultSubject(d.inst.DeviceID()), data,
			jetstream.WithMsgID(res.JobID+":result"))
		cancel()
		if err == nil {
			return nil
		}
		log.Printf("driver: report %s: %v (retrying)", res.JobID, err)
		if !sleep(ctx, time.Second) {
			return ctx.Err()
		}
	}
}

// sayAlive publishes the instrument's status on core NATS every
// StatusInterval until ctx ends. Nothing stores it: a lost one is replaced by
// the next, and silence is what the executor watches for.
func sayAlive(ctx context.Context, nc *nats.Conn, inst *instrument.Instrument) {
	tick := time.NewTicker(protocol.StatusInterval)
	defer tick.Stop()
	for {
		st := inst.Status()
		data, _ := json.Marshal(protocol.DeviceStatus{DeviceID: inst.DeviceID(), Busy: st.Busy, CurrentJobID: st.CurrentJobID})
		_ = nc.Publish(protocol.StatusSubject(inst.DeviceID()), data)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// announce publishes that a job is on the device, once, without retrying: if
// it is lost, the step shows as queued until its result arrives.
func announce(ctx context.Context, js jetstream.JetStream, device string, s protocol.Started) {
	data, err := json.Marshal(s)
	if err != nil {
		return
	}
	// Run may return, and end ctx, while this is on its way.
	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := js.Publish(pubCtx, protocol.StartedSubject(device), data,
		jetstream.WithMsgID(s.JobID+":started")); err != nil {
		log.Printf("driver: announce %s: %v (the result will still come)", s.JobID, err)
	}
}

// heartbeat tells JetStream the command is still being worked on, so it is
// not redelivered while a job runs, hangs, or waits for S3.
func heartbeat(ctx context.Context, msg jetstream.Msg) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		tick := time.NewTicker(protocol.Heartbeat)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				_ = msg.InProgress()
			}
		}
	}()
	return cancel
}

// sleep waits for d and reports whether ctx is still alive.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
