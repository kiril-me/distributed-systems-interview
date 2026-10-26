package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
	"github.com/automata-tech/distributed-systems-interview/services/driver/instrument"
)

// putter is the part of object storage the driver uses.
type putter interface {
	Put(ctx context.Context, key string, data []byte) (string, error)
}

// driver turns one command into one result. The ledger decides (ADR-2): a job
// already in it is reported, never run again, and every reported field comes
// from the ledger.
type driver struct {
	inst  *instrument.Instrument
	store putter
	spool string        // plate data waits here between Run and S3 (ADR-4)
	poll  time.Duration // how often to read the ledger while waiting

	// started tells the executor a job is on the device. Optional.
	started func(context.Context, protocol.Started)
	// revoked says whether the executor no longer wants a job run. Optional.
	revoked func(ctx context.Context, jobID string) (bool, error)
}

// execute runs the command's job at most once and returns how it ended.
// An error means nothing was decided; the command should come back later.
func (d *driver) execute(ctx context.Context, cmd protocol.Command) (protocol.Result, error) {
	if _, found := d.find(cmd.JobID); !found {
		// Not run yet: the run may have been cancelled while this was queued.
		if d.revoked != nil {
			revoked, err := d.revoked(ctx, cmd.JobID)
			if err != nil {
				return protocol.Result{}, fmt.Errorf("revoked list: %w", err)
			}
			if revoked {
				return protocol.Result{JobID: cmd.JobID, Outcome: protocol.OutcomeRevoked,
					Error: "run cancelled before the job started", Retryable: d.inst.FailuresRetryable()}, nil
			}
		}
		if err := d.run(ctx, cmd); err != nil {
			return protocol.Result{}, err
		}
	}

	exec, err := d.waitEnded(ctx, cmd.JobID)
	if err != nil {
		return protocol.Result{}, err
	}
	res := protocol.Result{
		JobID:       cmd.JobID,
		ExecutionID: exec.ExecutionID,
		Outcome:     exec.Outcome,
		Error:       exec.Error,
		Retryable:   d.inst.FailuresRetryable(),
		StartedAt:   exec.StartedAt,
		FinishedAt:  *exec.FinishedAt,
	}
	if exec.Outcome == instrument.OutcomeCompleted && d.inst.ProducesData() {
		return d.storeData(ctx, res, exec)
	}
	return res, nil
}

// run starts the job and waits for Run to return. Run is never given up on
// for time alone: a slow job may still complete and hand back data. It is
// cancelled only when the ledger says the job hung, because a hung job never
// returns from Run.
func (d *driver) run(ctx context.Context, cmd protocol.Command) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go d.watch(runCtx, cancel, cmd.JobID)

	out, err := d.inst.Run(runCtx, instrument.Job{JobID: cmd.JobID, RunID: cmd.RunID, StepName: cmd.Step})
	switch {
	case err != nil && runCtx.Err() != nil && ctx.Err() == nil:
		return nil // hung; waitEnded reads it from the ledger
	case err != nil:
		return err // busy, ledger unwritable, or shutting down: nothing to report
	case out.Data != nil:
		return d.writeSpool(cmd.JobID, out.Data)
	}
	return nil
}

// watch reads the ledger while Run waits. It announces the job once the ledger
// has it, and cancels the wait once the job is recorded as hung.
func (d *driver) watch(ctx context.Context, cancel context.CancelFunc, jobID string) {
	announced := false
	for {
		exec, found := d.find(jobID)
		if found && !announced && d.started != nil {
			d.started(ctx, protocol.Started{JobID: jobID, ExecutionID: exec.ExecutionID, StartedAt: exec.StartedAt})
			announced = true
		}
		if found && exec.Outcome == instrument.OutcomeHung {
			cancel()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.poll):
		}
	}
}

// find looks the job up in the ledger.
func (d *driver) find(jobID string) (instrument.Execution, bool) {
	for _, e := range d.inst.Status().Executions {
		if e.JobID == jobID {
			return e, true
		}
	}
	return instrument.Execution{}, false
}

// waitEnded polls the ledger until the job is no longer running.
func (d *driver) waitEnded(ctx context.Context, jobID string) (instrument.Execution, error) {
	for {
		exec, found := d.find(jobID)
		if !found {
			return exec, fmt.Errorf("job %s is not in the ledger", jobID)
		}
		if exec.Outcome != instrument.OutcomeRunning && exec.FinishedAt != nil {
			return exec, nil
		}
		select {
		case <-ctx.Done():
			return exec, ctx.Err()
		case <-time.After(d.poll):
		}
	}
}

// storeData puts the spooled data in S3, retrying until it is stored, and checks
// it against the ledger. Without a spool the data is gone: the attempt is
// reported as failed with the ledger's completed outcome.
func (d *driver) storeData(ctx context.Context, res protocol.Result, exec instrument.Execution) (protocol.Result, error) {
	data, err := os.ReadFile(d.spoolPath(res.JobID))
	if errors.Is(err, os.ErrNotExist) {
		res.Error = "data lost after completion"
		return res, nil
	}
	if err != nil {
		return res, err
	}

	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if exec.DataSHA256 == nil || digest != *exec.DataSHA256 {
		res.Error = "spooled data does not match the ledger"
		return res, nil
	}
	for {
		ref, err := d.store.Put(ctx, protocol.DataKey(res.JobID), data)
		if err == nil {
			res.DataRef, res.DataSHA256 = ref, digest
			return res, nil
		}
		log.Printf("driver: store %s: %v (retrying)", res.JobID, err)
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(d.poll):
		}
	}
}

// writeSpool writes the data and syncs it, so it survives a crash.
func (d *driver) writeSpool(jobID string, data []byte) error {
	tmp := d.spoolPath(jobID) + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("spool: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("spool: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("spool: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("spool: %w", err)
	}
	return os.Rename(tmp, d.spoolPath(jobID))
}

func (d *driver) removeSpool(jobID string) {
	if err := os.Remove(d.spoolPath(jobID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("driver: remove spool %s: %v", jobID, err)
	}
}

func (d *driver) spoolPath(jobID string) string {
	return filepath.Join(d.spool, strings.ReplaceAll(jobID, "/", "_")+".csv")
}
