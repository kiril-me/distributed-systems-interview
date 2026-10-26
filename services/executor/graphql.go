package main

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/graph-gophers/graphql-go"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// The GraphQL view of the run record (schema.graphql). Every query reads one
// snapshot, so a run, its steps and their attempts always belong together.

//go:embed schema.graphql
var schemaSDL string

// NewGraphQLSchema parses the schema against the resolvers below.
func NewGraphQLSchema(e *Executor, workflowsPath string) (*graphql.Schema, error) {
	return graphql.ParseSchema(schemaSDL, &gqlRoot{e: e, workflowsPath: workflowsPath})
}

// gqlError puts a code in the error's extensions: NOT_FOUND, INVALID_STATE or
// BAD_INPUT.
type gqlError struct{ code, msg string }

func (e *gqlError) Error() string              { return e.msg }
func (e *gqlError) Extensions() map[string]any { return map[string]any{"code": e.code} }

// invalidState also says what state the run is in, so a client retrying a
// mutation can tell "already done" from a real refusal.
type invalidState struct {
	gqlError
	status string
}

func (e *invalidState) Extensions() map[string]any {
	return map[string]any{"code": e.code, "status": strings.ToUpper(e.status)}
}

func newInvalidState(status, format string, args ...any) error {
	return &invalidState{gqlError{"INVALID_STATE", fmt.Sprintf(format, args...)}, status}
}

func notFound(format string, args ...any) error {
	return &gqlError{"NOT_FOUND", fmt.Sprintf(format, args...)}
}

// runRecord is one run as a single snapshot: what the HTTP view shows, plus
// what only GraphQL shows.
type runRecord struct {
	RunView
	cancelRequested bool
	attempts        map[string][]attemptRecord // by step, oldest first
}

type attemptRecord struct {
	n                                           int
	jobID, runID, step, device, status          string
	outcome, executionID, err, dataRef, dataSHA *string
	startedAt, finishedAt                       *time.Time
	dispatchedAt                                time.Time
}

func readRecord(ctx context.Context, tx *sql.Tx, runID string) (*runRecord, error) {
	rv, err := readRun(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	rec := &runRecord{RunView: rv, attempts: map[string][]attemptRecord{}}
	if err := tx.QueryRowContext(ctx,
		`SELECT cancel_requested FROM runs WHERE id = $1`, runID).Scan(&rec.cancelRequested); err != nil {
		return nil, err
	}
	attempts, err := readAttempts(ctx, tx, `WHERE run_id = $1 ORDER BY step, n`, runID)
	if err != nil {
		return nil, err
	}
	for _, a := range attempts {
		rec.attempts[a.step] = append(rec.attempts[a.step], a)
	}
	return rec, nil
}

// readAttempts reads attempts, filtered and ordered by where.
func readAttempts(ctx context.Context, tx *sql.Tx, where string, args ...any) ([]attemptRecord, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT run_id, step, device_id, n, job_id, status, outcome, execution_id, error, data_ref, data_sha256,
		        started_at, finished_at, created_at
		   FROM attempts `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []attemptRecord
	for rows.Next() {
		var a attemptRecord
		if err := rows.Scan(&a.runID, &a.step, &a.device, &a.n, &a.jobID, &a.status, &a.outcome, &a.executionID, &a.err,
			&a.dataRef, &a.dataSHA, &a.startedAt, &a.finishedAt, &a.dispatchedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

type gqlRoot struct {
	e             *Executor
	workflowsPath string
}

// record reads one run; ErrRunNotFound when there is none.
func (r *gqlRoot) record(ctx context.Context, id string) (*runRecord, error) {
	var rec *runRecord
	err := r.e.snapshot(ctx, func(tx *sql.Tx) (err error) {
		rec, err = readRecord(ctx, tx, id)
		return err
	})
	return rec, err
}

func (r *gqlRoot) Run(ctx context.Context, args struct{ ID graphql.ID }) (*runResolver, error) {
	rec, err := r.record(ctx, string(args.ID))
	if errors.Is(err, ErrRunNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &runResolver{rec}, nil
}

// Runs pages newest first. The cursor is a run id; the next page holds the
// runs created before it (id breaks ties).
func (r *gqlRoot) Runs(ctx context.Context, args struct {
	Status *string
	First  int32
	After  *graphql.ID
}) (*connectionResolver, error) {
	if args.First < 1 || args.First > 200 {
		return nil, &gqlError{"BAD_INPUT", "first must be between 1 and 200"}
	}
	status, after := "", ""
	if args.Status != nil {
		status = strings.ToLower(*args.Status)
	}
	if args.After != nil {
		after = string(*args.After)
	}

	conn := &connectionResolver{}
	err := r.e.snapshot(ctx, func(tx *sql.Tx) error {
		if after != "" {
			var exists bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE id = $1)`, after).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return notFound("no run %q to page after", after)
			}
		}
		rows, err := tx.QueryContext(ctx,
			`SELECT id FROM runs
			  WHERE ($1 = '' OR status = $1)
			    AND ($2 = '' OR (created_at, id) < (SELECT created_at, id FROM runs WHERE id = $2))
			  ORDER BY created_at DESC, id DESC
			  LIMIT $3`, status, after, args.First+1)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		if len(ids) > int(args.First) {
			ids, conn.hasNext = ids[:args.First], true
		}
		for _, id := range ids {
			rec, err := readRecord(ctx, tx, id)
			if err != nil {
				return err
			}
			conn.nodes = append(conn.nodes, &runResolver{rec})
		}
		if len(ids) > 0 {
			end := graphql.ID(ids[len(ids)-1])
			conn.end = &end
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (r *gqlRoot) Workflows() ([]*workflowResolver, error) {
	set, err := LoadWorkflows(r.workflowsPath)
	if err != nil {
		return nil, err
	}
	var out []*workflowResolver
	for _, name := range set.Names() {
		steps, err := set.Get(name)
		if err != nil {
			return nil, err
		}
		out = append(out, &workflowResolver{name: name, isDefault: name == set.Default(), steps: steps})
	}
	return out, nil
}

// Devices joins two views of each device: what its driver last said (memory)
// and the open attempts on it (one snapshot of the record).
func (r *gqlRoot) Devices(ctx context.Context) ([]*deviceResolver, error) {
	byID := map[string]*deviceResolver{}
	device := func(id string) *deviceResolver {
		if byID[id] == nil {
			byID[id] = &deviceResolver{id: id}
		}
		return byID[id]
	}

	if set, err := LoadWorkflows(r.workflowsPath); err == nil {
		for _, name := range set.Names() {
			steps, _ := set.Get(name)
			for _, s := range steps {
				device(s.DeviceID)
			}
		}
	}
	seen, _ := r.e.seen()
	now := time.Now()
	for id, s := range seen {
		d := device(id)
		at := s.at
		d.lastSeen, d.online, d.status = &at, now.Sub(at) <= DeviceTimeout, s.status
	}

	err := r.e.snapshot(ctx, func(tx *sql.Tx) error {
		open, err := readAttempts(ctx, tx, `WHERE status IN ($1, $2) ORDER BY created_at, job_id`,
			AttemptDispatched, AttemptStarted)
		if err != nil {
			return err
		}
		for _, a := range open {
			d := device(a.device)
			if a.status == AttemptStarted {
				d.current = append(d.current, &attemptResolver{a})
			} else {
				d.queued = append(d.queued, &attemptResolver{a})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out := make([]*deviceResolver, 0, len(byID))
	for _, d := range byID {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out, nil
}

type deviceResolver struct {
	id              string
	online          bool
	lastSeen        *time.Time
	status          protocol.DeviceStatus
	current, queued []*attemptResolver
}

func (d *deviceResolver) ID() graphql.ID            { return graphql.ID(d.id) }
func (d *deviceResolver) Online() bool              { return d.online }
func (d *deviceResolver) LastSeenAt() *graphql.Time { return gqlTime(d.lastSeen) }
func (d *deviceResolver) Busy() bool                { return d.status.Busy }
func (d *deviceResolver) InstrumentJobID() *string {
	if d.status.CurrentJobID == "" {
		return nil
	}
	return &d.status.CurrentJobID
}
func (d *deviceResolver) Current() []*attemptResolver { return nonNilAttempts(d.current) }
func (d *deviceResolver) Queued() []*attemptResolver  { return nonNilAttempts(d.queued) }

func nonNilAttempts(a []*attemptResolver) []*attemptResolver {
	if a == nil {
		return []*attemptResolver{}
	}
	return a
}

func (r *gqlRoot) CreateRun(ctx context.Context, args struct {
	WorkflowName   *string
	IdempotencyKey *string
}) (*runResolver, error) {
	set, err := LoadWorkflows(r.workflowsPath)
	if err != nil {
		return nil, err
	}
	name := set.Default()
	if args.WorkflowName != nil {
		name = *args.WorkflowName
	}
	steps, err := set.Get(name)
	if err != nil {
		return nil, notFound("no workflow %q", name)
	}
	key := ""
	if args.IdempotencyKey != nil {
		if key = *args.IdempotencyKey; key == "" {
			return nil, &gqlError{"BAD_INPUT", "idempotencyKey must not be empty"}
		}
	}
	rv, err := r.e.createRun(ctx, name, steps, key)
	if errors.Is(err, errKeyReused) {
		return nil, &gqlError{"BAD_INPUT", err.Error()}
	}
	if err != nil {
		return nil, err
	}
	return r.reload(ctx, rv.ID)
}

// StartRun and CancelRun act first and explain after: the check and the change
// are one statement or one locked transaction, so of two racing calls only
// the one that changed the run succeeds.
func (r *gqlRoot) StartRun(ctx context.Context, args struct{ ID graphql.ID }) (*runResolver, error) {
	started, err := r.e.startRun(ctx, string(args.ID))
	if err != nil {
		return nil, err
	}
	if !started {
		rec, err := r.existing(ctx, args.ID)
		if err != nil {
			return nil, err
		}
		return nil, newInvalidState(rec.Status, "run %s is %s, only a pending run can start", rec.ID, rec.Status)
	}
	return r.reload(ctx, string(args.ID))
}

func (r *gqlRoot) CancelRun(ctx context.Context, args struct{ ID graphql.ID }) (*runResolver, error) {
	cancelled, err := r.e.cancelRun(ctx, string(args.ID))
	if errors.Is(err, ErrRunNotFound) {
		return nil, notFound("no run %q", args.ID)
	}
	if err != nil {
		return nil, err
	}
	if !cancelled {
		rec, err := r.existing(ctx, args.ID)
		if err != nil {
			return nil, err
		}
		if rec.cancelRequested && rec.Status == RunRunning {
			return nil, newInvalidState(rec.Status, "run %s is already cancelling", rec.ID)
		}
		return nil, newInvalidState(rec.Status, "run %s is already %s", rec.ID, rec.Status)
	}
	return r.reload(ctx, string(args.ID))
}

// existing reads a run a mutation names, or fails with NOT_FOUND.
func (r *gqlRoot) existing(ctx context.Context, id graphql.ID) (*runRecord, error) {
	rec, err := r.record(ctx, string(id))
	if errors.Is(err, ErrRunNotFound) {
		return nil, notFound("no run %q", id)
	}
	return rec, err
}

// reload reads the run again after a mutation, so the answer shows its effect.
func (r *gqlRoot) reload(ctx context.Context, id string) (*runResolver, error) {
	rec, err := r.record(ctx, id)
	if err != nil {
		return nil, err
	}
	return &runResolver{rec}, nil
}

type connectionResolver struct {
	nodes   []*runResolver
	end     *graphql.ID
	hasNext bool
}

func (c *connectionResolver) Nodes() []*runResolver {
	if c.nodes == nil {
		return []*runResolver{}
	}
	return c.nodes
}
func (c *connectionResolver) PageInfo() *pageInfoResolver { return &pageInfoResolver{c.end, c.hasNext} }

type pageInfoResolver struct {
	end     *graphql.ID
	hasNext bool
}

func (p *pageInfoResolver) EndCursor() *graphql.ID { return p.end }
func (p *pageInfoResolver) HasNextPage() bool      { return p.hasNext }

type runResolver struct{ rec *runRecord }

func (r *runResolver) ID() graphql.ID            { return graphql.ID(r.rec.ID) }
func (r *runResolver) WorkflowName() string      { return r.rec.WorkflowName }
func (r *runResolver) Status() string            { return strings.ToUpper(r.rec.Status) }
func (r *runResolver) CancelRequested() bool     { return r.rec.cancelRequested }
func (r *runResolver) CreatedAt() graphql.Time   { return graphql.Time{Time: r.rec.CreatedAt} }
func (r *runResolver) StartedAt() *graphql.Time  { return gqlTime(r.rec.StartedAt) }
func (r *runResolver) FinishedAt() *graphql.Time { return gqlTime(r.rec.FinishedAt) }

func (r *runResolver) Steps() []*stepResolver {
	out := make([]*stepResolver, len(r.rec.Steps))
	for i, s := range r.rec.Steps {
		out[i] = &stepResolver{r.rec, s}
	}
	return out
}

type stepResolver struct {
	rec *runRecord
	s   StepView
}

func (s *stepResolver) Name() string              { return s.s.Name }
func (s *stepResolver) DeviceID() string          { return s.s.DeviceID }
func (s *stepResolver) DependsOn() []string       { return nonNil(s.s.DependsOn) }
func (s *stepResolver) Status() string            { return strings.ToUpper(s.s.Status) }
func (s *stepResolver) StartedAt() *graphql.Time  { return gqlTime(s.s.StartedAt) }
func (s *stepResolver) FinishedAt() *graphql.Time { return gqlTime(s.s.FinishedAt) }
func (s *stepResolver) Error() *string            { return s.s.Error }
func (s *stepResolver) DataRef() *string          { return s.s.DataRef }

// Dependencies are the steps named in dependsOn, from the same snapshot.
func (s *stepResolver) Dependencies() []*stepResolver {
	out := []*stepResolver{}
	for _, dep := range s.s.DependsOn {
		for _, other := range s.rec.Steps {
			if other.Name == dep {
				out = append(out, &stepResolver{s.rec, other})
			}
		}
	}
	return out
}

func (s *stepResolver) Attempts() []*attemptResolver {
	out := []*attemptResolver{}
	for _, a := range s.rec.attempts[s.s.Name] {
		out = append(out, &attemptResolver{a})
	}
	return out
}

type attemptResolver struct{ a attemptRecord }

func (a *attemptResolver) N() int32                  { return int32(a.a.n) }
func (a *attemptResolver) JobID() graphql.ID         { return graphql.ID(a.a.jobID) }
func (a *attemptResolver) RunID() graphql.ID         { return graphql.ID(a.a.runID) }
func (a *attemptResolver) Step() string              { return a.a.step }
func (a *attemptResolver) Status() string            { return strings.ToUpper(a.a.status) }
func (a *attemptResolver) ExecutionID() *string      { return a.a.executionID }
func (a *attemptResolver) StartedAt() *graphql.Time  { return gqlTime(a.a.startedAt) }
func (a *attemptResolver) FinishedAt() *graphql.Time { return gqlTime(a.a.finishedAt) }
func (a *attemptResolver) Error() *string            { return a.a.err }
func (a *attemptResolver) DataRef() *string          { return a.a.dataRef }
func (a *attemptResolver) DataSha256() *string       { return a.a.dataSHA }
func (a *attemptResolver) DispatchedAt() graphql.Time {
	return graphql.Time{Time: a.a.dispatchedAt}
}

func (a *attemptResolver) Outcome() *string {
	if a.a.outcome == nil {
		return nil
	}
	o := strings.ToUpper(*a.a.outcome)
	return &o
}

type workflowResolver struct {
	name      string
	isDefault bool
	steps     []StepTemplate
}

func (w *workflowResolver) Name() string    { return w.name }
func (w *workflowResolver) IsDefault() bool { return w.isDefault }
func (w *workflowResolver) Steps() []*workflowStepResolver {
	out := make([]*workflowStepResolver, len(w.steps))
	for i, s := range w.steps {
		out[i] = &workflowStepResolver{s}
	}
	return out
}

type workflowStepResolver struct{ s StepTemplate }

func (w *workflowStepResolver) Name() string        { return w.s.Name }
func (w *workflowStepResolver) DeviceID() string    { return w.s.DeviceID }
func (w *workflowStepResolver) DependsOn() []string { return nonNil(w.s.DependsOn) }

func gqlTime(t *time.Time) *graphql.Time {
	if t == nil {
		return nil
	}
	return &graphql.Time{Time: *t}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
