package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/graph-gophers/graphql-go"

	"github.com/automata-tech/distributed-systems-interview/services/common/protocol"
)

// ParseSchema checks every field of the schema against a resolver method with
// a matching name, arguments and return type.
func TestSchemaMatchesResolvers(t *testing.T) {
	if _, err := graphql.ParseSchema(schemaSDL, &gqlRoot{}); err != nil {
		t.Fatal(err)
	}
}

// query runs one GraphQL request and returns its data and error codes.
func query(t *testing.T, schema *graphql.Schema, q string) (map[string]any, []string) {
	t.Helper()
	resp := schema.Exec(context.Background(), q, "", nil)
	var codes []string
	for _, err := range resp.Errors {
		code, _ := err.Extensions["code"].(string)
		codes = append(codes, code)
	}
	var data map[string]any
	if len(resp.Data) > 0 {
		if err := json.Unmarshal(resp.Data, &data); err != nil {
			t.Fatal(err)
		}
	}
	return data, codes
}

func newSchema(t *testing.T, e *Executor) *graphql.Schema {
	t.Helper()
	schema, err := NewGraphQLSchema(e, "../../config/workflows.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

// Of many startRun or cancelRun calls racing on one pending run, exactly one
// changes it; the others get INVALID_STATE, not a success they did not cause.
func TestGraphQLConcurrentMutationsOnlyOneWins(t *testing.T) {
	e, _ := startExecutor(t)
	schema := newSchema(t, e)
	for _, mutation := range []string{"startRun", "cancelRun"} {
		rv, err := e.CreateRun(context.Background(), "test", workflow)
		if err != nil {
			t.Fatal(err)
		}
		const callers = 10
		codes := make(chan []string, callers)
		var wg sync.WaitGroup
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, c := query(t, schema, `mutation { `+mutation+`(id: "`+rv.ID+`") { status } }`)
				codes <- c
			}()
		}
		wg.Wait()
		close(codes)
		won := 0
		for c := range codes {
			switch {
			case c == nil:
				won++
			case len(c) != 1 || c[0] != "INVALID_STATE":
				t.Errorf("%s: codes %v", mutation, c)
			}
		}
		if won != 1 {
			t.Errorf("%s: %d of %d callers succeeded, want 1", mutation, won, callers)
		}
	}
}

// devices shows each instrument's liveness, from its driver's status, and
// its work, from the record: what is on it and what is queued for it.
func TestGraphQLDevicesShowLivenessAndQueue(t *testing.T) {
	e, js := startExecutor(t)
	schema := newSchema(t, e)
	status, _ := json.Marshal(protocol.DeviceStatus{DeviceID: "lh", Busy: true, CurrentJobID: "other/x/1"})
	if err := e.nc.Publish(protocol.StatusSubject("lh"), status); err != nil {
		t.Fatal(err)
	}
	rv, err := e.CreateRun(context.Background(), "test", []StepTemplate{{Name: "a", DeviceID: "lh"}, {Name: "b", DeviceID: "inc"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.StartRun(context.Background(), rv.ID); err != nil {
		t.Fatal(err)
	}
	jobA := protocol.JobID(rv.ID, "a", 1)

	devices := func() map[string]map[string]any {
		data, codes := query(t, schema, `{ devices { id online lastSeenAt busy instrumentJobId
			current { jobId runId step } queued { jobId runId step status } } }`)
		if codes != nil {
			t.Fatalf("codes %v", codes)
		}
		out := map[string]map[string]any{}
		for _, d := range data["devices"].([]any) {
			out[d.(map[string]any)["id"].(string)] = d.(map[string]any)
		}
		return out
	}
	queuedJobs := func(d map[string]any) (jobs []string) {
		for _, a := range d["queued"].([]any) {
			jobs = append(jobs, a.(map[string]any)["jobId"].(string))
		}
		return jobs
	}
	waitFor(t, "a and b queued", func() bool {
		d := devices()
		return d["lh"] != nil && d["inc"] != nil && len(queuedJobs(d["lh"])) == 1 && len(queuedJobs(d["inc"])) == 1
	})
	d := devices()
	if lh := d["lh"]; lh["online"] != true || lh["busy"] != true || lh["instrumentJobId"] != "other/x/1" || lh["lastSeenAt"] == nil ||
		queuedJobs(lh)[0] != jobA || lh["queued"].([]any)[0].(map[string]any)["step"] != "a" {
		t.Fatalf("lh: %v", lh)
	}
	if inc := d["inc"]; inc["online"] != false || inc["lastSeenAt"] != nil {
		t.Fatalf("inc was never heard from: %v", inc)
	}

	data, _ := json.Marshal(protocol.Started{JobID: jobA, ExecutionID: "x-a", StartedAt: 1_700_000_000})
	if _, err := js.Publish(context.Background(), protocol.StartedSubject("lh"), data); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a on lh", func() bool {
		lh := devices()["lh"]
		cur := lh["current"].([]any)
		return len(cur) == 1 && cur[0].(map[string]any)["jobId"] == jobA && len(queuedJobs(lh)) == 0
	})
}

// A tool that retries createRun with the same key gets the same run, not a
// second one. A retried startRun says what state the run is in.
func TestGraphQLCreateRunIsIdempotent(t *testing.T) {
	e, _ := startExecutor(t)
	schema := newSchema(t, e)
	create := func(args string) (string, []string) {
		data, codes := query(t, schema, `mutation { createRun(`+args+`) { id } }`)
		if codes != nil {
			return "", codes
		}
		return data["createRun"].(map[string]any)["id"].(string), nil
	}

	first, codes := create(`idempotencyKey: "tool-42"`)
	if codes != nil {
		t.Fatalf("codes %v", codes)
	}
	if again, codes := create(`idempotencyKey: "tool-42"`); again != first || codes != nil {
		t.Fatalf("retry gave %q %v, want %q", again, codes, first)
	}
	if _, codes := create(`workflowName: "Triple Assay", idempotencyKey: "tool-42"`); len(codes) != 1 || codes[0] != "BAD_INPUT" {
		t.Fatalf("same key, other workflow: codes %v", codes)
	}
	if other, _ := create(`idempotencyKey: "tool-43"`); other == "" || other == first {
		t.Fatalf("another key gave %q", other)
	}
	if plain, _ := create(``); plain == "" || plain == first {
		t.Fatalf("no key gave %q", plain)
	}
	ids := make(chan string, 10) // racing retries: the unique index decides
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); id, _ := create(`idempotencyKey: "tool-44"`); ids <- id }()
	}
	wg.Wait()
	close(ids)
	raced := map[string]bool{}
	for id := range ids {
		raced[id] = true
	}
	if len(raced) != 1 || raced[""] {
		t.Fatalf("10 racing creates with one key gave %v", raced)
	}
	runs, err := e.ListRuns(context.Background())
	if err != nil || len(runs) != 4 {
		t.Fatalf("%d runs, %v", len(runs), err)
	}

	start := `mutation { startRun(id: "` + first + `") { status } }`
	query(t, schema, start)
	resp := schema.Exec(context.Background(), start, "", nil)
	if len(resp.Errors) != 1 || resp.Errors[0].Extensions["code"] != "INVALID_STATE" || resp.Errors[0].Extensions["status"] != "RUNNING" {
		t.Fatalf("second start: %v", resp.Errors)
	}
}

func TestGraphQLShowsEveryAttempt(t *testing.T) {
	e, js := startExecutor(t)
	fakeDrivers(t, js, func(cmd protocol.Command, n int) (string, bool) {
		if cmd.Step == "b" && n == 1 {
			return "hung", true
		}
		return "completed", true
	}, "lh", "inc")
	runID := startRun(t, e)
	waitFinished(t, e, runID)

	data, codes := query(t, newSchema(t, e), `{ run(id: "`+runID+`") {
		status cancelRequested
		steps { name status dependencies { name } attempts { n jobId status outcome executionId startedAt } } } }`)
	if codes != nil {
		t.Fatal(codes)
	}
	run := data["run"].(map[string]any)
	if run["status"] != "COMPLETED" || run["cancelRequested"] != false {
		t.Fatalf("run: %v", run)
	}
	steps := map[string]map[string]any{}
	for _, s := range run["steps"].([]any) {
		steps[s.(map[string]any)["name"].(string)] = s.(map[string]any)
	}
	attempts := steps["b"]["attempts"].([]any)
	if len(attempts) != 2 {
		t.Fatalf("attempts of b: %v", attempts)
	}
	first, second := attempts[0].(map[string]any), attempts[1].(map[string]any)
	if first["status"] != "FAILED" || first["outcome"] != "HUNG" ||
		second["status"] != "COMPLETED" || second["jobId"] != protocol.JobID(runID, "b", 2) || second["startedAt"] == nil {
		t.Fatalf("attempts of b: %v", attempts)
	}
	deps := steps["d"]["dependencies"].([]any)
	if len(deps) != 2 || deps[0].(map[string]any)["name"] != "b" || deps[1].(map[string]any)["name"] != "c" {
		t.Fatalf("dependencies of d: %v", deps)
	}
}

func TestGraphQLErrorCodes(t *testing.T) {
	e, _ := startExecutor(t)
	schema := newSchema(t, e)

	data, codes := query(t, schema, `{ run(id: "run-nope") { id } }`)
	if codes != nil || data["run"] != nil {
		t.Fatalf("missing run: data %v, codes %v", data, codes)
	}
	cases := map[string]string{
		`mutation { startRun(id: "run-nope") { id } }`:        "NOT_FOUND",
		`mutation { cancelRun(id: "run-nope") { id } }`:       "NOT_FOUND",
		`mutation { createRun(workflowName: "Nope") { id } }`: "NOT_FOUND",
		`{ runs(after: "run-nope") { nodes { id } } }`:        "NOT_FOUND",
		`{ runs(first: 0) { nodes { id } } }`:                 "BAD_INPUT",
	}
	for q, want := range cases {
		if _, codes := query(t, schema, q); len(codes) != 1 || codes[0] != want {
			t.Errorf("%s: codes %v, want %s", q, codes, want)
		}
	}

	data, _ = query(t, schema, `mutation { createRun { id status } }`)
	id := data["createRun"].(map[string]any)["id"].(string)
	if data, codes := query(t, schema, `mutation { startRun(id: "`+id+`") { status } }`); codes != nil || data["startRun"].(map[string]any)["status"] != "RUNNING" {
		t.Fatalf("start: %v %v", data, codes)
	}
	if _, codes := query(t, schema, `mutation { startRun(id: "`+id+`") { status } }`); len(codes) != 1 || codes[0] != "INVALID_STATE" {
		t.Fatalf("second start: codes %v", codes)
	}
	data, _ = query(t, schema, `mutation { createRun { id } }`)
	other := data["createRun"].(map[string]any)["id"].(string)
	if data, codes := query(t, schema, `mutation { cancelRun(id: "`+other+`") { status cancelRequested } }`); codes != nil ||
		data["cancelRun"].(map[string]any)["status"] != "CANCELLED" || data["cancelRun"].(map[string]any)["cancelRequested"] != true {
		t.Fatalf("cancel: %v %v", data, codes)
	}
	if _, codes := query(t, schema, `mutation { cancelRun(id: "`+other+`") { status } }`); len(codes) != 1 || codes[0] != "INVALID_STATE" {
		t.Fatalf("second cancel: codes %v", codes)
	}
}

func TestGraphQLPagesNewestFirst(t *testing.T) {
	e, _ := startExecutor(t)
	schema := newSchema(t, e)
	var ids []string
	for i := 0; i < 3; i++ {
		rv, err := e.CreateRun(context.Background(), "test", workflow)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, rv.ID)
	}

	page := func(q string) ([]string, map[string]any) {
		data, codes := query(t, schema, q)
		if codes != nil {
			t.Fatal(codes)
		}
		runs := data["runs"].(map[string]any)
		var got []string
		for _, n := range runs["nodes"].([]any) {
			got = append(got, n.(map[string]any)["id"].(string))
		}
		return got, runs["pageInfo"].(map[string]any)
	}

	got, info := page(`{ runs(first: 2) { nodes { id } pageInfo { endCursor hasNextPage } } }`)
	if len(got) != 2 || got[0] != ids[2] || got[1] != ids[1] || info["hasNextPage"] != true || info["endCursor"] != ids[1] {
		t.Fatalf("first page %v %v, created %v", got, info, ids)
	}
	got, info = page(`{ runs(first: 2, after: "` + ids[1] + `") { nodes { id } pageInfo { endCursor hasNextPage } } }`)
	if len(got) != 1 || got[0] != ids[0] || info["hasNextPage"] != false {
		t.Fatalf("second page %v %v", got, info)
	}
	if got, _ = page(`{ runs(status: RUNNING) { nodes { id } pageInfo { hasNextPage } } }`); len(got) != 0 {
		t.Fatalf("no run is running, got %v", got)
	}
}
