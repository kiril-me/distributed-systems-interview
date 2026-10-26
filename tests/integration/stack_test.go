//go:build integration

// Package integration breaks infrastructure under a run on the running stack
// and checks the record afterwards with scripts/check_record.sh. Needs
// `docker compose up -d` with instrument faults at 0:
//
//	go test -tags integration -v -count=1 ./tests/integration/
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

var execURL = env("EXEC", "http://localhost:5002")

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// root is the repository root, where docker compose and the scripts live.
const root = "../.."

func sh(t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

type run struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Steps  []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	} `json:"steps"`
}

func (r run) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s:", r.ID, r.Status)
	for _, s := range r.Steps {
		fmt.Fprintf(&b, " %s=%s", s.Name, s.Status)
	}
	return b.String()
}

func call(method, path string) (run, error) {
	req, _ := http.NewRequest(method, execURL+path, bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "application/json")
	c := http.Client{Timeout: 5 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return run{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return run{}, fmt.Errorf("%s %s: %s", method, path, resp.Status)
	}
	var r run
	return r, json.NewDecoder(resp.Body).Decode(&r)
}

// waitHealthy waits until every compose service with a healthcheck is healthy.
func waitHealthy(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		out := sh(t, "docker", "compose", "ps", "--format", "{{.Service}} {{.Health}}")
		bad := []string{}
		for _, line := range strings.Split(out, "\n") {
			if f := strings.Fields(line); len(f) == 2 && f[1] != "healthy" {
				bad = append(bad, line)
			}
		}
		if len(bad) == 0 && strings.Count(out, "\n") >= 6 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stack not healthy:\n%s", out)
		}
		time.Sleep(time.Second)
	}
}

// settle polls the run until it is terminal. Errors while the fault is on are
// expected and only logged.
func settle(t *testing.T, id string, within time.Duration) run {
	t.Helper()
	var last run
	var lastErr error
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(time.Second) {
		r, err := call("GET", "/runs/"+id)
		if err != nil {
			if lastErr == nil || err.Error() != lastErr.Error() {
				t.Logf("GET /runs/%s: %v", id, err)
			}
			lastErr = err
			continue
		}
		lastErr, last = nil, r
		switch r.Status {
		case "completed", "failed", "cancelled":
			return r
		}
	}
	t.Fatalf("run did not settle within %v; last seen %v", within, last)
	return run{}
}

// underFault starts the default workflow, lets it get going, applies the fault,
// and then requires the run to settle and the checker to pass on it.
func underFault(t *testing.T, fault func(t *testing.T)) {
	waitHealthy(t)
	r, err := call("POST", "/runs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := call("POST", "/runs/"+r.ID+"/start"); err != nil {
		t.Fatal(err)
	}
	t.Logf("run %s started", r.ID)
	time.Sleep(3 * time.Second) // fill_sample_plate done, its successors in flight

	before, _ := call("GET", "/runs/"+r.ID)
	t.Logf("fault at: %v", before)
	fault(t)

	r = settle(t, r.ID, 3*time.Minute)
	t.Logf("settled: %v", r)

	cmd := exec.Command("./scripts/check_record.sh", r.ID)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	t.Logf("check_record.sh %s:\n%s", r.ID, out)
	if err != nil {
		t.Fatalf("check_record.sh: %v", err)
	}
	if r.Status != "completed" {
		t.Errorf("run %s with instrument faults at 0", r.Status)
	}
}

func TestRestartNATSMidRun(t *testing.T) {
	underFault(t, func(t *testing.T) { sh(t, "docker", "compose", "restart", "nats") })
}

func TestRestartPostgresMidRun(t *testing.T) {
	underFault(t, func(t *testing.T) { sh(t, "docker", "compose", "restart", "postgres") })
}

func TestKillNATSMidRun(t *testing.T) {
	underFault(t, func(t *testing.T) {
		id := sh(t, "docker", "compose", "ps", "-q", "nats")
		sh(t, "docker", "kill", id)
		time.Sleep(3 * time.Second)
		sh(t, "docker", "start", id)
	})
}
