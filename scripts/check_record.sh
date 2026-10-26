#!/usr/bin/env bash
# Checks finished run records against physical truth.
#
#   ./scripts/check_record.sh                  # every finished run in GET /runs
#   ./scripts/check_record.sh <run_id> ...     # only these runs
#
# The record is GET /runs/{id}. The truth is each instrument's ledger
# (GET :7101-7103/status) and the plate data in S3. The rules are the
# "Invariants (what the checker verifies)" and the lifecycles in docs/plan.md.
# Read-only: it never writes to the executor, the instruments or S3.
#
# Exit 0: every rule passed. Exit 1: some rule failed. Exit 2: could not read
# the executor or an instrument, so nothing could be checked.
set -uo pipefail

EXEC=${EXEC:-http://localhost:5002}
S3=${S3:-http://localhost:8343}
INSTRUMENTS=${INSTRUMENTS:-"http://localhost:7101 http://localhost:7102 http://localhost:7103"}
MAX_ATTEMPTS=${MAX_ATTEMPTS:-3}
# Devices that produce data (plan.md: "For data-producing devices the object is
# also at data_ref"). /status does not say, so it is configured here.
DATA_DEVICES=${DATA_DEVICES:-plate-reader-1}
# Step times in the view are RFC3339, ledger times are float seconds.
TOL_MS=${TOL_MS:-5}

pass=0; fail=0
ok()   { printf '  PASS  %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  FAIL  %s\n' "$1"; fail=$((fail+1)); }
note() { printf '  ....  %s\n' "$1"; }

sha256() { if command -v sha256sum >/dev/null; then sha256sum | cut -d' ' -f1; else shasum -a 256 | cut -d' ' -f1; fi; }

# ---------------------------------------------------------------------------
# Shared jq definitions. Every rule runs against one context document:
#   .run      the RunView from GET /runs/{id}
#   .execs    ledger executions with this run_id, each with .device (from
#             /status), and .jstep/.n parsed from job_id (null if malformed)
#   .all      every ledger execution on every device (for overlap checks)
#   .retry    {device_id: failures_retryable}
# ---------------------------------------------------------------------------
JQLIB='
# RFC3339 (any fraction, Z or offset) -> epoch float seconds, or null.
def ts:
  if . == null then null else
    [capture("^(?<b>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?<f>\\.[0-9]+)?(?<z>Z|[+-][0-9]{2}:[0-9]{2})$")]
    | if length == 0 then null else .[0] as $m
      | ($m.b + "Z" | fromdateiso8601)
        + (if $m.f then ("0" + $m.f | tonumber) else 0 end)
        - (if $m.z == "Z" then 0
           else (($m.z[1:3] | tonumber) * 3600 + ($m.z[4:6] | tonumber) * 60)
                * (if $m.z[0:1] == "-" then -1 else 1 end) end)
      end
  end;
def ms: . * 1000 | round;
# Well-formed executions of one step, in attempt order (malformed job_ids
# are reported by rule_no_unsolicited).
def execs_of($s): [ .execs[] | select(.step_name == $s and .n != null) ] | sort_by(.n // 1e9, .started_at);
def last_of($s): execs_of($s) | last;
def terminal: . == "completed" or . == "failed" or . == "cancelled";
def data_lost: (. // "") | test("data (lost|loss)"; "i");
def lost_ending: .outcome == "hung" and ((.error // "") | test("restarted"; "i"));
'

# Prints one PASS or FAIL for a rule. $1 = rule name, $2 = text to print on
# PASS, stdin = rule output: lines starting with "note " are notes, any other
# line is a problem.
judge() {
  local name=$1 okmsg=$2 line problems=() notes=()
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    case "$line" in
      "note "*) notes+=("${line#note }") ;;
      *)        problems+=("$line") ;;
    esac
  done
  if [ "${#problems[@]}" -eq 0 ]; then
    ok "$name: $okmsg"
  else
    local p; for p in "${problems[@]}"; do bad "$name: $p"; done
  fi
  local n; for n in ${notes[@]+"${notes[@]}"}; do note "$name: $n"; done
}

# Runs a jq filter (the last argument) against the run context; any earlier
# arguments are passed to jq (e.g. --argjson).
rule() {
  local filter=${*: -1}
  jq -r "${@:1:$#-1}" "$JQLIB $filter" <<<"$ctx"
}

# --- rules -------------------------------------------------------------------

# plan.md invariants 1-2, ADR-2: job_id = <run_id>/<step>/<n> everywhere. Every
# ledger execution for this run must be an attempt of one of its steps, on that
# step's device. Nothing on an instrument is unaccounted for.
rule_no_unsolicited() {
  local out
  out=$(rule '
    (.run.steps | map({key: .name, value: .device_id}) | from_entries) as $dev
    | .execs[]
    | if .n == null then "job \(.job_id) on \(.device) is not <run_id>/<step>/<n>"
      elif .jstep != .step_name then "job \(.job_id) on \(.device) has step_name \(.step_name)"
      elif $dev[.step_name] == null then "job \(.job_id) on \(.device): the run has no step \(.step_name)"
      elif $dev[.step_name] != .device then "job \(.job_id) ran on \(.device), the step belongs to \($dev[.step_name])"
      else empty end
  ') || out="checker error: jq failed on this rule"
  judge "no unsolicited executions" "every ledger execution is an attempt of a step of this run" <<<"$out"
}

# plan.md invariant 9, ADR-2: attempts are numbered 1..n with no gaps, and each
# attempt (job_id) ran physically once.
rule_attempt_numbers() {
  local out
  out=$(rule '
    ( [ .execs[] | select(.n != null) ] | group_by(.step_name)[]
      | (map(.n) | sort) as $ns
      | select($ns != [range(1; ($ns | length) + 1)])
      | "\(.[0].step_name): attempt numbers \($ns | map(tostring) | join(",")), expected 1..\($ns | length)" ),
    ( .execs | group_by(.job_id)[] | select(length > 1)
      | "job \(.[0].job_id) ran \(length) times on the instrument" )
  ') || out="checker error: jq failed on this rule"
  judge "attempt numbers" "consecutive from 1, one execution per job_id" <<<"$out"
}

# plan.md invariants 4 and 9: a non-retryable device (liquid handler) runs a
# step at most once; a retryable one at most max_attempts times.
rule_attempt_limit() {
  local out
  out=$(rule --argjson max "$MAX_ATTEMPTS" '
    .retry as $retry
    | [ .execs[] ] | group_by(.step_name)[]
    | (.[0].device) as $d
    | (if $retry[$d] == false then 1 else $max end) as $lim
    | select(length > $lim)
    | "\(.[0].step_name) ran \(length) times on \($d), limit \($lim)"
      + (if $lim == 1 then " (device is not retryable)" else "" end)
  ') || out="checker error: jq failed on this rule"
  judge "attempt limit" "no step exceeded its device's limit (non-retryable 1, else $MAX_ATTEMPTS)" <<<"$out"
}

# plan.md invariant 7, "Step" lifecycle: cancelled means never dispatched, and
# "cancelled never hides an execution".
rule_cancelled_has_no_executions() {
  local out
  out=$(rule '
    . as $c | .run.steps[] | select(.status == "cancelled")
    | . as $s | ($c | execs_of($s.name)) as $ex | select(($ex | length) > 0)
    | "\(.name) is cancelled but ran \($ex | length) time(s): \($ex | map(.job_id + "=" + .outcome) | join(", "))"
  ') || out="checker error: jq failed on this rule"
  judge "cancelled steps never ran" "no cancelled step has an execution" <<<"$out"
}

# plan.md invariant 7 + "Mapping an outcome to a step": the step status follows
# from its last attempt's ledger outcome.
#   completed -> last execution completed
#   failed    -> last execution failed or hung, or completed with "data lost"
#   a completed/failed step has at least one execution
# A completed step whose last execution is hung "restarted during the job" is
# the known lost-ending limit (plan.md Limits): reported as FAIL, by name.
rule_step_status_matches_ledger() {
  local out
  out=$(rule '
    . as $c | .run.steps[] | select(.status == "completed" or .status == "failed")
    | . as $s | ($c | last_of($s.name)) as $l
    | if $l == null then "\($s.name) is \($s.status) but no instrument ran it"
      elif $s.status == "completed" then
        if $l.outcome == "completed" then empty
        elif ($l | lost_ending) then
          "\($s.name) is completed but the ledger now says \($l.job_id) hung (\($l.error)): lost ledger ending after a restart, known limit in plan.md -- record and ledger disagree"
        else "\($s.name) is completed but its last execution \($l.job_id) is \($l.outcome)" end
      else
        if $l.outcome == "failed" or $l.outcome == "hung" then empty
        elif $l.outcome == "completed" and ($s.error | data_lost) then
          "note \($s.name) failed although \($l.job_id) completed on the instrument: data lost (\($s.error)), allowed by plan.md"
        elif $l.outcome == "completed" then
          "\($s.name) is failed but its last execution \($l.job_id) completed, and the error does not say data was lost (error: \($s.error // "null"))"
        else "\($s.name) is failed but its last execution \($l.job_id) is \($l.outcome)" end
      end
  ') || out="checker error: jq failed on this rule"
  judge "step status matches ledger" "every completed/failed step agrees with its last execution" <<<"$out"
}

# plan.md invariant 3, ADR-4: a completed step on a data device has
# data_ref = s3://<bucket>/runs/<run>/<step>/<n>.csv for its last attempt, and
# the object's sha256 equals the ledger's data_sha256.
rule_plate_data() {
  local rows row name ref want key got problems=() checked=0 empty_sha
  empty_sha=$(printf '' | sha256)
  rows=$(rule --arg dd "$DATA_DEVICES" '
    ($dd | split(" ") | map(select(. != ""))) as $data
    | . as $c | .run.steps[]
    | select(.status == "completed" and ((.device_id as $d | $data | index($d)) != null))
    | . as $s | ($c | last_of($s.name)) as $l
    | [ $s.name, ($s.data_ref // ""), ($l.data_sha256 // ""),
        "runs/\($c.run.id)/\($s.name)/\($l.n // "?").csv" ] | @tsv
  ')
  while IFS=$'\t' read -r name ref want key; do
    [ -z "$name" ] && continue
    checked=$((checked+1))
    if [ -z "$ref" ]; then problems+=("$name is completed but has no data_ref"); continue; fi
    case "$ref" in s3://*/*) ;; *) problems+=("$name: data_ref '$ref' is not s3://bucket/key"); continue ;; esac
    path=${ref#s3://}
    if [ "${path#*/}" != "$key" ]; then problems+=("$name: data_ref '$ref', expected key $key (last attempt)"); fi
    if [ -z "$want" ]; then problems+=("$name: the ledger has no data_sha256 for its last execution"); continue; fi
    got=$(curl -sf --max-time 10 "$S3/$path" | sha256)
    if [ "$got" = "$empty_sha" ]; then problems+=("$name: object $S3/$path is missing or empty")
    elif [ "$got" != "$want" ]; then problems+=("$name: object sha256 $got, ledger says $want"); fi
  done <<<"$rows"
  # data_ref only belongs on data-producing steps.
  while IFS= read -r row; do
    [ -n "$row" ] && problems+=("$row")
  done < <(rule --arg dd "$DATA_DEVICES" '
    ($dd | split(" ") | map(select(. != ""))) as $data
    | .run.steps[] | select(.data_ref != null and ((.device_id as $d | $data | index($d)) == null))
    | "\(.name) on \(.device_id) has data_ref \(.data_ref), but that device produces no data"')
  if [ "${#problems[@]}" -gt 0 ]; then
    local p; for p in "${problems[@]}"; do bad "plate data in S3: $p"; done
  elif [ "$checked" -eq 0 ]; then
    note "plate data in S3: no completed step on a data device, nothing to check"
  else
    ok "plate data in S3: $checked object(s) present with the ledger's sha256"
  fi
}

# plan.md invariant 8, "Step times in the HTTP view": started_at/finished_at
# are the last attempt's instrument times. A step that never ran has none.
rule_step_times() {
  local out
  out=$(rule --argjson tol "$TOL_MS" '
    . as $c | .run.steps[] | . as $s | ($c | last_of($s.name)) as $l
    | if $l == null then
        if $s.started_at != null or $s.finished_at != null then
          "\($s.name) never ran but shows started_at=\($s.started_at) finished_at=\($s.finished_at)" else empty end
      else
        ( [ ["started_at", ($s.started_at | ts), $l.started_at, $s.started_at],
            ["finished_at", ($s.finished_at | ts), $l.finished_at, $s.finished_at] ][]
          | . as [$f, $view, $ledger, $raw]
          | if $view == null and $ledger == null then empty
            elif $view == null then "\($s.name).\($f) is \($raw // "null"), ledger \($l.job_id) has \($ledger)"
            elif $ledger == null then "\($s.name).\($f) is \($raw), ledger \($l.job_id) has none"
            elif ((($view - $ledger) | ms) | fabs) > $tol then
              "\($s.name).\($f) is \($raw), ledger \($l.job_id) says \($ledger) (off by \(($view - $ledger) | ms) ms)"
            else empty end )
      end
  ') || out="checker error: jq failed on this rule"
  judge "step times = last attempt" "view times match the last attempt's ledger times (within ${TOL_MS}ms)" <<<"$out"
}

# plan.md invariant 6, "Ordering and clocks": the first execution of a step
# started after the completing execution of each dependency finished.
rule_dependency_order() {
  local out
  out=$(rule '
    . as $c | .run.steps[] | . as $s
    | ($c | execs_of($s.name) | first) as $first | select($first != null)
    | $s.depends_on[]? as $d
    | ([ $c | execs_of($d)[] | select(.outcome == "completed") ] | last) as $done
    | if $done == null then
        "\($s.name) ran (\($first.job_id)) but dependency \($d) has no completed execution in the ledger"
      elif $done.finished_at == null or $first.started_at <= $done.finished_at then
        "\($s.name) started at \($first.started_at), before \($d) (\($done.job_id)) finished at \($done.finished_at)"
      else empty end
  ') || out="checker error: jq failed on this rule"
  judge "dependency order" "every step started after its dependencies completed (instrument times)" <<<"$out"
}

# plan.md invariant 5: one job at a time per device. Checked over the whole
# ledger of each device, reported where this run is involved.
rule_no_device_overlap() {
  local out
  out=$(rule '
    .run.id as $r
    | .all | group_by(.device)[] | sort_by(.started_at)
    | . as $e | range(1; length) as $i
    | $e[$i - 1] as $a | $e[$i] as $b
    | select($a.run_id == $r or $b.run_id == $r)
    | select($a.finished_at == null or $b.started_at < $a.finished_at)
    | "\($a.device): \($b.job_id) started at \($b.started_at) while \($a.job_id) was running (finished \($a.finished_at))"
  ') || out="checker error: jq failed on this rule"
  judge "no device overlap" "no device ran two jobs at once" <<<"$out"
}

# plan.md "Run": failed if any step failed; else cancelled if any cancelled;
# else completed when all completed. A terminal run has no pending/running
# step and no execution still running on an instrument.
rule_run_status() {
  local out
  out=$(rule '
    .run.status as $rs | [ .run.steps[].status ] as $st
    | ( if ($st | index("failed")) then "failed"
        elif ($st | map(terminal) | all | not) then "not terminal"
        elif ($st | index("cancelled")) then "cancelled"
        else "completed" end ) as $want
    | ( if $want != $rs then "run is \($rs), its steps say \($want) (\($st | group_by(.) | map("\(.[0])=\(length)") | join(" ")))" else empty end ),
      ( .run.steps[] | select(.status | terminal | not) | "run is \($rs) but step \(.name) is \(.status)" ),
      ( .execs[] | select(.outcome == "running") | "run is \($rs) but \(.job_id) is still running on \(.device)" )
  ') || out="checker error: jq failed on this rule"
  judge "run status" "derived correctly from its steps; nothing still running" <<<"$out"
}

# --- driver ------------------------------------------------------------------

check_run() {
  local id=$1 run=$2 ledgers=$3
  local status wf
  status=$(jq -r '.status' <<<"$run")
  wf=$(jq -r '.workflow_name' <<<"$run")
  echo
  echo "run $id ($wf): $status"
  case "$status" in
    completed|failed|cancelled) ;;
    *) note "not finished, skipped"; return ;;
  esac

  ctx=$(jq -nc --argjson run "$run" --argjson led "$ledgers" '
    ($run.id) as $r
    | [ $led[] | .device_id as $d | .executions[] | . + {device: $d} ] as $all
    | { run: $run,
        retry: ($led | map({key: .device_id, value: .failures_retryable}) | from_entries),
        all: $all,
        execs: [ $all[] | select(.run_id == $r)
                 | . as $e
                 | ( if (.job_id | startswith($r + "/")) then
                       [ .job_id[($r | length) + 1:] | capture("^(?<s>[^/]+)/(?<n>[1-9][0-9]*)$") ] | first
                     else null end ) as $p
                 | $e + { jstep: ($p.s // null), n: (if $p then ($p.n | tonumber) else null end) } ] }')

  rule_no_unsolicited
  rule_attempt_numbers
  rule_attempt_limit
  rule_cancelled_has_no_executions
  rule_step_status_matches_ledger
  rule_plate_data
  rule_step_times
  rule_dependency_order
  rule_no_device_overlap
  rule_run_status
}

main() {
  local ids=() id runs=() run ledgers one url skipped=0 checked=0
  if [ "$#" -gt 0 ]; then
    ids=("$@")
  else
    local list
    list=$(curl -sf --max-time 10 "$EXEC/runs") || { echo "could not read $EXEC/runs -- is the executor up?" >&2; exit 2; }
    while IFS= read -r id; do [ -n "$id" ] && ids+=("$id"); done < <(jq -r '.[].id' <<<"$list")
  fi
  [ "${#ids[@]}" -eq 0 ] && { echo "no runs to check"; exit 0; }

  # Read the records first, the ledgers second: a run that was terminal when
  # read had all its executions ended, so the later ledger read has them all.
  for id in "${ids[@]}"; do
    run=$(curl -sf --max-time 10 "$EXEC/runs/$id") || run=""
    runs+=("$run")
  done

  ledgers='[]'
  for url in $INSTRUMENTS; do
    one=$(curl -sf --max-time 10 "$url/status") || { echo "could not read $url/status -- the ledgers are incomplete, nothing checked" >&2; exit 2; }
    ledgers=$(jq -c --argjson one "$one" '. + [$one]' <<<"$ledgers")
  done

  local i
  for i in "${!ids[@]}"; do
    id=${ids[$i]}; run=${runs[$i]}
    if [ -z "$run" ]; then
      echo; echo "run $id"; bad "could not read $EXEC/runs/$id"; continue
    fi
    case "$(jq -r '.status' <<<"$run")" in
      completed|failed|cancelled) checked=$((checked+1)) ;;
      *) skipped=$((skipped+1)) ;;
    esac
    check_run "$id" "$run" "$ledgers"
  done

  echo
  echo "$checked run(s) checked, $skipped not finished and skipped: $pass passed, $fail failed."
  [ "$fail" -eq 0 ]
}

main "$@"
