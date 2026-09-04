#!/usr/bin/env bash
#
# The --s2-perf DRIVER, executed. Not the Python stages composed by a test — the
# shell chain that run_controlled.sh runs, sourced from the same file it sources.
#
#   contract -> symmetric warm-up -> latency -> noise pilot -> finalization -> cleanup
#
# `bench/test_s2perf_integration.py` proves the stage modules compose. It cannot
# prove the ORDER, the stop-on-failure clauses, which status reaches the caller, or
# what is skipped when a stage dies, because that logic lives in the shell. This
# file drives exactly that, against stand-in servers started as real child
# processes and stopped by the real `stop_service` from lib_procs.sh.
#
# WHAT THIS DOES NOT DO, so nothing green here is read as more than it is:
#
#   * run_controlled.sh is not executed end to end. It needs Linux /proc, odb24's
#     hostname, a production interpreter and the real store. What IS executed from
#     it here: every guard that fires before the host check, invoked as the real
#     script (grant, mode exclusivity, worker-count assertion, the budget banner).
#   * no gunicorn arbiter is launched and no store is read. The arms are Python HTTP
#     servers, so this says nothing about worker forking or about WOA23 data.
#   * provenance validation is stubbed for the two gates, exactly as in
#     bench/test_s2perf_integration.py and for the same reason: it reads /proc on the
#     backend's own host. bench/test_provenance.py covers it.
#
#     bash scripts/test_s2perf_driver.sh
set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RUNNER="$HERE/run_controlled.sh"
RUNSRC="$(cat "$HERE/run_controlled.sh")"
CHAINSRC="$(cat "$HERE/lib_s2perf.sh")"
pass=0; fail=0

check() {   # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}
has_text() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

WORK="$(mktemp -d)"
PY="$ROOT/.venv/bin/python"
# S2PERF_KEEP_WORK=1 leaves the scenario directories behind, which is how a failure
# in any stage below is diagnosed: every log, artefact and journal is under $WORK.
# THE SUMMARY CONTRACT: `ASSERTIONS=<n> FAILED=<m>` is the LAST line, exactly once, and
# nothing may follow it.
#
# This EXIT trap used to print `state kept for inspection: …` AFTER `suite_summary` had
# already printed the contract line, because a trap runs after the script's last command.
# The runner then reported `SUMMARY CONTRACT VIOLATION: the summary is not the final line`
# and, worse, printed that instead of the failing assertions — so a failing run HID its own
# failures behind a formatting fault. It fired only when `fail > 0`, i.e. only when the
# detail mattered.
#
# The notice is now emitted BEFORE the summary, by `keep_notice`, which is called
# immediately before `suite_summary`. The trap keeps the directory and prints only if the
# notice has not already been given — which covers an abnormal exit that never reaches the
# summary at all.
_keep_noticed=no
keep_notice() {
  if [ -n "${S2PERF_KEEP_WORK:-}" ] || [ "$fail" -gt 0 ]; then
    echo "state kept for inspection: $WORK"
    _keep_noticed=yes
  fi
}
cleanup_work() {
  # A failing run keeps everything. The one cleanup failure this test has seen was
  # undiagnosable afterwards because its scenario directory had been deleted on the
  # way out — so the artefacts, journals, logs and .diag files of a failed run are
  # now evidence, not temporary files.
  if [ -n "${S2PERF_KEEP_WORK:-}" ] || [ "$fail" -gt 0 ]; then
    [ "$_keep_noticed" = yes ] || echo "state kept for inspection: $WORK"
    return 0
  fi
  "$PY" -c "import shutil,sys; shutil.rmtree(sys.argv[1], ignore_errors=True)" "$WORK"
}
trap cleanup_work EXIT

# ------------------------------------------------------------ the stand-in arm ---
# A real child process, so cleanup has a process tree to stop and verify. It answers
# the contract cases with their own recorded status, because contract_diff refuses a
# status the case does not declare — a backend that always said 200 would fail the
# gate for reasons that have nothing to do with the driver.
cat > "$WORK/arm.py" <<'PYEOF'
import json, sys, time, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
sys.path.insert(0, sys.argv[3])
from bench.contract_cases import all_cases
from bench.queries import select

STATUS = {(c.path, tuple(sorted((k, str(v)) for k, v in c.params.items()))):
          c.expect_status for c in all_cases()}
QID = {tuple(sorted((k, str(v)) for k, v in q.params().items())): q.id
       for q in select(None, True)}
DELAY = float(sys.argv[4])          # seconds per gate-case request
DIE_AFTER = int(sys.argv[5])        # stop answering after N requests; 0 = never
# STATUS_AFTER: answer this status once N requests have been served. The arm stays
# up and keeps replying — it is simply answering wrongly, which is a different
# finding from a connection that went away and must not share its classification.
BAD_AFTER = int(sys.argv[7]) if len(sys.argv) > 7 else 0
BAD_STATUS = int(sys.argv[8]) if len(sys.argv) > 8 else 500
STATE = {"n": 0}

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        params = tuple(sorted((k, v) for k, v in
                              urllib.parse.parse_qsl(u.query, keep_blank_values=True)
                              if k != "_cb"))
        STATE["n"] += 1
        if DIE_AFTER and STATE["n"] > DIE_AFTER:
            self.close_connection = True
            self.wfile.close()
            return
        if QID.get(params) and DELAY:
            time.sleep(DELAY)
        body = json.dumps({"path": u.path, "params": [list(p) for p in params]},
                          sort_keys=True).encode()
        if len(sys.argv) > 6 and sys.argv[6] == "differ" and u.path == "/api/woa23" \
                and params and params[0][0] == "append":
            body += b"  DIFFERENT"
        code = STATUS.get((u.path, params), 200)
        if BAD_AFTER and STATE["n"] > BAD_AFTER and QID.get(params):
            code = BAD_STATUS
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

srv = ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H)
open(sys.argv[2], "w").write(str(srv.server_address[1]))
srv.serve_forever()
PYEOF

# The provenance stub, as a sitecustomize the arms never see: it wraps only the two
# gates, which refuse to issue a request without /proc-collected backend metadata.
cat > "$WORK/stub.py" <<'PYEOF'
import importlib, sys
mod = importlib.import_module(sys.argv[1])
for name in ("validate_meta", "validate_store_agreement",
             "verify_group_path_agreement", "post_run_runtime_check"):
    if hasattr(mod, name):
        setattr(mod, name, lambda *a, **k: [])
if hasattr(mod, "load_meta"):
    mod.load_meta = lambda path, label: ({"stub_provenance": True}, [])
sys.argv = [sys.argv[1]] + sys.argv[2:]
raise SystemExit(mod.main())
PYEOF

# `uv run python -m bench.X` is what the library invokes. Shadowing `uv` with a stub
# that routes the two gates through the provenance stub, and everything else
# straight through, keeps the library's own command lines untouched.
mkdir -p "$WORK/bin"
cat > "$WORK/bin/uv" <<PYEOF
#!/usr/bin/env bash
# args: run python -m <module> ...
if [ "\$1" = run ] && [ "\$2" = python ] && [ "\$3" = -m ]; then
  mod="\$4"; shift 4
  case "\$mod" in
    bench.contract_diff|bench.paired_bench)
      # \`python script.py\` puts the SCRIPT's directory on sys.path, not the cwd,
      # so the stub needs the repo named explicitly to import what it wraps.
      exec env PYTHONPATH="$ROOT" "$ROOT/.venv/bin/python" "$WORK/stub.py" \\
        "\$mod" "\$@" ;;
    *) exec "$ROOT/.venv/bin/python" -m "\$mod" "\$@" ;;
  esac
fi
if [ "\$1" = run ] && [ "\$2" = python ]; then shift 2; exec "$ROOT/.venv/bin/python" "\$@"; fi
exec "$ROOT/.venv/bin/python" "\$@"
PYEOF
chmod +x "$WORK/bin/uv"
PATH="$WORK/bin:$PATH"
export PATH

# Started through the REAL launcher. start_tracked records the pidfile, the start
# time and the process tree that cleanup is later verified against — so the arms
# here are stopped by the same code that stops a gunicorn arbiter on VM24.
start_arm() {   # start_arm <name> <delay> <die-after> [differ] [bad-after] [status]
  local name="$1" delay="$2" die="$3" extra="${4:-}" portfile="$WORK/$1.port" i=0
  local bad_after="${5:-0}" bad_status="${6:-500}"
  [ -e "$portfile" ] && rm "$portfile"
  start_tracked "$name" "" "$ROOT/.venv/bin/python" "$WORK/arm.py" 0 "$portfile" \
    "$ROOT" "$delay" "$die" "${extra:-none}" "$bad_after" "$bad_status" \
    >/dev/null || return 1
  while [ ! -s "$portfile" ] && [ "$i" -lt 200 ]; do sleep 0.05; i=$((i + 1)); done
  ARM_URL="http://127.0.0.1:$(cat "$portfile")"
}

# start_tracked/stop_tracked and the request counters keep their state here, as
# they do in a real run. Set before the libraries are sourced: lib_requests.sh reads
# $RUN at source time to place its counters.
RUN="$WORK/run"
mkdir -p "$RUN"

# The real libraries, sourced the way run_controlled.sh sources them.
# shellcheck source=lib_procs.sh
. "$HERE/lib_procs.sh"
# shellcheck source=lib_requests.sh
. "$HERE/lib_requests.sh"
# shellcheck source=lib_d1_finalize.sh
. "$HERE/lib_d1_finalize.sh"
# shellcheck source=lib_s2perf.sh
. "$HERE/lib_s2perf.sh"

BUDGET_CONTRACT=64
BUDGET_ARM=496

fresh() {   # fresh <label> -> RES, JRN, REQUEST_COUNTS_DIR for one scenario
  RES="$WORK/$1/results"; JRN="$WORK/$1/journals"
  export REQUEST_COUNTS_DIR="$WORK/$1/requests"
  mkdir -p "$RES" "$JRN" "$REQUEST_COUNTS_DIR"
  # The arm records d1_finalize parses a worker count out of.
  local arm
  for arm in candidate reference; do
    printf '{"launch_argv": ["gunicorn", "-w", "1", "--graceful-timeout", "10", "api.app:app"], "launch_command": "gunicorn -w 1", "worker_pids": [1]}\n' \
      > "$RES/$1_meta_$arm.json"
  done
}

cd "$ROOT"

# ================================================================================
echo "the driver runs the stages in spec 007's order, and only that order"
# ================================================================================
fresh good
start_arm cand 0.004 0;  CAND="$ARM_URL"
start_arm ref  0.012 0;  REF="$ARM_URL"

# What the runner records before the gate: readiness at its worst case and the two
# store probes, then the contract count through its single owner. Readiness and the
# probes are issued by lib_http.sh in a real run and are modelled here at their
# ceiling, because the ceiling is what the authorisation is written against.
for arm in candidate reference; do
  request_add "$arm" readiness 30
  request_add "$arm" store_probe 2
done
record_contract_count reference candidate
check "the contract stage is recorded once, and only once" "64" \
      "$(request_count candidate contract)"

s2perf_contract "$CAND" "$REF" 5.2A both-pinned good "$RES" > "$WORK/contract.log" 2>&1
check "the contract gate passes on 64 cases" "0" "$?"
check "and wrote its artefact" "yes" \
      "$([ -s "$RES/good_contract.json" ] && echo yes || echo no)"

s2perf_warmup "$CAND" "$REF" good "$RES" "$JRN" > "$WORK/warmup.log" 2>&1
check "the symmetric warm-up completes" "0" "$?"
check "it journalled 16 attempts per arm before the gate ran" "16" \
      "$("$ROOT/.venv/bin/python" -c '
import json,sys
print(sum(1 for l in open(sys.argv[1]) if json.loads(l)["arm"]=="candidate"))' \
        "$JRN/symmetric_warmup.jsonl")"

s2perf_latency "$CAND" "$REF" good "$RES" "$JRN" > "$WORK/latency.log" 2>&1
LAT_RC=$?
check "the latency gate returns PASS" "0" "$LAT_RC"
check "the gate's artefact is complete" "yes" "$(s2perf_latency_complete good "$RES")"
check "176 latency attempts per arm are journalled" "176" \
      "$("$ROOT/.venv/bin/python" -c '
import json,sys
print(sum(1 for l in open(sys.argv[1]) if json.loads(l)["arm"]=="candidate"))' \
        "$JRN/latency.jsonl")"

s2perf_pilot "$CAND" "$REF" good "$RES" "$JRN" > "$WORK/pilot.log" 2>&1
check "the noise pilot runs on both arms, after the gate" "0" "$?"
check "208 pilot attempts per arm" "208" \
      "$("$ROOT/.venv/bin/python" -c '
import json,sys
print(sum(1 for l in open(sys.argv[1]) if json.loads(l)["arm"]=="candidate"))' \
        "$JRN/noise_pilot.jsonl")"

s2perf_finish good "$RES" "$JRN" 496 "$LAT_RC" 0 > "$WORK/finish.log" 2>&1
check "the tail returns 0 on a PASS" "0" "$?"
finish_log="$(cat "$WORK/finish.log")"
check "the counts are exact" "yes" "$(s2perf_counts_exact good "$RES" "$JRN")"
check "the request record was written" "yes" \
      "$([ -s "$RES/good_requests.json" ] && echo yes || echo no)"
check "and the worker record" "yes" \
      "$([ -s "$RES/good_workers.json" ] && echo yes || echo no)"
# THE BUDGET REGRESSION. This is the number the authorisation is granted against,
# accumulated by the real chain through the real counters — and it is 496 only
# because the contract stage has exactly one owner. When the runner recorded it
# before the gate and `s2perf_record_counts` recorded it again, this was 528.
check "496 per arm: 30 + 2 + 64 + 16 + 176 + 208" "496" \
      "$(request_arm_total candidate)"
check "and the reference arm matches" "496" "$(request_arm_total reference)"
check "992 across both arms" "992" "$(request_total candidate reference)"
check "the contract stage was not counted twice" "64" \
      "$(request_count candidate contract)"
check "each stage is what spec 007 s5.4.1 budgets" \
      "30 2 64 16 176 208" \
      "$(request_count candidate readiness) $(request_count candidate store_probe) \
$(request_count candidate contract) $(request_count candidate symmetric_warmup) \
$(request_count candidate latency) $(request_count candidate noise_pilot)"
check "and the ceiling holds against it" "0" \
      "$(assert_request_ceiling 496 candidate reference >/dev/null 2>&1; echo $?)"
check "the contract count is confirmed usable as attempts" "yes" \
      "$("$PY" -c '
import json, sys
sys.path.insert(0, sys.argv[1])
from bench.perf_counts import contract_exact
ok, _ = contract_exact(__import__("pathlib").Path(sys.argv[2]), "good")
print("yes" if ok else "no")' "$ROOT" "$RES")"

# The S2 worker record: same shape as D1's, none of D1's sentences.
workers="$(cat "$RES/good_workers.json")"
check "the worker record names this mode" "s2perf" \
      "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["mode"])' "$RES/good_workers.json")"
check "and carries no D1-only wording" "no" "$(has_text "$workers" "D1")"
check "nor calls itself a characterization" "no" \
      "$(has_text "$workers" "characterization")"
check "it still refuses to be read as a production measurement" "false" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["derived_from_production_measurement"]).lower())' \
        "$RES/good_workers.json")"
check "and says what one worker per arm does not claim" "yes" \
      "$(has_text "$workers" "makes no claim about multi-worker")"
check "pointing production's worker count at S4" "yes" \
      "$(has_text "$workers" "S4")"
check "no INCOMPLETE classification" "no" \
      "$([ -f "$RES/good_perf_classification.json" ] && echo yes || echo no)"
check "the report refuses to pool the cases" "yes" \
      "$(has_text "$finish_log" "NEVER pooled across cases")"
check "refuses the coverage claim" "yes" \
      "$(has_text "$finish_log" "not a proven 95% coverage interval")"
check "and states the scope as rung 21 only" "yes" \
      "$(has_text "$finish_log" "rung 21 ONLY")"
check "saying it is not the whole of spec 007" "yes" \
      "$(has_text "$finish_log" "NO startup measurement")"

# --------------------------------------------------------------------- cleanup ---
# The real stop path, over the real child processes these stages talked to.
echo
echo "cleanup stops what the run started, and is verified, not assumed"
cand_pid="$(cat "$RUN/cand.pid")"; ref_pid="$(cat "$RUN/ref.pid")"
check "a process tree was recorded for each arm at launch" "yes" \
      "$([ -s "$RUN/cand.tree" ] && [ -s "$RUN/ref.tree" ] && echo yes || echo no)"
# Both arms' stop output is kept: a cleanup that refuses to signal something says
# WHY on stderr, and discarding that leaves a failure here undiagnosable.
stop_out="$(stop_tracked cand "" 2>&1)"; c_stop=$?
stop_out_ref="$(stop_tracked ref "" 2>&1)"; r_stop=$?
if [ "$c_stop" -ne 0 ] || [ "$r_stop" -ne 0 ]; then
  echo "  --- cleanup diagnostics (candidate) ---"
  printf '  %s\n' "$stop_out"
  echo "  --- cleanup diagnostics (reference) ---"
  printf '  %s\n' "$stop_out_ref"
  echo "  --- load at the time ---"
  uptime 2>&1 | sed 's/^/  /'
fi
check "the candidate arm stopped" "0" "$c_stop"
check "the reference arm stopped" "0" "$r_stop"
check "cleanup proves the whole tree exited, not just the pid" "yes" \
      "$(has_text "$stop_out" "whole tree exited")"
check "the candidate process is gone" "no" \
      "$(kill -0 "$cand_pid" 2>/dev/null && echo yes || echo no)"
check "the reference process is gone" "no" \
      "$(kill -0 "$ref_pid" 2>/dev/null && echo yes || echo no)"

# ================================================================================
echo
echo "a run that exceeds its authorised ceiling has no quotable result"
# ================================================================================
# An EVIDENCE-BACKED excess: every counter reconciles with the artefact or journal
# it came from, and the total still exceeds what was authorised. That is a statement
# about the host, and it is the only shape entitled to the CEILING_EXCEEDED label —
# a miscount issues no HTTP at all and is classified separately, below.
#
# The excess comes from AUTHORISING LESS, not from inventing traffic: a complete
# chain, its real 496 per arm, held against a 400 per-arm ceiling. It gets its own
# scenario because the counters are per run and recording them twice is itself the
# fault the next section tests.
fresh ceiling
start_arm cand9 0.001 0; CAND9="$ARM_URL"
start_arm ref9  0.001 0; REF9="$ARM_URL"
for arm in candidate reference; do
  request_add "$arm" readiness 30
  request_add "$arm" store_probe 2
done
s2perf_contract "$CAND9" "$REF9" 5.2A both-pinned ceiling "$RES" >/dev/null 2>&1
record_contract_count reference candidate
s2perf_warmup "$CAND9" "$REF9" ceiling "$RES" "$JRN" >/dev/null 2>&1
s2perf_latency "$CAND9" "$REF9" ceiling "$RES" "$JRN" >/dev/null 2>&1
LAT9=$?
s2perf_pilot "$CAND9" "$REF9" ceiling "$RES" "$JRN" >/dev/null 2>&1
s2perf_finish ceiling "$RES" "$JRN" 400 "$LAT9" 0 > "$WORK/ceil.log" 2>&1
CEIL_RC=$?
check "the counters reconcile with the evidence" "0" \
      "$(s2perf_reconcile ceiling "$RES" "$JRN" candidate reference >/dev/null 2>&1; \
         echo $?)"
check "the run really did issue 496 per arm" "496" "$(request_arm_total candidate)"
check "the tail returns 8" "8" "$CEIL_RC"
check "which is S2PERF_CEILING_EXCEEDED" "8" "$S2PERF_CEILING_EXCEEDED"
ceil_msg="$(cat "$WORK/ceil.log")"
check "naming what was issued against what was authorised" "yes" \
      "$(has_text "$ceil_msg" "issued 496, authorised 400")"
check "and the total too" "yes" \
      "$(has_text "$ceil_msg" "992 in total, authorised 800")"
check "refusing the run any quotable result" "yes" \
      "$(has_text "$ceil_msg" "NO quotable result")"
check "it is classified CEILING_EXCEEDED" "CEILING_EXCEEDED" \
      "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["classification"])' \
        "$RES/ceiling_perf_classification.json")"
check "the classification carries what was authorised" "400" \
      "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["authorised_per_arm"])' \
        "$RES/ceiling_perf_classification.json")"
check "and what was issued" "496" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["issued_per_arm"]["candidate"])' \
        "$RES/ceiling_perf_classification.json")"
check "and no PASS statement was printed" "no" \
      "$(has_text "$ceil_msg" "NEVER pooled across cases")"
# Recording the measured stages twice is refused outright, not merely detected.
check "a second recording of the measured stages is refused" "1" \
      "$(s2perf_record_counts ceiling "$RES" "$JRN" >/dev/null 2>&1; echo $?)"
check "and it says a second count is not a second request" "yes" \
      "$(has_text "$(s2perf_record_counts ceiling "$RES" "$JRN" 2>&1)" \
         "requests that were issued once")"
stop_tracked cand9 "" >/dev/null 2>&1; stop_tracked ref9 "" >/dev/null 2>&1

# ================================================================================
echo
echo "a failing contract gate never reaches the latency gate"
# ================================================================================
fresh bad
start_arm cand2 0.002 0;        CAND2="$ARM_URL"
start_arm ref2  0.002 0 differ; REF2="$ARM_URL"
(
  set -e
  s2perf_contract "$CAND2" "$REF2" 5.2A both-pinned bad "$RES" >/dev/null 2>&1 \
    || { echo "STOPPED" > "$WORK/order.txt"; exit 1; }
  s2perf_latency "$CAND2" "$REF2" bad "$RES" "$JRN" >/dev/null 2>&1
  echo "REACHED_LATENCY" > "$WORK/order.txt"
) >/dev/null 2>&1
chain_rc=$?
check "the chain exits 1" "1" "$chain_rc"
check "and stopped at the contract gate" "STOPPED" "$(cat "$WORK/order.txt")"
check "the contract artefact records FAIL" "FAIL" \
      "$("$ROOT/.venv/bin/python" -c '
import json,sys; print(json.load(open(sys.argv[1]))["gate"])' "$RES/bad_contract.json")"
check "no latency artefact exists" "no" \
      "$([ -f "$RES/bad_paired.json" ] && echo yes || echo no)"
check "no latency journal exists" "no" \
      "$([ -f "$JRN/latency.jsonl" ] && echo yes || echo no)"
stop_tracked cand2 "" >/dev/null 2>&1; stop_tracked ref2 "" >/dev/null 2>&1

# ================================================================================
echo
echo "an arm that goes away mid-gate: a partial artefact, an exact attempt count, exit 7"
# ================================================================================
fresh dead
# 64 contract requests, 16 warm-up, then it stops answering partway through latency.
start_arm cand3 0.001 120; CAND3="$ARM_URL"
start_arm ref3  0.001 0;   REF3="$ARM_URL"
s2perf_contract "$CAND3" "$REF3" 5.2A both-pinned dead "$RES" >/dev/null 2>&1
record_contract_count reference candidate
s2perf_warmup "$CAND3" "$REF3" dead "$RES" "$JRN" >/dev/null 2>&1
s2perf_latency "$CAND3" "$REF3" dead "$RES" "$JRN" > "$WORK/dead.log" 2>&1
DEAD_RC=$?
check "the latency gate exits non-zero" "yes" \
      "$([ "$DEAD_RC" -ne 0 ] && echo yes || echo no)"
check "it wrote a partial artefact rather than none" "yes" \
      "$([ -s "$RES/dead_paired.json" ] && echo yes || echo no)"
check "marked incomplete" "no" "$(s2perf_latency_complete dead "$RES")"
check "with no gate verdict invented from it" "INVALID_TRANSPORT_FAILURE" \
      "$("$ROOT/.venv/bin/python" -c '
import json,sys; print(json.load(open(sys.argv[1]))["gate"])' "$RES/dead_paired.json")"

# The pilot must NOT run: 416 requests to plan an escalation of a measurement that
# never finished is the opposite of a budget.
PILOT_RAN=no
if [ "$(s2perf_latency_complete dead "$RES")" = yes ]; then PILOT_RAN=yes; fi
check "the noise pilot is skipped" "no" "$PILOT_RAN"

s2perf_finish dead "$RES" "$JRN" 496 "$DEAD_RC" 0 > "$WORK/deadfinish.log" 2>&1
check "the tail exits 7, not 1" "7" "$?"
check "which is S2PERF_INCOMPLETE" "7" "$S2PERF_INCOMPLETE"
dead_log="$(cat "$WORK/deadfinish.log")"
check "it is classified, not merely non-zero" "yes" \
      "$(has_text "$dead_log" "INCOMPLETE_STAGE_FAILURE")"
check "a CAUGHT failure is different evidence: the count is exact" \
      "journal_writer_exited_normally" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["attempt_evidence"]["latency"])' \
        "$RES/dead_perf_counts.json")"
check "the stage artefact says so too" "true" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["host_attempt_count_exact"]).lower())' \
        "$RES/dead_paired.json")"
# THE THREE FILES MUST AGREE. The classification hardcoded host_attempt_count_exact
# to false, so a caught failure — measurement incomplete, attempt count exact —
# contradicted both perf_counts.json and the stage artefact beside it.
check "the classification agrees with perf_counts.json about exactness" "true" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["host_attempt_count_exact"]).lower())' \
        "$RES/dead_perf_classification.json")"
check "which is what perf_counts.json says" "true" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["host_attempt_count_exact"]).lower())' \
        "$RES/dead_perf_counts.json")"
check "and what the stage artefact says" "true" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["host_attempt_count_exact"]).lower())' \
        "$RES/dead_paired.json")"
check "the classification records the measurement as incomplete" "false" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["measurement_complete"]).lower())' \
        "$RES/dead_perf_classification.json")"
check "and says an exact count may be compared against the ceiling" "yes" \
      "$(has_text "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["reporting_rule"])' \
        "$RES/dead_perf_classification.json")" "may be compared against the authorised")"
check "and the cause it carries is the transport one this time" "yes" \
      "$(has_text "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["stage_failure_classification"])' \
        "$RES/dead_perf_classification.json")" "STAGE_ABORTED_TRANSPORT_FAILURE")"
check "the classification artefact was written" "yes" \
      "$([ -s "$RES/dead_perf_classification.json" ] && echo yes || echo no)"
# NOT "a floor": a stage that caught its own failure has an exact attempt count,
# and one killed asynchronously has a number that bounds nothing. Neither is a floor.
# A CAUGHT failure: the measurement is incomplete and the attempt count is exact,
# so the report says exactly that rather than calling the numbers inexact.
check "the report says the measurement is what is incomplete" "yes" \
      "$(has_text "$dead_log" "The MEASUREMENT is incomplete either way")"
check "and reports the exactness as yes" "yes" \
      "$(has_text "$dead_log" "exact     : yes")"
check "and the word floor is not used for them" "no" \
      "$(has_text "$dead_log" "a floor, not a total")"
check "and is reported WITH the ceiling" "yes" "$(has_text "$dead_log" "496 per arm")"
check "992 total" "yes" "$(has_text "$dead_log" "992 total")"
check "the run is refused as a latency result" "yes" \
      "$(has_text "$dead_log" "NOT a complete latency result")"
check "and no case may be quoted" "yes" \
      "$(has_text "$dead_log" "quoted as performance")"
# The arm went away, the stage CAUGHT it and wrote a partial artefact — so its
# process closed its own journal and the attempt count is exact. What is incomplete
# is the measurement. Conflating the two is what this split exists to prevent.
check "the attempt count is exact" "yes" "$(s2perf_counts_exact dead "$RES" "$JRN")"
check "while the measurement is not complete" "false" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["measurement_complete"]).lower())' \
        "$RES/dead_perf_counts.json")"
check "the journal still counted what the dead arm was sent" "yes" \
      "$([ "$("$ROOT/.venv/bin/python" -c '
import json,sys
print(sum(1 for l in open(sys.argv[1]) if json.loads(l)["arm"]=="candidate"))' \
        "$JRN/latency.jsonl")" -gt 0 ] && echo yes || echo no)"
check "and the journalled attempts exceed what the kept samples show" "yes" \
      "$("$ROOT/.venv/bin/python" - "$RES/dead_paired.json" "$JRN/latency.jsonl" <<'PYEOF'
import json, sys
doc = json.load(open(sys.argv[1]))
kept = sum(len(r["samples_ms"]["candidate"]) for r in doc["results"])
issued = sum(1 for l in open(sys.argv[2]) if json.loads(l)["arm"] == "candidate")
print("yes" if issued > kept else "no")
PYEOF
)"
stop_tracked cand3 "" >/dev/null 2>&1; stop_tracked ref3 "" >/dev/null 2>&1

# ================================================================================
echo
echo "a gate that returns a verdict is a RESULT: everything is still recorded"
# ================================================================================
fresh regress
start_arm cand4 0.030 0; CAND4="$ARM_URL"
start_arm ref4  0.008 0; REF4="$ARM_URL"
s2perf_contract "$CAND4" "$REF4" 5.2A both-pinned regress "$RES" >/dev/null 2>&1
record_contract_count reference candidate
s2perf_warmup "$CAND4" "$REF4" regress "$RES" "$JRN" >/dev/null 2>&1
s2perf_latency "$CAND4" "$REF4" regress "$RES" "$JRN" > "$WORK/reg.log" 2>&1
REG_RC=$?
check "the gate exits non-zero" "yes" \
      "$([ "$REG_RC" -ne 0 ] && echo yes || echo no)"
check "its artefact is COMPLETE — this is a verdict, not a crash" "yes" \
      "$(s2perf_latency_complete regress "$RES")"
check "and the verdict is FAIL" "FAIL" \
      "$("$ROOT/.venv/bin/python" -c '
import json,sys; print(json.load(open(sys.argv[1]))["gate"])' "$RES/regress_paired.json")"
s2perf_pilot "$CAND4" "$REF4" regress "$RES" "$JRN" >/dev/null 2>&1
s2perf_finish regress "$RES" "$JRN" 496 "$REG_RC" 0 > "$WORK/regfinish.log" 2>&1
FIN_RC=$?
# `set -e` used to end the run at the gate, skipping all of this and surfacing a
# complete measurement as a bare non-zero exit — the c2d and d1a shape.
check "the tail still ran and returned the gate's status" "$REG_RC" "$FIN_RC"
check "the request record was still written" "yes" \
      "$([ -s "$RES/regress_requests.json" ] && echo yes || echo no)"
check "the worker record too" "yes" \
      "$([ -s "$RES/regress_workers.json" ] && echo yes || echo no)"
check "the counts are exact, because every stage finished" "yes" \
      "$(s2perf_counts_exact regress "$RES" "$JRN")"
check "464 per arm — every stage but readiness and the probes" "464" \
      "$(request_arm_total candidate)"
check "and the report says the verdict is in the artefact" "yes" \
      "$(has_text "$(cat "$WORK/regfinish.log")" "did not return PASS")"
check "it is NOT classified as incomplete" "no" \
      "$([ -f "$RES/regress_perf_classification.json" ] && echo yes || echo no)"
stop_tracked cand4 "" >/dev/null 2>&1; stop_tracked ref4 "" >/dev/null 2>&1

# ================================================================================
echo
echo "a noise pilot that exited non-zero is not silently ignored"
# ================================================================================
# An ABSENT pilot artefact is legitimate at rung 150, so absence alone cannot be read
# as failure. Its exit status is the only thing that tells "did not run" from "ran
# and died", which is why it is carried into the tail rather than dropped.
fresh pilotfail
start_arm cand6 0.001 0; CAND6="$ARM_URL"
start_arm ref6  0.001 0; REF6="$ARM_URL"
s2perf_contract "$CAND6" "$REF6" 5.2A both-pinned pilotfail "$RES" >/dev/null 2>&1
record_contract_count reference candidate
s2perf_warmup "$CAND6" "$REF6" pilotfail "$RES" "$JRN" >/dev/null 2>&1
s2perf_latency "$CAND6" "$REF6" pilotfail "$RES" "$JRN" >/dev/null 2>&1
LAT6=$?
check "the counts would be exact but for the pilot" "yes" \
      "$(s2perf_counts_exact pilotfail "$RES" "$JRN")"
# No pilot artefact and a non-zero pilot status: it ran and died.
s2perf_finish pilotfail "$RES" "$JRN" 496 "$LAT6" 1 > "$WORK/pilotfail.log" 2>&1
check "the run is INCOMPLETE, not a PASS" "7" "$?"
pf="$(cat "$WORK/pilotfail.log")"
check "the pilot's status is stated" "yes" \
      "$(has_text "$pf" "the noise pilot exited 1")"
check "and no noise floor may be quoted" "yes" \
      "$(has_text "$pf" "no noise floor may" )"
check "the classification artefact was written" "yes" \
      "$([ -s "$RES/pilotfail_perf_classification.json" ] && echo yes || echo no)"
check "and no PASS statement was printed" "no" \
      "$(has_text "$pf" "NEVER pooled across cases")"
stop_tracked cand6 "" >/dev/null 2>&1; stop_tracked ref6 "" >/dev/null 2>&1

# ================================================================================
echo
echo "a noise pilot answered a wrong status AFTER issuing requests"
# ================================================================================
# Not a transport failure: the arm is up and replying, it is simply answering 500.
# That path used to `raise SystemExit`, which derives from BaseException and so
# walked past the handler that writes the partial artefact — the pilot exited with
# no artefact at all, and an absent pilot artefact is the LEGITIMATE "no pilot at
# this rung" state, so perf_counts counted zero traffic and called the run exact.
fresh badstatus
# The REFERENCE arm is the one that goes wrong, and only once the pilot starts:
# 64 contract + 16 warm-up + 176 latency = 256 requests answered correctly first, so
# the gates complete and the pilot is what meets the 500s. The pilot samples the
# reference arm first, which is what makes this also the fail-fast case.
start_arm cand7 0.001 0;             CAND7="$ARM_URL"
start_arm ref7  0.001 0 "" 256 500;  REF7="$ARM_URL"
s2perf_contract "$CAND7" "$REF7" 5.2A both-pinned badstatus "$RES" >/dev/null 2>&1
record_contract_count reference candidate
s2perf_warmup "$CAND7" "$REF7" badstatus "$RES" "$JRN" >/dev/null 2>&1
s2perf_latency "$CAND7" "$REF7" badstatus "$RES" "$JRN" >/dev/null 2>&1
LAT7=$?
s2perf_pilot "$CAND7" "$REF7" badstatus "$RES" "$JRN" > "$WORK/badpilot.log" 2>&1
PILOT7=$?
check "the pilot exits non-zero" "yes" \
      "$([ "$PILOT7" -ne 0 ] && echo yes || echo no)"
check "it wrote a partial artefact rather than none" "yes" \
      "$([ -s "$RES/badstatus_noise_pilot_reference.json" ] && echo yes || echo no)"
check "marked incomplete" "false" \
      "$("$PY" -c '
import json,sys; print(str(json.load(open(sys.argv[1]))["complete"]).lower())' \
        "$RES/badstatus_noise_pilot_reference.json")"
check "classified as a wrong status, not as a transport failure" \
      "STAGE_ABORTED_UNEXPECTED_STATUS" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["aborted"]["classification"])' \
        "$RES/badstatus_noise_pilot_reference.json")"
check "and the journal kept every attempt it had issued" "yes" \
      "$([ -s "$JRN/noise_pilot.jsonl" ] && echo yes || echo no)"
pilot_issued="$("$PY" -c '
import json,sys
print(sum(1 for l in open(sys.argv[1]) if json.loads(l)["arm"]=="reference"))' \
  "$JRN/noise_pilot.jsonl")"
check "which is more than zero" "yes" \
      "$([ "$pilot_issued" -gt 0 ] && echo yes || echo no)"
check "and fewer than a full pass, because it stopped early" "yes" \
      "$([ "$pilot_issued" -lt 208 ] && echo yes || echo no)"

s2perf_finish badstatus "$RES" "$JRN" 496 "$LAT7" "$PILOT7" > "$WORK/badfin.log" 2>&1
check "the run is INCOMPLETE" "7" "$?"
bf="$(cat "$WORK/badfin.log")"
check "the report says the pilot did not complete" "yes" \
      "$(has_text "$bf" "the noise pilot exited")"
# The artefact and the report must agree. Writing perf_counts.json before applying
# the pilot's status produced a file saying exact beside a report saying INCOMPLETE.
check "perf_counts.json agrees with the report about the MEASUREMENT" "false" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["measurement_complete"]).lower())' \
        "$RES/badstatus_perf_counts.json")"
check "it names the pilot as the failed stage" "noise_pilot" \
      "$("$PY" -c '
import json,sys; print(",".join(json.load(open(sys.argv[1]))["failed_stages"]))' \
        "$RES/badstatus_perf_counts.json")"
# The four fields, asserted on the REAL s2perf_finish path. The old test checked
# only counts_exact=false and so missed a caught failure being labelled a hard kill.
check "the measurement is not complete" "false" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["measurement_complete"]).lower())' \
        "$RES/badstatus_perf_counts.json")"
check "but the ATTEMPT count is exact — the writer closed its own journal" "true" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["host_attempt_count_exact"]).lower())' \
        "$RES/badstatus_perf_counts.json")"
check "and the evidence says so, rather than calling it a hard kill" \
      "journal_writer_exited_normally" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["attempt_evidence"]["noise_pilot"])' \
        "$RES/badstatus_perf_counts.json")"
check "the failure classification is the stage's own" "yes" \
      "$(has_text "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["stage_failure_classification"])' \
        "$RES/badstatus_perf_classification.json")" "STAGE_ABORTED_UNEXPECTED_STATUS")"
# An exact count may be checked against the ceiling even though there is no
# performance result — that is the whole point of separating the two questions.
check "an exact count is still ceiling-checked" "yes" \
      "$(has_text "$bf" "requests recorded")"
check "and the ceiling was NOT skipped for it" "no" \
      "$(has_text "$bf" "the authorised ceiling is not")"
check "the observed pilot count came from the journal" "$pilot_issued" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["per_arm"]["reference"]["noise_pilot"])' \
        "$RES/badstatus_perf_counts.json")"
check "and no PASS statement was printed" "no" \
      "$(has_text "$bf" "NEVER pooled across cases")"
# The run-level headline must not turn an HTTP 500 into a transport failure. It is
# the headline that gets quoted, and it used to be hardcoded.
check "the run classification is neutral" "INCOMPLETE_STAGE_FAILURE" \
      "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["classification"])' \
        "$RES/badstatus_perf_classification.json")"
check "it does NOT call an HTTP 500 a transport failure" "no" \
      "$(has_text "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["classification"])' \
        "$RES/badstatus_perf_classification.json")" "TRANSPORT")"
check "and it carries the stage's own reason" "yes" \
      "$(has_text "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["stage_failure_classification"])' \
        "$RES/badstatus_perf_classification.json")" "STAGE_ABORTED_UNEXPECTED_STATUS")"
check "which the report repeats" "yes" \
      "$(has_text "$bf" "STAGE_ABORTED_UNEXPECTED_STATUS")"
# Fail-fast across the arms: the noise floor is a property of the PAIR, so a
# candidate-only figure plans nothing and its 208 requests would buy nothing.
check "the candidate arm was not sampled after the reference arm failed" "no" \
      "$([ -e "$RES/badstatus_noise_pilot_candidate.json" ] && echo yes || echo no)"
stop_tracked cand7 "" >/dev/null 2>&1; stop_tracked ref7 "" >/dev/null 2>&1

# ================================================================================
echo
echo "a double count is an accounting fault, not a ceiling breach"
# ================================================================================
# A second recording issues no HTTP whatsoever. Calling it CEILING_EXCEEDED would
# report traffic that never reached the host.
fresh doublecount
start_arm cand8 0.001 0; CAND8="$ARM_URL"
start_arm ref8  0.001 0; REF8="$ARM_URL"
s2perf_contract "$CAND8" "$REF8" 5.2A both-pinned doublecount "$RES" >/dev/null 2>&1
record_contract_count reference candidate
check "a second call to the owner is refused outright" "1" \
      "$(record_contract_count reference candidate >/dev/null 2>&1; echo $?)"
check "and it says a miscount is not traffic" "yes" \
      "$(has_text "$(record_contract_count reference candidate 2>&1)" \
         "would not issue a single")"
check "the counter still holds one contract count" "64" \
      "$(request_count candidate contract)"
# Force the fault past the guard, the way a second owner in the code would.
request_add candidate contract 64
request_add reference contract 64
s2perf_warmup "$CAND8" "$REF8" doublecount "$RES" "$JRN" >/dev/null 2>&1
s2perf_latency "$CAND8" "$REF8" doublecount "$RES" "$JRN" >/dev/null 2>&1
LAT8=$?
s2perf_pilot "$CAND8" "$REF8" doublecount "$RES" "$JRN" >/dev/null 2>&1
s2perf_finish doublecount "$RES" "$JRN" 496 "$LAT8" 0 > "$WORK/dcfin.log" 2>&1
check "the run exits 9, not 8" "9" "$?"
check "which is S2PERF_ACCOUNTING_INCONSISTENT" "9" "$S2PERF_ACCOUNTING_INCONSISTENT"
dc="$(cat "$WORK/dcfin.log")"
check "the counter is reported as disagreeing with the evidence" "yes" \
      "$(has_text "$dc" "REQUEST ACCOUNTING INCONSISTENT")"
check "naming the stage and both numbers" "yes" \
      "$(has_text "$dc" "contract counter says 128")"
check "the classification is the accounting one" "REQUEST_ACCOUNTING_INCONSISTENT" \
      "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["classification"])' \
        "$RES/doublecount_perf_classification.json")"
check "and it refuses to claim a ceiling breach" "yes" \
      "$(has_text "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["not"])' \
        "$RES/doublecount_perf_classification.json")" "NOT a demonstrated ceiling breach")"
check "making no claim about how many requests reached the host" "yes" \
      "$(has_text "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["not"])' \
        "$RES/doublecount_perf_classification.json")" "no claim is made here about how many")"
check "and no PASS statement was printed" "no" \
      "$(has_text "$dc" "NEVER pooled across cases")"
stop_tracked cand8 "" >/dev/null 2>&1; stop_tracked ref8 "" >/dev/null 2>&1

# ================================================================================
echo
echo "a pilot hard-killed with no artefact: the journal is what counts"
# ================================================================================
# SIGKILL leaves no artefact at all, and an absent pilot artefact is the LEGITIMATE
# "no pilot at this rung" state. Until the canonical record existed, the shell
# counter recorded 0 for it, reconciliation compared that 0 against a freshly
# computed 0 and passed, and the run reported no pilot traffic while the journal
# held every attempt the pilot had made.
fresh killed
start_arm cand10 0.02 0; CAND10="$ARM_URL"
start_arm ref10  0.02 0; REF10="$ARM_URL"
for arm in candidate reference; do
  request_add "$arm" readiness 30
  request_add "$arm" store_probe 2
done
s2perf_contract "$CAND10" "$REF10" 5.2A both-pinned killed "$RES" >/dev/null 2>&1
record_contract_count reference candidate
s2perf_warmup "$CAND10" "$REF10" killed "$RES" "$JRN" >/dev/null 2>&1
s2perf_latency "$CAND10" "$REF10" killed "$RES" "$JRN" >/dev/null 2>&1
LAT10=$?
# The pilot, killed mid-flight. Its journal survives because every line is flushed
# before the request it describes is issued.
uv run python -m bench.noise_pilot --base-url "$REF10" --warm 25 --arm reference \
  --request-log "$JRN/noise_pilot.jsonl" \
  --out "$RES/killed_noise_pilot_reference.json" >/dev/null 2>&1 &
pilot_pid=$!
i=0
while [ ! -s "$JRN/noise_pilot.jsonl" ] && [ "$i" -lt 200 ]; do sleep 0.1; i=$((i+1)); done
sleep 2
kill -9 "$pilot_pid" 2>/dev/null
wait "$pilot_pid" 2>/dev/null
PILOT10=137
check "no pilot artefact was written" "no" \
      "$([ -e "$RES/killed_noise_pilot_reference.json" ] && echo yes || echo no)"
journal_ref="$("$PY" -c '
import json,sys
print(sum(1 for l in open(sys.argv[1]) if json.loads(l)["arm"]=="reference"))' \
  "$JRN/noise_pilot.jsonl")"
check "but the journal holds the attempts it issued" "yes" \
      "$([ "$journal_ref" -gt 0 ] && echo yes || echo no)"

# THE CEILING IS SET BELOW WHAT THE JOURNALS HOLD, deliberately. A journal written
# before each call can be one too high and one too low at once, so it cannot
# demonstrate that anything reached the host — let alone that too much did. This run
# must classify INCOMPLETE and never CEILING_EXCEEDED, however small the ceiling.
#
# One s2perf_finish call, because the tail is once per run: recording the counts a
# second time is itself the accounting fault the chain refuses.
low_ceiling=10
# The ceiling is a PER-ARM TOTAL, which is what assert_request_ceiling compares —
# not one stage's number. This arm's total is far above 10, so an exact count would
# have been a breach; an inexact one may not be called that.
check "the recorded per-arm total exceeds that ceiling" "yes" \
      "$([ "$(request_arm_total reference)" -gt "$low_ceiling" ] && echo yes || echo no)"
s2perf_finish killed "$RES" "$JRN" "$low_ceiling" "$LAT10" "$PILOT10" \
  > "$WORK/killfin.log" 2>&1
KILL_RC=$?
check "the run is INCOMPLETE" "7" "$KILL_RC"
check "and not CEILING_EXCEEDED" "yes" \
      "$([ "$KILL_RC" != "$S2PERF_CEILING_EXCEEDED" ] && echo yes || echo no)"
check "no ceiling breach is claimed" "no" \
      "$(has_text "$(cat "$WORK/killfin.log")" "CEILING EXCEEDED")"
check "the run says why the ceiling was not applied" "yes" \
      "$(has_text "$(cat "$WORK/killfin.log")" \
         "a count that bounds nothing cannot demonstrate a")"
# THE POINT: the shell counter must hold the journal's number, not zero.
check "the shell counter holds the journal count, not zero" "$journal_ref" \
      "$(request_count reference noise_pilot)"
check "which is not zero" "yes" \
      "$([ "$(request_count reference noise_pilot)" -gt 0 ] && echo yes || echo no)"
check "reconciliation did not accept an absent artefact as a legitimate zero" "yes" \
      "$([ "$(request_count reference noise_pilot)" = "$journal_ref" ] && echo yes || echo no)"
check "requests.json carries the same number" "$journal_ref" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["per_arm"]["reference"]["noise_pilot"])' \
        "$RES/killed_requests.json")"
check "perf_counts.json carries it too" "$journal_ref" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["per_arm"]["reference"]["noise_pilot"])' \
        "$RES/killed_perf_counts.json")"
check "and says the record is inexact" "false" \
      "$("$PY" -c '
import json,sys; print(str(json.load(open(sys.argv[1]))["counts_exact"]).lower())' \
        "$RES/killed_perf_counts.json")"
check "naming the pilot as the failed stage" "noise_pilot" \
      "$("$PY" -c '
import json,sys; print(",".join(json.load(open(sys.argv[1]))["failed_stages"]))' \
        "$RES/killed_perf_counts.json")"
check "the classification agrees" "INCOMPLETE_STAGE_FAILURE" \
      "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["classification"])' \
        "$RES/killed_perf_classification.json")"
check "and says no artefact recorded a cause, rather than inventing one" "yes" \
      "$(has_text "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["stage_failure_classification"])' \
        "$RES/killed_perf_classification.json")" "NO_STAGE_ARTEFACT_RECORDS_A_CAUSE")"
# The classification must name the stage on its own. "See the run report" is no use
# to anyone holding only this file.
check "the classification names the failed stage itself" "noise_pilot" \
      "$("$PY" -c '
import json,sys
print(",".join(json.load(open(sys.argv[1]))["failed_stages"]))' \
        "$RES/killed_perf_classification.json")"
# THE EVIDENCE SEMANTICS. A hard kill leaves a journal whose count is neither a
# bound nor exact: the writer can die between the flush and the call, and a
# truncated final record cannot be attributed to an arm.
check "the classification says the attempt count is not exact" "false" \
      "$("$PY" -c '
import json,sys
print(str(json.load(open(sys.argv[1]))["host_attempt_count_exact"]).lower())' \
        "$RES/killed_perf_classification.json")"
check "the number is named as journaled attempts, not as issued requests" "yes" \
      "$("$PY" -c '
import json,sys
print("yes" if "journaled_attempts_per_arm" in json.load(open(sys.argv[1])) else "no")' \
        "$RES/killed_perf_classification.json")"
check "the ceiling is still recorded beside it" "$low_ceiling" \
      "$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["ceiling_per_arm"])' \
        "$RES/killed_perf_classification.json")"
kill_rule="$("$PY" -c '
import json,sys; print(json.load(open(sys.argv[1]))["reporting_rule"])' \
  "$RES/killed_perf_classification.json")"
for banned in floor "at least" "actually issued"; do
  check "the reporting rule does not say \"$banned\"" "no" \
        "$(has_text "$kill_rule" "$banned")"
done
check "it says the journal count is not a bound on host traffic" "yes" \
      "$(has_text "$kill_rule" "NOT a bound on what reached the host")"
check "the record marks the pilot as abrupt-kill evidence" \
      "journal_after_abrupt_termination" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["attempt_evidence"]["noise_pilot"])' \
        "$RES/killed_perf_counts.json")"
kill_log="$(cat "$WORK/killfin.log")"
check "the report says no bound is claimed" "yes" \
      "$(has_text "$kill_log" "No bound is claimed")"
check "and does not call the number a floor" "no" \
      "$(has_text "$kill_log" "a floor, not a total")"
check "no performance result is quotable" "no" \
      "$(has_text "$(cat "$WORK/killfin.log")" "NEVER pooled across cases")"
stop_tracked cand10 "" >/dev/null 2>&1; stop_tracked ref10 "" >/dev/null 2>&1

# ================================================================================
echo
echo "a warm-up whose arm answers 500 never reaches the latency gate"
# ================================================================================
# The warm-up existed to leave both arms in a common state, and it reported success
# whenever the request COUNT was right — so an arm answering 500 to every request
# was called warm and the latency gate measured it against one that had served
# every request. Proving "the right number were sent" is exactly what did not help.
fresh coldwarm
start_arm cand11 0.001 0;            CAND11="$ARM_URL"
start_arm ref11  0.001 0 "" 64 500;  REF11="$ARM_URL"    # fine for contract, 500 after
s2perf_contract "$CAND11" "$REF11" 5.2A both-pinned coldwarm "$RES" >/dev/null 2>&1
record_contract_count reference candidate
s2perf_warmup "$CAND11" "$REF11" coldwarm "$RES" "$JRN" > "$WORK/coldwarm.log" 2>&1
WARM_RC=$?
check "the warm-up fails" "yes" "$([ "$WARM_RC" -ne 0 ] && echo yes || echo no)"
cw="$(cat "$WORK/coldwarm.log")"
check "and says the latency gate must not run" "yes" \
      "$(has_text "$cw" "latency gate must not run")"
check "the artefact was written anyway" "yes" \
      "$([ -s "$RES/coldwarm_symmetric_warmup.json" ] && echo yes || echo no)"
check "marked not complete" "false" \
      "$("$PY" -c '
import json,sys; print(str(json.load(open(sys.argv[1]))["complete"]).lower())' \
        "$RES/coldwarm_symmetric_warmup.json")"
check "the journal was kept" "yes" \
      "$([ -s "$JRN/symmetric_warmup.jsonl" ] && echo yes || echo no)"
check "every attempt was still issued" "16" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["requests_per_arm"]["reference"])' \
        "$RES/coldwarm_symmetric_warmup.json")"
check "which is why counting attempts was never the test" "16" \
      "$("$PY" -c '
import json,sys
print(json.load(open(sys.argv[1]))["unusable_per_arm"]["reference"])' \
        "$RES/coldwarm_symmetric_warmup.json")"

# THE POINT: the chain stops. Driven through the runner's own clause.
(
  set -e
  s2perf_warmup "$CAND11" "$REF11" coldwarm "$RES" "$JRN" >/dev/null 2>&1 \
    || { echo STOPPED > "$WORK/coldorder.txt"; exit 1; }
  s2perf_latency "$CAND11" "$REF11" coldwarm "$RES" "$JRN" >/dev/null 2>&1
  echo REACHED_LATENCY > "$WORK/coldorder.txt"
) >/dev/null 2>&1
cold_rc=$?
check "the chain exits 1 at the warm-up" "1" "$cold_rc"
check "and stopped there" "STOPPED" "$(cat "$WORK/coldorder.txt")"
check "NO latency artefact was produced" "no" \
      "$([ -e "$RES/coldwarm_paired.json" ] && echo yes || echo no)"
check "NO latency journal was produced" "no" \
      "$([ -e "$JRN/latency.jsonl" ] && echo yes || echo no)"
check "and no pilot artefact either" "no" \
      "$([ -e "$RES/coldwarm_noise_pilot_reference.json" ] && echo yes || echo no)"
check "the runner's own clause is the one that stops it" "yes" \
      "$(has_text "$RUNSRC" "the symmetric warm-up did not complete; stopping before any")"
stop_tracked cand11 "" >/dev/null 2>&1; stop_tracked ref11 "" >/dev/null 2>&1

# ================================================================================
echo
echo "a once-only marker that cannot be written fails the run"
# ================================================================================
# The guard's whole contract is that a second call is refused. If the marker cannot
# be created, that promise is unenforceable — and an unenforceable guard reported as
# enforced is worse than none.
# The marker lives in the counts directory, so a directory that cannot take the
# marker cannot take the counters either — the marker-only failure is not separately
# reproducible on a real filesystem. What IS reachable, and what matters, is that an
# unwritable counts directory stops the run rather than letting it issue requests it
# cannot account for. The marker's own branch is asserted from the source below.
MARKDIR="$WORK/readonly-counts"
mkdir -p "$MARKDIR"
chmod a-w "$MARKDIR"
(
  export REQUEST_COUNTS_DIR="$MARKDIR"
  record_contract_count candidate reference > "$WORK/marker.out" 2>&1
  echo $? > "$WORK/marker.rc"
)
chmod u+w "$MARKDIR" 2>/dev/null
check "an unwritable counts directory fails the recording" "1" \
      "$(cat "$WORK/marker.rc")"
check "and the counter says the requests would go unaccounted" "yes" \
      "$(has_text "$(cat "$WORK/marker.out")" "would" )"
check "refusing to proceed rather than counting nothing" "yes" \
      "$(has_text "$(cat "$WORK/marker.out")" "must not proceed")"
check "the once-only marker fails closed too" "yes" \
      "$([ "$(has_text "$CHAINSRC" "unenforceable guard is not a guard")" = yes ] \
         && echo yes || echo no)"
check "in both owners" "2" \
      "$(printf '%s\n' "$CHAINSRC" | grep -c "unenforceable guard is not a guard")"

# ================================================================================
echo
echo "finalization failure has its own status, and does not become the gate's"
# ================================================================================
fresh nofinal
start_arm cand5 0.001 0; CAND5="$ARM_URL"
start_arm ref5  0.001 0; REF5="$ARM_URL"
s2perf_contract "$CAND5" "$REF5" 5.2A both-pinned nofinal "$RES" >/dev/null 2>&1
record_contract_count reference candidate
s2perf_warmup "$CAND5" "$REF5" nofinal "$RES" "$JRN" >/dev/null 2>&1
s2perf_latency "$CAND5" "$REF5" nofinal "$RES" "$JRN" >/dev/null 2>&1
s2perf_pilot "$CAND5" "$REF5" nofinal "$RES" "$JRN" >/dev/null 2>&1
# The arm records the worker-count reader needs are removed: the measurements are
# complete and the artefacts cannot be.
rm "$RES/nofinal_meta_candidate.json" "$RES/nofinal_meta_reference.json"
s2perf_finish nofinal "$RES" "$JRN" 496 0 0 > "$WORK/nofinal.log" 2>&1
check "the tail returns 6, not 1 and not 7" "6" "$?"
check "which is D1_FINALIZE_FAILED" "6" "$D1_FINALIZE_FAILED"
check "and it is classified" "yes" \
      "$(has_text "$(cat "$WORK/nofinal.log")" "INVALID_POST_MEASUREMENT_HARNESS")"
check "saying the gate's own result is still recorded" "yes" \
      "$(has_text "$(cat "$WORK/nofinal.log")" "IS recorded in")"
stop_tracked cand5 "" >/dev/null 2>&1; stop_tracked ref5 "" >/dev/null 2>&1

# ================================================================================
echo
echo "a cleanup failure raises the status, and never lowers a more specific one"
# ================================================================================
# The trap's own arithmetic, run as the trap runs it. An EXIT trap's `return` never
# sets the exit status — verified on bash 3.2 and 5.2 — so raising it takes an exit,
# and c2d exited 0 while saying the run had failed.
cleanup_status() {   # cleanup_status <rc> <cleanup-failed>
  bash -c '
    cleanup() {
      local rc=$?
      if [ "'"$2"'" != "0" ]; then
        echo "CLEANUP DID NOT COMPLETE" >&2
        if [ "$rc" -eq 0 ]; then exit 1; fi
      fi
      return $rc
    }
    trap cleanup EXIT
    exit '"$1"'
  ' >/dev/null 2>&1
  echo $?
}
check "cleanup ok, run ok: 0" "0" "$(cleanup_status 0 0)"
check "cleanup ok, gate verdict: 1 survives" "1" "$(cleanup_status 1 0)"
check "cleanup FAILED on a passing run: 0 becomes 1" "1" "$(cleanup_status 0 1)"
check "cleanup FAILED, finalisation 6: stays 6" "6" "$(cleanup_status 6 1)"
check "cleanup FAILED, incomplete 7: stays 7" "7" "$(cleanup_status 7 1)"
check "the runner's trap uses exit, not return, to raise it" "yes" \
      "$(has_text "$(cat "$RUNNER")" 'if [ "$rc" -eq 0 ]; then')"
check "and says why return would not work" "yes" \
      "$(has_text "$(cat "$RUNNER")" "An EXIT trap's return")"
check "a cleanup failure is a run failure whatever the gates said" "yes" \
      "$(has_text "$(cat "$RUNNER")" "this run is a failure regardless of its gates")"

# ================================================================================
echo
echo "the real runner: the guards that fire before it needs a host"
# ================================================================================
# These run scripts/run_controlled.sh itself. Everything above the host check is
# reachable on any machine, and that is where the grant, the mode exclusivity, the
# worker-count assertion and the budget banner live.
PB="$WORK/pybin"; printf '#!/bin/sh\nexit 0\n' > "$PB"; chmod +x "$PB"
mkdir -p "$WORK/clone"
# The runner refuses a writable package clone: a clone this user can write to may
# already have been modified, and the run could modify it further.
chmod a-w "$WORK/clone"
printf 'a.py\t%s\t12\t1722906835056193900\n' "$(printf 'f%.0s' $(seq 64))" \
  > "$WORK/manifest"
runner() {
  env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
      -u WOA23_D1_GRANTED -u WOA23_S2PERF_GRANTED "$@" \
      "$RUNNER" --s2-perf --python-binary "$PB" --package-clone "$WORK/clone" \
      --clone-manifest "$WORK/manifest" 2>&1
}
rc_of() { runner "$@" >/dev/null 2>&1; echo $?; }
check "no grant: exit 3" "3" "$(rc_of)"
check "a C1 grant does not authorise it" "3" "$(rc_of WOA23_S2_C1_GRANTED=yes)"
check "a C2 grant does not either" "3" "$(rc_of WOA23_S2_C2_GRANTED=yes)"
check "a D1 grant does not either" "3" "$(rc_of WOA23_D1_GRANTED=yes)"
check "a D2b grant does not either" "3" "$(rc_of WOA23_D2B_GRANTED=yes)"
# THIS ASSERTION USED TO DEPEND ON A PREVIOUS RUN'S LEFTOVERS, and passed only on a
# dirty tree.
#
# `run_controlled.sh` reaches exit 4 here through `refuse_label_collision "$HERE" "$LABEL"`
# (line ~1010), which fires when `$HERE/results/<label>_*` or `$HERE/run/<label>` already
# exist. Later, at line ~1062, a run that gets past that guard WRITES
# `results/${LABEL}_shutdown_budget.json` itself. So the first run in a clean tree sailed
# past the guard, continued to the production-interpreter check (~1097) and exited 1, while
# every later run found the artefact the first one left and exited 4.
#
# Measured across one batch set: batch 1 precheck untracked 0 -> exit 1, FAIL; batches 2
# and 3 precheck untracked 2 -> exit 4, pass. The assertion was testing whether some
# earlier suite had run, not whether the guard works.
#
# THE GUARD AND ITS MEANING ARE UNCHANGED. What changes is that this test now builds the
# precondition the guard exists to detect, in a THROWAWAY FIXTURE, and asserts the REAL
# guard in `run_controlled.sh` against it. Nothing is stubbed: the runner, its libraries
# and `refuse_label_collision` are the real ones. The fixture root is a directory of
# symlinks to this tree, so `$HERE` resolves to the fixture and the guard reads the
# fixture's `results/` and `run/` -- never the shared tree.
#
# This also lands well before anything expensive: the collision guard is at ~1010, the
# interpreter check at ~1097 and `uv sync` at ~1125, so no benchmark, venv build or
# interpreter probe is reached.
FIX="$WORK/labelfix"
mkdir -p "$FIX" "$FIX/results" "$FIX/run"
for _entry in scripts deploy bench api conf; do
  [ -e "$ROOT/$_entry" ] && ln -s "$ROOT/$_entry" "$FIX/$_entry"
done
fixture_runner() {
  env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
      -u WOA23_D1_GRANTED -u WOA23_S2PERF_GRANTED "$@" \
      "$FIX/scripts/run_controlled.sh" --s2-perf --python-binary "$PB" \
      --package-clone "$WORK/clone" --clone-manifest "$WORK/manifest" 2>&1
}
fixture_rc() { fixture_runner "$@" >/dev/null 2>&1; echo $?; }

# The fixture starts CLEAN, and is asserted clean: if a stray artefact were present the
# guard would fire for the wrong reason and the next assertion would pass vacuously.
# `ls -1 dirA dirB` prints a `dirA:` header line for each directory when given more than
# one, so counting its output counted the headers and reported 2 for two EMPTY directories.
# `find -mindepth 1` counts entries and nothing else.
check "the fixture starts with no label artefacts" "0" \
      "$(find "$FIX/results" "$FIX/run" -mindepth 1 2>/dev/null | grep -c .)"

# The precondition this test needs, created BY THIS TEST: exactly what the guard looks for.
: > "$FIX/results/s2perf_shutdown_budget.json"
mkdir -p "$FIX/run/s2perf"
check "the fixture now carries the state the guard detects" "yes" \
      "$([ -f "$FIX/results/s2perf_shutdown_budget.json" ] && [ -d "$FIX/run/s2perf" ] \
         && echo yes || echo no)"

check "with its own grant it proceeds to the host check" "4" \
      "$(fixture_rc WOA23_S2PERF_GRANTED=yes)"
check "and the refusal names the colliding evidence" "yes" \
      "$(has_text "$(fixture_runner WOA23_S2PERF_GRANTED=yes)" "already names evidence on disk")"

# REGRESSION 1 — ORDER INDEPENDENCE. Repeating it must give the same answer, because the
# precondition is the fixture's and not some other suite's side effect.
check "the guard verdict repeats (order independence)" "4" \
      "$(fixture_rc WOA23_S2PERF_GRANTED=yes)"

# REGRESSION 2 — THE ABSENT PRECONDITION IS RECORDED, not assumed. A clean fixture must
# NOT reach exit 4 by this route; that is precisely the state that used to fail. Its exit
# is asserted to be something other than 4, so a guard that fired unconditionally -- which
# would make the assertion above vacuous -- is caught here.
FIX2="$WORK/labelfix-clean"
mkdir -p "$FIX2" "$FIX2/results" "$FIX2/run"
for _entry in scripts deploy bench api conf; do
  [ -e "$ROOT/$_entry" ] && ln -s "$ROOT/$_entry" "$FIX2/$_entry"
done
clean_rc="$(env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
      -u WOA23_D1_GRANTED -u WOA23_S2PERF_GRANTED WOA23_S2PERF_GRANTED=yes \
      "$FIX2/scripts/run_controlled.sh" --s2-perf --python-binary "$PB" \
      --package-clone "$WORK/clone" --clone-manifest "$WORK/manifest" >/dev/null 2>&1; echo $?)"
check "without the precondition the collision guard does NOT fire" "yes" \
      "$([ "$clean_rc" != 4 ] && echo yes || echo no)"
echo "  note: a clean fixture exits $clean_rc here — recorded, not asserted as a value,"
echo "        because it depends on host state beyond this guard."
banner="$(runner WOA23_S2PERF_GRANTED=yes)"
check "the ceiling is 496 per arm" "yes" "$(has_text "$banner" "496 per arm")"
check "and 992 total" "yes" "$(has_text "$banner" "992 total")"
check "the pilot is budgeted at 208, per case x cases" "yes" \
      "$(has_text "$banner" "noise pilot 208")"
check "the contract gate is inside the number" "yes" \
      "$(has_text "$banner" "+ contract 64")"
check "so is the symmetric warm-up" "yes" \
      "$(has_text "$banner" "+ symmetric warm-up 16")"
check "and the latency samples" "yes" "$(has_text "$banner" "+ latency 176")"
check "bootstrap resampling is excluded in writing" "yes" \
      "$(has_text "$banner" "Bootstrap resampling issues NO HTTP")"
check "production is stated as zero requests" "yes" \
      "$(has_text "$banner" "0 requests")"
check "the margin is an APPROVED threshold, still not an SLA" "yes" \
      "$(has_text "$banner" "APPROVED ENGINEERING THRESHOLD: 0.05")"
check "the superseded pre-approval wording is gone" "no" \
      "$(has_text "$banner" "PROPOSED ENGINEERING THRESHOLD")"
# The shutdown budget is asserted AFTER the host gate, so it is not reachable on
# this machine — it is checked from the source, and so is its position relative to
# the first launch, which is what makes it an assertion rather than a report.
check "the shutdown budget is asserted before any service starts" "yes" \
      "$("$PY" - "$RUNNER" <<'PYEOF'
import sys
s = open(sys.argv[1]).read()
print("yes" if s.index("assert_shutdown_budget") < s.index("start_tracked ") else "no")
PYEOF
)"
check "and refuses to start when the inequality does not hold" "yes" \
      "$(has_text "$(cat "$RUNNER")" "assert_shutdown_budget || exit 4")"
# One worker per arm is the mode's own setting; asserting production's count here
# would make the median a median over an allocation policy.
check "--expected-workers is refused by this mode" "2" \
      "$(env -u WOA23_D2B_GRANTED WOA23_S2PERF_GRANTED=yes "$RUNNER" --s2-perf \
           --python-binary "$PB" --package-clone "$WORK/clone" \
           --clone-manifest "$WORK/manifest" --expected-workers 2 \
           >/dev/null 2>&1; echo $?)"
check "the s2perf launch line sets one worker" "yes" \
      "$(has_text "$(cat "$RUNNER")" '-w 1')"
check "--s2-perf and --c1 are mutually exclusive" "2" \
      "$("$RUNNER" --s2-perf --c1 >/dev/null 2>&1; echo $?)"
check "--s2-perf and --d1 too" "2" \
      "$("$RUNNER" --s2-perf --d1 >/dev/null 2>&1; echo $?)"
echo
echo "every stage has exactly one count owner"
# The contract stage had two, and the run accumulated 528 per arm under a 992
# authorisation with nothing failing. The invariant is per stage, so it is checked
# per stage: one call site each, in the file that owns it.
owners="$("$PY" - "$HERE" <<'PYEOF'
import pathlib, re, sys
scripts = pathlib.Path(sys.argv[1])
# Every place in the harness that records a request, excluding the counter library
# itself and the tests.
sites = {}
for f in sorted(scripts.glob("*.sh")):
    if f.name.startswith("test_") or f.name == "lib_requests.sh":
        continue
    for n, line in enumerate(f.read_text().splitlines(), 1):
        m = re.search(r'request_(?:add|attempt)\s+\S+\s+("?)([a-z_]+)\1', line)
        if m:
            sites.setdefault(m.group(2), []).append(f"{f.name}:{n}")
# The dynamic stage name in s2perf_record_counts covers three stages in one loop;
# it is one call site and one owner for each of them.
print("|".join(f"{k}={len(v)}" for k, v in sorted(sites.items())))
PYEOF
)"
check "every named stage is recorded from exactly one place" \
      "characterization=1|contract=1|readiness=1|recovery=1|store_probe=1" "$owners"
# symmetric_warmup, latency and noise_pilot are recorded by one loop over a stage
# variable, so they carry no literal call site — the loop is their single owner.
check "and the three measured stages have exactly one loop between them" "1" \
      "$(printf '%s\n' "$CHAINSRC" | grep -c 'request_add "\$arm" "\$stage"')"
check "the measured stages are recorded by that one loop" "yes" \
      "$([ "$(has_text "$CHAINSRC" 'for stage in symmetric_warmup latency noise_pilot')" = yes ] \
         && echo yes || echo no)"
check "and nothing outside the chain records them" "0" \
      "$(printf '%s\n' "$RUNSRC" | grep -cE 'request_add .*(symmetric_warmup|latency|noise_pilot)')"

echo
echo "the runner drives the chain this test drove"
check "the runner calls the chain this test drove" "yes" \
      "$([ "$(has_text "$(cat "$RUNNER")" "s2perf_contract ")" = yes ] \
         && [ "$(has_text "$(cat "$RUNNER")" "s2perf_warmup ")" = yes ] \
         && [ "$(has_text "$(cat "$RUNNER")" "s2perf_latency ")" = yes ] \
         && [ "$(has_text "$(cat "$RUNNER")" "s2perf_pilot ")" = yes ] \
         && [ "$(has_text "$(cat "$RUNNER")" "s2perf_finish ")" = yes ] \
         && echo yes || echo no)"
check "and sources it" "yes" "$(has_text "$(cat "$RUNNER")" "lib_s2perf.sh")"

echo
# Diagnostics BEFORE the contract line, never after it.
keep_notice
suite_summary "$pass" "$fail"
