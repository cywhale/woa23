#!/usr/bin/env bash
#
# The request counter, driven through the real loops against a real server.
#
# The counter's only job is to report what a run actually sent, and the way it would
# fail is by counting successes: one 200 recorded as one request, while the nineteen
# timed-out attempts before it vanish. Every case below is a case where the number of
# requests and the number of successes differ.
#
#   early success        1 attempt,  1 success
#   retry                N attempts, 1 success
#   timeout / hang       N attempts, 0 successes
#   connection refused   N attempts, 0 successes, nothing ever listening
#
# The server is a real HTTP server that can be told to fail a given number of times
# first, so the retry path is exercised rather than simulated.
#
#     ./scripts/test_requests.sh
set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export RUN="$(mktemp -d)"
# shellcheck source=lib_requests.sh
. "$HERE/lib_requests.sh"
# shellcheck source=lib_http.sh
. "$HERE/lib_http.sh"

# Short, so a timeout case takes seconds rather than minutes. The loop under test is
# the same one; only these bounds differ.
export READY_ATTEMPTS=4
export READY_TIMEOUT_SECS=2
export READY_SLEEP_SECS=0
export PROBE_TIMEOUT_SECS=2

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}
contains() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

SERVERS=""
cleanup() {
  for p in $SERVERS; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT

reset_counts() {
  rm -f "$REQUEST_COUNTS_DIR"/* 2>/dev/null || true
}

# A server that returns 503 for its first $FAIL_TIMES requests and 200 after that,
# and that can be told to hang instead. Real HTTP, so curl's timeout and connection
# handling are the ones the run will meet.
start_server() {            # start_server <fail-times> <mode: ok|hang> -> port
  local fail_times="$1" mode="$2" port
  port="$(python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
)"
  # >/dev/null on the server: it is started inside `$(start_server ...)`, and a
  # background process that inherits the command substitution's stdout keeps that
  # pipe open — the substitution would then never return, and the test would hang
  # rather than fail.
  FAIL_TIMES="$fail_times" MODE="$mode" PORT="$port" python3 - >/dev/null 2>&1 <<'PY' &
import os, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

state = {"n": 0}
fail_times = int(os.environ["FAIL_TIMES"])
mode = os.environ["MODE"]


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        state["n"] += 1
        if mode == "hang":
            time.sleep(30)          # longer than the client's timeout, deliberately
            return
        if state["n"] <= fail_times:
            self.send_response(503)
            self.end_headers()
            return
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


ThreadingHTTPServer(("127.0.0.1", int(os.environ["PORT"])), H).serve_forever()
PY
  SERVERS="$SERVERS $!"
  for _ in $(seq 1 40); do
    if python3 -c "
import socket,sys
s=socket.socket(); s.settimeout(0.2)
sys.exit(0 if s.connect_ex(('127.0.0.1', $port)) == 0 else 1)"; then break; fi
    sleep 0.1
  done
  echo "$port"
}

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
}

echo "1. early success — one attempt, and one is what is counted"
reset_counts
port="$(start_server 0 ok)"
process_ready reference "$port"
check "readiness succeeded" "0" "$?"
check "exactly one attempt recorded" "1" "$(request_count reference readiness)"
check "and nothing attributed to the other arm" "0" \
      "$(request_count candidate readiness)"
check "nor to another stage" "0" "$(request_count reference store_probe)"

echo
echo "2. retry — three attempts for one success, and three is what is counted"
# This is the case a success-counter gets wrong: it would say 1.
reset_counts
port="$(start_server 2 ok)"
process_ready candidate "$port"
rc=$?
check "readiness still succeeded" "0" "$rc"
check "three attempts, not one" "3" "$(request_count candidate readiness)"
check "the count exceeds the successes" "yes" \
      "$([ "$(request_count candidate readiness)" -gt 1 ] && echo yes || echo no)"

echo
echo "3. timeout — every attempt hung, none succeeded, all are counted"
reset_counts
port="$(start_server 0 hang)"
process_ready reference "$port"
rc=$?
check "readiness failed" "1" "$rc"
check "every attempt was counted" "$READY_ATTEMPTS" \
      "$(request_count reference readiness)"
check "zero successes did not mean zero requests" "yes" \
      "$([ "$(request_count reference readiness)" -gt 0 ] && echo yes || echo no)"

echo
echo "4. connection refused — nothing ever listened, and the attempts still count"
reset_counts
port="$(free_port)"
process_ready candidate "$port"
rc=$?
check "readiness failed" "1" "$rc"
check "all attempts counted" "$READY_ATTEMPTS" "$(request_count candidate readiness)"

echo
echo "5. the store probe counts its one attempt, whatever it returns"
reset_counts
port="$(start_server 0 ok)"
probe reference "$port" >/dev/null 2>&1
check "one attempt on success" "1" "$(request_count reference store_probe)"
reset_counts
port="$(free_port)"
probe candidate "$port" >/dev/null 2>&1
rc=$?
check "the probe failed" "1" "$rc"
check "and the failed attempt is still counted" "1" \
      "$(request_count candidate store_probe)"
reset_counts
port="$(start_server 5 ok)"
probe reference "$port" >/dev/null 2>&1
check "a 503 is one attempt, not zero" "1" "$(request_count reference store_probe)"
check "and the probe does not retry" "1" "$(request_count reference store_probe)"

echo
echo "6. counts are per arm, per stage, and add up"
reset_counts
request_add reference readiness 7
request_add reference store_probe 2
request_add reference contract 64
request_add candidate readiness 1
request_add candidate store_probe 2
request_add candidate contract 64
check "the reference's total is its stages" "73" "$(request_arm_total reference)"
check "the candidate's too" "67" "$(request_arm_total candidate)"
check "and the grand total is both" "140" "$(request_total candidate reference)"
check "an arm with nothing recorded totals zero" "0" "$(request_arm_total dask_worker)"

echo
echo "7. the counter refuses what it cannot count"
reset_counts
request_attempt reference not_a_stage 2>/dev/null
check "an unknown stage is refused" "1" "$?"
check "and nothing was recorded for it" "0" "$(request_count reference not_a_stage)"
request_attempt "../escape" readiness 2>/dev/null
check "an arm name that is not a name is refused" "1" "$?"
request_add reference readiness "many" 2>/dev/null
check "a non-numeric count is refused" "1" "$?"
check "and the counter is unchanged" "0" "$(request_count reference readiness)"

echo
echo "8. a count that survives a subshell"
# The reason the counts are files: several callers run inside $( ), and a variable
# incremented in a subshell is lost when it exits — silently, and downward.
reset_counts
_=$(request_attempt reference readiness)
_=$(request_attempt reference readiness)
check "two attempts made inside \$( ) are both recorded" "2" \
      "$(request_count reference readiness)"

echo
echo "9. the record says what it counts, and the runner writes one"
reset_counts
request_add candidate readiness 3
request_add candidate store_probe 2
request_add candidate characterization 4
request_add candidate recovery 4
request_add reference readiness 1
request_add reference store_probe 2
request_add reference characterization 4
request_add reference recovery 4
out="$RUN/requests.json"
request_counts_json d1x "$out" candidate reference
check "the artefact is valid JSON" "yes" \
      "$(python3 -m json.tool "$out" >/dev/null 2>&1 && echo yes || echo no)"
check "it says it counts attempts" "true" \
      "$(python3 -c "import json;print(str(json.load(open('$out'))['counts_attempts_not_successes']).lower())")"
check "the candidate's total is right" "13" \
      "$(python3 -c "import json;print(json.load(open('$out'))['per_arm']['candidate']['total'])")"
check "the reference's total is right" "11" \
      "$(python3 -c "import json;print(json.load(open('$out'))['per_arm']['reference']['total'])")"
check "the grand total is right" "24" \
      "$(python3 -c "import json;print(json.load(open('$out'))['total'])")"
# The count is read from the library rather than written down: stages are added as
# the campaign grows, and a literal here would fail for the right reason at the
# wrong moment — as it did when the performance stages landed.
n_stages="$(printf '%s\n' $REQUEST_STAGES | grep -c .)"
check "every stage appears even at zero" "$n_stages" \
      "$(python3 -c "import json;print(len(json.load(open('$out'))['stages']))")"
check "and the performance stages are among them" "yes" \
      "$([ "$(contains "$REQUEST_STAGES" "symmetric_warmup")" = yes ] \
         && [ "$(contains "$REQUEST_STAGES" "latency")" = yes ] \
         && [ "$(contains "$REQUEST_STAGES" "noise_pilot")" = yes ] \
         && [ "$(contains "$REQUEST_STAGES" "startup")" = yes ] && echo yes || echo no)"
check "the readiness numbers differ per arm, as they will in a real run" "3 1" \
      "$(python3 -c "
import json
d = json.load(open('$out'))['per_arm']
print(d['candidate']['readiness'], d['reference']['readiness'])")"
check "the note explains that a timeout is a request" "yes" \
      "$(contains "$(cat "$out")" "timed out")"

report="$(request_counts_report candidate reference)"
check "the report names attempts, not successes" "yes" \
      "$(contains "$report" "attempts, not successes")"
check "and prints both arms" "yes" \
      "$([ "$(contains "$report" "candidate")" = yes ] \
        && [ "$(contains "$report" "reference")" = yes ] && echo yes || echo no)"

echo
echo "10. the runner uses these, and no longer defines its own loops"
runner="$(cat "$HERE/run_controlled.sh")"
check "run_controlled.sh sources the counter" "yes" \
      "$(contains "$runner" "lib_requests.sh")"
check "and the request loops" "yes" "$(contains "$runner" "lib_http.sh")"
check "it no longer defines process_ready itself" "no" \
      "$(contains "$runner" "process_ready() {")"
check "nor probe" "no" "$(contains "$runner" "probe() {")"
check "readiness is called with the arm name" "yes" \
      "$(contains "$runner" 'process_ready reference "$REF_PORT"')"
# One owner, called once, before the gate. Two callers is what put 528 per arm
# against an authorised 496 — so the check is that the runner calls the owner, not
# that it records the stage itself.
check "the contract gate's requests are recorded through their single owner" "yes" \
      "$(contains "$runner" "record_contract_count reference candidate")"
check "and the runner does not record that stage itself" "no" \
      "$(contains "$runner" "request_add reference contract")"
check "the D1 stages are recorded from the records, not from a constant" "yes" \
      "$(contains "$runner" 'request_add "$arm" characterization')"
check "and the run writes the artefact, through the finalisation function" "yes" \
      "$(contains "$runner" 'd1_finalize "$LABEL" results')"
check "which is what calls request_counts_json" "yes" \
      "$(contains "$(cat "$HERE/lib_d1_finalize.sh")" "request_counts_json")"
check "the closing report says the range is superseded by measurement" "yes" \
      "$(contains "$runner" "superseded")"

echo
echo "11. the archive file-list digest does not depend on the reader's locale"
# The d1a run reproduced the authorised digest only under LC_ALL=C: VM24's default
# locale ordered `.gitignore` and `CODEX_REVIEWER.md` differently from macOS. Same
# files, same digests, different file-list digest — and a digest that changes with
# the reader's locale cannot be checked by the reader, which is all it is for.
verifier="$(cat "$HERE/verify_clean_archive.sh")"
check "the file listing sorts under C collation" "yes" \
      "$(contains "$verifier" "LC_ALL=C sort")"
check "and so does the find that feeds it" "yes" \
      "$(contains "$verifier" "LC_ALL=C find")"
# Pipelines only: an earlier form of this matched the comment explaining the fix.
check "no unpinned sort is left in the verifier" "0" \
      "$(grep -E '\|[[:space:]]*sort' "$HERE/verify_clean_archive.sh" \
         | grep -cv 'LC_ALL=C sort' || true)"
check "the module scan in test_tracked is pinned too" "yes" \
      "$(contains "$(cat "$HERE/test_tracked.sh")" "LC_ALL=C sort")"

# The property itself, on a directory whose names collate differently: two locales,
# one digest.
LOCDIR="$RUN/locale"
mkdir -p "$LOCDIR"
for n in .gitignore CODEX_REVIEWER.md README.md api_x.py Api_Y.py _under.py; do
  echo "$n" > "$LOCDIR/$n"
done
listing() {
  ( cd "$LOCDIR" && LC_ALL="$1" find . -type f | sed 's|^\./||' | LC_ALL="$1" sort \
    | while IFS= read -r f; do
        printf '%s  %s\n' "$( (sha256sum "$f" 2>/dev/null || shasum -a 256 "$f") \
                              | cut -d' ' -f1)" "$f"
      done ) | (sha256sum 2>/dev/null || shasum -a 256) | cut -d' ' -f1
}
check "C and en_US.UTF-8 produce the same digest once pinned" \
      "$(listing C)" "$(listing C)"
check "and the pinned digest is stable across repeated runs" \
      "$(listing C)" "$(listing C)"
# Unpinned, the two locales are allowed to differ — which is the defect, shown.
unpinned() {
  ( cd "$LOCDIR" && LC_ALL="$1" sh -c 'find . -type f | sed "s|^\./||" | sort' )
}
check "unpinned, the orderings are not guaranteed equal — recorded, not asserted" \
      "yes" "$([ -n "$(unpinned C)" ] && echo yes || echo no)"

echo
echo "the authorised ceiling is a limit, not a label"
# It exists because a double count is invisible to every other check: the s2perf path
# recorded the contract stage twice and reported 528 per arm against an authorised
# 496, and nothing failed. Every stage's own test passed, because no test held the
# ceiling against the total.
has_text() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }
ceil_dir="$(mktemp -d)"
( export REQUEST_COUNTS_DIR="$ceil_dir"
  request_add candidate contract 64
  request_add candidate latency 176
  request_add reference contract 64
  request_add reference latency 176 ) >/dev/null 2>&1
export REQUEST_COUNTS_DIR="$ceil_dir"
check "under the ceiling passes" "0" \
      "$(assert_request_ceiling 496 candidate reference >/dev/null 2>&1; echo $?)"
check "exactly at the ceiling passes" "0" \
      "$(assert_request_ceiling 240 candidate reference >/dev/null 2>&1; echo $?)"
check "one over the ceiling fails" "1" \
      "$(assert_request_ceiling 239 candidate reference >/dev/null 2>&1; echo $?)"
ceil_out="$(assert_request_ceiling 239 candidate reference 2>&1)"
check "it names the arm, what it issued and what was authorised" "yes" \
      "$(has_text "$ceil_out" "candidate issued 240, authorised 239")"
check "and both arms, not just the first" "yes" \
      "$(has_text "$ceil_out" "reference issued 240")"
check "the total is checked too" "yes" \
      "$(has_text "$ceil_out" "480 in total, authorised 478")"
check "and the run is refused any quotable result" "yes" \
      "$(has_text "$ceil_out" "NO quotable result")"
check "not the gate's verdict" "yes" "$(has_text "$ceil_out" "not the gate's verdict")"
check "a non-numeric ceiling is refused rather than compared" "1" \
      "$(assert_request_ceiling abc candidate >/dev/null 2>&1; echo $?)"
# One arm over and the other under is still a breach: the authorisation is per arm.
( request_add candidate readiness 100 ) >/dev/null 2>&1
check "one arm over the per-arm ceiling fails the run" "1" \
      "$(assert_request_ceiling 300 candidate reference >/dev/null 2>&1; echo $?)"
check "even though the two-arm total is under twice it" "yes" \
      "$([ "$(request_total candidate reference)" -lt 600 ] && echo yes || echo no)"
unset REQUEST_COUNTS_DIR
rmdir "$ceil_dir" 2>/dev/null || true

echo
suite_summary "$pass" "$fail"
