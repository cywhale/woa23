#!/usr/bin/env bash
#
# End-to-end offline tests for scripts/run_c2_cycles.sh.
#
# bench/test_c2_summary.py covers the summary's own logic. This covers the path the
# result actually travels: three shell invocations, a summary process, a case
# statement and an exit status — which is where a distinct outcome most easily gets
# flattened back into "it passed" or misread as a crash.
#
# The real run_controlled.sh is replaced by a stub that writes the artefacts a cycle
# would write and records that it was called. Nothing starts a service, binds a port
# or sends a request. `uv` is shadowed by a shim that execs this interpreter, so the
# repository's venv is neither synced nor touched.
#
#     ./scripts/test_c2_driver.sh

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
REAL_PY="${REAL_PY:-$(command -v python3)}"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}
has_text() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

# ---------------------------------------------------------------- the harness ---
# $1 = the seed each cycle reports ("distinct" or "same")
# $2 = a cycle number that should fail, or "" for none
# $3 = a cycle number after which leftover run state appears, or ""
# $4 = the --graceful-timeout the arms report, default 10. 30 is gunicorn's own
#      default, which is what the arms inherited when C2 cycle 1 stranded an
#      arbiter, and it is larger than STOP_WAIT_SECS.
build_env() {
  local seeds="$1" fail_at="$2" leftover_after="$3" grace="${4:-10}"
  local T; T="$(mktemp -d)"
  mkdir -p "$T/.local/bin" "$T/dev2026/scripts" "$T/dev2026/run" "$T/dev2026/results"

  # `uv run python -m X` -> `python -m X`. Keeps the real venv out of the test.
  # `uv run python -m X ...` -> `<this python> -m X ...`. Both leading words are
  # dropped: leaving `python` in place makes the interpreter try to open a file
  # called "python", which fails in a way that looks nothing like the thing under
  # test.
  cat > "$T/.local/bin/uv" <<UVEOF
#!/bin/sh
[ "\$1" = run ] && shift
case "\$1" in python|python3|python3.*) shift ;; esac
exec "$REAL_PY" "\$@"
UVEOF
  chmod +x "$T/.local/bin/uv"

  cp "$HERE/run_c2_cycles.sh" "$T/dev2026/scripts/"
  ln -s "$REPO/bench" "$T/dev2026/bench"

  cat > "$T/dev2026/scripts/run_controlled.sh" <<STUBEOF
#!/usr/bin/env bash
# Stub. Writes what a --c2-cycle invocation writes, records that it ran, and
# never starts anything.
set -euo pipefail
HERE="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")/.." && pwd)"
label=""
prev=""
for a in "\$@"; do
  [ "\$prev" = "--label" ] && label="\$a"
  prev="\$a"
done
n=\$(( \$(cat "\$HERE/run/.calls" 2>/dev/null || echo 0) + 1 ))
echo "\$n" > "\$HERE/run/.calls"
printf '%s\n' "\$label" >> "\$HERE/run/.labels"

if [ "$fail_at" = "\$n" ]; then
  echo "stub: cycle \$n fails" >&2
  exit 1
fi

gate=PASS
case "$seeds" in
  distinct) seed="\$(printf '%064d' "\$n")" ;;
  same)     seed="\$(printf '%064d' 7)" ;;
esac
cat > "\$HERE/results/\${label}_contract.json" <<JSON
{"kind":"contract_diff","gate":"\$gate","variant":"5.2B",
 "results":[{"id":"C1","verdict":"MATCH",
   "reference_order":{"body_sha256":"b","row_order_sha256":"r","columns":["lon"],"n_rows":1},
   "candidate_order":{"body_sha256":"b","row_order_sha256":"c","columns":["lon"],"n_rows":1}}]}
JSON
for arm in candidate reference; do
  cat > "\$HERE/results/\${label}_interp_\${arm}.json" <<JSON
{"label":"\$arm","seed_digest":"\$seed","problems":[],"hashseed_env":null,
 "flags":{"no_site":1,"ignore_environment":0,"hash_randomization":1},
 "hash_probe":{"strings":["a","b"],"hashes":[1,2]}}
JSON
done
echo '{"kind":"s2_package_clone_environment"}' > "\$HERE/results/\${label}_environment.json"
# The shutdown budget and the arms' launch argv. The stub writes them because the
# real runner does: the summary reads back what each cycle allowed its arms at stop
# time, and a cycle that produced no such record is not summarisable.
cat > "\$HERE/results/\${label}_shutdown_budget.json" <<JSON
{"kind":"shutdown_budget","label":"\$label","arm_graceful_timeout":$grace,
 "stop_wait_secs":20,"stop_wait_source":"default","holds":true}
JSON
for arm in candidate reference; do
  app=woa23_app:app
  [ "\$arm" = candidate ] && app=api.app:app
  cat > "\$HERE/results/\${label}_meta_\${arm}.json" <<JSON
{"kind":"backend_meta","label":"\$arm",
 "launch_argv":["python3.11","-S","-m","gunicorn","\$app","-w","2",
   "-k","uvicorn.workers.UvicornWorker","--graceful-timeout","$grace",
   "-b","127.0.0.1:18071","--timeout","120"]}
JSON
done

# A cycle that cannot confirm its cleanup leaves run state behind. This is how the
# driver is told, and the next cycle must refuse to start on it.
if [ "$leftover_after" = "\$n" ]; then
  echo "1234:99999" > "\$HERE/run/leftover.tree"
fi
echo "stub: cycle \$n (\$label) complete, cleanup verified"
STUBEOF
  chmod +x "$T/dev2026/scripts/run_controlled.sh"
  printf '%s' "$T"
}

drive() {                   # drive <tempdir> [extra args...]; echoes exit code
  local T="$1"; shift
  ( cd "$T/dev2026" && env HOME="$T" WOA23_S2_C2_GRANTED=yes \
      ./scripts/run_c2_cycles.sh \
        --python-binary /bin/sh --package-clone /tmp --clone-manifest /etc/hostname \
        --workdir-base "$T/work" --candidate-port 18071 --reference-port 18072 \
        --scheduler-port 18799 "$@" >"$T/out.txt" 2>"$T/err.txt" ) && echo 0 || echo $?
}

# ============================================================ three distinct ===
echo "three cycles, distinct seeds: a plain PASS"
T="$(build_env distinct "" "")"
rc="$(drive "$T")"
out="$(cat "$T/out.txt")"
check "the driver exits 0" "0" "$rc"
check "exactly three cycles ran" "3" "$(cat "$T/dev2026/run/.calls")"
check "each cycle had its own label" "c2_cycle1 c2_cycle2 c2_cycle3" \
      "$(tr '\n' ' ' < "$T/dev2026/run/.labels" | sed 's/ $//')"
check "the outcome is named PASS" "yes" "$(has_text "$out" "C2 OUTCOME: PASS")"
check "and the driver says so" "yes" "$(has_text "$out" "C2 complete: PASS")"
check "a summary was written" "yes" \
      "$([ -f "$T/dev2026/results/c2_summary.json" ] && echo yes || echo no)"
rm -r "$T"

# ================================================== three identical -> exit 5 ===
echo
echo "three cycles, identical seeds: PASS_WITH_INSUFFICIENT_SEED_DIVERSITY"
T="$(build_env same "" "")"
rc="$(drive "$T")"
out="$(cat "$T/out.txt")"; err="$(cat "$T/err.txt")"
sum="$T/dev2026/results/c2_summary.json"

# 1. not flattened to a plain PASS.
check "the driver does NOT exit 0" "yes" "$([ "$rc" != 0 ] && echo yes || echo no)"
check "it exits 5 specifically" "5" "$rc"
check "the outcome is not the string PASS on its own" "no" \
      "$(has_text "$out" "C2 OUTCOME: PASS
")"
check "the outcome is named in full" "yes" \
      "$(has_text "$out" "C2 OUTCOME: PASS_WITH_INSUFFICIENT_SEED_DIVERSITY")"
check "the driver's closing line names it in full too" "yes" \
      "$(has_text "$out" "C2 complete: PASS_WITH_INSUFFICIENT_SEED_DIVERSITY")"
check "and never prints a bare 'C2 complete: PASS'" "no" \
      "$(has_text "$out" "C2 complete: PASS
")"

# 2. every cycle still ran to completion, so every cycle's cleanup ran.
check "all three cycles still ran" "3" "$(cat "$T/dev2026/run/.calls")"
check "each reported its cleanup verified" "3" \
      "$(grep -c 'cleanup verified' "$T/out.txt")"
check "no run state was left behind" "0" \
      "$(find "$T/dev2026/run" -name '*.tree' -o -name '*.pid' -o -name '*.uncertain' \
         | wc -l | tr -d ' ')"

# 3. no fourth cycle, and no way to ask for one.
check "there is no fourth cycle" "3" "$(cat "$T/dev2026/run/.calls")"
check "only three labels were ever used" "3" "$(sort -u "$T/dev2026/run/.labels" | wc -l | tr -d ' ')"
check "--cycles is refused outright" "2" "$(drive "$T" --cycles 4)"

# 4. the summary keeps the whole outcome, not just the exit code.
check "the summary records the full outcome" "PASS_WITH_INSUFFICIENT_SEED_DIVERSITY" \
      "$("$REAL_PY" -c "import json,sys;print(json.load(open(sys.argv[1]))['outcome'])" "$sum")"
check "and the exit code it produced" "5" \
      "$("$REAL_PY" -c "import json,sys;print(json.load(open(sys.argv[1]))['exit_code'])" "$sum")"
check "the semantic gate is preserved as PASS" "PASS" \
      "$("$REAL_PY" -c "import json,sys;print(json.load(open(sys.argv[1]))['contract']['gate'])" "$sum")"
check "the seed observation is preserved as INSUFFICIENT" "INSUFFICIENT" \
      "$("$REAL_PY" -c "import json,sys;print(json.load(open(sys.argv[1]))['seed_diversity']['status'])" "$sum")"
check "the two are separate fields, not one verdict" "yes" \
      "$("$REAL_PY" -c "
import json,sys
d=json.load(open(sys.argv[1]))
print('yes' if d['contract']['gate']!=d['seed_diversity']['status'] else 'no')" "$sum")"
check "per-cycle gates are kept" "['PASS', 'PASS', 'PASS']" \
      "$("$REAL_PY" -c "import json,sys;print(json.load(open(sys.argv[1]))['contract']['per_cycle'])" "$sum")"
check "order stability is kept as its own section" "yes" \
      "$("$REAL_PY" -c "
import json,sys
print('yes' if 'order_stability' in json.load(open(sys.argv[1])) else 'no')" "$sum")"

# 5. not an unclassified harness crash.
check "nothing was written to stderr" "" "$err"
check "the outcome did not fall through to the failure branch" "no" \
      "$(has_text "$out$err" "C2 complete: FAIL")"
check "no traceback" "no" "$(has_text "$out$err" "Traceback")"
check "the exit code is not one a crash would produce" "yes" \
      "$([ "$rc" != 1 ] && [ "$rc" != 2 ] && [ "$rc" != 127 ] && [ "$rc" != 130 ] \
         && echo yes || echo no)"
check "and the run explains why it is not a plain pass" "yes" \
      "$(has_text "$out" "be a false report")"
check "and that no fourth cycle may be added without authorisation" "yes" \
      "$(has_text "$out" "none may be added without")"
rm -r "$T"

# =========================================================== a failing cycle ===
echo
echo "a failing cycle stops the run and is not an INSUFFICIENT result"
T="$(build_env distinct 2 "")"
rc="$(drive "$T")"
check "the driver exits non-zero" "1" "$rc"
check "it stopped at the failing cycle" "2" "$(cat "$T/dev2026/run/.calls")"
check "the third cycle never ran" "no" \
      "$(has_text "$(cat "$T/dev2026/run/.labels")" "c2_cycle3")"
check "and it says two cycles plus a failure is not an answer" "yes" \
      "$(has_text "$(cat "$T/err.txt")" "not two thirds of an answer")"
check "no summary claiming a result was written" "no" \
      "$([ -f "$T/dev2026/results/c2_summary.json" ] && echo yes || echo no)"
rm -r "$T"

# ================================================ leftover state between cycles ===
echo
echo "run state left by a cycle stops the next one, and is not cleaned away"
T="$(build_env distinct "" 1)"
rc="$(drive "$T")"
check "the driver exits non-zero" "1" "$rc"
check "only the first cycle ran" "1" "$(cat "$T/dev2026/run/.calls")"
check "the leftover state is still there" "yes" \
      "$([ -f "$T/dev2026/run/leftover.tree" ] && echo yes || echo no)"
check "and it says not to remove it to make the next cycle run" "yes" \
      "$(has_text "$(cat "$T/err.txt")" "do not remove it to make the next cycle run")"
check "no summary was written" "no" \
      "$([ -f "$T/dev2026/results/c2_summary.json" ] && echo yes || echo no)"
rm -r "$T"

# ======================================== a cycle whose stop window was too short ===
echo
echo "a cycle that could not outlast its own arms is not a pass"
# The C2 cycle-1 configuration, end to end: the arms report gunicorn's 30-second
# default while the harness waits 20. Every gate inside the cycle passes and the
# seeds are distinct, so the only thing standing between this and a reported PASS is
# whether anything reads the budget back.
T="$(build_env distinct "" "" 30)"
rc="$(drive "$T")"
out="$(cat "$T/out.txt")"
check "the driver does not exit 0" "yes" "$([ "$rc" != 0 ] && echo yes || echo no)"
check "it is a FAIL, not an INSUFFICIENT" "1" "$rc"
check "all three cycles still ran" "3" "$(cat "$T/dev2026/run/.calls")"
check "the budget block reports INCONSISTENT" "yes" \
      "$(has_text "$out" "status: INCONSISTENT")"
check "and names the cycle and both numbers" "yes" \
      "$(has_text "$out" "STOP_WAIT_SECS=20 does not exceed")"
check "the outcome is FAIL" "yes" "$(has_text "$out" "C2 OUTCOME: FAIL")"
check "and the reason is the configuration, not the contract" "yes" \
      "$(has_text "$out" "authorised")"
check "while the semantic gate is still reported as having passed" "yes" \
      "$(has_text "$out" "gate: PASS")"
rm -r "$T"

# ================================================== labels name a body of evidence ===
echo
echo "a rerun's cycles are named apart from the run before them"
T="$(build_env distinct "" "")"
rc="$(drive "$T" --label-prefix c2e)"
out="$(cat "$T/out.txt")"
check "the driver still exits 0" "0" "$rc"
check "the three cycles carry the prefix" "c2e_cycle1 c2e_cycle2 c2e_cycle3" \
      "$(tr '\n' ' ' < "$T/dev2026/run/.labels" | sed 's/ $//')"
check "and the summary is named for the run, not the campaign" "yes" \
      "$([ -f "$T/dev2026/results/c2e_summary.json" ] && echo yes || echo no)"
check "nothing was written under the default prefix" "no" \
      "$([ -f "$T/dev2026/results/c2_summary.json" ] && echo yes || echo no)"
check "and the banner announces the labels it will use" "yes" \
      "$(has_text "$out" "c2e_cycle1 .. c2e_cycle3")"

# The point of the prefix: a second run in the same export cannot quietly replace
# the first run's artefacts.
rc2="$(drive "$T" --label-prefix c2e)"
err2="$(cat "$T/err.txt")"
check "running it again with the same prefix is refused" "1" "$rc2"
check "and it names a file it would have overwritten" "yes" \
      "$(has_text "$err2" "c2e_cycle1_contract.json")"
check "no further cycle ran" "3" "$(cat "$T/dev2026/run/.calls")"
check "the earlier summary is still there" "yes" \
      "$([ -f "$T/dev2026/results/c2e_summary.json" ] && echo yes || echo no)"

# A different prefix in the same export is fine — that is the escape hatch, and it
# leaves the first run's files untouched.
rc3="$(drive "$T" --label-prefix c2f)"
check "a different prefix runs" "0" "$rc3"
check "and both runs' results now coexist" "yes" \
      "$([ -f "$T/dev2026/results/c2e_summary.json" ] \
         && [ -f "$T/dev2026/results/c2f_summary.json" ] && echo yes || echo no)"
rm -r "$T"

echo
echo "and a prefix that could write outside results/ is refused"
T="$(build_env distinct "" "")"
for bad in "../escape" "a/b" "" "9lives" "with space" "semi;colon"; do
  rc="$(drive "$T" --label-prefix "$bad")"
  check "'$bad' is rejected before anything runs" "2" "$rc"
done
check "not one cycle ran" "no" \
      "$([ -f "$T/dev2026/run/.calls" ] && echo yes || echo no)"
rm -r "$T"

echo
if [ "$fail" -gt 0 ]; then
  echo "FAILED $fail/$((pass + fail))"
  exit 1
fi
echo "all passed ($pass assertions)"
