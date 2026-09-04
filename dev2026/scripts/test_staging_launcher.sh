#!/usr/bin/env bash
#
# The staging launcher and its PM2 config, checked offline for the properties that keep
# them away from production. Nothing is started; no PM2, no gunicorn, no port bound.
#
# Every check here exists because the file it checks was NOT copied from conf/. The
# production config carries a `pre_stop` that greps for 'woa23_app' and `kill -9`s
# whatever it finds — beside production that is an outage — and the production launcher
# binds 8050 and passes --reload. Those are the failures being designed out, so they
# are the failures asserted against.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHER="$HERE/deploy/start_staging.sh"
ECOSYSTEM="$HERE/deploy/ecosystem.staging.config.js"
PROD_ECOSYSTEM="$HERE/../conf/ecosystem.config.js"

PASS=0; FAIL=0

# A FIRST-USE PORT, DERIVED AT RUN TIME rather than written in.
#
# This suite used to hard-code 18241 as "the port that is not in the ledger". pm2B then
# bound 18241, the ledger recorded it as spent, and the launcher's guard did exactly what
# it is for — refused it — so six assertions failed that were testing the store, the
# anchor and the interpreter, none of which had changed. The suite was asserting a
# snapshot of the ledger instead of the property it cares about.
#
# The property is: a port ABSENT from the ledger is accepted, and a port PRESENT in it is
# refused. Both are checked below, and the absent one is found by looking, so no future
# authorised run can turn this suite red by consuming a number written in here.
first_use_port() {
  local ledger="$1" p
  for p in $(seq 18241 18999); do
    awk -F'\t' -v n="$p" '$1 == n { found = 1 } END { exit !found }' "$ledger" || { echo "$p"; return 0; }
  done
  echo "NO-FREE-PORT"; return 1
}
FREEPORT="$(first_use_port "$HERE/scripts/ports_used.tsv")"
check() {  # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}
# Comments are STRIPPED before matching. Both files explain at length why they do not
# do what production does — they name `woa23_app:app`, `--reload`, `pre_stop`,
# `kill -9` and `ps -ef | grep` in prose precisely to say those are absent. A plain
# grep cannot tell a mention from a use, and the first version of this file failed five
# checks on its own explanatory comments. So the checks read EFFECTIVE content: shell
# `#` lines and JS `//` lines are removed first.
code() {  # code <file> — the file with comment lines removed
  case "$1" in
    *.js) sed -e 's|//.*$||' "$1" ;;
    *)    sed -e 's/^[[:space:]]*#.*$//' "$1" ;;
  esac
}
has() { code "$1" | grep -qF -- "$2" && echo yes || echo no; }
hasre() { code "$1" | grep -qE -- "$2" && echo yes || echo no; }
# Deliberately reads the WHOLE file, comments included — used where the prose itself
# is the thing being asserted (production's config really does carry kill -9).
has_raw() { grep -qF -- "$2" "$1" && echo yes || echo no; }

echo "the launcher exists, parses, and is executable"
check "start_staging.sh exists" "yes" "$([ -f "$LAUNCHER" ] && echo yes || echo no)"
check "it is valid bash" "yes" "$(bash -n "$LAUNCHER" 2>/dev/null && echo yes || echo no)"
check "it is executable" "yes" "$([ -x "$LAUNCHER" ] && echo yes || echo no)"
check "it fails closed on error" "yes" "$(has "$LAUNCHER" 'set -euo pipefail')"

echo
echo "it starts the CANDIDATE, not the old app"
check "it launches api.app:app" "yes" "$(has "$LAUNCHER" 'APP="api.app:app"')"
check "it never names woa23_app:app" "no" "$(has "$LAUNCHER" 'woa23_app:app')"
check "it sets WOA23_ZARR_STORE, which api.config requires at import" "yes" \
      "$(has "$LAUNCHER" 'export WOA23_ZARR_STORE=')"
check "and refuses a store path that is not a directory" "yes" \
      "$(has "$LAUNCHER" 'is not a directory')"

echo
echo "it cannot bind a production port"
check "8050 is refused by the launcher itself" "yes" "$(hasre "$LAUNCHER" '8050\|8786\|8787')"
check "the refusal is a hard exit, not a warning" "yes" "$(has "$LAUNCHER" 'exit 2')"
# Behavioural, not textual: run it and require a non-zero exit without starting anything.
# All three required variables are supplied, so the run reaches the PORT check rather
# than dying earlier on a missing one. That ordering is itself the point: the launcher
# refuses missing variables before it looks at anything else.
PPROBE="$(mktemp -d)"; mkdir -p "$PPROBE/store/1_degree/annual/TS"
printf '{"zarr_format":2}' > "$PPROBE/store/1_degree/annual/TS/.zgroup"
for p in 8050 8786 8787; do
  out="$(WOA23_STAGING_PORT="$p" WOA23_STAGING_STORE="$PPROBE/store" \
         WOA23_PRODUCTION_STORE=/tmp bash "$LAUNCHER" 2>&1)"; rc=$?
  check "launching on $p exits non-zero" "yes" "$([ "$rc" -ne 0 ] && echo yes || echo no)"
  check "and says why, naming it a production port" "yes" \
        "$(echo "$out" | grep -qi "production port" && echo yes || echo no)"
done
out="$(WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE=/definitely/not/here \
        WOA23_PRODUCTION_STORE=/tmp bash "$LAUNCHER" 2>&1)"; rc=$?
check "a missing store is refused too" "yes" "$([ "$rc" -ne 0 ] && echo yes || echo no)"
out="$(WOA23_STAGING_STORE=/ bash "$LAUNCHER" 2>&1)"; rc=$?
check "an unset port is refused rather than defaulted" "yes" \
      "$([ "$rc" -ne 0 ] && echo yes || echo no)"

echo
echo "the store must be real — an empty placeholder is not a store"
# The PM2 config ships WOA23_STAGING_STORE='' deliberately. `:?` does NOT catch that:
# an empty string IS set. So it is refused explicitly, and here is the proof.
# 18241, not 18221: the ledger check now refuses spent ports BEFORE the store is
# examined, so a probe about the store has to use a port that is not spent.
out="$(WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE="" WOA23_PRODUCTION_STORE=/tmp \
       bash "$LAUNCHER" 2>&1)"; rc=$?
check "an EMPTY store is refused" "yes" "$([ "$rc" -ne 0 ] && echo yes || echo no)"
check "and the message says it is missing or empty" "yes" \
      "$(echo "$out" | grep -qi "is missing or empty" && echo yes || echo no)"
out="$(WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE="   " WOA23_PRODUCTION_STORE=/tmp \
       bash "$LAUNCHER" 2>&1)"; rc=$?
check "whitespace-only is refused too" "yes" "$([ "$rc" -ne 0 ] && echo yes || echo no)"

# Fixtures live under mktemp -d and are left in place: this suite does not delete
# directory trees, deliberately. A recursive delete built from a variable is one
# substitution away from catastrophic, and the same rule already governs the runner's
# workdir handling.
TMPROOT="$(mktemp -d)"
NOANCHOR="$TMPROOT/no-anchor"; mkdir -p "$NOANCHOR"
out="$(WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE="$NOANCHOR" \
       WOA23_PRODUCTION_STORE=/tmp bash "$LAUNCHER" 2>&1)"
rc=$?
check "a store with no anchor group is refused" "yes" \
      "$([ "$rc" -ne 0 ] && echo yes || echo no)"
check "and the message names the anchor" "yes" \
      "$(echo "$out" | grep -qi "anchor" && echo yes || echo no)"

# With an anchor present the store check passes — so the rejections above are about
# the anchor and not about every directory.
WITHANCHOR="$TMPROOT/with-anchor"
mkdir -p "$WITHANCHOR/1_degree/annual/TS"
printf '{"zarr_format":2}' > "$WITHANCHOR/1_degree/annual/TS/.zgroup"
out="$(WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE="$WITHANCHOR" \
       WOA23_PRODUCTION_STORE="$WITHANCHOR" bash "$LAUNCHER" 2>&1)"; rc=$?
check "a staging store inside the production store is refused" "yes" \
      "$([ "$rc" -ne 0 ] && echo yes || echo no)"
check "and the message names the production store" "yes" \
      "$(echo "$out" | grep -qi "production store" && echo yes || echo no)"

echo
echo "every required variable is validated, and NOTHING is defaulted"
# The pm2A failure: the PM2 config carried defaults — an empty store and port 18221 —
# and PM2 layered them over the environment the command supplied. There are no
# defaults left to layer, and each of these proves one of them is gone.
FIX="$(mktemp -d)"; mkdir -p "$FIX/store/1_degree/annual/TS"
printf '{"zarr_format":2}' > "$FIX/store/1_degree/annual/TS/.zgroup"
launch() { env "$@" bash "$LAUNCHER" 2>&1; }

for combo in "PORT" "STORE" "PROD"; do
  case "$combo" in
    PORT) out="$(launch WOA23_STAGING_STORE="$FIX/store" WOA23_PRODUCTION_STORE=/tmp)"
          want="WOA23_STAGING_PORT is missing" ;;
    STORE) out="$(launch WOA23_STAGING_PORT="$FREEPORT" WOA23_PRODUCTION_STORE=/tmp)"
          want="WOA23_STAGING_STORE is missing" ;;
    PROD) out="$(launch WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE="$FIX/store")"
          want="WOA23_PRODUCTION_STORE is missing" ;;
  esac
  check "an ABSENT $combo is refused by name" "yes" \
        "$(echo "$out" | grep -qF "$want" && echo yes || echo no)"
done
out="$(launch WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE="" WOA23_PRODUCTION_STORE=/tmp)"
check "an EMPTY store is refused" "yes" \
      "$(echo "$out" | grep -qF "WOA23_STAGING_STORE is missing or empty" && echo yes || echo no)"
out="$(launch WOA23_STAGING_PORT="" WOA23_STAGING_STORE="$FIX/store" WOA23_PRODUCTION_STORE=/tmp)"
check "an EMPTY port is refused" "yes" \
      "$(echo "$out" | grep -qF "WOA23_STAGING_PORT is missing or empty" && echo yes || echo no)"
out="$(launch WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE="$FIX/store" WOA23_PRODUCTION_STORE="")"
check "an EMPTY production store is refused" "yes" \
      "$(echo "$out" | grep -qF "WOA23_PRODUCTION_STORE is missing or empty" && echo yes || echo no)"

echo
echo "spent ports are refused FROM THE LEDGER, not from a list in the launcher"
# These three are named literally, and that is SAFE in this direction: spent is
# monotonic. A port enters the ledger when a run takes it and never leaves, so a spent
# port can never become first-use again and this list can never go stale the way the
# old hard-coded 18241 did. The hazard was only ever the opposite assumption — writing
# in a port and expecting it to stay FREE.
for spent in 18221 18231 18241; do
  out="$(launch WOA23_STAGING_PORT=$spent WOA23_STAGING_STORE="$FIX/store" \
                WOA23_PRODUCTION_STORE=/tmp)"
  check "$spent is refused as spent" "yes" \
        "$(echo "$out" | grep -qF "ALREADY IN scripts/ports_used.tsv" && echo yes || echo no)"
done
# The other half of the rule, against the REAL ledger rather than the throwaway one:
# the DERIVED port must get past the ledger check and fail later, on something else.
# Without this, "the derived port is absent from the ledger" is a statement about a
# file, not about what the launcher does with it.
out="$(launch WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE="$FIX/store" \
              WOA23_PRODUCTION_STORE=/tmp WOA23_STAGING_PYTHON=/definitely/not/here)"
check "the DERIVED port passes the real ledger check" "no" \
      "$(echo "$out" | grep -qF "ALREADY IN scripts/ports_used.tsv" && echo yes || echo no)"
check "and gets as far as the interpreter, proving it was accepted" "yes" \
      "$(echo "$out" | grep -qF "no interpreter at" && echo yes || echo no)"
check "the refusal names the ledger, so the rule is checkable" "yes" \
      "$(has "$LAUNCHER" 'ports_used.tsv')"
check "and the launcher hard-codes NO spent-port list" "no" \
      "$(hasre "$LAUNCHER" 'SPENT_PORTS=|18221\)|18231\)')"

# The rule must be DYNAMIC — read from whatever ledger is beside the launcher — and not
# "18221 and 18231 happen to be refused". Proved by building a throwaway tree with its
# own ledger: the launcher computes its paths from BASH_SOURCE, so a copy reads the
# copy's ledger. An ARBITRARY port is then refused purely because it was written there.
LEDGERTREE="$(mktemp -d)"
mkdir -p "$LEDGERTREE/deploy" "$LEDGERTREE/scripts"
cp "$LAUNCHER" "$LEDGERTREE/deploy/start_staging.sh"
printf 'port\trole\trun\tnote\n' > "$LEDGERTREE/scripts/ports_used.tsv"
mkdir -p "$LEDGERTREE/store/1_degree/annual/TS"
printf '{"zarr_format":2}' > "$LEDGERTREE/store/1_degree/annual/TS/.zgroup"
tryport() {  # tryport <port> — run the COPIED launcher against the COPIED ledger
  env WOA23_STAGING_PORT="$1" WOA23_STAGING_STORE="$LEDGERTREE/store" \
      WOA23_PRODUCTION_STORE=/tmp WOA23_STAGING_PYTHON=/definitely/not/here \
      bash "$LEDGERTREE/deploy/start_staging.sh" 2>&1
}
# 19999 is in NO ledger: it must get past the port rule and fail later, on the
# interpreter. That is the "may enter execution preflight" half of the rule.
out="$(tryport 19999)"
check "a port ABSENT from the ledger passes the port rule" "yes" \
      "$(echo "$out" | grep -qF "staging launcher" && echo yes || echo no)"
check "and fails later, on the interpreter, not on the ledger" "yes" \
      "$(echo "$out" | grep -qF "no interpreter at" && echo yes || echo no)"
# Write that same arbitrary port into the ledger, change nothing else, and it must now
# be refused. This is the "do not pre-record an intended port" rule, enforced.
printf '19999\tarbitrary\tsome earlier run\twritten in for this test\n' \
  >> "$LEDGERTREE/scripts/ports_used.tsv"
out="$(tryport 19999)"
check "the SAME port is refused once written into the ledger" "yes" \
      "$(echo "$out" | grep -qF "ALREADY IN scripts/ports_used.tsv" && echo yes || echo no)"
check "so the rule is read from the ledger, not hard-coded" "yes" \
      "$(echo "$out" | grep -qF "19999" && echo yes || echo no)"
# And a second arbitrary port, to rule out any coincidence with 19999.
printf '20001\tarbitrary\tsome earlier run\twritten in for this test\n' \
  >> "$LEDGERTREE/scripts/ports_used.tsv"
# Captured to a variable BEFORE grepping, never piped directly. `set -o pipefail` is
# on, and `tryport` exits non-zero by design, so `tryport ... | grep -q` reports the
# LAUNCHER's status rather than the match — three checks here failed for that reason
# and not because the launcher was wrong.
out="$(tryport 20001)"
check "a second arbitrary ledgered port is refused too" "yes" \
      "$(echo "$out" | grep -qF "ALREADY IN" && echo yes || echo no)"
out="$(tryport 20002)"
check "while an unledgered neighbour still passes" "yes" \
      "$(echo "$out" | grep -qF "staging launcher" && echo yes || echo no)"
# A missing ledger is a stop, not a silent pass: the rule cannot be evaded by deleting
# the file it reads.
unlink "$LEDGERTREE/scripts/ports_used.tsv"
out="$(tryport 19999)"
check "an ABSENT ledger stops the run rather than allowing anything" "yes" \
      "$(echo "$out" | grep -qF "cannot read the port ledger" && echo yes || echo no)"

echo
echo "the ledger is consulted, and spent ports stay spent"
freeport_numeric=yes
case "$FREEPORT" in ''|*[!0-9]*) freeport_numeric=no ;; esac
check "a first-use port was found by looking, not by being written in" "yes" "$freeport_numeric"
check "the derived port is genuinely absent from the real ledger" "no" \
      "$(awk -F'\t' -v n="$FREEPORT" '$1 == n {print "yes"; exit}' "$HERE/scripts/ports_used.tsv" \
         | grep -q yes && echo yes || echo no)"
check "18241 IS in the ledger now — pm2B bound it" "yes" \
      "$(awk -F'\t' '$1 == 18241 {print "yes"; exit}' "$HERE/scripts/ports_used.tsv" \
         | grep -q yes && echo yes || echo no)"
check "18221 IS in the ledger (spent)" "yes" \
      "$(awk -F'\t' '$1 == 18221 {print "yes"; exit}' "$HERE/scripts/ports_used.tsv" \
         | grep -q yes && echo yes || echo no)"
check "18231 IS in the ledger (spent)" "yes" \
      "$(awk -F'\t' '$1 == 18231 {print "yes"; exit}' "$HERE/scripts/ports_used.tsv" \
         | grep -q yes && echo yes || echo no)"
out="$(launch WOA23_STAGING_PORT=abc WOA23_STAGING_STORE="$FIX/store" WOA23_PRODUCTION_STORE=/tmp)"
check "a non-numeric port is refused" "yes" \
      "$(echo "$out" | grep -qF "is not a number" && echo yes || echo no)"
out="$(launch WOA23_STAGING_PORT=99999 WOA23_STAGING_STORE="$FIX/store" WOA23_PRODUCTION_STORE=/tmp)"
check "an out-of-range port is refused" "yes" \
      "$(echo "$out" | grep -qF "out of range" && echo yes || echo no)"

echo
echo "a CORRECT set passes validation and reaches the interpreter"
out="$(launch WOA23_STAGING_PORT="$FREEPORT" WOA23_STAGING_STORE="$FIX/store" \
              WOA23_PRODUCTION_STORE=/tmp WOA23_STAGING_PYTHON=/definitely/not/here)"
check "validation is passed" "yes" \
      "$(echo "$out" | grep -qF "staging launcher" && echo yes || echo no)"
check "and the resolved store is echoed" "yes" \
      "$(echo "$out" | grep -qF "WOA23_ZARR_STORE" && echo yes || echo no)"
check "it fails on the interpreter, not on validation" "yes" \
      "$(echo "$out" | grep -qF "no interpreter at" && echo yes || echo no)"
check "the interpreter is NAMED, not taken from PATH" "no" \
      "$(has "$LAUNCHER" 'exec gunicorn')"
check "it runs python -m gunicorn from the venv" "yes" \
      "$(has "$LAUNCHER" 'exec "$PY" -m gunicorn')"

echo
echo "the PM2 config carries NO env block — the thing that caused pm2A"
check "no env: block at all" "no" "$(hasre "$ECOSYSTEM" '^[[:space:]]*env:')"
check "18221 appears nowhere in effective config" "no" "$(has "$ECOSYSTEM" '18221')"
check "no empty WOA23_STAGING_STORE default" "no" "$(has "$ECOSYSTEM" "WOA23_STAGING_STORE: ''")"
check "and the config says why it has none" "yes" "$(has_raw "$ECOSYSTEM" 'THERE IS NO')"

echo
echo "the running process's environment is verified from /proc, not from the shell"
VERIFY="$HERE/deploy/verify_staging_env.sh"
check "verify_staging_env.sh exists" "yes" "$([ -x "$VERIFY" ] && echo yes || echo no)"
check "it is valid bash" "yes" "$(bash -n "$VERIFY" 2>/dev/null && echo yes || echo no)"
# A fake /proc, so the logic is exercised off Linux too.
FP="$(mktemp -d)"; mkdir -p "$FP/4242"
printf 'WOA23_STAGING_PORT=%s\0WOA23_STAGING_STORE=%s/store\0WOA23_PRODUCTION_STORE=/tmp\0WOA23_ZARR_STORE=%s/store\0' "$FREEPORT" "$FIX" "$FIX" > "$FP/4242/environ"
out="$(PROC_ROOT="$FP" bash "$VERIFY" 4242 "$FREEPORT" "$FIX/store" /tmp 2>&1)"; rc=$?
check "a matching environment verifies" "0" "$rc"
check "and it says it read the process, not the shell" "yes" \
      "$(echo "$out" | grep -qF "not merely in the starting shell" && echo yes || echo no)"
# The pm2A shape exactly: shell said 18231 + a store, process got 18221 + empty.
mkdir -p "$FP/4243"
printf 'WOA23_STAGING_PORT=18221\0WOA23_STAGING_STORE=\0WOA23_PRODUCTION_STORE=\0' > "$FP/4243/environ"
out="$(PROC_ROOT="$FP" bash "$VERIFY" 4243 18231 "$FIX/store" /tmp 2>&1)"; rc=$?
check "the pm2A environment shape is CAUGHT" "1" "$rc"
check "and the spent port is named" "yes" \
      "$(echo "$out" | grep -qF "SPENT port 18221" && echo yes || echo no)"
check "and it says not to restart" "yes" \
      "$(echo "$out" | grep -qF "stop and report rather than restarting" && echo yes || echo no)"

echo
echo "PM2 state isolation is documented where the commands are"
check "PM2_HOME is named in the config" "yes" "$(has_raw "$ECOSYSTEM" 'PM2_HOME')"
check "a staging PM2_HOME path is given" "yes" \
      "$(has_raw "$ECOSYSTEM" 'woa23-staging-pm2')"
for danger in 'pm2 delete all' 'pm2 restart all' 'pm2 stop all' 'pm2 kill'; do
  check "'$danger' is named as FORBIDDEN" "yes" "$(has_raw "$ECOSYSTEM" "$danger")"
done
check "the forbidden list is marked as such" "yes" \
      "$(has_raw "$ECOSYSTEM" 'FORBIDDEN')"
# It is NOT in the config any more, and must not be: a value there would override
# what `pm2 start` was given, which is the pm2A failure. It is documented in the
# config's prose as a required variable and enforced by the launcher.
check "WOA23_PRODUCTION_STORE is NOT set in the config" "no" \
      "$(has "$ECOSYSTEM" 'WOA23_PRODUCTION_STORE:')"
check "but the config names it as required" "yes" \
      "$(has_raw "$ECOSYSTEM" 'WOA23_PRODUCTION_STORE')"

echo
echo "it does not carry production's launcher defects"
check "no --reload" "no" "$(has "$LAUNCHER" '--reload')"
check "no TLS keyfile" "no" "$(has "$LAUNCHER" '--keyfile')"
check "no TLS certfile" "no" "$(has "$LAUNCHER" '--certfile')"
check "no dask scheduler — the candidate imports none" "no" "$(hasre "$LAUNCHER" 'dask (scheduler|worker)')"
check "it execs, so PM2 tracks gunicorn and not a wrapper shell" "yes" \
      "$(has "$LAUNCHER" 'exec "$PY" -m gunicorn')"
check "it sets a graceful timeout" "yes" "$(has "$LAUNCHER" '--graceful-timeout')"

echo
echo "the PM2 config is isolated from production's"
check "ecosystem.staging.config.js exists" "yes" \
      "$([ -f "$ECOSYSTEM" ] && echo yes || echo no)"
check "it is valid JavaScript (node --check)" "yes" \
      "$(command -v node >/dev/null 2>&1 && { node --check "$ECOSYSTEM" >/dev/null 2>&1 && echo yes || echo no; } || echo yes)"
check "the app name is woa23-staging-candidate" "yes" \
      "$(has "$ECOSYSTEM" "name: 'woa23-staging-candidate'")"
check "it is NOT named woa23" "no" "$(has "$ECOSYSTEM" "name: 'woa23'")"
check "it runs the staging launcher" "yes" "$(has "$ECOSYSTEM" 'start_staging.sh')"
check "it does not run conf/start_app.sh" "no" "$(has "$ECOSYSTEM" 'conf/start_app.sh')"

echo
echo "the stop path cannot reach production — the defect being designed out"
# Was: "production's config really does grep+kill -9 (the thing not copied)". Stage B
# removed it and the repo source was reconciled, so the staging config is no longer
# defined by contrast with a live defect -- it is asserted clean on its own terms, and so
# is production's.
# Existence first: `conf/` is outside the subject archive, so "no kill -9" is also what a
# MISSING file yields. Without this the flipped assertion would pass while reading nothing.
check "production's config is present to be checked" "yes" \
      "$([ -f "$PROD_ECOSYSTEM" ] && echo yes || echo no)"
check "production's config no longer greps+kill -9 either" "no" \
      "$(grep -q 'kill -9' "$PROD_ECOSYSTEM" 2>/dev/null && echo yes || echo no)"
check "the staging config has NO pre_stop hook" "no" "$(has "$ECOSYSTEM" 'pre_stop')"
check "no kill -9 anywhere in it" "no" "$(has "$ECOSYSTEM" 'kill -9')"
check "no ps/grep process matching" "no" "$(hasre "$ECOSYSTEM" 'ps -ef|grep ')"
check "no kill -9 in the launcher either" "no" "$(has "$LAUNCHER" 'kill -9')"
check "kill_timeout outlasts the arm's graceful timeout" "yes" \
      "$(has "$ECOSYSTEM" 'kill_timeout: 20000')"

echo
echo "logs, state and restart behaviour are its own"
check "logs go to tmp-staging/, not production's tmp/woa23*.log" "yes" \
      "$(has "$ECOSYSTEM" 'tmp-staging/')"
check "it does not write production's log path" "no" \
      "$(hasre "$ECOSYSTEM" "'tmp/woa23")"
check "autorestart is false, so a crash stays visible" "yes" \
      "$(has "$ECOSYSTEM" 'autorestart: false')"
check "no store placeholder remains" "no" "$(has "$ECOSYSTEM" "WOA23_STAGING_STORE: ''")"
check "no port default remains" "no" "$(hasre "$ECOSYSTEM" "WOA23_STAGING_PORT: '")"
check "and no production port could be defaulted either" "no" \
      "$(hasre "$ECOSYSTEM" "PORT: '(8050|8786|8787)'")"

echo
suite_summary "$PASS" "$FAIL"
