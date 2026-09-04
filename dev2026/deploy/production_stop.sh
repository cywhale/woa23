#!/usr/bin/env bash
#
# Stop the woa23 PM2 app by IDENTITY, and prove it stopped. PROPOSED, NOT INSTALLED.
#
# This is the B1 replacement for:
#
#   pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
#
# and the point is not that the pipeline was written badly. The point is that **matching
# processes by their command line is the wrong operation**. A command line is not an
# identity: it is shared by every copy of a program, it is not unique to the instance this
# app started, and on this host it is also carried by unrelated projects. `conf/simu.sh`
# documents killing `tide_app` by exactly this method (spec 013 §5).
#
# WHAT AN IDENTITY IS HERE. The pair (pid, starttime), where starttime is field 22 of
# /proc/<pid>/stat — jiffies since boot. A PID alone is reusable; the pair is not, within
# one boot. Every process this script signals or reports on must match a pair recorded
# BEFORE the stop, and anything that does not match is left alone.
#
# WHAT IT WILL NEVER DO:
#   - match a process by grepping `ps` output;
#   - send SIGKILL. If a graceful stop does not complete, that is a CLEANUP_FAIL to be
#     looked at, not a signal to escalate. Escalation is how a half-served request
#     becomes a corrupted response;
#   - accept `all`, or operate without an explicit PM2_HOME.
#
#   WOA23_PM2_HOME=/home/odbadmin/.pm2 ./deploy/production_stop.sh woa23
#
set -uo pipefail

jlist_resolve() {   # <jlist json on stdin> <app>; prints "OK <pid>" | "NOTFOUND" | "PROBLEM <why>"
  local app="${1:-}"
  command -v node >/dev/null 2>&1 || { printf 'PROBLEM node is unavailable, so the listing cannot be parsed as JSON'; return 0; }
  node -e '
    let raw = "";
    process.stdin.on("data", d => raw += d).on("end", () => {
      const app = process.argv[1];
      let list;
      try { list = JSON.parse(raw); }
      catch (e) { console.log("PROBLEM pm2 jlist is not valid JSON: " + e.message); return; }
      if (!Array.isArray(list)) { console.log("PROBLEM pm2 jlist is not a JSON array"); return; }
      const hits = list.filter(p => p && p.name === app);
      if (hits.length === 0) { console.log("NOTFOUND"); return; }
      if (hits.length > 1) {
        console.log("PROBLEM " + hits.length + " apps are named " + app +
                    "; refusing to guess which one to stop");
        return;
      }
      const e = hits[0];
      const pid = e.pid;
      if (pid === undefined || pid === null || pid === "") {
        console.log("PROBLEM app " + app + " exists in the listing but carries no pid");
        return;
      }
      if (typeof pid !== "number" || !Number.isInteger(pid)) {
        console.log("PROBLEM app " + app + " has a non-integer pid: " + JSON.stringify(pid));
        return;
      }
      if (pid === 0) {
        // pm2 represents a CLEANLY STOPPED app as pid 0 with status "stopped". That is a
        // POSITIVE statement, not an absence of information, and stopping an already
        // stopped app must stay idempotent. It is accepted ONLY with that status --
        // pid 0 alongside "online", "errored" or a missing status is a contradiction,
        // and a contradiction is never read as "nothing to stop".
        const st = (e.pm2_env && e.pm2_env.status) || "";
        if (st === "stopped") { console.log("STOPPED"); return; }
        console.log("PROBLEM app " + app + " has pid 0 but status " +
                    (st ? JSON.stringify(st) : "<missing>") +
                    "; pid 0 is only credible with status \"stopped\"");
        return;
      }
      if (pid < 0) {
        console.log("PROBLEM app " + app + " has a negative pid: " + pid);
        return;
      }
      console.log("OK " + pid);
    });
  ' "$app" 2>/dev/null || printf 'PROBLEM the JSON parser failed to run'
}


die() { printf '%s\n' "$@" >&2; exit 2; }

PROC="${PROC_ROOT:-/proc}"     # test seam only; never set in production use

# ---------------------------------------------------------------- /proc, read correctly
#
# THE BUG BOTH OF THESE USED TO HAVE. `/proc/<pid>/stat` is
#
#     <pid> (<comm>) <state> <ppid> ... <starttime> ...
#
# and `comm` is the executable name IN PARENTHESES, which MAY CONTAIN SPACES AND
# PARENTHESES. So `awk '{print $4}'` is the ppid only for a process whose name happens to
# contain neither. For `(my prog)` every field after the second shifts right by one, and
# the reader silently gets the WRONG NUMBER — not an error, a plausible wrong answer.
#
# That mattered in two different ways:
#   children_of   read a wrong ppid, so a real child could be missed and a stranger's
#                 process could be mistaken for one. A missed child is a survivor that is
#                 never checked.
#   starttime_of  read a wrong field 22, so the (pid, starttime) IDENTITY — the whole
#                 defence against PID reuse — could compare two unrelated numbers.
#
# ppid now comes from `/proc/<pid>/status`, whose `PPid:` line is a labelled field and
# cannot shift. starttime has no equivalent in `status`, so it still comes from `stat`,
# but the parenthesised comm is REMOVED FIRST, at the LAST ')', before any field is taken.

ppid_of() {   # ppid_of <pid> — prints the ppid; rc 0 ok, 2 vanished, 3 unreadable, 4 unparsable
  local st="$PROC/$1/status" v
  [ -e "$st" ] || return 2
  [ -r "$st" ] || return 3
  v="$(sed -n 's/^PPid:[[:space:]]*\([0-9][0-9]*\)[[:space:]]*$/\1/p' "$st" 2>/dev/null | head -1)"
  [ -n "$v" ] || return 4
  printf '%s' "$v"
}

starttime_of() {   # starttime_of <pid> — field 22 of stat, read AFTER the comm is removed
  #                  rc 0 = a validated all-digits starttime on stdout
  #                  rc 1 = cannot be determined. NOTHING is printed.
  #
  # THIS USED TO RETURN 0 WITH AN EMPTY STRING. A truncated `stat` produced an empty
  # starttime and a SUCCESS code, so `MASTER_START="$(starttime_of "$PID")" || die` never
  # fired, the identity became "<pid>:" with no starttime, and the survivor check -- which
  # tests `[ -n "$now" ]` -- then read a LIVE process as GONE and reported a clean stop.
  # That is the bs3v1 failure shape (success on an unverified premise) in a second place.
  #
  # So every way of not knowing is now rc 1, and a value is returned ONLY when it is
  # present and all digits. An empty string, a 0 that came from a short field list, and a
  # non-numeric token are all "I do not know", and none of them may masquerade as an
  # identity.
  local line rest v
  [ -r "$PROC/$1/stat" ] || return 1
  line="$(cat "$PROC/$1/stat" 2>/dev/null)" || return 1
  [ -n "$line" ] || return 1
  case "$line" in *')'*) ;; *) return 1 ;; esac     # no comm terminator: malformed
  # `sed 's/.*) //'` is greedy, so it cuts at the LAST ')' — correct even when comm
  # itself contains one. What remains begins at field 3, so field 22 is token 20.
  rest="$(printf '%s' "$line" | sed 's/.*) //')"
  [ -n "$rest" ] || return 1
  # TRUNCATION IS DETECTED BY COUNTING, not by hoping awk returns something. A short line
  # makes `$20` empty -- or, worse, makes some other field land in position 20.
  [ "$(printf '%s' "$rest" | awk '{print NF}')" -ge 20 ] 2>/dev/null || return 1
  v="$(printf '%s' "$rest" | awk '{print $20}')"
  # ALL DIGITS *AND* NON-ZERO. A short-but-not-short-enough line can put a real `0` from
  # some other column into position 20, which is all digits and looks like an answer. A
  # genuine starttime is jiffies since boot for a process that PM2 started, so it is never
  # 0; accepting one means accepting a field that drifted into place.
  case "$v" in ''|*[!0-9]*) return 1 ;; esac
  [ "$v" != "0" ] || return 1
  printf '%s' "$v"
}

# THREE OUTCOMES, NEVER TWO. The survivor check used to ask "did I get a starttime?" and
# treat "no" as "gone". Those are different states and conflating them is fail-OPEN:
#
#   GONE            /proc/<pid> does not exist. The process is genuinely gone.
#   ALIVE <start>   it exists and its identity was read.
#   UNKNOWN         it EXISTS but its identity cannot be established.
#
# UNKNOWN is not gone, is not a survivor, and is not droppable. It is a reason to stop.
proc_state() {   # proc_state <pid> -> "GONE" | "ALIVE <starttime>" | "UNKNOWN <why>"
  local st
  [ -e "$PROC/$1" ] || { printf 'GONE'; return 0; }
  if st="$(starttime_of "$1" 2>/dev/null)" && [ -n "$st" ]; then
    printf 'ALIVE %s' "$st"; return 0
  fi
  if [ ! -r "$PROC/$1/stat" ]; then printf 'UNKNOWN stat-unreadable'
  else printf 'UNKNOWN stat-unparsable'; fi
}

# A PID THIS SCAN CANNOT CLASSIFY IS NOT "NOT A CHILD".
#
# The old loop did `[ -r ... ] || continue`, which silently treated every unreadable entry
# as "not a child of mine" — the same fail-OPEN shape as the jlist parser reading an empty
# result as "nothing to stop". A process whose parentage cannot be determined might be a
# child, and a stop that skips it reports success over a survivor.
#
# So the two cases are separated. A pid that VANISHED between the listing and the read is
# genuinely gone and is not a running child. A pid that EXISTS but cannot be read, or whose
# PPid is missing or malformed, is UNRESOLVED and is recorded for the caller to fail on.
#
# It is recorded in a FILE, not a variable: children_of is called inside $( ), which is a
# subshell, and a variable set there would never reach the caller. That is the same
# subshell trap that has already cost this campaign three separate defects.
SCAN_UNRESOLVED="${TMPDIR:-/tmp}/woa23-stop-unresolved.$$"
: > "$SCAN_UNRESOLVED" 2>/dev/null || SCAN_UNRESOLVED=""

children_of() {    # children_of <pid> — direct children, from /proc, never from ps
  local parent="$1" p v rc
  for p in $(ls "$PROC" 2>/dev/null | grep '^[0-9][0-9]*$'); do
    v="$(ppid_of "$p")"; rc=$?
    case "$rc" in
      0) [ "$v" = "$parent" ] && printf '%s\n' "$p" ;;
      2) : ;;   # vanished mid-scan: gone, therefore not a running child
      *) [ -n "$SCAN_UNRESOLVED" ] && printf '%s rc=%s\n' "$p" "$rc" >> "$SCAN_UNRESOLVED" ;;
    esac
  done
  return 0
}

# DESCENDANTS, NOT ONLY DIRECT CHILDREN. gunicorn's workers are direct children of the
# master, but nothing guarantees the tree is only two deep — a worker that spawns its own
# helper leaves a grandchild, and a stop that recorded only direct children would never
# check it and would then report success over a survivor.
#
# The walk is breadth-first with an explicit depth bound. The bound is not decoration:
# /proc is read while processes are starting and exiting, and a walk with no bound is one
# malformed parentage away from not terminating.
descendants_of() {   # descendants_of <pid> — every descendant, breadth-first, bounded
  local frontier="$1" next="" seen=" $1 " depth=0 f c
  while [ -n "$frontier" ] && [ "$depth" -lt "${WOA23_STOP_MAX_DEPTH:-16}" ]; do
    next=""
    for f in $frontier; do
      for c in $(children_of "$f"); do
        case "$seen" in *" $c "*) continue ;; esac   # already recorded: no cycles, no repeats
        seen="$seen$c "
        printf '%s\n' "$c"
        next="$next $c"
      done
    done
    frontier="$next"
    depth=$((depth + 1))
  done
  [ -z "$frontier" ] || die \
    "PROBLEM: the process tree under $1 is deeper than ${WOA23_STOP_MAX_DEPTH:-16} levels." \
    "  Refusing to report a partial tree as the whole tree."
}

unresolved_scan_must_be_empty() {   # called after every children_of scan
  [ -n "$SCAN_UNRESOLVED" ] || die \
    "could not create the unresolved-scan record; refusing to scan /proc without it." \
    "  Without it an unreadable process would be silently treated as 'not a child'."
  [ -s "$SCAN_UNRESOLVED" ] || return 0
  die "PROBLEM: the /proc scan could not determine the parent of $(wc -l < "$SCAN_UNRESOLVED" | tr -d ' ') process(es):" \
      "$(sed 's/^/    pid /' "$SCAN_UNRESOLVED")" \
      "" \
      "  rc=3 means /proc/<pid>/status exists but is not readable; rc=4 means it has no" \
      "  usable PPid: line. Either way the parentage is UNKNOWN, and an unknown parent is" \
      "  NOT the same as 'not a child of this app'." \
      "" \
      "  This is NOT reported as 'no children'. A child missed here is a survivor that" \
      "  would never be checked, which is the failure this script exists to prevent."
}


# Sourcing with WOA23_STOP_LIB_ONLY=1 defines the resolver AND the /proc readers, then
# stops, so the offline suite can feed both fixtures directly rather than inferring their
# behaviour from a run. The /proc readers are above this line deliberately: they are the
# part most worth driving with hand-built fixtures, because the cases that break them --
# a comm containing a space, an unreadable status -- are ones a real host rarely produces
# on demand.
if [ "${WOA23_STOP_LIB_ONLY:-}" = 1 ]; then
  return 0 2>/dev/null || exit 0
fi


# ------------------------------------------------------------ 0. THE B1 GRANT, FIRST
# BEFORE the app name, before PM2_HOME, before any pm2 invocation and long before any
# stop. This script terminates processes; it needs its own explicit authorisation, and
# that authorisation must be checked at the point where nothing has happened yet.
#
# IT IS ITS OWN GRANT AND NOTHING SUBSTITUTES FOR IT. WOA23_PM2C_GRANTED authorises a
# STAGING VALIDATION -- creating a tree, starting an app, binding a port. It has never
# authorised stopping anything, and until now this script asked for no grant at all, so a
# staging grant was in practice enough to reach a `pm2 stop`. A grant that authorises one
# act must not silently license a different one.
B1_GRANT="${WOA23_B1_GRANTED:-}"
if [ "$B1_GRANT" != "yes" ]; then
  die "REFUSING: WOA23_B1_GRANTED is not 'yes' (got '${B1_GRANT:-<unset>}')." \
      "  This script stops a named PM2 app and its process tree. It requires its OWN" \
      "  grant, checked here before anything else happens." \
      "" \
      "  No other grant is accepted in its place. WOA23_PM2C_GRANTED authorises a staging" \
      "  validation -- a tree, an app, a port -- and has never authorised a stop."
fi
# Any OTHER run's grant present alongside means two authorisations are in scope at once,
# and the target of this stop is then ambiguous.
for other in WOA23_PM2C_GRANTED WOA23_S2PERF_GRANTED WOA23_S2_C1_GRANTED \
             WOA23_S2_C2_GRANTED WOA23_D1_GRANTED WOA23_D2A_GRANTED WOA23_D2B_GRANTED \
             WOA23_BASH5_VERIFY_GRANTED; do
  eval "v=\${$other:-}"
  [ -n "$v" ] && die "REFUSING: $other is set alongside WOA23_B1_GRANTED." \
      "  A stop must not run inside another run's authorisation: which daemon this stop" \
      "  was meant for would be a matter of inference, and this script does not infer."
done
# CONSUMED HERE. `pm2` may spawn the God Daemon, which passes its environment to every
# app it starts, so leaving the grant set would put this run's authorisation token into a
# serving process's environment.
unset WOA23_B1_GRANTED

APP="${1:-}"
[ -n "$APP" ] || die "usage: production_stop.sh <app-name>" \
  "  The app is named explicitly. There is no default and no wildcard."

# `all` is how a stop meant for one app becomes a stop of every app the daemon knows —
# including, under production's PM2_HOME, dask-scheduler, dask-worker, ghrsst, mhwapi and
# tide. It is refused here rather than discouraged in a runbook.
case "$APP" in
  all|ALL|*'*'*) die "refusing app name '$APP': this script stops ONE named app." \
                     "  'all' would reach every app under this PM2_HOME." ;;
esac

# PM2_HOME is required and never defaulted. A pm2 command without it silently uses
# ~/.pm2 — production's — so a defaulted value is the difference between stopping a
# staging app and stopping production.
PM2_HOME_ARG="${WOA23_PM2_HOME:-}"
[ -n "$PM2_HOME_ARG" ] || die \
  "WOA23_PM2_HOME is required and has no default." \
  "  Without it, pm2 falls back to ~/.pm2 — production's daemon. A stop that picks its" \
  "  own target is not a stop you authorised."
[ -d "$PM2_HOME_ARG" ] || die "WOA23_PM2_HOME is not a directory: $PM2_HOME_ARG"
export PM2_HOME="$PM2_HOME_ARG"

PM2="${WOA23_PM2_BIN:-pm2}"
command -v "$PM2" >/dev/null 2>&1 || die "no pm2 binary: $PM2"

GRACE="${WOA23_STOP_GRACE:-30}"

echo "== stopping '$APP' by identity =="
echo "  PM2_HOME : $PM2_HOME"
echo "  grace    : ${GRACE}s"

# ------------------------------------------------------------- 1. resolve, from PM2
# The PID comes from PM2's own record of the app it started — the only source that knows
# which process belongs to THIS app. Not from a process listing, which knows only what
# programs are running.
#
# THE PARSER IS ORDER-INDEPENDENT, AND THAT IS THE WHOLE POINT.
#
# The version this replaces walked `pm2 jlist` with awk, setting a flag on `"name"` and
# then taking the NEXT `"pid"`. In pm2 5.4.2 the fields arrive the other way round --
#     [{"pid":1709484 , "name":"woa23-bs3v1-candidate" ...
# -- so the only `"pid"` passed BEFORE the flag was ever set, the parser extracted
# nothing, and empty was read as "nothing to stop". In `bs3v1` this script reported
# success and exited 0 WHILE THE SERVICE WAS STILL RUNNING AND HOLDING ITS PORT.
#
# That is the same defect pm2F found and fixed in staging_execute.sh by moving to a JSON
# parse; this file was never fixed and carried it until now. A stop path that fails OPEN
# -- telling an operator the service stopped when it did not -- is worse than one that
# refuses, because every later check here is downstream of a pid it never obtained.
#
# So: parse the JSON as JSON, and FAIL CLOSED on anything short of one unambiguous answer.
# "Nothing to stop" is reported ONLY when a well-formed listing is read and the named app
# is genuinely absent from it.
JLIST="$("$PM2" jlist 2>/dev/null)" || die "cannot read pm2 jlist under $PM2_HOME"
VERDICT="$(printf '%s' "$JLIST" | jlist_resolve "$APP")"
[ -n "$VERDICT" ] || die "the pid resolver produced no verdict at all" \
  "  Refusing: an empty verdict is not evidence that there is nothing to stop."

case "$VERDICT" in
  "OK "*)
    PID="${VERDICT#OK }"
    ;;
  NOTFOUND)
    # A well-formed listing was read and the named app is genuinely not in it.
    echo "  pm2's listing is well-formed and contains no app named '$APP' — nothing to stop."
    exit 0
    ;;
  STOPPED)
    # pm2 POSITIVELY reports the app as stopped (pid 0, status "stopped"). Stopping an
    # already-stopped app is idempotent and must stay so. This is distinct from the
    # bs3v1 failure, where the parser could not determine anything and empty was
    # mistaken for stopped -- that path now lands in PROBLEM below.
    echo "  pm2 reports '$APP' as stopped (pid 0, status stopped) — nothing to stop."
    exit 0
    ;;
  *)
    die "cannot determine the pid for '$APP': ${VERDICT#PROBLEM }" \
      "  This is NOT reported as 'nothing to stop'. The app may well be running." \
      "  bs3v1 is why: a parser that returned nothing was read as an empty app list," \
      "  and this script exited 0 while the service was still up on its port." \
      "  Nothing has been stopped, signalled or changed. Inspect and decide."
    ;;
esac

case "$PID" in
  ''|*[!0-9]*) die "the resolved pid is not a number: '$PID'" ;;
esac
[ "$PID" -gt 0 ] 2>/dev/null || die "the resolved pid is not positive: '$PID'"

MASTER_START="$(starttime_of "$PID")" \
  || die "cannot read $PROC/$PID/stat — refusing to act on a pid I cannot identify."
echo "  master   : pid=$PID starttime=$MASTER_START"

# ------------------------------------------------------- 2. record the tree, with identity
TREE=""
for c in $(descendants_of "$PID"); do
  # A DESCENDANT WHOSE STARTTIME CANNOT BE READ IS NOT DROPPED. The old loop kept only
  # children with a readable starttime and silently discarded the rest, so a process that
  # existed but could not be identified vanished from the record and would never be
  # checked for survival — fail-open, in the middle of the fail-closed path.
  cs="$(starttime_of "$c" 2>/dev/null || true)"
  if [ -z "$cs" ]; then
    if [ -e "$PROC/$c" ]; then
      die "PROBLEM: descendant pid $c exists but its starttime cannot be read." \
          "  It cannot be given an identity, so its survival could never be verified." \
          "  It is NOT dropped from the record and NOT assumed gone."
    fi
    continue   # vanished between the scan and this read: genuinely gone
  fi
  TREE="$TREE $c:$cs"; echo "  worker   : pid=$c starttime=$cs"
done
unresolved_scan_must_be_empty
RECORDED="$PID:$MASTER_START$TREE"

# ---------------------------------------------------------------- 3. graceful stop
echo "  issuing  : pm2 stop $APP   (named app only; never 'all'; no SIGKILL)"
"$PM2" stop "$APP" >/dev/null 2>&1
rc=$?
[ "$rc" -eq 0 ] || echo "  NOTE: pm2 stop returned $rc; verification below is what decides"

# ------------------------------------------------------- 4. wait, then verify by identity
# EVERY RECORDED PROCESS ENDS IN EXACTLY ONE OF FOUR BUCKETS, and "I could not tell" is
# one of them rather than being folded into "gone":
#
#   gone         /proc/<pid> is absent
#   recycled     alive, but the starttime differs -- the pid now belongs to someone else
#   survivor     alive with OUR starttime
#   INDETERMINATE  exists, identity unreadable. NOT gone, NOT dropped, NOT a clean stop.
#
# Nothing is ever removed from RECORDED. The old loop rebuilt `survivors` from scratch each
# pass and silently forgot anything it could not read; a process that became unreadable
# after being recorded simply disappeared from the accounting.
waited=0
while [ "$waited" -lt "$GRACE" ]; do
  survivors=""; indeterminate=""
  for entry in $RECORDED; do
    p="${entry%%:*}"; s="${entry##*:}"
    state="$(proc_state "$p")"
    case "$state" in
      GONE)     ;;                                    # genuinely gone
      "ALIVE $s") survivors="$survivors $entry" ;;    # alive AND ours
      ALIVE\ *) ;;                                    # alive but recycled: not ours
      *)        indeterminate="$indeterminate $p:${state#UNKNOWN }" ;;
    esac
  done
  [ -z "$survivors" ] && [ -z "$indeterminate" ] && break
  sleep 1
  waited=$((waited + 1))
done

echo
# INDETERMINATE IS REPORTED BEFORE SURVIVORS and is its own exit code, because the two
# demand different follow-up: a survivor is a process that would not stop, while an
# indeterminate one is a process this script could not even ask about. Reporting the
# second as a clean stop is the defect this block exists to prevent.
if [ -n "$indeterminate" ]; then
  echo "  INDETERMINATE: these recorded processes still EXIST but could not be identified" >&2
  for e in $indeterminate; do echo "    pid=${e%%:*} reason=${e#*:}" >&2; done
  echo >&2
  echo "  /proc/<pid> is present, so they are NOT gone. Their stat could not be read or" >&2
  echo "  parsed, so their (pid, starttime) identity could not be confirmed either." >&2
  echo "  An unverifiable process is NOT a stopped process, and this is NOT a clean stop." >&2
  echo "  Nothing has been escalated. State is preserved exactly as it is." >&2
  exit 8
fi

if [ -z "$survivors" ]; then
  echo "  STOPPED: every recorded process is gone (or its pid now belongs to something else)"
  echo "  waited : ${waited}s of ${GRACE}s"
  exit 0
fi

# --------------------------------------------------------------------- 5. fail closed
echo "  CLEANUP_FAIL: these recorded processes are still alive after ${GRACE}s" >&2
for entry in $survivors; do
  p="${entry%%:*}"
  echo "    pid=${p} starttime=${entry##*:}" >&2
done
echo >&2
echo "  NOT ESCALATING. SIGKILL is not sent here, by policy and on purpose: a worker" >&2
echo "  killed mid-response produces a truncated answer to a real caller, and the" >&2
echo "  surviving process is evidence of why the graceful path did not finish." >&2
echo "  State is left exactly as it is for inspection." >&2
exit 7
