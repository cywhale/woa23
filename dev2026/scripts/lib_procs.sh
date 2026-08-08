# Process-identity and process-tree helpers, shared by run_candidate.sh and
# run_controlled.sh. Sourced, never executed. Exercised offline by
# scripts/test_procs.sh, which builds real parent/child processes and kills them.
#
# Why a tree and not a PID: **every service here is more than one process.**
# `gunicorn -w 1` is an arbiter that forks one worker, and `dask worker` defaults to
# `--nanny`, which is a supervisor that forks the worker. Tracking only the PID that
# `$!` returns tracks the arbiter or the nanny and says nothing about the process
# that actually serves requests or reads the store.
#
# Requires $RUN to be set to the run-state directory.
#
# $STOP_WAIT_SECS bounds how long a stop waits for a tree to drain and a port to be
# released. The runners leave it at the default; scripts/test_procs.sh shortens it,
# because several of its cases are *designed* never to drain and would otherwise
# spend the full wait each time.
: "${STOP_WAIT_SECS:=20}"

# The PID's start time — an identity token that PID number alone is not, because
# PIDs are recycled. Field 22 of /proc/<pid>/stat, parsed past the parenthesised
# comm field (which may itself contain spaces and brackets).
#
# The `ps` branch exists so the offline tests can run on a machine without procfs.
# On VM24 the /proc branch is always the one taken.
# Strip `<pid> (<comm>) ` from a /proc/<pid>/stat line, leaving state as the first
# field — so /proc field N is token N-2.
#
# The `##` is load-bearing. `comm` is the executable's basename, it is only
# parenthesised — not escaped — and it may contain `) `. With the shortest match
# (`#`) a process named `my (weird) app` truncates at the first `) `, and the line
# `4321 (my (weird) app) S 4320 ... 987654321 ...` yields ppid `S` and start time
# `0`. The start time is the identity token that distinguishes a recycled PID from
# the original, so two such processes would both parse as `0` and compare equal —
# the recycled-PID guard would pass on a process that is not ours. The longest match
# is correct because no field after `comm` can contain `) `: they are a single
# character and then numbers.
_stat_fields() {            # _stat_fields <stat-line>
  printf '%s\n' "${1##*) }"
}

starttime_of() {
  local raw root="${PROC_ROOT:-/proc}"
  if [ -r "$root/$1/stat" ]; then
    raw="$(cat "$root/$1/stat" 2>/dev/null)" || return 1
    _stat_fields "$raw" | awk '{print $20}'          # field 22: starttime
    return 0
  fi
  raw="$(ps -o lstart= -p "$1" 2>/dev/null)" || return 1
  [ -n "$raw" ] || return 1
  printf '%s\n' "$raw" | tr -s ' ' '_'
}

ppid_of() {
  local raw root="${PROC_ROOT:-/proc}"
  if [ -r "$root/$1/stat" ]; then
    raw="$(cat "$root/$1/stat" 2>/dev/null)" || return 1
    _stat_fields "$raw" | awk '{print $2}'           # field 4: ppid
    return 0
  fi
  raw="$(ps -o ppid= -p "$1" 2>/dev/null)" || return 1
  [ -n "$raw" ] || return 1
  printf '%s\n' "$raw" | tr -d ' '
}

# Is this a start-time token that `starttime_of` on this host could have produced?
#
# The distinction that matters: a *legal but different* token means the PID was
# recycled and the process is not ours — a normal, expected observation. A token
# that is not well formed at all means the tree file is corrupt, and nothing in it
# can be trusted. Without this check the two collapsed: a garbage value simply
# compared unequal to the live process's real start time, so a malformed tree
# reported "no survivors" and cleanup deleted its state over whatever was running.
#
# On Linux the token is /proc field 22, an integer. The `ps` form exists only so the
# tests can run on a machine without procfs; its charset check is weaker than the
# integer one, which is acceptable because production never takes that path.
_valid_starttime() {        # _valid_starttime <token>
  local root="${PROC_ROOT:-/proc}"
  [ -n "$1" ] || return 1
  if [ -r "$root/1/stat" ]; then
    case "$1" in *[!0-9]*) return 1 ;; esac
    return 0
  fi
  # `ps -o lstart=` with runs of spaces collapsed: alphanumerics, `_` and the
  # colons of the clock time, which is always present.
  case "$1" in *[!A-Za-z0-9_:]*) return 1 ;; esac
  case "$1" in *:*) ;; *) return 1 ;; esac
  return 0
}

# Does this PID name a live process? On Linux /proc answers regardless of who owns
# it; `kill -0` would report EPERM for another user's process and is only the
# fallback for machines without procfs.
pid_exists() {
  local root="${PROC_ROOT:-/proc}"
  if [ -d "$root/1" ]; then
    [ -d "$root/$1" ]
    return
  fi
  kill -0 "$1" 2>/dev/null
}

# The one PID in the set whose parent is outside it. Used to tell a server's master
# from the workers that inherited its listening socket.
master_of() {
  local pids="$1" roots="" p pp
  [ -n "$pids" ] || return 1
  for p in $pids; do
    pp="$(ppid_of "$p")" || return 1      # unreadable parent -> ambiguous
    case " $pids " in *" $pp "*) ;; *) roots="$roots $p" ;; esac
  done
  # shellcheck disable=SC2086
  set -- $roots
  [ $# -eq 1 ] || return 1
  echo "$1"
}

# The host's boot identifier. A PID means nothing across a reboot: the number is
# reused and the start time is measured from boot, so a *different* process can
# carry the same pid:starttime pair. Every recorded tree is stamped with this.
boot_id() {
  local raw
  if [ -r /proc/sys/kernel/random/boot_id ]; then
    cat /proc/sys/kernel/random/boot_id
    return 0
  fi
  # No procfs — the offline tests. Boot time is a stable per-boot token.
  raw="$(sysctl -n kern.boottime 2>/dev/null)" || return 1
  [ -n "$raw" ] || return 1
  printf '%s\n' "$raw" | tr -s ' ' '_'
}

# `pid ppid` for every process. On Linux this reads procfs directly and never
# invokes `ps`, so the path taken on VM24 needs no process-listing privilege beyond
# what /proc already grants. The `ps` branch is for machines without procfs.
# $PROC_ROOT is a test seam only: scripts/test_procs.sh points it at a synthetic
# procfs so the Linux parsing path — the one VM24 takes — can be verified on a
# machine that has no /proc. It is never set in the runners.
_pid_ppid_snapshot() {
  local root="${PROC_ROOT:-/proc}"
  if [ -r "$root/1/stat" ]; then
    # One awk over every stat file. The comm field is parenthesised and may itself
    # contain spaces and brackets, so it is stripped greedily before splitting.
    awk 'FNR==1 {
           line = $0; pid = $1
           sub(/^[0-9]+ \(.*\) /, "", line)
           split(line, f, " ")
           print pid, f[2]
         }' "$root"/[0-9]*/stat 2>/dev/null
    return 0
  fi
  ps -eo pid=,ppid= 2>/dev/null
}

# True when this machine can enumerate processes at all. The offline tests check
# this first so a sandbox that denies `ps` reports "cannot verify here" instead of
# dying before the first assertion and looking like a failure.
can_enumerate_processes() {
  local snap
  snap="$(_pid_ppid_snapshot)" || return 1
  [ -n "$snap" ] || return 1
  starttime_of $$ >/dev/null 2>&1 || return 1
  boot_id >/dev/null 2>&1 || return 1
}

# Every PID below $1, at any depth. One snapshot, so the walk is consistent.
descendants_of() {
  local snapshot frontier next out="" parent child c
  snapshot="$(_pid_ppid_snapshot)" || return 1
  [ -n "$snapshot" ] || return 1
  frontier="$1"
  while [ -n "$frontier" ]; do
    next=""
    for parent in $frontier; do
      child="$(printf '%s\n' "$snapshot" | awk -v p="$parent" '$2 == p {print $1}')"
      for c in $child; do
        case " $out " in
          *" $c "*) ;;                    # already seen; also breaks any ppid cycle
          *) out="$out $c"; next="$next $c" ;;
        esac
      done
    done
    frontier="$next"
  done
  printf '%s' "${out# }"
}

# A tree that omits a live process is worse than one that is corrupt: it looks
# valid. Both writers below can fail to see everything — process enumeration can
# fail outright, and a PID found in the snapshot can become unreadable before its
# start time is read — and both used to swallow that and carry on, leaving a
# short tree that `tree_survivors` then reported as fully exited.
#
# So incompleteness is recorded *in the file*. A tree carrying this line can still
# be used to identify the tracked process (so cleanup may still signal it), but it
# can never be read as "everything exited".
# Every function in this file is called in a context that tests its status —
# `record_tree ... || {...}`, `stop_tracked ... || CLEANUP_FAILED=1`, `if ! ...`.
# In bash that switches `set -e` OFF for the whole function body, so an unchecked
# failure inside does not abort anything; it simply carries on to the next line.
# Nothing here may rely on errexit. Every write is checked.
# "This tree may understate what was started" as a state, not just as a line in a
# file that could not be written.
#
# Three layers, because each can fail where the next still works:
#   1. the `incomplete:` line inside the tree — survives across invocations;
#   2. a `<name>.uncertain` sentinel beside it — a *new* file, so it still works
#      when the tree itself is unwritable (a read-only file in a writable
#      directory, which is the common case);
#   3. a shell variable — needs no filesystem at all, so it holds even on a
#      read-only mount, for the remainder of this process.
# Layers 1 and 2 persist; layer 3 does not, which is why 2 exists.
_uncertain_var() {          # _uncertain_var <name>
  printf '_TREE_UNCERTAIN_%s' "$(printf '%s' "$1" | tr -c 'A-Za-z0-9_' '_')"
}

_set_uncertain() {          # _set_uncertain <name>
  local v; v="$(_uncertain_var "$1")"
  printf -v "$v" '1'
  printf 'tree write uncertain\n' > "$RUN/$1.uncertain" 2>/dev/null || return 1
}

# Is this tree's completeness in doubt? Checked before the tree is read at all.
_is_uncertain() {           # _is_uncertain <name>
  local v; v="$(_uncertain_var "$1")"
  [ "${!v:-0}" = "1" ] && return 0
  [ -f "$RUN/$1.uncertain" ]
}

_mark_incomplete() {        # _mark_incomplete <name> <reason>
  local f="$RUN/$1.tree"
  if printf 'incomplete:%s\n' "$2" >> "$f"; then
    return 0
  fi
  # The marker is the only thing standing between a short tree and a reader that
  # believes it. If it cannot be written, the file must not survive looking
  # complete — a missing tree makes tree_boot_matches return 2, which is the
  # fail-closed answer for every later reader too.
  echo "$1: CANNOT WRITE the incompleteness marker to $f" >&2
  # Whatever else happens, this tree is now in doubt for the rest of this process.
  _set_uncertain "$1"
  local sentinel=$?
  if rm -- "$f" 2>/dev/null; then
    echo "  the partial tree has been removed so it cannot be misread as complete" >&2
  elif [ "$sentinel" -eq 0 ]; then
    echo "  and it could not be removed either, so $RUN/$1.uncertain now marks it" >&2
    echo "  unusable — for this run and for any later one" >&2
  else
    echo "  AND it could not be removed, AND the .uncertain sentinel could not be" >&2
    echo "  written. This process will refuse to trust the tree, but nothing on" >&2
    echo "  disk records that. INSPECT $f BEFORE RUNNING ANY CLEANUP AGAINST IT." >&2
  fi
  return 1
}

# Append `pid:starttime` for one PID. Status 1 — and a marker — when the process is
# live but cannot be recorded; status 0 with nothing written when it has simply
# exited since the snapshot, which is a benign race, not a gap.
_record_one() {             # _record_one <name> <pid>
  local name="$1" p="$2" st
  if ! st="$(starttime_of "$p" 2>/dev/null)" || [ -z "$st" ]; then
    if pid_exists "$p"; then
      echo "$name: PID $p is live but its start time cannot be read; the tree" >&2
      echo "  cannot be completed" >&2
      _mark_incomplete "$name" "unreadable-identity-$p" || true
      return 1
    fi
    return 0                # exited during the walk
  fi
  if ! _valid_starttime "$st"; then
    echo "$name: PID $p reported a malformed start time '$st'" >&2
    _mark_incomplete "$name" "malformed-token-$p" || true
    return 1
  fi
  # A pid line that fails to write is a live process dropped from the tree — the
  # same gap as never having looked, so it is marked and reported, not ignored.
  if ! printf '%s:%s\n' "$p" "$st" >> "$RUN/$name.tree"; then
    echo "$name: cannot write PID $p to the tree" >&2
    _mark_incomplete "$name" "unwritable-$p" || true
    return 1
  fi
}

# Snapshot <name>'s whole tree. The file is self-describing: a `boot:<id>` header
# followed by `pid:starttime` lines, plus an `incomplete:<reason>` line if anything
# could not be captured. Without a header that matches the running kernel the PIDs
# below it cannot be interpreted at all, so header and body live in one file rather
# than two that could drift apart.
#
# Call once the children exist — a gunicorn arbiter has not forked its worker in the
# first moments after exec, so a snapshot taken at launch records the arbiter alone.
record_tree() {             # record_tree <name>
  local name="$1" pid p kids rc=0 boot
  pid="$(cat "$RUN/$name.pid")" || return 1
  boot="$(boot_id)" || { echo "cannot read the host boot id" >&2; return 1; }
  if ! printf 'boot:%s\n' "$boot" > "$RUN/$name.tree"; then
    echo "$name: cannot write the tree header to $RUN/$name.tree" >&2
    rm -- "$RUN/$name.tree" 2>/dev/null || true
    return 1
  fi

  # Explicit status. In `for p in $(descendants_of "$pid")` the failure vanishes
  # into the command substitution and the loop simply runs over nothing, so a
  # broken enumeration produced a tree holding the master alone — and every child
  # of it silently stopped being cleanup's problem.
  if ! kids="$(descendants_of "$pid")"; then
    echo "$name: cannot enumerate processes; the tree cannot be completed" >&2
    _mark_incomplete "$name" "process-enumeration-failed" || true
    rc=1; kids=""
  fi
  for p in $pid $kids; do
    _record_one "$name" "$p" || rc=1
  done
  printf '%s tree: %s%s\n' "$name" "$(tree_pids "$name")" \
    "$([ "$rc" -ne 0 ] && echo "  (INCOMPLETE)" || true)"
  return $rc
}

# The PIDs in a recorded tree; header and markers excluded.
tree_pids() {               # tree_pids <name>
  [ -f "$RUN/$1.tree" ] || { printf ''; return 0; }
  grep -Ev '^(boot|incomplete):' "$RUN/$1.tree" | cut -d: -f1 | tr '\n' ' ' \
    | sed 's/ $//'
}

# Merge any children that appeared since the snapshot. gunicorn respawns a worker
# that died, so the tree recorded after startup can be stale by the time the run
# ends; a respawned worker is just as much ours as the one it replaced.
refresh_tree() {            # refresh_tree <name> <master-pid>
  local name="$1" pid="$2" p kids rc=0
  [ -f "$RUN/$name.tree" ] || return 1
  if ! kids="$(descendants_of "$pid")"; then
    echo "$name: cannot enumerate processes; the tree cannot be brought up to" >&2
    echo "  date" >&2
    _mark_incomplete "$name" "process-enumeration-failed" || true
    return 1
  fi
  for p in $kids; do
    grep -q "^$p:" "$RUN/$name.tree" 2>/dev/null && continue
    _record_one "$name" "$p" || rc=1
  done
  return $rc
}

# Status 2 — "cannot be interpreted" — when the tree has no boot header, the header
# does not match the running kernel, or the boot id cannot be read. This is not the
# same as "nothing survived": after a reboot the recorded PIDs may well be live
# processes belonging to someone else, so the only safe action is none.
tree_boot_matches() {       # tree_boot_matches <name>
  local name="$1" recorded current
  # A tree whose write could not be confirmed cannot be interpreted, whatever it
  # happens to contain — it may simply be missing the line that says so.
  _is_uncertain "$name" && return 2
  [ -f "$RUN/$name.tree" ] || return 2
  recorded="$(grep '^boot:' "$RUN/$name.tree" 2>/dev/null | head -1 | cut -d: -f2-)"
  current="$(boot_id || true)"
  [ -n "$recorded" ] && [ -n "$current" ] || return 2
  [ "$recorded" = "$current" ] || return 2
}

# Which of <name>'s recorded processes are still running *and* still the same
# process. A PID that no longer exists, or that now carries a different start time
# because the number was recycled, is not ours and is not a survivor — reporting a
# recycled PID as a stranded worker would be a false alarm that erodes the check.
#
# Status 2 if the tree is from another boot; the caller must not read the empty
# output as "all clear".
tree_survivors() {          # tree_survivors <name>
  local name="$1" line p want now out=""
  tree_boot_matches "$name" || return 2
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    case "$line" in
      boot:*)       continue ;;
      # The writer could not see everything. Whether every process exited is
      # therefore unknown, and unknown is never an all-clear.
      incomplete:*) return 2 ;;
    esac

    # A line that cannot be parsed makes the whole tree uninterpretable. Skipping it
    # would silently shrink the set of processes cleanup is held to.
    case "$line" in *:*) ;; *) return 2 ;; esac
    p="${line%%:*}"; want="${line#*:}"
    case "$p" in ''|*[!0-9]*) return 2 ;; esac
    # A recorded start time that is not a well-formed token is a corrupt file, not
    # a process that has exited.
    _valid_starttime "$want" || return 2

    if pid_exists "$p"; then
      now="$(starttime_of "$p" || true)"
      if [ -z "$now" ] || ! _valid_starttime "$now"; then
        # Two different things reach this point and they have to be told apart by
        # *re-checking*, exactly as _record_one already does:
        #
        #   - the process exited between `pid_exists` and the read. During a stop
        #     that is the normal case — we have just signalled it — and it means
        #     the process is gone, which is the opposite of unknown.
        #   - the process is still there and its identity genuinely cannot be read:
        #     a restricted /proc, a PID that changed hands. Only then is the answer
        #     "cannot determine".
        #
        # Without the re-check every stop raced its own kill. The 2026-08-08-r2 run
        # passed both gates, left nothing running and freed all three ports, and
        # still reported "cannot determine whether every tracked process exited" for
        # all four services — each was reaped inside that window.
        if pid_exists "$p"; then
          return 2
        fi
        continue                # exited while we were looking at it
      fi
      [ "$now" = "$want" ] && out="$out $p"
    fi
  done < "$RUN/$name.tree"
  printf '%s' "${out# }"
}

# ---------------------------------------------------------- tracked lifecycle ---
# Requires lib_ports.sh for port_released/pid_holds_port.

start_tracked() {           # start_tracked <name> <port|""> <cmd...>
  local name="$1" port="$2"; shift 2
  [ -f "$RUN/$name.pid" ] && { echo "$name already tracked" >&2; return 1; }
  "$@" > "$RUN/$name.log" 2>&1 &
  local pid=$!
  echo "$pid" > "$RUN/$name.pid"
  sleep 1
  starttime_of "$pid" > "$RUN/$name.starttime" || {
    echo "could not read the start time of $name (pid $pid)" >&2; return 1; }
  # Record the tree immediately, not later. stop_tracked refuses to signal anything
  # whose tree it cannot interpret, so a service that is tracked but has no tree
  # file yet would be left running by any cleanup between here and the first
  # explicit record_tree — precisely the abort paths (a port that never binds, a
  # readiness timeout) where cleanup matters most. This snapshot may hold the master
  # alone, since a gunicorn arbiter has not forked yet; refresh_tree picks up the
  # children at stop time, and the callers re-record once the arms are ready.
  record_tree "$name" > /dev/null || {
    echo "could not record the process tree for $name" >&2; return 1; }
  echo "$name started (pid $pid${port:+, port $port})"
}

# Stop <name> and prove it is gone — the whole tree, not just the PID we launched.
#
# The port is not the criterion. A gunicorn arbiter that exits releases the
# listening socket while a worker it forked can still be running: the socket is
# closed, the port test passes, and a process is left on the host. An earlier
# version removed the pidfile and reported success in exactly that state, which
# contradicted the claim that any stranded process fails the run.
#
# Nothing is escalated. Survivors are identity-verified as ours, so signalling them
# would be defensible, but the standing rule in this project is to refuse to act on
# anything ambiguous and leave it for a human — so survivors are named, the state
# files are kept, and the run fails.
stop_tracked() {            # stop_tracked <name> <port|"">
  local name="$1" port="${2:-}" pid recorded now surv i st incomplete=0 f
  [ -f "$RUN/$name.pid" ] || return 0
  pid="$(cat "$RUN/$name.pid")"

  # Before anything else, and before any signal: does the recorded tree even
  # describe this boot? A PID is reused after a reboot and its start time is
  # measured from boot, so the same pid:starttime pair can name a completely
  # different process — someone else's. Killing on that basis is the worst thing
  # this script could do, so a mismatch stops it here, with nothing signalled and
  # nothing deleted.
  st=0; tree_boot_matches "$name" || st=$?
  if [ "$st" -ne 0 ]; then
    echo "$name: REFUSING TO ACT — the recorded process tree is not from the" >&2
    echo "  running kernel (missing, unreadable, or a different boot id). The PIDs" >&2
    echo "  in it may now belong to unrelated processes. Nothing signalled, nothing" >&2
    echo "  removed; state left for inspection." >&2
    return 1
  fi

  recorded="$(cat "$RUN/$name.starttime" 2>/dev/null || echo none)"
  now="$(starttime_of "$pid" || true)"

  if [ -n "$now" ]; then
    if [ "$now" != "$recorded" ]; then
      echo "$name: REFUSING TO KILL — PID $pid has a different start time; the PID" >&2
      echo "  was recycled. Left in place for inspection." >&2
      return 1
    fi
    # Holding the port is NOT part of this process's identity, and requiring it
    # before signalling was a refusal to clean up on the paths where cleanup
    # matters most. A service aborted before it ever bound — a readiness timeout, a
    # provenance check that failed, a scheduler that never came up — has a live PID
    # that holds nothing, and the old form left it running with "REFUSING TO KILL".
    #
    # `pid` + `starttime` + the tree's boot id already identify this process
    # uniquely and are what authorise the signal. The socket is reported, not
    # required.
    if [ -n "$port" ] && ! pid_holds_port "$pid" "$port"; then
      echo "$name: PID $pid does not currently hold port ${port} — not yet bound," >&2
      echo "  or already released. Stopping it anyway; it is provably the process" >&2
      echo "  this run started." >&2
    fi
    # The status is carried in a local, not inferred from the file afterwards.
    # refresh_tree does try to mark the tree, but that write can itself fail — and
    # then the only record that this stop is unverifiable would be a log line.
    #
    # Still signal: this PID's identity is already established by start time and
    # boot id, so stopping it is safe and leaves the host cleaner than refusing
    # would. What must not happen is *reporting* a clean stop.
    if ! refresh_tree "$name" "$pid"; then
      incomplete=1
      echo "$name: the tree could not be brought up to date; stopping the tracked" >&2
      echo "  process anyway (its identity is established), but this stop cannot" >&2
      echo "  be verified complete and the run will fail." >&2
    fi
    kill "$pid" 2>/dev/null || true
  else
    echo "$name: PID $pid is already gone; its children are still accounted for" >&2
  fi

  for i in $(seq 1 "$STOP_WAIT_SECS"); do
    surv="$(tree_survivors "$name")" || break     # boot changed mid-stop
    if [ -z "$surv" ]; then
      [ -z "$port" ] && break
      port_released "$port" && break
    fi
    sleep 1
  done

  st=0; surv="$(tree_survivors "$name")" || st=$?
  if [ "$incomplete" -ne 0 ] && [ "$st" -eq 0 ]; then
    echo "$name: the tracked process was stopped, but the tree could not be" >&2
    echo "  brought up to date during this stop, so whether every process it had" >&2
    echo "  forked has exited is unknown. State left for inspection." >&2
    return 1
  fi
  if [ "$st" -ne 0 ]; then
    # Status 2 covers three things and the message must not name only one: the boot
    # id changed mid-stop, a recorded line is malformed, or a PID is live but its
    # identity cannot be read. All three mean the same thing here — whether every
    # process exited is unknown, and unknown is not clean.
    echo "$name: cannot determine whether every tracked process exited — the" >&2
    echo "  recorded tree became uninterpretable (boot id changed, a line is" >&2
    echo "  malformed, or a live PID's identity is unreadable). State left for" >&2
    echo "  inspection." >&2
    return 1
  fi
  if [ -n "$surv" ]; then
    echo "$name: still running after stop: PID(s) $surv" >&2
    echo "  pidfile and tree left for inspection; do not start another instance" >&2
    return 1
  fi
  if [ -n "$port" ] && ! port_released "$port"; then
    echo "$name: every tracked process has exited but port ${port} could not be" >&2
    echo "  confirmed free — either something else holds it, or the port state is" >&2
    echo "  unreadable. State left for inspection." >&2
    return 1
  fi
  if _is_uncertain "$name"; then
    echo "$name: refusing to remove state — this tree's completeness was never" >&2
    echo "  confirmed, so a clean stop cannot be claimed for it." >&2
    return 1
  fi
  # Every removal is checked. errexit is off inside this function — it is always
  # called with its status tested — so an unchecked `rm` that failed would fall
  # straight through to `return 0`, reporting a clean stop with the state files
  # still on disk. The next preflight would then block on state this run claimed
  # to have cleared.
  local left=""
  for f in "$RUN/$name.pid" "$RUN/$name.starttime" "$RUN/$name.tree"; do
    [ -e "$f" ] || continue
    rm -- "$f" 2>/dev/null || left="$left $f"
  done
  if [ -n "$left" ]; then
    echo "$name: every tracked process exited and the port is free, but the state" >&2
    echo "  file(s) could not be removed:$left" >&2
    echo "  Not reporting a clean stop while they are still on disk." >&2
    return 1
  fi
  echo "$name stopped; whole tree exited, port ${port:-n/a} ${port:+confirmed free}"
  return 0
}
