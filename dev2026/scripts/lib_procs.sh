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

# Snapshot <name>'s whole tree. The file is self-describing: a `boot:<id>` header
# followed by `pid:starttime` lines. Without a header that matches the running
# kernel the PIDs below it cannot be interpreted at all, so the header and the body
# live in one file rather than two that could drift apart.
#
# Call once the children exist — a gunicorn arbiter has not forked its worker in the
# first moments after exec, so a snapshot taken at launch records the arbiter alone.
record_tree() {             # record_tree <name>
  local name="$1" pid p st boot
  pid="$(cat "$RUN/$name.pid")" || return 1
  boot="$(boot_id)" || { echo "cannot read the host boot id" >&2; return 1; }
  printf 'boot:%s\n' "$boot" > "$RUN/$name.tree"
  # shellcheck disable=SC2046
  for p in $pid $(descendants_of "$pid"); do
    st="$(starttime_of "$p" || true)"
    [ -n "$st" ] && printf '%s:%s\n' "$p" "$st" >> "$RUN/$name.tree"
  done
  printf '%s tree: %s\n' "$name" "$(tree_pids "$name")"
}

# The PIDs in a recorded tree, header excluded.
tree_pids() {               # tree_pids <name>
  [ -f "$RUN/$1.tree" ] || { printf ''; return 0; }
  grep -v '^boot:' "$RUN/$1.tree" | cut -d: -f1 | tr '\n' ' ' | sed 's/ $//'
}

# Merge any children that appeared since the snapshot. gunicorn respawns a worker
# that died, so the tree recorded after startup can be stale by the time the run
# ends; a respawned worker is just as much ours as the one it replaced.
refresh_tree() {            # refresh_tree <name> <master-pid>
  local name="$1" pid="$2" p st
  [ -f "$RUN/$name.tree" ] || return 1
  for p in $(descendants_of "$pid"); do
    grep -q "^$p:" "$RUN/$name.tree" 2>/dev/null && continue
    st="$(starttime_of "$p" || true)"
    [ -n "$st" ] && printf '%s:%s\n' "$p" "$st" >> "$RUN/$name.tree"
  done
}

# Status 2 — "cannot be interpreted" — when the tree has no boot header, the header
# does not match the running kernel, or the boot id cannot be read. This is not the
# same as "nothing survived": after a reboot the recorded PIDs may well be live
# processes belonging to someone else, so the only safe action is none.
tree_boot_matches() {       # tree_boot_matches <name>
  local name="$1" recorded current
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
    case "$line" in boot:*) continue ;; esac

    # A line that cannot be parsed makes the whole tree uninterpretable. Skipping it
    # would silently shrink the set of processes cleanup is held to.
    case "$line" in *:*) ;; *) return 2 ;; esac
    p="${line%%:*}"; want="${line#*:}"
    case "$p" in ''|*[!0-9]*) return 2 ;; esac
    [ -n "$want" ] || return 2

    if pid_exists "$p"; then
      now="$(starttime_of "$p" || true)"
      if [ -z "$now" ]; then
        # The PID is live but its identity cannot be read — a restricted /proc, a
        # process that changed hands. "Exited" is the one thing it is definitely
        # not, and treating it as exited is what would let cleanup delete the state
        # files and report success over a running process.
        return 2
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
  local name="$1" port="${2:-}" pid recorded now surv i st
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
    refresh_tree "$name" "$pid"
    kill "$pid" 2>/dev/null || true
  else
    echo "$name: PID $pid is already gone; its children are still accounted for" >&2
  fi

  for i in $(seq 1 20); do
    surv="$(tree_survivors "$name")" || break     # boot changed mid-stop
    if [ -z "$surv" ]; then
      [ -z "$port" ] && break
      port_released "$port" && break
    fi
    sleep 1
  done

  st=0; surv="$(tree_survivors "$name")" || st=$?
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
  rm "$RUN/$name.pid"
  [ -f "$RUN/$name.starttime" ] && rm "$RUN/$name.starttime"
  [ -f "$RUN/$name.tree" ] && rm "$RUN/$name.tree"
  echo "$name stopped; whole tree exited, port ${port:-n/a} ${port:+confirmed free}"
  return 0
}
