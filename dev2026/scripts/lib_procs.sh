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
starttime_of() {
  local raw
  if [ -r "/proc/$1/stat" ]; then
    raw="$(cat "/proc/$1/stat" 2>/dev/null)" || return 1
    printf '%s\n' "${raw#*) }" | awk '{print $20}'
    return 0
  fi
  raw="$(ps -o lstart= -p "$1" 2>/dev/null)" || return 1
  [ -n "$raw" ] || return 1
  printf '%s\n' "$raw" | tr -s ' ' '_'
}

ppid_of() {
  local raw
  if [ -r "/proc/$1/stat" ]; then
    raw="$(cat "/proc/$1/stat" 2>/dev/null)" || return 1
    printf '%s\n' "${raw#*) }" | awk '{print $2}'
    return 0
  fi
  raw="$(ps -o ppid= -p "$1" 2>/dev/null)" || return 1
  [ -n "$raw" ] || return 1
  printf '%s\n' "$raw" | tr -d ' '
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

# Every PID below $1, at any depth. One `ps` snapshot, so the walk is consistent.
descendants_of() {
  local snapshot frontier next out="" parent child c
  snapshot="$(ps -eo pid=,ppid= 2>/dev/null)" || return 1
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

# Snapshot <name>'s whole tree as `pid:starttime` lines. Call once the children
# exist — a gunicorn arbiter has not forked its worker in the first moments after
# exec, so a snapshot taken at launch records the arbiter alone.
record_tree() {             # record_tree <name>
  local name="$1" pid p st
  pid="$(cat "$RUN/$name.pid")" || return 1
  : > "$RUN/$name.tree"
  # shellcheck disable=SC2046
  for p in $pid $(descendants_of "$pid"); do
    st="$(starttime_of "$p" || true)"
    [ -n "$st" ] && printf '%s:%s\n' "$p" "$st" >> "$RUN/$name.tree"
  done
  printf '%s tree: %s\n' "$name" \
    "$(cut -d: -f1 "$RUN/$name.tree" | tr '\n' ' ')"
}

# Merge any children that appeared since the snapshot. gunicorn respawns a worker
# that died, so the tree recorded after startup can be stale by the time the run
# ends; a respawned worker is just as much ours as the one it replaced.
refresh_tree() {            # refresh_tree <name> <master-pid>
  local name="$1" pid="$2" p st
  [ -f "$RUN/$name.tree" ] || : > "$RUN/$name.tree"
  for p in $(descendants_of "$pid"); do
    grep -q "^$p:" "$RUN/$name.tree" 2>/dev/null && continue
    st="$(starttime_of "$p" || true)"
    [ -n "$st" ] && printf '%s:%s\n' "$p" "$st" >> "$RUN/$name.tree"
  done
}

# Which of <name>'s recorded processes are still running *and* still the same
# process. A PID that no longer exists, or that now carries a different start time
# because the number was recycled, is not ours and is not a survivor — reporting a
# recycled PID as a stranded worker would be a false alarm that erodes the check.
tree_survivors() {          # tree_survivors <name>
  local name="$1" p want now out=""
  [ -f "$RUN/$name.tree" ] || { printf ''; return 0; }
  while IFS=: read -r p want; do
    [ -n "$p" ] || continue
    now="$(starttime_of "$p" || true)"
    [ -n "$now" ] && [ "$now" = "$want" ] && out="$out $p"
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
  local name="$1" port="${2:-}" pid recorded now surv i
  [ -f "$RUN/$name.pid" ] || return 0
  pid="$(cat "$RUN/$name.pid")"
  recorded="$(cat "$RUN/$name.starttime" 2>/dev/null || echo none)"
  now="$(starttime_of "$pid" || true)"

  if [ -n "$now" ]; then
    if [ "$now" != "$recorded" ]; then
      echo "$name: REFUSING TO KILL — PID $pid has a different start time; the PID" >&2
      echo "  was recycled. Left in place for inspection." >&2
      return 1
    fi
    if [ -n "$port" ] && ! pid_holds_port "$pid" "$port"; then
      echo "$name: REFUSING TO KILL — PID $pid no longer holds port ${port}." >&2
      return 1
    fi
    refresh_tree "$name" "$pid"
    kill "$pid" 2>/dev/null || true
  else
    echo "$name: PID $pid is already gone; its children are still accounted for" >&2
  fi

  for i in $(seq 1 20); do
    surv="$(tree_survivors "$name")"
    if [ -z "$surv" ]; then
      [ -z "$port" ] && break
      port_released "$port" && break
    fi
    sleep 1
  done

  surv="$(tree_survivors "$name")"
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
