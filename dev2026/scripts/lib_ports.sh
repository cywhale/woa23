# Port-state helpers, shared by run_candidate.sh and run_controlled.sh.
#
# Sourced, never executed. Both runners decide whether to kill a process, whether a
# socket was released, and whether production is the process it was — all from this
# file, so it is kept separate and exercised by scripts/test_ports.sh against a
# captured `ss` fixture.
#
# Two rules the callers depend on:
#
#   * "could not determine" is never "nothing there". `ss` failing must not read as
#     a free port, or preflight would happily start a second server over a live one.
#     Every function here returns **status 2** for unknown, distinct from 1.
#   * An empty result is an observation, not an error. `grep` exits 1 when nothing
#     matches and `set -o pipefail` propagates that, so an unguarded pipeline made
#     "no listeners" a failure. Under `set -e`, in `x="$(pids_on_port 8050)"`, that
#     aborted the script with no message at all.

# LISTEN rows whose local-address port is exactly $1. Status 2 if `ss` cannot run.
#
# `ss` writes the local address as `addr:port` and the address half may contain
# colons of its own, so a substring test for ":8050" also matches `[fe80::8050]:9000`
# — a different service on a different port — and a `\b`-anchored one matches any
# IPv6 address ending in the port's digits. Take the text after the last colon of
# the local-address column and compare it as a number. LISTEN-only also drops the
# header row and stops a *client* of the port being counted as holding it.
ss_rows_on_port() {
  local raw
  if ! raw="$(ss -lntp 2>/dev/null)"; then
    echo "ss failed: port state cannot be determined" >&2
    return 2
  fi
  printf '%s\n' "$raw" | awk -v want="$1" '
    $1 == "LISTEN" && NF >= 4 {
      n = split($4, part, ":")
      if (n >= 2 && part[n] ~ /^[0-9]+$/ && part[n] + 0 == want + 0) print
    }'
}

# 0 = held, 1 = free, 2 = unknown.
port_held() {
  local rows
  rows="$(ss_rows_on_port "$1")" || return 2
  [ -n "$rows" ]
}

# True only when the port is *observed* free. Unknown is not released — this is the
# form to use before removing a pidfile or reporting a clean stop, because treating
# an unreadable `ss` as "released" is how a stranded process gets forgotten.
port_released() {
  local st=0
  port_held "$1" || st=$?
  [ "$st" -eq 1 ]
}

# Does PID $1 hold port $2? Unknown propagates as 2, so `if ! pid_holds_port` — the
# refuse-to-kill guard — fails closed.
pid_holds_port() {
  local rows
  rows="$(ss_rows_on_port "$2")" || return 2
  printf '%s\n' "$rows" | grep -qE "pid=$1,"
}

# Every PID holding the listening socket, space-separated, sorted, possibly empty.
# Status 2 if the port state could not be read.
#
# A forking server has several: gunicorn's master creates the socket and each worker
# inherits it, so `ss` reports all of them and the order is not meaningful. An
# earlier version took the first `pid=` match as the master; on the live production
# port that returned a worker (4366) while the master was 3960.
pids_on_port() {
  local rows out
  rows="$(ss_rows_on_port "$1")" || return 2
  out="$(printf '%s\n' "$rows" | grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -un \
         | tr '\n' ' ')" || out=""
  printf '%s' "$out"
}

# ------------------------------------------------------- the used-port ledger ---
# `port_held` answers "is anything listening RIGHT NOW". That is not the same
# question as "has this campaign bound this port before", and conflating the two is
# how 18091/18092 came to be described as new ports when c2c had already used them:
# every port a finished run used is free again afterwards, precisely because its
# cleanup worked.
#
# So the two checks are separate and both run. This one is a lookup in a committed
# file, reads nothing from the host, and cannot be satisfied by waiting.
PORTS_LEDGER="${PORTS_LEDGER:-$(dirname "${BASH_SOURCE[0]}")/ports_used.tsv}"

# The ledger rows naming port $1, tab-separated, empty if none. Status 0 if the port
# has been used before, 1 if it has not, 2 if the ledger cannot be read — which is
# NOT the same as "not used" and must not be treated as one.
port_previously_used() {
  local port="$1" rows
  [ -r "$PORTS_LEDGER" ] || return 2
  rows="$(grep -E "^${port}	" "$PORTS_LEDGER" || true)"
  [ -n "$rows" ] || return 1
  printf '%s\n' "$rows"
  return 0
}

# One line per port: "<port> first-use" or "<port> REUSED <role>, <run>".
port_ledger_status() {
  local port="$1" rows st=0
  rows="$(port_previously_used "$port")" || st=$?
  case "$st" in
    0) printf '%s REUSED %s\n' "$port" \
         "$(printf '%s' "$rows" | head -1 | cut -f2,3 | tr '\t' ',')" ;;
    1) printf '%s first-use\n' "$port" ;;
    *) printf '%s UNKNOWN (ledger %s is unreadable)\n' "$port" "$PORTS_LEDGER" ;;
  esac
  return "$st"
}
