#!/usr/bin/env bash
#
# Read back what the RUNNING staging process actually got, from its own
# /proc/<pid>/environ — not from the shell that started it.
#
# The pm2A failure is why this exists. The start command passed a store path and port
# 18231; the process received an empty store and port 18221, because PM2 layered the
# config's `env` block over the environment it was given. Every value in the starting
# shell was correct and every value in the process was wrong, and nothing in the run
# would have noticed if the launcher had not refused the empty store.
#
# So the environment is verified where it matters: in the process. A shell variable is
# an intention; /proc/<pid>/environ is what happened.
#
#   deploy/verify_staging_env.sh <pid> <expected-port> <expected-store> <expected-prod-store>
#
# PROC_ROOT can be overridden so the logic is testable off Linux; it defaults to /proc.

set -uo pipefail

PROC_ROOT="${PROC_ROOT:-/proc}"

[ "$#" -eq 4 ] || {
  echo "usage: $0 <pid> <expected-port> <expected-store> <expected-production-store>" >&2
  exit 2
}
pid="$1"; want_port="$2"; want_store="$3"; want_prod="$4"

envfile="$PROC_ROOT/$pid/environ"
[ -r "$envfile" ] || {
  echo "cannot read $envfile — the process is gone, or this is not Linux" >&2
  exit 2
}

# NUL-separated. `tr` rather than a shell loop so an embedded newline in a value
# cannot split one entry into two.
getenv() {   # getenv <NAME>
  tr '\0' '\n' < "$envfile" | sed -n "s/^$1=//p" | head -1
}

fail=0
check() {   # check <label> <expected> <actual>
  if [ "$2" = "$3" ]; then
    printf '  ok   %-22s = %s\n' "$1" "$3"
  else
    fail=$((fail + 1))
    printf '  FAIL %-22s expected [%s], process has [%s]\n' "$1" "$2" "$3"
  fi
}

got_port="$(getenv WOA23_STAGING_PORT)"
got_store="$(getenv WOA23_STAGING_STORE)"
got_prod="$(getenv WOA23_PRODUCTION_STORE)"
got_zarr="$(getenv WOA23_ZARR_STORE)"

echo "process $pid environment, read from $envfile"
check "WOA23_STAGING_PORT"     "$want_port"  "$got_port"
check "WOA23_STAGING_STORE"    "$want_store" "$got_store"
check "WOA23_PRODUCTION_STORE" "$want_prod"  "$got_prod"
# The launcher exports this from the RESOLVED staging store, so it is the value the
# candidate's api.config actually read.
check "WOA23_ZARR_STORE"       "$want_store" "$got_zarr"

# The port that must never appear again, whatever else is true.
for spent in 18221 18231; do
  if [ "$got_port" = "$spent" ]; then
    fail=$((fail + 1))
    echo "  FAIL the process is using SPENT port $spent"
  fi
done
[ "$got_port" = 18221 ] || [ "$got_port" = 18231 ] || \
  echo "  ok   the port is neither 18221 nor 18231"

echo
if [ "$fail" -ne 0 ]; then
  echo "ENVIRONMENT MISMATCH: $fail check(s) failed."
  echo "The process is NOT running with the values it was started with. This is the"
  echo "pm2A failure mode; stop and report rather than restarting."
  exit 1
fi
echo "environment verified in the process itself, not merely in the starting shell"
