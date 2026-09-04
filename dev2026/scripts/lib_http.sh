#!/usr/bin/env bash
#
# The two request-issuing loops, sourced so they can be tested against a real server.
#
# They were inline in `run_controlled.sh`, which meant the retry loop that decides
# how many requests a run sends to the host had no test at all. That is the same
# shape of problem as logic inside a shell heredoc: right until it isn't, and nothing
# would notice. `scripts/test_requests.sh` drives both of these against a server that
# answers late, hangs, and refuses.
#
# Both count **attempts**, before issuing, via `request_attempt`. See
# `scripts/lib_requests.sh` for why the ordering matters.
#
# Requires lib_requests.sh to be sourced first. Sourced, never executed.

#: How many times readiness is attempted before giving up, and how long each attempt
#: may take. Named rather than inline so a test can shorten them without editing the
#: loop it is testing.
: "${READY_ATTEMPTS:=30}"
: "${READY_TIMEOUT_SECS:=5}"
: "${READY_SLEEP_SECS:=1}"
: "${PROBE_TIMEOUT_SECS:=60}"

# PROCESS readiness. A 200 on the OpenAPI document says the process is serving; it
# says nothing about whether the store can be read. That is the data probe's job.
#
# Every iteration is one request and is counted as one, including the ones that time
# out and the ones that are refused because nothing is listening yet. A run that
# needed nineteen attempts issued nineteen requests.
process_ready() {           # process_ready <arm> <port>
  local arm="$1" port="$2" out
  local i=0
  while [ "$i" -lt "$READY_ATTEMPTS" ]; do
    i=$((i + 1))
    request_attempt "$arm" readiness
    out="$(curl -s --max-time "$READY_TIMEOUT_SECS" -o /dev/null \
           -w '%{http_code} %{size_download}' \
           "http://127.0.0.1:${port}/api/swagger/woa23/openapi.json" || true)"
    if [ "${out%% *}" = "200" ] && [ "${out##* }" -gt 0 ]; then
      return 0
    fi
    sleep "$READY_SLEEP_SECS"
  done
  return 1
}

# STORE readiness. One request against the data path, and the only thing in the run
# before the gate that establishes the store can actually be read.
#
# No retry, deliberately: the arms are already process-ready by this point, so a
# failure here is a fact about the store rather than a race, and retrying would turn
# a clear answer into a slow one.
probe() {                   # probe <arm> <port>
  local arm="$1" port="$2" out
  request_attempt "$arm" store_probe
  out="$(curl -s --max-time "$PROBE_TIMEOUT_SECS" -o /dev/null \
         -w '%{http_code} %{size_download}' \
         "http://127.0.0.1:${port}/api/woa23?lon0=135&lat0=15&parameter=temperature" \
         || true)"
  if [ "${out%% *}" != "200" ] || [ "${out##* }" -le 0 ]; then
    echo "$arm cannot serve the data path (got '$out'); see ${RUN:-?}/$arm.log" >&2
    return 1
  fi
}
