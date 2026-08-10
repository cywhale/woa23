#!/usr/bin/env bash
#
# Offline fixture tests for scripts/lib_ports.sh.
#
# Runs under the same `set -euo pipefail` as the runners, because half of what is
# being tested is behaviour *under* those options — an empty result that aborts the
# script is invisible without them.
#
# `ss` is replaced by a stub on PATH, so this needs no host, no ports and no root,
# and it runs on any machine including the development laptop.
#
#     ./scripts/test_ports.sh

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0; fail=0
check() {                   # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}

# Every row below defeated an earlier form of these helpers.
cat > "$TMP/fixture.txt" <<'EOF'
State  Recv-Q Send-Q Local Address:Port  Peer Address:Port Process
LISTEN 0      2048   127.0.0.1:18050     0.0.0.0:*         users:(("other",pid=111,fd=3))
LISTEN 0      2048   [fe80::8050]:9000   [::]:*            users:(("v6svc",pid=222,fd=3))
LISTEN 0      2048   127.0.0.1:8050      0.0.0.0:*         users:(("gunicorn",pid=3960,fd=5))
LISTEN 0      2048   [::]:8050           [::]:*            users:(("gunicorn",pid=4366,fd=5))
LISTEN 0      2048   [fe80::1%eth0]:50   [::]:*            users:(("zoned",pid=777,fd=3))
LISTEN 0      2048   0.0.0.0:*           0.0.0.0:*         users:(("noport",pid=888,fd=3))
ESTAB  0      0      127.0.0.1:54321     127.0.0.1:8050    users:(("curl",pid=9999,fd=3))
EOF

mkdir -p "$TMP/bin"
cat > "$TMP/bin/ss" <<EOF
#!/bin/sh
[ -n "\${SS_MUST_FAIL:-}" ] && exit 1
cat "$TMP/fixture.txt"
EOF
chmod +x "$TMP/bin/ss"
export PATH="$TMP/bin:$PATH"

# shellcheck source=lib_ports.sh
. "$HERE/lib_ports.sh"

echo "port parsing is exact"
check "8050 finds both address families and nothing else" "3960 4366 " "$(pids_on_port 8050)"
check "the IPv6 hextet [fe80::8050] is not port 8050" "222 " "$(pids_on_port 9000)"
check "18050 is not 8050"                          "111 " "$(pids_on_port 18050)"
check "a zone suffix is parsed, :50 is really 50"  "777 " "$(pids_on_port 50)"
check "a peer-column match is not a listener"      "3960 4366 " "$(pids_on_port 8050)"
check "an address column with no port is skipped"  ""     "$(pids_on_port 0)"

echo
echo "the empty set is an observation, not an error"
# The regression: `grep` finds nothing, pipefail propagates 1, and under `set -e`
# the assignment aborted the whole script here — silently, before any of the
# reporting the callers do on an empty result.
empty="$(pids_on_port 7)"
check "an unused port yields empty output" "" "$empty"
st=0; pids_on_port 7 >/dev/null || st=$?
check "and status 0, not a failure" "0" "$st"
st=0; port_held 7 || st=$?
check "port_held reports free as 1"   "1" "$st"
st=0; port_held 8050 || st=$?
check "port_held reports held as 0"   "0" "$st"
check "port_released is true for a free port"     "yes" "$(port_released 7 && echo yes || echo no)"
check "port_released is false for a held port"    "no"  "$(port_released 8050 && echo yes || echo no)"
check "pid_holds_port confirms a real holder"     "yes" "$(pid_holds_port 3960 8050 && echo yes || echo no)"
check "pid_holds_port rejects a non-holder"       "no"  "$(pid_holds_port 111 8050 && echo yes || echo no)"

echo
echo "unknown is never mistaken for free"
export SS_MUST_FAIL=1
st=0; pids_on_port 8050 >/dev/null 2>&1 || st=$?
check "pids_on_port reports 2 when ss fails"      "2" "$st"
st=0; port_held 8050 2>/dev/null || st=$?
check "port_held reports 2, not 1"                "2" "$st"
check "port_released refuses to call it released" "no" \
      "$(port_released 8050 2>/dev/null && echo yes || echo no)"
check "pid_holds_port fails closed"               "no" \
      "$(pid_holds_port 3960 8050 2>/dev/null && echo yes || echo no)"
unset SS_MUST_FAIL

# `case` inside a command substitution breaks bash 3.2's parser, as it has several
# times across this suite; a helper keeps it out of `$( )`.
contains() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

echo
echo "the used-port ledger answers a question the host cannot"
# A finished run's ports are free again — that is what a working cleanup means. So
# `port_held` passes on every port this campaign has ever used, and cannot tell a
# fresh one from c2c's. That is the gap 18091/18092 fell into when they were called
# new ports in a request.
check "a port an earlier run bound is known to have been used" "0" \
      "$(port_previously_used 18091 >/dev/null; echo $?)"
check "and the row says which run" "yes" \
      "$(contains "$(port_previously_used 18091)" "c2c")"
check "a port nothing has used is not in it" "1" \
      "$(port_previously_used 18141 >/dev/null 2>&1; echo $?)"
check "production's own ports are listed, so they can never be proposed" "0" \
      "$(port_previously_used 8050 >/dev/null; echo $?)"
check "and so is the isolated scheduler D2b used" "0" \
      "$(port_previously_used 18787 >/dev/null; echo $?)"

check "the status line names a first use" "18141 first-use" "$(port_ledger_status 18141)"

# c2e never bound its ports — it aborted before any service started — but its
# evidence is named against them, so they are listed and the row says which.
# "Never bound" and "free to propose again" are not the same statement.
check "a port allocated to a run that never started is still listed" "0" \
      "$(port_previously_used 18121 >/dev/null; echo $?)"
check "and the row says it was never bound" "yes" \
      "$(contains "$(port_previously_used 18121)" "never bound")"
check "and marks a reuse as a reuse" "yes" \
      "$(contains "$(port_ledger_status 18091)" "REUSED")"

# A prefix must not match: 1809 is not 18091, and 18091 is not 180915.
check "a shorter number does not match a longer port" "1" \
      "$(port_previously_used 1809 >/dev/null 2>&1; echo $?)"
check "and a longer one does not match a shorter port" "1" \
      "$(port_previously_used 180915 >/dev/null 2>&1; echo $?)"

# An unreadable ledger is not an answer. Treating it as "not used" would silently
# turn the check off the moment the file went missing.
PORTS_LEDGER=/nonexistent/ports.tsv
check "an unreadable ledger reports 2, not 'unused'" "2" \
      "$(port_previously_used 18141 >/dev/null 2>&1; echo $?)"
check "and the status line says so rather than guessing" "yes" \
      "$(contains "$(port_ledger_status 18141 2>&1)" "UNKNOWN")"
PORTS_LEDGER="$HERE/ports_used.tsv"

# Every port in the ledger must be a port. A typo'd row is a row that silently
# never matches anything.
bad="$(grep -vE '^#' "$PORTS_LEDGER" | grep -vE '^[0-9]{2,5}\t' | grep -c . || true)"
check "every non-comment row starts with a port and a tab" "0" "$bad"
dupes="$(grep -vE '^#' "$PORTS_LEDGER" | cut -f1 | sort | uniq -d | grep -c . || true)"
check "and no port is listed twice" "0" "$dupes"

echo
if [ "$fail" -gt 0 ]; then
  echo "FAILED $fail/$((pass + fail))"
  exit 1
fi
echo "all passed ($pass assertions)"
