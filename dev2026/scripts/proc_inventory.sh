#!/usr/bin/env bash
#
# THE CANONICAL PROCESS INVENTORY — the driver.
#
# The method lives in `lib_proc_inventory.sh`, which documents it and can be exercised
# against fixtures. This file does the three things that need a real machine: take the
# snapshot, establish the observer's own identity, and print the evidence.
#
# WHY THIS REPLACED A DIGEST. The campaign quoted `ecb0aeba…6489` across four documents as
# proof that no unexpected survivor existed. D-3 execution required it to be re-confirmed
# and it could not be: the row count and content still matched, but the method by which the
# digest had been produced was never recorded anywhere. That digest stays in the record
# marked `method not recorded / not reproducible`, and nothing here is tuned to reproduce it.
#
# THE SNAPSHOT IS TAKEN FIRST, before any pattern or matcher exists in any process. The
# D-3 preflight's first attempt refused because its own `grep` held the daemon signature in
# argv and `ps` captured it; the same artefact recurred during D-3 execution when this
# inventory was passed to `ssh` as a command ARGUMENT rather than on stdin. Everything below
# reads the variable; `ps` is never re-run.
#
#     ./scripts/proc_inventory.sh [user]        # default: the invoking user
#
# Exit 0  inventory taken and printed.
# Exit 2  it could not be taken, an excluded row carries a daemon signature, or a command
#         line contains a TAB.
#
# It reads. It creates nothing, writes nothing but stdout, and signals no process.

set -uo pipefail
_PI_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$_PI_HERE/lib_sentinel.sh"       || { echo "cannot load lib_sentinel.sh" >&2; exit 2; }
# shellcheck source=/dev/null
. "$_PI_HERE/lib_proc_inventory.sh" || { echo "cannot load lib_proc_inventory.sh" >&2; exit 2; }
for _f in proc_starttime proc_inv_included proc_inv_excluded proc_inv_canonical proc_inv_digest; do
  command -v "$_f" >/dev/null 2>&1 || { echo "REFUSING: $_f is not defined" >&2; exit 2; }
done

PI_USER="${1:-$(id -un)}"

# ------------------------------------------------------------------ 1. THE SNAPSHOT, FIRST
PI_SNAP="$(ps -o pid=,ppid=,pgid=,ruid=,args= -u "$PI_USER" 2>/dev/null)"
[ -n "$PI_SNAP" ] || { echo "REFUSING: ps returned nothing for user $PI_USER" >&2; exit 2; }

# ------------------------------------------------------- 2. THE OBSERVER'S OWN IDENTITY
PI_PID=$$
PI_PGID="$(ps -o pgid= -p "$PI_PID" 2>/dev/null | tr -d ' ')"
PI_PPID="$(ps -o ppid= -p "$PI_PID" 2>/dev/null | tr -d ' ')"
PI_START="$(proc_starttime "$PI_PID" 2>/dev/null || echo unknown)"
case "$PI_PGID" in ''|*[!0-9]*) echo "REFUSING: cannot read this process's pgid" >&2; exit 2 ;; esac
case "$PI_PPID" in ''|*[!0-9]*) echo "REFUSING: cannot read this process's ppid" >&2; exit 2 ;; esac

# --------------------------------------------------- 3. SPLIT, CANONICALISE, DIGEST
PI_EXCLUDED="$(proc_inv_excluded "$PI_SNAP" "$PI_PGID" "$PI_PPID")"
PI_INCLUDED="$(proc_inv_included "$PI_SNAP" "$PI_PGID" "$PI_PPID")"
PI_SORTED="$(proc_inv_canonical "$PI_INCLUDED")"
PI_ROWS="$(printf '%s\n' "$PI_SORTED" | grep -c .)"
PI_DIGEST="$(proc_inv_digest "$PI_SORTED")"

PI_SIG="$(proc_inv_signature)"
PI_EXC_HITS="$(printf '%s\n' "$PI_EXCLUDED" | grep -cE "$PI_SIG" || true)"
PI_INC_HITS="$(printf '%s\n' "$PI_INCLUDED" | grep -cE "$PI_SIG" || true)"
PI_TABS="$(proc_inv_tab_rows "$PI_SORTED")"

# ------------------------------------------------------------------------- 4. THE REPORT
echo "=== canonical process inventory ==="
echo "host            : $(hostname 2>/dev/null)"
echo "user            : $PI_USER   (uid $(id -u))"
echo "timestamp UTC   : $(date -u '+%F %T')"
echo "script sha256   : $(sha256sum "${BASH_SOURCE[0]}" 2>/dev/null | cut -d' ' -f1)"
echo "library sha256  : $(sha256sum "$_PI_HERE/lib_proc_inventory.sh" 2>/dev/null | cut -d' ' -f1)"
echo "kernel          : $(uname -srm)"
echo
echo "-- method --"
echo "field order     : pid TAB starttime TAB ppid TAB pgid TAB ruid TAB args"
echo "separator       : single TAB between fields, single LF after each row"
echo "sort            : LC_ALL=C sort over the whole canonical row"
echo "included        : all processes of $PI_USER that are not observer rows"
echo "excluded        : observer rows, by PID/PGID identity only, never by command match"
echo "digest input    : the sorted canonical rows, nothing else"
echo "digest algorithm: sha256, all 64 hex characters"
echo
echo "-- exclusion identity --"
echo "observer pid    : $PI_PID"
echo "observer pgid   : $PI_PGID"
echo "observer ppid   : $PI_PPID"
echo "observer start  : $PI_START"
echo
echo "-- raw excluded rows ($(printf '%s\n' "$PI_EXCLUDED" | grep -c .)) --"
printf '%s\n' "$PI_EXCLUDED" | grep . | sed 's/^/  /'
echo
echo "-- raw included rows ($(printf '%s\n' "$PI_INCLUDED" | grep -c .)) --"
printf '%s\n' "$PI_INCLUDED" | grep . | sed 's/^/  /'
echo
echo "-- canonical rows, sorted ($PI_ROWS) --"
printf '%s\n' "$PI_SORTED" | grep . | sed 's/^/  /'
echo
echo "-- assertions --"
echo "excluded rows carrying a daemon signature : $PI_EXC_HITS   (must be 0)"
echo "included rows carrying a daemon signature : $PI_INC_HITS   (informational)"
echo "rows whose args contain a TAB            : $PI_TABS   (must be 0)"
echo
echo "ROWS=$PI_ROWS"
echo "PROC_INVENTORY_SHA256=$PI_DIGEST"

[ "$PI_EXC_HITS" -eq 0 ] || {
  echo "REFUSING: an EXCLUDED row carries a daemon signature -- the exclusion may be hiding a survivor." >&2
  exit 2; }
[ "$PI_TABS" -eq 0 ] || {
  echo "REFUSING: a command line contains a TAB, which this separator cannot represent." >&2
  exit 2; }
exit 0
