#!/usr/bin/env bash
#
# THE CANONICAL PROCESS INVENTORY, tested as behaviour.
#
# WHY THIS FILE EXISTS. The campaign quoted a uid-994 inventory digest,
# `ecb0aeba…6489`, across four documents as evidence that no unexpected survivor existed.
# When D-3 required it to be re-confirmed it could not be: the row count and content still
# matched, but no projection reproduced the digest, no script computed it, and no document
# recorded the method. `proc_inventory.sh` replaces it with a method that is written down
# and executable; this file is what stops the method from being written down WRONG.
#
# THE STRONGEST TEST HERE IS 3a: the digest a run reports must be recomputable, by hand,
# from the canonical rows that same run printed. That makes every inventory self-verifying
# -- a reader never has to trust that the digest input was what the report displayed --
# and it is the property the old digest lacked entirely.
#
# Processes spawned by this suite are tracked in a FILE, not in a variable set inside
# `$( )`. `test_requests.sh` leaked 15 processes because `SERVERS="$SERVERS $!"` ran in a
# command substitution, so the parent's list stayed empty and its EXIT trap killed nothing.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INV="$HERE/proc_inventory.sh"
LIB="$HERE/lib_proc_inventory.sh"
# shellcheck source=/dev/null
. "$HERE/lib_sentinel.sh"
# shellcheck source=/dev/null
. "$LIB"

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s\n' "$1"; [ $# -gt 1 ] && printf '       %s\n' "$2"; }
is()  { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "expected [$3], got [$2]"; fi; }
has() { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1" "output does not contain [$3]" ;; esac; }
hasnt(){
  if [ -z "$2" ]; then bad "$1" "the output was EMPTY — nothing was checked"; return; fi
  case "$2" in *"$3"*) bad "$1" "output unexpectedly contains [$3]" ;; *) ok "$1" ;; esac
}

T="$(mktemp -d "${TMPDIR:-/tmp}/procinv.XXXXXX")"
KIDS="$T/kids"; : > "$KIDS"
cleanup() {
  while IFS= read -r p; do
    [ -n "$p" ] || continue
    kill "$p" 2>/dev/null
  done < "$KIDS"
  rm -rf "$T"
}
trap cleanup EXIT

#: The canonical rows section of a report, exactly as printed (leading 2 spaces stripped).
canon_rows() { printf '%s\n' "$1" | sed -n '/^-- canonical rows, sorted/,/^$/p' | sed '1d;/^$/d;s/^  //'; }
field()     { printf '%s\n' "$1" | grep -E "^$2=" | head -1 | cut -d= -f2-; }

echo "=== the canonical process inventory ==="
echo

# ============================================================ 1. it runs and reports ===
echo "1. the report carries everything a reader needs to recompute the digest"
OUT1="$(bash "$INV" 2>&1)"; RC1=$?
is "1a  the inventory exits 0"                       "$RC1" "0"
for k in "field order" "separator" "sort" "included" "excluded" "digest input" "digest algorithm"; do
  has "1b  the method records: $k" "$OUT1" "$k"
done
for k in "observer pid" "observer pgid" "observer ppid" "observer start"; do
  has "1c  the exclusion identity records: $k" "$OUT1" "$k"
done
for k in "raw excluded rows" "raw included rows" "canonical rows, sorted" \
         "host " "user " "timestamp UTC" "script sha256" ; do
  has "1d  the evidence records: $k" "$OUT1" "$k"
done
D1="$(field "$OUT1" PROC_INVENTORY_SHA256)"
R1="$(field "$OUT1" ROWS)"
is "1e  the digest is 64 hex characters"             "$(printf '%s' "$D1" | wc -c | tr -d ' ')" "64"
case "$D1" in *[!0-9a-f]*) bad "1f  the digest is lowercase hex" "got [$D1]" ;; *) ok "1f  the digest is lowercase hex" ;; esac
case "$R1" in ''|*[!0-9]*) bad "1g  ROWS is a number" "got [$R1]" ;; *) ok "1g  ROWS is a number" ;; esac
echo

# ================================================= 2. the canonical row shape =========
echo "2. the canonical row shape is exactly what the method claims"
ROWS1="$(canon_rows "$OUT1")"
n="$(printf '%s\n' "$ROWS1" | grep -c .)"
is "2a  the printed row count equals ROWS"           "$n" "$R1"
badf="$(printf '%s\n' "$ROWS1" | awk -F'\t' 'NF!=6' | grep -c . || true)"
is "2b  every row has exactly 6 tab-separated fields" "$badf" "0"
badp="$(printf '%s\n' "$ROWS1" | awk -F'\t' '$1 !~ /^[0-9]+$/' | grep -c . || true)"
is "2c  field 1 is always a numeric pid"             "$badp" "0"
bads="$(printf '%s\n' "$ROWS1" | awk -F'\t' '$2 !~ /^(lin-[0-9]+|bsd-[0-9]+|unknown)$/' | grep -c . || true)"
is "2d  field 2 is a canonical starttime token"      "$bads" "0"
# The token is CONVERTED, never scrubbed: no colons or spaces folded into separators.
hasnt "2e  no scrubbed lstart text leaks into the token" "$ROWS1" "bsd-Mon"
sorted="$(printf '%s\n' "$ROWS1" | LC_ALL=C sort)"
is "2f  the rows are already LC_ALL=C sorted"        "$(printf '%s' "$sorted" | sha256sum | cut -d' ' -f1)" \
                                                     "$(printf '%s' "$ROWS1" | sha256sum | cut -d' ' -f1)"
echo

# ======================================== 3. the digest is recomputable from the report =
#
# THE POINT OF THE WHOLE EXERCISE. A reader with nothing but the printed report must be
# able to arrive at the same 64 characters. If this fails, the digest is describing
# something the report did not show -- which is exactly the position `ecb0aeba…` left the
# campaign in.
echo "3. the digest is recomputable, by hand, from the rows the report printed"
RECOMP="$(printf '%s\n' "$ROWS1" | sha256sum | cut -d' ' -f1)"
is "3a  sha256 of the printed canonical rows == the reported digest" "$RECOMP" "$D1"
# Non-vacuity: perturbing one byte must change it, or 3a proves nothing.
PERT="$(printf '%s\n' "$ROWS1" | sed '1s/$/ /' | sha256sum | cut -d' ' -f1)"
if [ "$PERT" != "$D1" ]; then ok "3b  ... and a one-byte change breaks it"
else bad "3b  ... and a one-byte change breaks it" "digest unchanged by perturbation"; fi
echo

# ================================= 4. DETERMINISM, proved against a fixture ===========
#
# THE LIVE MACHINE CANNOT PROVE THIS. On any busy host two honest retakes differ because the
# host differs -- this development machine carries ~1900 churning processes -- and there is
# no way to tell that apart from a method that is itself unstable. So determinism is proved
# where it can be: the same input, canonicalised twice, must give the same bytes and the
# same digest. Live self-agreement is then a statement about the TARGET HOST (VM24's
# woa23c1ro account holds 12 stable processes), checked when the baseline is taken.
echo "4. determinism - the same input always gives the same canonical rows and digest"

# A resolver that never touches the machine, so fixture pids need not exist.
fake_start() { printf 'lin-%s000' "$1"; }
export PROC_INV_START_FN=fake_start

DSIG="God Dae""mon"
FIX="  4242     1  4242 994 /usr/lib/systemd/systemd --user
  1111  4242  1111 994 PM2 v5.4.2: $DSIG (/home/u/woa23-x-pm2)
   777     1   777 994 /usr/bin/pipewire"

C1="$(proc_inv_canonical "$FIX")"
C2="$(proc_inv_canonical "$FIX")"
is "4a  the same input gives byte-identical canonical rows" "$C1" "$C2"
is "4b  ... and the same digest" "$(proc_inv_digest "$C1")" "$(proc_inv_digest "$C2")"
is "4c  three rows in, three rows out" "$(printf '%s\n' "$C1" | grep -c .)" "3"
is "4d  every row is six tab fields" "0" \
   "$(printf '%s\n' "$C1" | awk -F'\t' 'NF!=6' | grep -c . || true)"
is "4e  the rows are LC_ALL=C sorted" \
   "$(printf '%s\n' "$C1" | LC_ALL=C sort | sha256sum | cut -d' ' -f1)" \
   "$(printf '%s\n' "$C1" | sha256sum | cut -d' ' -f1)"
is "4f  field order is pid,starttime,ppid,pgid,ruid,args" \
   "$(printf '%s\n' "$C1" | awk -F'\t' '$1==1111{print $1"|"$2"|"$3"|"$4"|"$5}')" \
   "1111|lin-1111000|4242|1111|994"
# ORDER IS BY THE WHOLE ROW, so the pid STRING sorts, not its value. Said out loud because a
# reader expecting numeric order would mis-verify a digest by hand.
is "4g  sorting is lexical over the whole row, not numeric by pid" \
   "$(printf '%s\n' "$C1" | awk -F'\t' '{print $1}' | tr '\n' ',')" "1111,4242,777,"
SHUF="   777     1   777 994 /usr/bin/pipewire
  4242     1  4242 994 /usr/lib/systemd/systemd --user
  1111  4242  1111 994 PM2 v5.4.2: $DSIG (/home/u/woa23-x-pm2)"
is "4h  input order does not affect the digest" \
   "$(proc_inv_digest "$(proc_inv_canonical "$SHUF")")" "$(proc_inv_digest "$C1")"
# NON-VACUITY: a real change must move the digest, or 4a/4b/4h prove nothing.
ALT="$(printf '%s\n' "$FIX" | sed 's/pipewire/pipewire-pulse/')"
if [ "$(proc_inv_digest "$(proc_inv_canonical "$ALT")")" != "$(proc_inv_digest "$C1")" ]; then
  ok "4i  changing one row changes the digest"
else bad "4i  changing one row changes the digest" "digest unchanged"; fi
is "4j  an unresolvable starttime becomes 'unknown', never empty" \
   "$(PROC_INV_START_FN=false proc_inv_canonical '  55  1  55 994 x' | awk -F'\t' '{print $2}')" "unknown"
unset PROC_INV_START_FN
echo

echo "4k. the exclusion split, on a fixture with a known observer"
SNAP="  100    1  100 994 /usr/lib/systemd/systemd --user
  200  100  200 994 PM2 v5.4.2: $DSIG (/home/u/pm2)
  300  299  300 994 bash -s
  301  300  300 994 ps -o pid=
  299  298  298 994 sshd: u@notty"
EXC="$(proc_inv_excluded "$SNAP" 300 299)"
INC="$(proc_inv_included "$SNAP" 300 299)"
is "4k1 the observer shell, its child and its sshd are excluded" "$(printf '%s\n' "$EXC" | grep -c .)" "3"
is "4k2 ... and the two real processes are kept"                 "$(printf '%s\n' "$INC" | grep -c .)" "2"
has "4k3 the daemon is KEPT, not excluded"                       "$INC" "PM2 v5.4.2"
hasnt "4k4 ... and is not among the excluded"                    "$EXC" "PM2 v5.4.2"
is "4k5 every excluded row is the observer by identity" "0" \
   "$(printf '%s\n' "$EXC" | awk '$3!=300 && $1!=299' | grep -c . || true)"
echo

# ============ 5. a daemon signature among the EXCLUDED rows is a REFUSAL ==============
#
# The include/exclude decision itself is proved at 4k, against fixtures with a known
# observer: a process carrying a daemon signature is KEPT, and would only be dropped by a
# command-line match, which this method never performs.
#
# What is proved HERE is the guard that sits on top of it. PGID exclusion removes everything
# in the observer's process group -- that is what makes it precise over SSH, where the group
# holds only sshd, the shell and ps. But if a process carrying a daemon signature ever lands
# in that group, the exclusion might be hiding a survivor, and the inventory must REFUSE
# rather than quietly report a clean baseline.
#
# The decoy below is a background job of this suite, so it inherits this shell's process
# group -- and the inventory runs as a child in that same group. That is exactly the
# condition the guard exists for, so it is used here to fire it deliberately. The first
# draft of this section asserted the opposite and failed, which is how the guard's real
# behaviour came to be written down.
echo "5. a daemon signature among the excluded rows is refused, never reported clean"
DECOY_SIG="God Dae""mon"
bash -c "exec -a 'PM2 v5.4.2: $DECOY_SIG (/tmp/fake-pm2-home)' sleep 900" &
DECOY=$!
printf '%s\n' "$DECOY" >> "$KIDS"
sleep 1
DECOY_UP=no; kill -0 "$DECOY" 2>/dev/null && DECOY_UP=yes
OUT5="$(bash "$INV" 2>&1)"; RC5=$?
if [ "$DECOY_UP" = yes ]; then
  ok  "5a  the decoy process started"
  is  "5b  the inventory REFUSES rather than reporting a clean baseline" "$RC5" "2"
  has "5c  ... saying the exclusion may be hiding a survivor" "$OUT5" "may be hiding a survivor"
  has "5d  ... and the count it refused on is visible" "$OUT5" "excluded rows carrying a daemon signature : 1"
  exc="$(printf '%s\n' "$OUT5" | sed -n '/^-- raw excluded rows/,/^$/p' | awk -v p="$DECOY" '$1==p' | grep -c . || true)"
  is  "5e  ... and the offending row is shown in full, not merely counted" "$exc" "1"
else
  for c in a b c d e; do bad "5$c  decoy process could not be started (exec -a unsupported?)"; done
fi
kill "$DECOY" 2>/dev/null
sleep 1
# With the decoy gone the same inventory must exit 0 again, or 5b proved nothing about the
# decoy and everything about some unrelated permanent condition.
OUT5B="$(bash "$INV" 2>&1)"; RC5B=$?
is  "5f  with the decoy gone it exits 0 again" "$RC5B" "0"
has "5g  ... with zero excluded signatures"    "$OUT5B" "excluded rows carrying a daemon signature : 0"
echo

# =================================== 6. the script cannot match itself ==================
#
# Two recorded self-observation artefacts, both prevented here. The first was a `grep` whose
# own argv held the signature. The second was this inventory passed to `ssh` as a command
# ARGUMENT, which put the signature into the remote shell's argv.
echo "6. no self-observation — the signature never exists in any command line"
SIG_FRAG="God Dae""mon"
is "6a  the literal signature appears in neither the driver nor the library" "0" \
   "$(cat "$INV" "$LIB" | grep -cF "$SIG_FRAG|guni" || true)"
is "6b  the library assembles it from fragments instead" "1" \
   "$(grep -c 'God Dae""mon' "$LIB" || true)"
is "6c  ps is captured ONCE, before any matcher exists" "1" \
   "$(grep -c '^PI_SNAP="\$(ps ' "$INV" || true)"
is "6d  the exclusion is a numeric identity test, not a grep" "2" \
   "$(grep -c 'awk -v g=' "$LIB" || true)"
is "6e  no command-line grep is used to exclude" "0" \
   "$(cat "$INV" "$LIB" | grep -cE 'grep -v.*(bash|ssh|ps )' || true)"
# Invoked with the signature in ITS OWN argv, it must still report 0 excluded matches.
OUT6="$(bash "$INV" "$(id -un)" 2>&1)"
has "6f  invoked normally, excluded signature count is 0" "$OUT6" "excluded rows carrying a daemon signature : 0   (must be 0)"
echo

# ============================================== 7. the observer is always excluded ======
echo "7. the observer never appears in its own inventory"
OUT7="$(bash "$INV" 2>&1)"
OPID="$(printf '%s\n' "$OUT7" | grep '^observer pid'  | awk '{print $NF}')"
OPGID="$(printf '%s\n' "$OUT7" | grep '^observer pgid' | awk '{print $NF}')"
R7="$(canon_rows "$OUT7")"
is "7a  the observer's own pid is not an included row" "0" \
   "$(printf '%s\n' "$R7" | awk -F'\t' -v p="$OPID" '$1==p' | grep -c . || true)"
is "7b  nothing in its process group is included"      "0" \
   "$(printf '%s\n' "$R7" | awk -F'\t' -v g="$OPGID" '$4==g' | grep -c . || true)"
has "7c  the excluded section is non-empty"            "$OUT7" "-- raw excluded rows ("
is "7d  ... and it is not empty in fact" "0" \
   "$(printf '%s\n' "$OUT7" | grep -c 'raw excluded rows (0)' || true)"
echo

# ================================================== 8. it changes nothing ===============
echo "8. the inventory is read-only"
MUTATORS="mkdir|touch|chmod|chown|mv |cp |ln -s"
is "8a  it creates, removes or re-permissions nothing" "0" \
   "$(cat "$INV" "$LIB" | grep -cE "^[^#]*($MUTATORS)" || true)"
# Only redirects to a PATH count. Matching bare `>` swept up trailing comments
# (`# <observer-pgid>`) and awk's `NF>6`, neither of which is a redirect -- a check that
# reports a violation for a comparison operator teaches the reader to ignore it.
is "8a2 ... and no redirect targets a path other than /dev/null" "0" \
   "$(cat "$INV" "$LIB" | grep -oE '>>?[ ]*[A-Za-z0-9_.$"]*/[^ ;)&|]*' \
      | grep -vcE '>+ *"?/dev/null' || true)"
is "8b  it signals no process"          "0" \
   "$(grep -cE '^[^#]*\bkill\b' "$INV" || true)"
is "8c  it reuses proc_starttime rather than reimplementing it" "1" \
   "$(grep -cE '^\. "\$_PI_HERE/lib_sentinel\.sh"' "$INV" || true)"
is "8c2 ... and parses no /proc stat of its own" "0" \
   "$(cat "$INV" "$LIB" | grep -cE '^[^#]*proc/[^/]*/stat' || true)"
is "8d  ... and refuses to run if any required symbol is missing" "1" \
   "$(grep -c 'command -v "\$_f"' "$INV" || true)"
is "8d2 ... checking proc_starttime by name among them" "1" \
   "$(grep -c 'for _f in proc_starttime ' "$INV" || true)"
echo

suite_summary "$PASS" "$FAIL"
