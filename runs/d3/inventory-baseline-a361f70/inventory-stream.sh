#!/usr/bin/env bash
# composed from subject a361f70668f28eaec49fabf078ca4c9c05d5d4ed
PI_DRIVER_SHA=e17944b71034aa53b8e013383f04552352e39a30c8b49f30b4f4c32647fa0351
PI_LIB_SHA=4e5b7106001bbf6ad716db094cb9534985e2c8b51b2b00c8197a17b43acde989
PI_SENTINEL_SHA=0cea4aa158f431142e4118edd52bea7cc8134292a129c6b9160c7a6b332af679
#!/usr/bin/env bash
#
# Batch completion sentinels, bound to a subject and a run.
#
# WHY THIS EXISTS. The batch driver used to end with a literal
# `echo "D3_6CE915E_BATCHES_DONE"`, and every part of that was wrong:
#
#   * the subject name was HARDCODED, so a driver copied for a new subject announced
#     completion under the OLD subject's name -- a provenance claim that was simply false;
#   * it was written AFTER the loop with no reference to any batch's exit status. The
#     driver captured `rc=$?` , printed it, and never tested it, so a run with a FAILING
#     batch still announced success;
#   * a reader could not tell WHICH run wrote it, so a stale line in a reused log file was
#     indistinguishable from a fresh success;
#   * the watcher matched a fuzzy pattern that could match its own command line, which had
#     already produced one false positive in this campaign.
#
# This is not a display problem. A completion marker is the thing later work trusts when it
# says "the batches passed", so a marker that can be stale, mislabelled, or written after a
# failure is a defect in provenance and completion accounting.
#
# THE RULE THIS FILE ENFORCES: a success sentinel exists ONLY if a complete run of the
# named subject, under the named run label, finished with nothing failing. Every other
# state -- missing, partial, aborted, uncertain, mismatched, duplicated -- is a refusal.

# ------------------------------------------------------------------ the sentinel format
#
#   WOA23_BATCH_COMPLETE subject=<40 hex> label=<run label> token=<run token> \
#                        batches=<n> nonzero=0
#
# Every field is required. `token` is generated per run by the driver, so two runs of the
# same subject and label are still distinguishable, and a watcher can wait on THIS run.
SENTINEL_TAG='WOA23_BATCH_COMPLETE'

_sent_is_sha()   { case "$1" in *[!0-9a-f]*|'') return 1 ;; esac; [ "${#1}" -eq 40 ]; }
_sent_is_ident() { case "$1" in ''|*[!A-Za-z0-9._-]*) return 1 ;; esac; return 0; }

# sentinel_write <file> <subject> <label> <token> <expected> <completed> <nonzero> <post_ok>
#
# Writes the success sentinel, or refuses. There is no partial-success form and no "write
# it anyway" flag: the absence of a sentinel is how an incomplete run is reported.
sentinel_write() {
  local file="${1:-}" subject="${2:-}" label="${3:-}" token="${4:-}"
  local expected="${5:-}" completed="${6:-}" nonzero="${7:-}" post_ok="${8:-}"

  [ -n "$file" ] || { echo "sentinel_write: no file given" >&2; return 2; }

  # A missing subject SHA is a refusal, never a default and never a value carried over
  # from an earlier run.
  _sent_is_sha "$subject" || {
    echo "sentinel_write: REFUSING -- subject is not a 40-hex SHA: '${subject:-<empty>}'" >&2
    return 3; }
  _sent_is_ident "$label" || {
    echo "sentinel_write: REFUSING -- label is empty or not a plain identifier: '${label:-<empty>}'" >&2
    return 4; }
  _sent_is_ident "$token" || {
    echo "sentinel_write: REFUSING -- token is empty or not a plain identifier: '${token:-<empty>}'" >&2
    return 5; }

  case "$expected$completed$nonzero" in *[!0-9]*|'')
    echo "sentinel_write: REFUSING -- batch counts are not numeric" >&2; return 6 ;;
  esac

  # PARTIAL IS NOT SUCCESS.
  [ "$completed" = "$expected" ] || {
    echo "sentinel_write: REFUSING -- $completed of $expected batches completed." >&2
    echo "  An incomplete run has no success sentinel; that absence IS the report." >&2
    return 7; }

  # ANY failure anywhere means no sentinel.
  [ "$nonzero" = 0 ] || {
    echo "sentinel_write: REFUSING -- $nonzero non-zero suite result(s) in the run." >&2
    return 8; }

  # Postconditions (HEAD unmoved, tree clean, whatever the caller checked) must have passed.
  [ "$post_ok" = yes ] || {
    echo "sentinel_write: REFUSING -- postconditions did not pass (post_ok='${post_ok:-<empty>}')." >&2
    echo "  'uncertain' is not 'passed'." >&2
    return 9; }

  # DUPLICATES ARE REFUSED. A second sentinel in the same file would make "did this run
  # succeed?" ambiguous, and ambiguity here reads as success to a careless reader.
  if [ -e "$file" ] && grep -q "^$SENTINEL_TAG " "$file" 2>/dev/null; then
    echo "sentinel_write: REFUSING -- a sentinel already exists in $file." >&2
    return 10
  fi

  printf '%s subject=%s label=%s token=%s batches=%s nonzero=%s\n' \
    "$SENTINEL_TAG" "$subject" "$label" "$token" "$completed" "$nonzero" >> "$file"
}

# sentinel_verify <file> <subject> <label> [token]
#
#   0 accepted     3 missing        4 subject mismatch (incl. an OLD subject's sentinel)
#   5 label mismatch                6 duplicate        7 malformed     8 token mismatch
#
# The caller states which subject and run it expects. Nothing is inferred from the file.
sentinel_verify() {
  local file="${1:-}" subject="${2:-}" label="${3:-}" token="${4:-}"

  _sent_is_sha "$subject" || {
    echo "sentinel_verify: REFUSING -- expected subject is not a 40-hex SHA" >&2; return 4; }

  [ -e "$file" ] || { echo "sentinel_verify: no sentinel -- $file does not exist" >&2; return 3; }

  local lines n
  lines="$(grep "^$SENTINEL_TAG " "$file" 2>/dev/null || true)"
  n="$(printf '%s' "$lines" | grep -c . || true)"

  [ "$n" -ge 1 ] || { echo "sentinel_verify: no sentinel found in $file" >&2; return 3; }
  [ "$n" -eq 1 ] || {
    echo "sentinel_verify: REFUSING -- $n sentinels found; exactly one is required." >&2; return 6; }

  local got_subject got_label got_token
  got_subject="$(printf '%s' "$lines" | sed -n 's/.* subject=\([0-9a-f]*\).*/\1/p')"
  got_label="$(printf '%s'   "$lines" | sed -n 's/.* label=\([A-Za-z0-9._-]*\).*/\1/p')"
  got_token="$(printf '%s'   "$lines" | sed -n 's/.* token=\([A-Za-z0-9._-]*\).*/\1/p')"

  _sent_is_sha "$got_subject" || {
    echo "sentinel_verify: REFUSING -- malformed sentinel (no valid subject field)" >&2; return 7; }
  [ -n "$got_label" ] || {
    echo "sentinel_verify: REFUSING -- malformed sentinel (no label field)" >&2; return 7; }

  # AN OLD SUBJECT'S SENTINEL IS A MISMATCH, not a near-miss to be tolerated. This is the
  # exact case that a hardcoded name produced.
  [ "$got_subject" = "$subject" ] || {
    echo "sentinel_verify: REFUSING -- sentinel names subject $got_subject, expected $subject" >&2
    return 4; }
  [ "$got_label" = "$label" ] || {
    echo "sentinel_verify: REFUSING -- sentinel names label '$got_label', expected '$label'" >&2
    return 5; }

  if [ -n "$token" ]; then
    [ "$got_token" = "$token" ] || {
      echo "sentinel_verify: REFUSING -- sentinel token '$got_token' is not this run's '$token'" >&2
      return 8; }
  fi
  return 0
}

# ---------------------------------------------------------------- RUN IDENTITY: PID+STARTTIME
#
# A PID ALONE IS NOT AN IDENTITY. PIDs are reused: after wraparound the same number names a
# different process, so "is pid N alive?" can be answered `yes` about something that is not
# the run at all. An earlier version of this file persisted only the pid and derived the run
# token from it (`run-$$`), which made the token reusable for the same reason -- the weakness
# was inherited, not avoided.
#
# The pair (pid, starttime) IS unique on a running system: a reused pid necessarily has a
# later start time. Everything below keys on the pair, and refuses when it cannot read one.

# proc_starttime <pid> -> CANONICAL starttime token on stdout, rc 0; rc 1 and NO output
# on failure.
#
# THE FORMAT IS CANONICAL AND SELF-DESCRIBING, and it is NOT produced by cleaning characters
# out of a human-readable string. An earlier version folded the BSD `lstart` text -- colons
# and spaces rewritten to '-' -- which is lossy: two different start times can fold to the
# same token, so an identity meant to prevent collisions could create one. It also could not
# be re-parsed back into a time.
#
#   lin-<ticks>   Linux: field 22 of /proc/<pid>/stat, clock ticks since boot. Already
#                 numeric; nothing is rewritten.
#   bsd-<epoch>   BSD/macOS: `ps -o lstart=` CONVERTED to seconds since the epoch by
#                 date(1). A number, not a scrubbed string.
#
# The prefix names which clock the number is on, so the two are never compared as if they
# were the same quantity, and either can be parsed back.
proc_starttime() {
  local pid="${1:-}" line rest v raw
  case "$pid" in ''|*[!0-9]*) return 1 ;; esac

  if [ -r "/proc/$pid/stat" ]; then
    line="$(cat "/proc/$pid/stat" 2>/dev/null)" || return 1
    [ -n "$line" ] || return 1
    # THE COMM FIELD IS PARENTHESISED AND MAY CONTAIN SPACES AND PARENS, so a fixed field
    # index silently returns the wrong number. Split after the LAST ')' -- the same defect
    # this campaign already fixed once in production_stop.sh.
    case "$line" in *')'*) ;; *) return 1 ;; esac
    rest="$(printf '%s' "$line" | sed 's/.*) //')"
    [ "$(printf '%s' "$rest" | awk '{print NF}')" -ge 20 ] 2>/dev/null || return 1
    v="$(printf '%s' "$rest" | awk '{print $20}')"
    case "$v" in ''|*[!0-9]*) return 1 ;; esac
    [ "$v" != 0 ] || return 1
    printf 'lin-%s' "$v"; return 0
  fi

  # BSD/macOS: no /proc. Convert, do not scrub.
  raw="$(ps -o lstart= -p "$pid" 2>/dev/null | sed 's/  */ /g; s/^ //; s/ *$//')"
  [ -n "$raw" ] || return 1
  v="$(date -j -f '%a %b %d %T %Y' "$raw" +%s 2>/dev/null)" || return 1
  case "$v" in ''|*[!0-9]*) return 1 ;; esac
  [ "$v" != 0 ] || return 1
  printf 'bsd-%s' "$v"; return 0
}

# runid_write <file> <label> <subject>  -- records THIS process as the run.
runid_write() {
  local file="${1:-}" label="${2:-}" subject="${3:-}" st
  [ -n "$file" ] || return 2
  _sent_is_ident "$label" || { echo "runid_write: REFUSING -- bad label" >&2; return 4; }
  _sent_is_sha "$subject" || { echo "runid_write: REFUSING -- bad subject" >&2; return 3; }
  st="$(proc_starttime "$$")" || {
    echo "runid_write: REFUSING -- cannot read this process's start time." >&2
    echo "  Without it the run has no identity a watcher can verify, and a pid alone" >&2
    echo "  can be reused. Refusing rather than recording half an identity." >&2
    return 5; }
  printf 'pid=%s starttime=%s label=%s subject=%s\n' "$$" "$st" "$label" "$subject" > "$file"
}

# runid_token <file> -> the run token, built from the pair. Unique across time, unlike a
# pid-derived token.
runid_token() {
  local file="${1:-}" pid st label
  [ -r "$file" ] || return 3
  pid="$(sed -n 's/.*pid=\([0-9]*\).*/\1/p' "$file")"
  st="$(sed -n 's/.*starttime=\([A-Za-z0-9.:_-]*\).*/\1/p' "$file")"
  label="$(sed -n 's/.*label=\([A-Za-z0-9._-]*\).*/\1/p' "$file")"
  [ -n "$pid" ] && [ -n "$st" ] && [ -n "$label" ] || return 7
  printf '%s-%s-%s' "$label" "$pid" "$st"
}

# runid_alive <file>
#   0 the recorded run is still running        3 no/unreadable runid file
#   6 the pid is gone                          7 malformed runid
#   8 PID REUSE -- the pid is alive but is a DIFFERENT process
runid_alive() {
  local file="${1:-}" pid st now
  [ -r "$file" ] || return 3
  pid="$(sed -n 's/.*pid=\([0-9]*\).*/\1/p' "$file")"
  st="$(sed -n 's/.*starttime=\([A-Za-z0-9.:_-]*\).*/\1/p' "$file")"
  [ -n "$pid" ] && [ -n "$st" ] || return 7
  kill -0 "$pid" 2>/dev/null || return 6
  now="$(proc_starttime "$pid")" || return 6
  [ "$now" = "$st" ] || return 8
  return 0
}

#!/usr/bin/env bash
#
# THE CANONICAL PROCESS INVENTORY — the pure half.
#
# WHY IT IS ITS OWN FILE. The campaign carried a uid-994 inventory digest,
# `ecb0aeba3138fb9bfe03e073707683bdea078ae68ef369cb51154db6e55c6489`, quoted across four
# documents as evidence that no unexpected survivor existed. When D-3 execution required it
# to be re-confirmed it could not be: the row COUNT (12) and the row CONTENT still matched,
# but no projection of `ps` output reproduced the digest, no committed script computed it,
# and no document recorded the field order, separator, sort rule or digest input.
#
# So the method now lives in functions that take their input as ARGUMENTS rather than
# reading the machine. That is not tidiness. A method that can only be exercised against
# live processes cannot be tested for DETERMINISM -- on any busy host two honest retakes
# differ because the host differs, and there is no way to tell that apart from a method
# that is itself unstable. With the pure half separated, the same fixture can be canonicalised
# twice and the digest compared, which is the property the old digest could not offer.
#
# THE HISTORICAL DIGEST IS NOT REVERSE-ENGINEERED. Nothing here is tuned to reproduce
# `ecb0aeba…`; a projection found by trying variants until one matched would manufacture
# agreement rather than verify it. That digest stays in the record marked
# `method not recorded / not reproducible`.
#
# THE METHOD, fixed:
#
#   FIELD ORDER   pid, starttime, ppid, pgid, ruid, args
#   SEPARATOR     one TAB between fields; exactly one LF after each row
#   SORT          LC_ALL=C sort over the whole canonical row
#   INCLUDED      every process of the target user that is not an observer row
#   EXCLUDED      observer rows, by PID/PGID IDENTITY -- never by command-line matching
#   DIGEST INPUT  the sorted canonical rows and nothing else: no header, no host, no time
#   ALGORITHM     SHA-256, reported as all 64 hex characters
#
# Nothing variable may enter the digest input. A host name or timestamp inside it would make
# two honest retakes disagree, and a baseline that cannot agree with itself cannot be
# compared with anything.

#: Split observer rows from the rest, by NUMERIC IDENTITY only.
#:
#: A row is an observer iff its PGID is the observer's PGID, or its PID is the observer's
#: PPID -- the sshd or parent that owns the session. This is never a command-line match:
#: `grep -v` over command text is over-broad, and a survivor whose command happens to match
#: would vanish from the inventory that exists to find it.
proc_inv_excluded() {   # <ps-rows> <observer-pgid> <observer-ppid>
  printf '%s\n' "${1-}" | awk -v g="${2-}" -v p="${3-}" '$3==g || $1==p'
}
proc_inv_included() {   # <ps-rows> <observer-pgid> <observer-ppid>
  printf '%s\n' "${1-}" | awk -v g="${2-}" -v p="${3-}" '$3!=g && $1!=p'
}

#: Canonical rows from raw `ps -o pid=,ppid=,pgid=,ruid=,args=` rows, sorted.
#:
#: The starttime resolver is injectable through PROC_INV_START_FN so the canonicalisation
#: can be exercised against a fixture whose pids do not exist on this machine. It defaults
#: to `proc_starttime` from lib_sentinel.sh, which is what a real run uses; the seam changes
#: which function is called, never what the format is.
proc_inv_canonical() {   # <included-rows>  ->  sorted canonical rows on stdout
  local fn="${PROC_INV_START_FN:-proc_starttime}" out="" row pid ppid pgid ruid args st
  while IFS= read -r row; do
    [ -n "$row" ] || continue
    pid="$(printf '%s' "$row"  | awk '{print $1}')"
    ppid="$(printf '%s' "$row" | awk '{print $2}')"
    pgid="$(printf '%s' "$row" | awk '{print $3}')"
    ruid="$(printf '%s' "$row" | awk '{print $4}')"
    args="$(printf '%s' "$row" | awk '{ $1="";$2="";$3="";$4=""; sub(/^ +/,""); print }')"
    st="$("$fn" "$pid" 2>/dev/null)" || st=unknown
    [ -n "$st" ] || st=unknown
    out="$out$(printf '%s\t%s\t%s\t%s\t%s\t%s' "$pid" "$st" "$ppid" "$pgid" "$ruid" "$args")
"
  done <<EOF
${1-}
EOF
  printf '%s' "$out" | LC_ALL=C sort
}

#: The digest of canonical rows: sha256 over exactly those rows, each LF-terminated.
proc_inv_digest() {   # <canonical-rows>
  printf '%s\n' "${1-}" | sha256sum | cut -d' ' -f1
}

#: Rows whose args contain a TAB, which this separator cannot represent unambiguously.
#: Reported as a refusal rather than silently normalised away.
proc_inv_tab_rows() {   # <canonical-rows>
  printf '%s\n' "${1-}" | awk -F'\t' 'NF>6' | grep -c . || true
}

#: The daemon signature, ASSEMBLED AT RUN TIME so the literal exists nowhere in this file.
#:
#: Two recorded self-observation artefacts sit behind this. The D-3 preflight's first
#: inventory refused because its own `grep` carried the pattern in argv and `ps` captured
#: it. The same artefact recurred during D-3 execution when the inventory was passed to
#: `ssh` as a command argument. A literal in the file could still be matched by anything
#: grepping the file, so it is not written down whole even here.
proc_inv_signature() {
  printf '%s' "God Dae""mon|guni""corn|PM""2 v|api\\.a""pp|uvi""corn|node .*PM""2"
}

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
_PI_HERE=""   # composed: read from stdin, no source file
# shellcheck source=/dev/null
:
# shellcheck source=/dev/null
:
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
echo "script sha256   : $PI_DRIVER_SHA   (from the subject archive)"
echo "library sha256  : $PI_LIB_SHA   (from the subject archive)"
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
