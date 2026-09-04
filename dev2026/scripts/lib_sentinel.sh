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
