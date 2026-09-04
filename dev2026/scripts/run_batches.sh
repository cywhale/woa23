#!/usr/bin/env bash
#
# The serial batch driver, bound to a subject.
#
# It replaces an ad-hoc scratchpad driver that ended with a hardcoded
# `echo "D3_<old-subject>_BATCHES_DONE"`. That driver captured each batch's exit status,
# PRINTED it, and never tested it -- so a run with a failing batch still announced success
# under a subject name copied from a previous run. Both halves of that are provenance
# defects, which is why the driver now lives here, beside a library and a test suite,
# instead of in a scratch file nobody could check.
#
#   ./scripts/run_batches.sh --subject <40-hex> --label <run label> \
#                            --worktree <path> --batches <n> --out <dir>
#
# The subject SHA is REQUIRED and is supplied by the caller. It is never defaulted, never
# read back from a previous run's artefacts, and never inferred from the worktree alone --
# the worktree is CHECKED against it, which is the opposite relationship.

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HERE/scripts/lib_sentinel.sh"

die() { printf '%s\n' "$@" >&2; exit 2; }

SUBJECT=""; LABEL=""; WORKTREE=""; BATCHES=""; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --subject)  SUBJECT="${2:-}"; shift 2 ;;
    --label)    LABEL="${2:-}";   shift 2 ;;
    --worktree) WORKTREE="${2:-}";shift 2 ;;
    --batches)  BATCHES="${2:-}"; shift 2 ;;
    --out)      OUT="${2:-}";     shift 2 ;;
    *) die "unexpected argument: $1" ;;
  esac
done

case "$SUBJECT" in *[!0-9a-f]*|'') die "--subject is required and must be a 40-hex SHA (got '${SUBJECT:-<empty>}')" ;; esac
[ "${#SUBJECT}" -eq 40 ] || die "--subject must be 40 hex characters (got ${#SUBJECT})"
case "$LABEL" in ''|*[!A-Za-z0-9._-]*) die "--label is required and must be a plain identifier" ;; esac
case "$BATCHES" in ''|*[!0-9]*) die "--batches must be a number" ;; esac
[ "$BATCHES" -ge 1 ] || die "--batches must be at least 1"
[ -n "$WORKTREE" ] && [ -d "$WORKTREE" ] || die "--worktree must name an existing directory"
[ -n "$OUT" ] || die "--out is required"
mkdir -p "$OUT"

LOG="$OUT/$LABEL.log"
[ -e "$LOG" ] && die "REFUSING: $LOG already exists; a run writes its own log or none."

# RUN IDENTITY = PID + STARTTIME, not a pid alone.
#
# An earlier version of this driver wrote only the pid and derived the token from it
# (`run-$$`). A pid is REUSED after wraparound, so "is pid N alive?" can be answered `yes`
# about a different process entirely, and a pid-derived token inherits exactly that
# weakness. The pair (pid, starttime) is unique on a running system, so the watcher can
# tell this run from a later process that happens to get the same number.
RUNID="$OUT/$LABEL.runid"
runid_write "$RUNID" "$LABEL" "$SUBJECT" \
  || die "REFUSING: could not record a run identity (pid + start time)." \
         "  Without it a watcher cannot distinguish this run from a reused pid."
TOKEN="$(runid_token "$RUNID")" \
  || die "REFUSING: could not derive the run token from $RUNID"

{
  echo "subject : $SUBJECT"
  echo "label   : $LABEL"
  echo "token   : $TOKEN"
  echo "worktree: $WORKTREE"
  echo "batches : $BATCHES"
} | tee "$LOG"

COMPLETED=0
NONZERO_TOTAL=0
POST_OK=yes

for n in $(seq 1 "$BATCHES"); do
  echo "########## BATCH $n of $BATCHES ##########" | tee -a "$LOG"

  h="$(git -C "$WORKTREE" rev-parse HEAD 2>/dev/null || echo '')"
  if [ "$h" != "$SUBJECT" ]; then
    echo "REFUSING batch $n: worktree HEAD is '${h:-<unknown>}', expected $SUBJECT" | tee -a "$LOG"
    POST_OK=no; break
  fi
  echo "precheck  HEAD=$h == subject   OK" | tee -a "$LOG"

  ( cd "$WORKTREE/dev2026" && ./scripts/run_suites.sh ) > "$OUT/$LABEL.batch$n.log" 2>&1
  rc=$?
  echo "batch $n exit status: $rc" | tee -a "$LOG"

  # THE EXIT STATUS IS TESTED, not merely printed. This is the line the old driver did not
  # have, and its absence is why a failing run could announce success.
  if [ "$rc" -ne 0 ]; then
    echo "batch $n FAILED (exit $rc)" | tee -a "$LOG"
    NONZERO_TOTAL=$((NONZERO_TOTAL + 1))
  fi

  h_after="$(git -C "$WORKTREE" rev-parse HEAD 2>/dev/null || echo '')"
  if [ "$h_after" != "$SUBJECT" ]; then
    echo "VOID batch $n: HEAD moved to '${h_after:-<unknown>}' during the run" | tee -a "$LOG"
    POST_OK=no; break
  fi
  echo "postcheck HEAD=$h_after == subject   OK" | tee -a "$LOG"

  # Each batch's own self-record, quoted rather than restated.
  grep -E '^(git head|git subject|tracked dirty|untracked)' "$OUT/$LABEL.batch$n.log" | tee -a "$LOG"
  nz="$(grep -oE 'NON-ZERO: [0-9]+' "$OUT/$LABEL.batch$n.log" | grep -oE '[0-9]+' | tail -1)"
  nz="${nz:-unknown}"
  case "$nz" in
    ''|*[!0-9]*) echo "batch $n: NON-ZERO count unreadable — treating as uncertain" | tee -a "$LOG"
                 POST_OK=no ;;
    *) [ "$nz" -eq 0 ] || NONZERO_TOTAL=$((NONZERO_TOTAL + nz))
       echo "batch $n non-zero suites: $nz" | tee -a "$LOG" ;;
  esac

  grep -E '^exit=' "$OUT/$LABEL.batch$n.log" > "$OUT/$LABEL.batch$n.suites"
  COMPLETED=$((COMPLETED + 1))
done

echo | tee -a "$LOG"
echo "batches completed : $COMPLETED of $BATCHES" | tee -a "$LOG"
echo "non-zero total    : $NONZERO_TOTAL"          | tee -a "$LOG"
echo "postconditions    : $POST_OK"                | tee -a "$LOG"

# THE SENTINEL IS THE LAST THING, AND IT IS CONDITIONAL. The library refuses it unless the
# run was complete, clean and post-checked; there is no path here that writes it anyway.
if sentinel_write "$LOG" "$SUBJECT" "$LABEL" "$TOKEN" \
                  "$BATCHES" "$COMPLETED" "$NONZERO_TOTAL" "$POST_OK"; then
  echo "sentinel written for subject $SUBJECT label $LABEL" | tee -a "$LOG"
  exit 0
fi
echo "NO SUCCESS SENTINEL — this run is not complete." | tee -a "$LOG"
exit 1
