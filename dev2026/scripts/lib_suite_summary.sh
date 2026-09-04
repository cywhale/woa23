#!/usr/bin/env bash
#
# THE SUITE SUMMARY CONTRACT — one machine-readable line, and only one.
#
#     ASSERTIONS=<n> FAILED=<m>
#
# WHY THIS EXISTS. The batch runner used to display each suite's LAST stdout line and the
# totals were read off that display. Fifty-five suites ended with `all passed (N
# assertions)`; `test_bootstrap_delivery.sh` ended with `=== 100 passed, 0 failed ===`, a
# private shape nothing could tally; and `test_staging_store.py` printed the standard
# summary and then a CAVEAT, so the caveat was displayed and its 24 assertions vanished
# too. The batch log therefore reported 4667 when the true total was 4791 -- and it
# reported it as if it were the total. A count that is wrong is worse than a count that is
# absent, because nobody goes looking for it.
#
# THE PROSE LINE IS STILL PRINTED, for a person reading the log. It is NOT what is parsed.
# Those are different jobs and the whole defect came from making one line do both.
#
# THE CONTRACT LINE IS LAST. Nothing may follow it -- not a caveat, not a note, not a
# blank line. A suite with something to say after its result must say it BEFORE.

# The regex is written once, here, and both the writer and the reader below use this file.
# When the format lived in 56 places there were three of it.
SUITE_SUMMARY_RE='^ASSERTIONS=[0-9][0-9]* FAILED=[0-9][0-9]*$'

#: Print the contract line and nothing else. For the rare suite that must control its own
#: exit path (an early skip, for instance) and only needs the line.
#:
#: The counts are validated here rather than trusted: a summary line carrying a non-number
#: is not machine-readable, and emitting one would put the runner in the position of
#: guessing what a suite meant.
suite_summary_line() {   # <assertions> <failed>
  local a="${1-}" f="${2-}"
  case "$a" in ''|*[!0-9]*)
    printf 'SUITE SUMMARY ERROR: assertions is not a number: %s\n' "${a:-<empty>}" >&2
    exit 2 ;;
  esac
  case "$f" in ''|*[!0-9]*)
    printf 'SUITE SUMMARY ERROR: failed is not a number: %s\n' "${f:-<empty>}" >&2
    exit 2 ;;
  esac
  # MORE FAILURES THAN ASSERTIONS IS NOT A COUNT, it is a bug in whoever is counting.
  [ "$f" -le "$a" ] || {
    printf 'SUITE SUMMARY ERROR: FAILED=%s exceeds ASSERTIONS=%s\n' "$f" "$a" >&2
    exit 2
  }
  printf 'ASSERTIONS=%s FAILED=%s\n' "$a" "$f"
}

#: The whole ending: the prose line, then the contract line, then the exit status.
#:
#: THE EXIT STATUS IS DERIVED FROM THE COUNT, never passed in alongside it. A suite that
#: reported `FAILED=3` and exited 0 -- or `FAILED=0` and exited 1 -- would be making two
#: claims that cannot both be true, and the runner refuses that combination. Deriving it
#: here means a suite cannot produce it by accident.
suite_summary() {   # <passed> <failed>
  local p="${1-}" f="${2-}" total
  case "$p" in ''|*[!0-9]*)
    printf 'SUITE SUMMARY ERROR: passed is not a number: %s\n' "${p:-<empty>}" >&2
    exit 2 ;;
  esac
  case "$f" in ''|*[!0-9]*)
    printf 'SUITE SUMMARY ERROR: failed is not a number: %s\n' "${f:-<empty>}" >&2
    exit 2 ;;
  esac
  total=$((p + f))
  if [ "$f" -eq 0 ]; then printf 'all passed (%s assertions)\n' "$total"
  else printf '%s FAILED, %s passed\n' "$f" "$p"; fi
  suite_summary_line "$total" "$f"
  [ "$f" -eq 0 ] || exit 1
  exit 0
}

# ------------------------------------------------------------------- THE READING SIDE ---
#
# FAIL CLOSED. Every check below returns a REASON, and the runner treats a reason as a
# failed suite. That is deliberate: the alternative is a batch that quietly totals whatever
# it managed to parse and presents the result as the total, which is exactly what produced
# 4667 in place of 4791. A number that is wrong is worse than a number that is missing,
# because nobody goes looking for it.

#: Why this suite's summary cannot be trusted, or empty if it can.
suite_summary_problem() {   # <stdout-file> <exit-status>
  local f="${1-}" rc="${2-}" n last a fail
  [ -n "$f" ] && [ -f "$f" ] || { printf 'no stdout was captured'; return 0; }

  # EXACTLY ONE. Two summaries mean two claims, and picking either is guessing. A suite
  # that emits a second one -- because a helper it calls also emits one, say -- has to be
  # fixed, not disambiguated here.
  n="$(grep -c -E "$SUITE_SUMMARY_RE" "$f" 2>/dev/null || true)"
  n="${n:-0}"
  [ "$n" -ne 0 ] || { printf 'no ASSERTIONS=<n> FAILED=<m> summary line'; return 0; }
  [ "$n" -eq 1 ] || { printf 'the summary line appears %s times; exactly one is required' "$n"; return 0; }

  # IT MUST BE LAST. This is the test_staging_store.py case by name: it printed its summary
  # and then a caveat, so the caveat was the final line, the runner displayed the caveat,
  # and 24 assertions vanished from every total. Anything after the summary -- a caveat, a
  # note, a stray blank line -- is refused rather than skipped past.
  last="$(tail -1 "$f")"
  printf '%s\n' "$last" | grep -qE "$SUITE_SUMMARY_RE" || {
    printf 'the summary is not the final line; the final line is: %s' "${last:-<empty>}"
    return 0
  }

  a="${last#ASSERTIONS=}"; a="${a%% *}"
  fail="${last##*FAILED=}"
  [ "$fail" -le "$a" ] || { printf 'FAILED=%s exceeds ASSERTIONS=%s' "$fail" "$a"; return 0; }

  # THE TWO CLAIMS MUST AGREE. A suite reporting FAILED=0 while exiting non-zero, or
  # FAILED=3 while exiting 0, tells the batch two incompatible things about itself.
  # Believing the more convenient one is how a failing run gets reported as clean.
  case "$rc" in ''|*[!0-9]*) printf 'the exit status is not a number: %s' "${rc:-<empty>}"; return 0 ;; esac
  if [ "$fail" -eq 0 ] && [ "$rc" -ne 0 ]; then
    printf 'reports FAILED=0 but exited %s' "$rc"; return 0
  fi
  if [ "$fail" -ne 0 ] && [ "$rc" -eq 0 ]; then
    printf 'reports FAILED=%s but exited 0' "$fail"; return 0
  fi
  return 0
}

#: "<assertions> <failed>" from a summary ALREADY checked by suite_summary_problem. It is
#: not a second validator; calling it on an unchecked file is a caller error.
suite_summary_counts() {   # <stdout-file>
  local last a
  last="$(tail -1 "${1-}" 2>/dev/null)"
  a="${last#ASSERTIONS=}"; a="${a%% *}"
  printf '%s %s' "$a" "${last##*FAILED=}"
}
