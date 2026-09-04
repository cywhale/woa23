#!/usr/bin/env bash
#
# The PM2 discovery probe: its classification logic, and the promise that it executes
# no pm2 and creates nothing.
#
# The four-way classification is the point of the probe -- "absent", "present but not on
# PATH", "present but unreachable" and "usable for staging" are four different decisions,
# and collapsing any two of them would hand back a wrong one. So each is tested.
#
# The prohibitions are tested as SOURCE properties. A probe that wrongly spawned a PM2
# daemon would already have spawned it; running it cannot prove it does not.
#
#     ./scripts/test_probe_pm2_discovery.sh

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE="$HERE/probe_pm2_discovery.sh"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then pass=$((pass + 1)); echo "  ok   $1"
  else fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}
yn() { if "$@" >/dev/null 2>&1; then echo yes; else echo no; fi; }

# shellcheck disable=SC1090
WOA23_PM2DISC_LIB_ONLY=1 . "$PROBE"

echo "path_is_under — a true path prefix, not a string prefix"
check "exact match" yes "$(yn path_is_under /a/b /a/b)"
check "  trailing slash on either side" yes "$(yn path_is_under /a/b/ /a/b)"
check "a child is under" yes "$(yn path_is_under /a/b/c /a/b)"
check "  a deep child is under" yes "$(yn path_is_under /a/b/c/d/e /a/b)"
check "a SIBLING sharing the prefix is NOT under" no "$(yn path_is_under /a/bc /a/b)"
check "  nor is a lookalike" no "$(yn path_is_under /a/b-old /a/b)"
check "a parent is not under its child" no "$(yn path_is_under /a /a/b)"
check "empty inputs are not under" no "$(yn path_is_under '' /a/b)"

echo
echo "production_state_owner — which production path contains it"
check "production PM2_HOME itself" "/home/odbadmin/.pm2" \
      "$(production_state_owner /home/odbadmin/.pm2 || true)"
check "  something inside it" "/home/odbadmin/.pm2" \
      "$(production_state_owner /home/odbadmin/.pm2/modules/pm2/bin/pm2 || true)"
check "  the production tree" "/home/odbadmin/python/woa23" \
      "$(production_state_owner /home/odbadmin/python/woa23/conf/start_app.sh || true)"
check "root's pm2 home" "/root/.pm2" "$(production_state_owner /root/.pm2/x || true)"
check "a system path is NOT production state" "" \
      "$(production_state_owner /usr/lib/node_modules/pm2/bin/pm2 || true)"
check "  nor is the validation account's own home" "" \
      "$(production_state_owner /home/woa23c1ro/.local/bin/pm2 || true)"
check "  nor a lookalike of production's" "" \
      "$(production_state_owner /home/odbadmin/.pm2backup/bin/pm2 || true)"

echo
echo "classify_pm2 — the four outcomes must stay four"
check "nothing found -> A_ABSENT" "A_ABSENT" "$(classify_pm2 no no no no)"
check "  found is irrelevant if not found" "A_ABSENT" "$(classify_pm2 no yes yes yes)"
check "found, readable, executable, on PATH -> D_USABLE_FOR_STAGING" \
      "D_USABLE_FOR_STAGING" "$(classify_pm2 yes yes yes yes)"
check "found, readable, executable, NOT on PATH -> B_NOT_ON_PATH" \
      "B_NOT_ON_PATH" "$(classify_pm2 yes yes yes no)"
check "found but NOT readable -> C_NOT_ACCESSIBLE" \
      "C_NOT_ACCESSIBLE" "$(classify_pm2 yes no yes no)"
check "found but NOT executable -> C_NOT_ACCESSIBLE" \
      "C_NOT_ACCESSIBLE" "$(classify_pm2 yes yes no no)"
check "  unreachable beats on-PATH: a PATH entry you cannot execute is still C" \
      "C_NOT_ACCESSIBLE" "$(classify_pm2 yes no no yes)"
check "the four outcomes are distinct strings" 4 \
      "$(printf '%s\n%s\n%s\n%s\n' "$(classify_pm2 no no no no)" \
         "$(classify_pm2 yes yes yes no)" "$(classify_pm2 yes no yes no)" \
         "$(classify_pm2 yes yes yes yes)" | sort -u | wc -l | tr -d ' ')"

echo
echo "candidate_kind — a DIRECTORY is never an executable candidate (probeB defect 1)"
check "a directory is DIRECTORY, whatever its x bit" "DIRECTORY" \
      "$(candidate_kind d no yes)"
check "  even when it looks readable and executable" "DIRECTORY" \
      "$(candidate_kind d yes yes)"
check "an executable regular file is EXECUTABLE" "EXECUTABLE" \
      "$(candidate_kind f yes yes)"
check "a regular file without +x is NOT_EXECUTABLE" "NOT_EXECUTABLE" \
      "$(candidate_kind f yes no)"
check "something that is neither is NOT_A_REGULAR_FILE" "NOT_A_REGULAR_FILE" \
      "$(candidate_kind other no no)"
check "  a socket/fifo is not smuggled through as executable" "NOT_A_REGULAR_FILE" \
      "$(candidate_kind other no yes)"

echo
echo "package_search_dirs — nearest ancestor first (probeB defect 2)"
check "walks up from the file's directory" "/a/b/c
/a/b
/a" "$(package_search_dirs /a/b/c/pm2 3)"
check "  <pkg>/bin/pm2 reaches <pkg> at the second step" "/p/bin
/p" "$(package_search_dirs /p/bin/pm2 2)"
check "  <pkg>/pm2 reaches <pkg> at the FIRST step" "/p" \
      "$(package_search_dirs /p/pm2 1)"
check "stops at the root, never above it" "/a" "$(package_search_dirs /a/pm2 9)"
check "empty input yields nothing" "" "$(package_search_dirs '' 4)"

echo
echo "END TO END on a fixture tree — both defects reproduced and shown fixed"
FIX="$(mktemp -d)"
mkdir -p "$FIX/np/lib/node_modules/pm2/bin" "$FIX/plain"
printf '#!/bin/sh\nexit 0\n' > "$FIX/np/lib/node_modules/pm2/bin/pm2"
chmod 755 "$FIX/np/lib/node_modules/pm2/bin/pm2"
printf '{"name":"pm2","version":"9.9.9"}\n' > "$FIX/np/lib/node_modules/pm2/package.json"
# the two shapes probeB mis-reported: the package DIRECTORY, and a file one level in
printf '#!/bin/sh\nexit 0\n' > "$FIX/np/lib/node_modules/pm2/pm2"
chmod 755 "$FIX/np/lib/node_modules/pm2/pm2"
# a regular file called pm2 that is NOT executable
printf 'not a program\n' > "$FIX/plain/pm2"
chmod 644 "$FIX/plain/pm2"

OUT="$(WOA23_PM2DISC_ROOTS="$FIX/np:5 $FIX/plain:2" bash "$PROBE" 2>&1)"

# Two hits are legitimately non-executable here: the package DIRECTORY and the
# mode-644 file. The first draft of this assertion expected one and was simply wrong.
check "both non-executable hits are reported as such" 2 \
      "$(printf '%s' "$OUT" | grep -c 'hit (NOT an executable candidate)' || true)"
check "  exactly one of them is the DIRECTORY" 1 \
      "$(printf '%s' "$OUT" | grep -c 'kind      : DIRECTORY' || true)"
check "  with its x bit explained as traversable, not runnable" 1 \
      "$(printf '%s' "$OUT" | grep -c 'means TRAVERSABLE, not runnable' || true)"
# Assert no DERIVED package field is emitted -- not merely that the string
# "package.json" is absent, which the probe's own explanatory NOTE contains and which
# made the first draft of this check fail against its own prose.
check "  no derived 'package.json :' field for the directory" 0 \
      "$(printf '%s' "$OUT" | grep -A6 'kind      : DIRECTORY' | grep -cE '^  package\.json :' || true)"
check "  and no VERSION field for it" 0 \
      "$(printf '%s' "$OUT" | grep -A6 'kind      : DIRECTORY' | grep -cE '^  VERSION' || true)"
check "a non-executable regular file is NOT_EXECUTABLE" 1 \
      "$(printf '%s' "$OUT" | grep -c 'kind      : NOT_EXECUTABLE' || true)"
check "  and no version is derived for it either" 0 \
      "$(printf '%s' "$OUT" | grep -A4 'kind      : NOT_EXECUTABLE' | grep -c 'VERSION' || true)"

check "the real bin/pm2 IS an executable candidate" 1 \
      "$(printf '%s' "$OUT" | grep -c 'EXECUTABLE CANDIDATE:.*/pm2/bin/pm2' || true)"
check "  <pkg>/pm2 is ALSO an executable candidate (a real executable file)" 1 \
      "$(printf '%s' "$OUT" | grep -cE 'EXECUTABLE CANDIDATE:.*node_modules/pm2/pm2$' || true)"
check "  BOTH resolve to the SAME real package.json" 2 \
      "$(printf '%s' "$OUT" | grep -c 'VERSION   : 9.9.9' || true)"
check "  so neither invents a 'package.json NOT readable' line" 0 \
      "$(printf '%s' "$OUT" | grep -c 'package.json NOT readable' || true)"
check "  and the package name is read correctly for both" 2 \
      "$(printf '%s' "$OUT" | grep -c 'name      : pm2' || true)"
check "the overall verdict is not A_ABSENT when executables exist" 0 \
      "$(printf '%s' "$OUT" | grep -c 'A_ABSENT' || true)"
rm -r "$FIX"

echo
echo "search completeness is reported for EVERY outcome, not only A_ABSENT"
FIX2="$(mktemp -d)"
mkdir -p "$FIX2/blocked" "$FIX2/open"
chmod 000 "$FIX2/blocked"
OUT2="$(WOA23_PM2DISC_ROOTS="$FIX2/blocked:2 $FIX2/open:2" bash "$PROBE" 2>&1)"
check "an unreadable root is named NOT TRAVERSABLE" 1 \
      "$(printf '%s' "$OUT2" | grep -c 'NOT TRAVERSABLE by uid' || true)"
check "  a SEARCH COMPLETENESS section is emitted" 1 \
      "$(printf '%s' "$OUT2" | grep -c '5b. SEARCH COMPLETENESS' || true)"
check "  and it says the search is INCOMPLETE" 1 \
      "$(printf '%s' "$OUT2" | grep -c 'SEARCH IS INCOMPLETE' || true)"
check "  and warns A_ABSENT would not mean absent from the host" 1 \
      "$(printf '%s' "$OUT2" | grep -c "NOT the same as .absent from the host." || true)"
chmod 755 "$FIX2/blocked"
rm -r "$FIX2"

echo
echo "PROHIBITIONS — source properties"
code="$(grep -v '^[[:space:]]*#' "$PROBE")"

for sub in start stop delete kill save resurrect jlist list ping ls describe reload; do
  check "no 'pm2 $sub' in code" 0 \
        "$(printf '%s' "$code" | grep -cE "(^|[^a-zA-Z_./-])pm2 +$sub([^a-zA-Z]|\$)" || true)"
done
check "no 'pm2 --version'" 0 "$(printf '%s' "$code" | grep -cE 'pm2 +--version' || true)"
check "no 'pm2 -v'" 0 "$(printf '%s' "$code" | grep -cE 'pm2 +-v([^a-zA-Z]|$)' || true)"
check "no candidate path is ever executed" 0 \
      "$(printf '%s' "$code" | grep -cE '^\s*"\$(real|c|PM2|cand)"' || true)"
check "no mkdir" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])mkdir([^a-zA-Z]|$)' || true)"
check "no touch" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])touch([^a-zA-Z]|$)' || true)"
check "no chmod" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])chmod([^a-zA-Z]|$)' || true)"
check "no chown" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])chown([^a-zA-Z]|$)' || true)"
check "no setfacl" 0 "$(printf '%s' "$code" | grep -cE 'setfacl' || true)"
check "no rm" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])rm([^a-zA-Z]|$)' || true)"
check "no sudo" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])sudo([^a-zA-Z]|$)' || true)"
check "no su" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])su +' || true)"
check "no setpriv" 0 "$(printf '%s' "$code" | grep -cE 'setpriv' || true)"
check "no curl/wget/nc" 0 \
      "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])(curl|wget|nc)([^a-zA-Z]|$)' || true)"
check "PATH is never assigned" 0 \
      "$(printf '%s' "$code" | grep -cE '^[[:space:]]*(export +)?PATH=' || true)"
check "PM2_HOME is never assigned or exported" 0 \
      "$(printf '%s' "$code" | grep -cE '^[[:space:]]*(export +)?PM2_HOME=' || true)"
check "every redirect targets /dev/null or a descriptor" 0 \
      "$(printf '%s' "$code" | grep -oE '(^|[[:space:]])[0-9]*>>?[[:space:]]*[^[:space:];)&|]*' \
         | sed 's/^[[:space:]]*//' | grep -vE '^[0-9]*>>?$' \
         | grep -vcE '^[0-9]*>>?[[:space:]]*(/dev/null|&[0-9])$' || true)"
check "no ss at all — this probe checks no listeners" 0 \
      "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])ss +-' || true)"

echo
echo "SEARCH IS BOUNDED — an unbounded find would be a different, riskier thing"
check "every find has -maxdepth" 0 \
      "$(printf '%s' "$code" | grep -E '(^|[^a-zA-Z_])find ' | grep -vc 'maxdepth' || true)"
check "the candidate find uses -xdev" 1 \
      "$(printf '%s' "$code" | grep -cE 'find "\$root" -maxdepth "\$depth" -xdev' || true)"
check "no find rooted at / " 0 \
      "$(printf '%s' "$code" | grep -cE 'find +/ ' || true)"
# Intent, not an occurrence count: the phrase must appear at least once, and the
# untraversable branch must `continue` rather than fall through into a search.
check "root traversability is reported, not silently skipped" yes \
      "$([ "$(printf '%s' "$code" | grep -c 'NOT TRAVERSABLE' || true)" -ge 1 ] && echo yes || echo no)"
# The printf wraps over three lines, so the `continue` that skips the search sits a
# few lines below the message -- widen the window rather than narrow the property.
check "  and an untraversable root is skipped, not searched" yes \
      "$([ "$(printf '%s' "$code" | grep -A5 'NOT TRAVERSABLE by uid' | grep -c 'continue' || true)" -ge 1 ] && echo yes || echo no)"
# Now appears in BOTH the 5b completeness section and the A_ABSENT note -- the warning
# was deliberately moved to every outcome, so "at least once" is the property.
check "the verdict warns that untraversable roots were not searched" yes \
      "$([ "$(printf '%s' "$code" | grep -c 'were NOT searched' || true)" -ge 1 ] && echo yes || echo no)"
check "  and completeness is reported in its own section, for every outcome" 1 \
      "$(printf '%s' "$code" | grep -c '5b. SEARCH COMPLETENESS' || true)"

echo
echo "POSITIVE properties"
check "node IS run for its version" 1 \
      "$(printf '%s' "$code" | grep -cE '"\$NODE" --version' || true)"
check "the version comes from package.json as TEXT, never from running pm2" 1 \
      "$(printf '%s' "$code" | grep -cE 'tr .,. ..n. < "\$pj".*version' || true)"
# The derivation changed: instead of guessing dirname(dirname(path)), the probe now
# walks ancestors and takes the nearest one that ACTUALLY HAS a readable package.json.
check "  the package is the nearest ancestor that HAS a package.json" 1 \
      "$(printf '%s' "$code" | grep -cE 'package_search_dirs "\$real"' || true)"
check "  and the old dirname(dirname()) guess is gone" 0 \
      "$(printf '%s' "$code" | grep -cE 'pkg="\$\(dirname "\$\(dirname' || true)"
check "sha256 of each candidate is reported" 1 \
      "$(printf '%s' "$code" | grep -cE 'sha256sum "\$real"' || true)"
check "writability of the binary is reported" 1 \
      "$(printf '%s' "$code" | grep -cE 'writable \(binary\)' || true)"
check "  and of its parent dir" 1 \
      "$(printf '%s' "$code" | grep -cE 'writable \(parent dir\)' || true)"
check "  and of the package" 1 \
      "$(printf '%s' "$code" | grep -c 'pkg writable by this account' || true)"
# The defect probeA shipped with: `printf '%s'` leaves the final field unterminated,
# so `read` returns non-zero on it and the last PATH entry is never listed. What
# matters is that the TRUNCATING form appears NOWHERE -- not how often the safe form
# happens to appear.
check "the truncating printf form appears NOWHERE" 0 \
      "$(printf '%s' "$code" | grep -cF "printf '%s' \"\${PATH:-}\"" || true)"
check "  and PATH is split with the newline-terminated form" yes \
      "$([ "$(printf '%s' "$code" | grep -cF "printf '%s\n' \"\${PATH:-}\"" || true)" -ge 1 ] && echo yes || echo no)"
check "the result is explicitly NOT a B3/B5 pass" 1 \
      "$(printf '%s' "$code" | grep -c 'says NOTHING about B3 or B5' || true)"

echo
echo "the script parses, and runs from stdin (no BASH_SOURCE / \$0)"
check "bash -n" 0 "$(bash -n "$PROBE" >/dev/null 2>&1; echo $?)"
check "no BASH_SOURCE or \$0 dependency" 0 \
      "$(grep -cE 'BASH_SOURCE|\$0' "$PROBE" || true)"
check "no 'case' inside a command substitution" 0 \
      "$(grep -cE '\$\([^)]*case ' "$PROBE" || true)"

echo
suite_summary "$pass" "$fail"
