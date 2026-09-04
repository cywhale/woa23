#!/usr/bin/env bash
#
# The read-only probe's guards, and the promise that it has no side-effecting code path.
#
# The guards are tested as PURE FUNCTIONS. The refusal that matters -- "the effective
# PM2_HOME is production's" -- must fire on the exact path and on anything inside it, and
# must NOT fire on a path that merely starts with the same characters.
#
# The prohibitions are tested as SOURCE PROPERTIES, because "it does not start a daemon"
# cannot be demonstrated by running it: a probe that wrongly started one would have
# started it. So the source is required to contain no pm2 subcommand invocation and no
# filesystem-creating call at all.
#
#     ./scripts/test_probe_host_capability.sh

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE="$HERE/probe_host_capability.sh"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then pass=$((pass + 1)); echo "  ok   $1"
  else fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}
yn() { if "$@" >/dev/null 2>&1; then echo yes; else echo no; fi; }

# shellcheck disable=SC1090
WOA23_PROBE_LIB_ONLY=1 . "$PROBE"

echo "is_production_pm2_home — the refusal that matters"
check "production's exact PM2_HOME is refused" yes \
      "$(yn is_production_pm2_home /home/odbadmin/.pm2)"
check "  with a trailing slash too" yes \
      "$(yn is_production_pm2_home /home/odbadmin/.pm2/)"
check "  and anything INSIDE it" yes \
      "$(yn is_production_pm2_home /home/odbadmin/.pm2/pids)"
check "  including a deep path" yes \
      "$(yn is_production_pm2_home /home/odbadmin/.pm2/logs/woa23-out.log)"
check "root's pm2 home is refused" yes "$(yn is_production_pm2_home /root/.pm2)"

echo
echo "  and it must NOT false-positive on a lookalike"
check "a path that merely shares the prefix is allowed" no \
      "$(yn is_production_pm2_home /home/odbadmin/.pm2backup)"
check "  a sibling directory is allowed" no \
      "$(yn is_production_pm2_home /home/odbadmin/.pm2-old)"
check "the validation account's own is allowed" no \
      "$(yn is_production_pm2_home /home/woa23c1ro/.pm2)"
check "  and its run-specific one" no \
      "$(yn is_production_pm2_home /home/woa23c1ro/woa23-b35a1-pm2)"
check "an empty path is not production's" no "$(yn is_production_pm2_home '')"

echo
echo "is_foreign_home_path — the broader net"
check "another account's home is foreign" yes \
      "$(yn is_foreign_home_path /home/odbadmin/anything /home/woa23c1ro)"
check "  even a pm2 path production does not use today" yes \
      "$(yn is_foreign_home_path /home/odbadmin/.pm2-future /home/woa23c1ro)"
check "our own home is not foreign" no \
      "$(yn is_foreign_home_path /home/woa23c1ro/woa23-b35a1-pm2 /home/woa23c1ro)"
check "  our home itself is not foreign" no \
      "$(yn is_foreign_home_path /home/woa23c1ro /home/woa23c1ro)"
check "a path outside /home is not judged here" no \
      "$(yn is_foreign_home_path /tmp/pm2 /home/woa23c1ro)"
check "a lookalike home prefix IS foreign" yes \
      "$(yn is_foreign_home_path /home/woa23c1ro-other/x /home/woa23c1ro)"

echo
echo "effective_pm2_home — what pm2 would use"
check "unset falls back to \$HOME/.pm2" "/home/woa23c1ro/.pm2" \
      "$(effective_pm2_home '' /home/woa23c1ro)"
check "  a trailing slash on HOME does not double it" "/home/woa23c1ro/.pm2" \
      "$(effective_pm2_home '' /home/woa23c1ro/)"
check "an explicit value wins" "/home/woa23c1ro/woa23-b35a1-pm2" \
      "$(effective_pm2_home /home/woa23c1ro/woa23-b35a1-pm2 /home/woa23c1ro)"
check "  and an explicit PRODUCTION value is returned so the guard can refuse it" \
      "/home/odbadmin/.pm2" "$(effective_pm2_home /home/odbadmin/.pm2 /home/woa23c1ro)"

echo
echo "the fallback case the probe exists to prove"
eff="$(effective_pm2_home '' /home/woa23c1ro)"
check "with PM2_HOME unset, the account does NOT fall back to production's" no \
      "$(yn is_production_pm2_home "$eff")"
check "  nor to any foreign home" no "$(yn is_foreign_home_path "$eff" /home/woa23c1ro)"

echo
echo "PROHIBITIONS — properties of the source, since running it cannot prove them"
src="$(cat "$PROBE")"
# Strip comments: the header documents what is forbidden, and those words must not be
# mistaken for the code doing it.
code="$(grep -v '^[[:space:]]*#' "$PROBE")"

for sub in start stop delete kill save resurrect jlist list ping; do
  check "no 'pm2 $sub' invocation in code" 0 \
        "$(printf '%s' "$code" | grep -cE "(^|[^a-zA-Z_])(pm2|\\\$PM2|\"\\\$PM2\"|\\\$\{PM2\}) +$sub([^a-zA-Z]|\$)" || true)"
done
check "\$PM2 is never executed as a command at all" 0 \
      "$(printf '%s' "$code" | grep -cE '^\s*"?\$\{?PM2\}?"? ' || true)"
check "no mkdir anywhere in code" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])mkdir([^a-zA-Z]|$)' || true)"
check "no touch" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])touch([^a-zA-Z]|$)' || true)"
check "no chmod" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])chmod([^a-zA-Z]|$)' || true)"
check "no chown" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])chown([^a-zA-Z]|$)' || true)"
check "no rm" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])rm([^a-zA-Z]|$)' || true)"
check "no curl" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])curl([^a-zA-Z]|$)' || true)"
check "no wget" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])wget([^a-zA-Z]|$)' || true)"
check "no nc" 0 "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])nc([^a-zA-Z]|$)' || true)"
# A real redirect is `>` (or `N>`) preceded by whitespace or line start. That does not
# match the `<unset>` / `<unreadable>` placeholders inside strings, whose `>` follows a
# letter -- the first version of this check counted nine of those and was simply wrong.
# Every redirect target must be /dev/null or &N; anything else would create a file.
check "every redirect targets /dev/null or a descriptor, never a file" 0 \
      "$(printf '%s' "$code" | grep -oE '(^|[[:space:]])[0-9]*>>?[[:space:]]*[^[:space:];)&|]*' \
         | sed 's/^[[:space:]]*//' | grep -vE '^[0-9]*>>?$' \
         | grep -vcE '^[0-9]*>>?[[:space:]]*(/dev/null|&[0-9])$' || true)"
# `>&1` and `>&2` both merge descriptors and create nothing; `2>&1` in a version
# capture is legitimate. Anything targeting a descriptor OTHER than stdout or stderr
# would be unusual enough to want review.
check "  descriptor redirects target only stdout or stderr" 0 \
      "$(printf '%s' "$code" | grep -oE '>&[0-9]' | grep -vcE '>&[12]$' || true)"
check "no 'ss -p' ownership attribution" 0 \
      "$(printf '%s' "$code" | grep -cE 'ss +-[a-z]*p' || true)"
check "ss is used, and only with -ltn" 2 \
      "$(printf '%s' "$code" | grep -cE 'ss -ltn' || true)"
check "PM2_HOME is never exported" 0 \
      "$(printf '%s' "$code" | grep -cE 'export +PM2_HOME' || true)"

echo
echo "positive properties"
check "node IS executed for its version (node starts no daemon)" 1 \
      "$(printf '%s' "$code" | grep -cE '"\$NODE" --version' || true)"
check "pm2's version is read from package.json as TEXT" 1 \
      "$(printf '%s' "$code" | grep -c 'PKG_JSON' <<<"$(printf '%s' "$code" | grep 'tr .,. ..n. < "\$PKG_JSON"')" || true)"
check "the production path list is explicit" 1 \
      "$(printf '%s' "$code" | grep -cE '^PRODUCTION_PM2_PATHS=' || true)"
check "the lib-only seam exists" 1 \
      "$(printf '%s' "$code" | grep -cE 'WOA23_PROBE_LIB_ONLY' || true)"
check "it exits non-zero on refusal" 1 \
      "$(printf '%s' "$code" | grep -cE '^[[:space:]]*exit 3[[:space:]]*$' || true)"
check "  and zero only when every guard passed" 1 \
      "$(printf '%s' "$code" | grep -cE '^[[:space:]]*exit 0[[:space:]]*$' || true)"

echo
echo "the PATH listing must not drop its last entry (probeA silently omitted /snap/bin)"
# The exact shape of the bug: printf without a trailing newline leaves the final field
# unterminated, so `read` returns non-zero and the loop body never runs for it.
count_entries() {   # emulate the probe's loop over a PATH string
  printf '%s\n' "$1" | tr ':' '\n' | while IFS= read -r d; do
    [ -n "$d" ] || continue; echo "$d"; done | wc -l | tr -d ' '
}
check "a 3-entry PATH lists all three" 3 "$(count_entries /a:/b:/c)"
check "  a single entry is listed" 1 "$(count_entries /only)"
check "  the real shape from the host lists all nine" 9 \
      "$(count_entries /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin)"
check "the probe uses the newline-terminated form" 1 \
      "$(printf '%s' "$code" | grep -cF "printf '%s\\n' \"\${PATH:-}\"" || true)"
check "  and not the truncating one" 0 \
      "$(printf '%s' "$code" | grep -cF "printf '%s' \"\${PATH:-}\"" || true)"

echo
echo "the script parses under the strict shell"
check "bash -n" 0 "$(bash -n "$PROBE" >/dev/null 2>&1; echo $?)"
check "no 'case' inside a command substitution" 0 \
      "$(printf '%s' "$src" | grep -cE '\$\([^)]*case ' || true)"

echo
suite_summary "$pass" "$fail"
