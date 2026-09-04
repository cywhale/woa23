#!/bin/sh
# Process control for the batch driver. Lives OUTSIDE the subject, so fixing it needs
# no successor subject and 143bf8c is not modified.
#
# THE RULE THAT PRODUCED THE LAST INCIDENT, and what replaces it:
#
#   `pkill -f <pattern>` matched the argv of the very shell that ran it -- the observer
#   entered its own matcher -- and killed the session. This is the 8th occurrence of that
#   class in this campaign. `pkill -f` and `pgrep -f` are BANNED here.
#
#   Replacement: SNAPSHOT FIRST. The driver's identity is recorded as (pid, starttime) at
#   launch, read from /proc. Stopping re-reads /proc for that exact pid and signals ONLY
#   if the starttime still matches, so a reused pid cannot be hit and no pattern is ever
#   matched against any command line.
#
# starttime is field 22 of /proc/<pid>/stat, counted AFTER the final ')' -- comm can
# contain spaces and parentheses, so field-splitting the whole line is wrong.
set -u
LC_ALL=C; export LC_ALL

starttime_of() {   # starttime_of <pid> -> prints starttime, or nothing
  [ -r "/proc/$1/stat" ] || return 1
  sed 's/.*) //' "/proc/$1/stat" 2>/dev/null | awk '{print $20}'
}

identity_of() {    # identity_of <pid> -> "<pid> lin-<starttime>"
  st=$(starttime_of "$1") || return 1
  [ -n "$st" ] || return 1
  printf '%s lin-%s\n' "$1" "$st"
}

case "${1:-}" in
  record)   # record <pid> <file>
    id=$(identity_of "$2") || { echo "cannot read identity for pid $2" >&2; exit 1; }
    printf '%s\n' "$id" > "$3"
    printf 'recorded identity: %s\n' "$id"
    ;;
  alive)    # alive <file>   -> exit 0 if the SAME process is still running
    read -r pid want < "$2" || exit 1
    have=$(identity_of "$pid" 2>/dev/null | cut -d' ' -f2 || true)
    [ -n "$have" ] && [ "$have" = "$want" ]
    ;;
  stop)     # stop <file>    -> TERM only the exact recorded (pid, starttime)
    read -r pid want < "$2" || exit 1
    have=$(identity_of "$pid" 2>/dev/null | cut -d' ' -f2 || true)
    if [ -z "$have" ]; then
      printf 'pid %s is gone; nothing signalled\n' "$pid"; exit 0
    fi
    if [ "$have" != "$want" ]; then
      printf 'REFUSING: pid %s is now %s, recorded %s -- pid reuse, nothing signalled\n' \
        "$pid" "$have" "$want" >&2
      exit 3
    fi
    printf 'identity verified (%s %s); sending TERM\n' "$pid" "$want"
    kill -TERM "$pid"
    ;;
  children) # children <file> -> report descendants by pid, via /proc, no pattern matching
    read -r pid want < "$2" || exit 1
    for p in /proc/[0-9]*; do
      c=${p#/proc/}
      ppid=$(sed 's/.*) //' "$p/stat" 2>/dev/null | awk '{print $2}')
      [ "$ppid" = "$pid" ] && printf '  child %s %s\n' "$c" "$(identity_of "$c" | cut -d' ' -f2)"
    done
    ;;
  *) echo "usage: procctl.sh record|alive|stop|children ..." >&2; exit 2 ;;
esac
