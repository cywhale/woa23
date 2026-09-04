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
