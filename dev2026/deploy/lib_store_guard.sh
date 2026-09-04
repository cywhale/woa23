#!/usr/bin/env bash
#
# The real-production-store ownership guard.
#
# WHY IT IS ITS OWN FILE. The guard used to be three inline lines in
# `staging_execute.sh`, and it was written INVERTED:
#
#     [ "$STORE_UID" != "$ME_UID" ] && die "...owned by uid $STORE_UID, which is this account"
#
# It fired when the uids DIFFER -- the correct, safe state -- and would have passed
# silently when they MATCH, which is the dangerous one. D-3 halted on it: store uid 1000,
# account uid 994, refused with a message asserting they were the same.
#
# The tests did not catch it because they asserted the guard's PRESENCE -- a grep for its
# message -- and never its BEHAVIOUR. A guard whose decision is never exercised is a
# comment with a syntax error's blast radius. It lives here so the decision is a function
# that a test can call with every combination of uids, rather than three lines only a real
# production store could ever reach.

# store_owner_verdict <store_uid> <me_uid> <expected_uid>
#
# Echoes a one-word verdict and returns 0 ONLY for `ok`.
#
#   ok                  the store is owned by the expected uid, and not by this account
#   unreadable          the owner could not be read at all
#   nonnumeric          the owner is not a plain uid
#   self                the store is owned by THIS account -- it could be written
#   root                the store is owned by root
#   unexpected          a real uid, but not the authorised owner
#
# The order matters and is deliberate: `self` is reported before `root` and before
# `unexpected`, so the most dangerous case is never described as merely surprising.
store_owner_verdict() {
  local store_uid="${1-}" me_uid="${2-}" expect_uid="${3-}"

  # An unreadable owner is a refusal, never a default. `stat` failing must not be able to
  # produce an empty string that then compares equal to something.
  [ -n "$store_uid" ] || { echo unreadable; return 3; }
  case "$store_uid" in *[!0-9]*) echo nonnumeric; return 4 ;; esac
  [ -n "$me_uid" ] || { echo unreadable; return 3; }
  case "$me_uid" in *[!0-9]*) echo nonnumeric; return 4 ;; esac
  [ -n "$expect_uid" ] || { echo unreadable; return 3; }
  case "$expect_uid" in *[!0-9]*) echo nonnumeric; return 4 ;; esac

  # THE DANGEROUS CASE, FIRST. If this account owns the store it can write to it whatever
  # the mode bits say, because an owner may always change them back.
  [ "$store_uid" = "$me_uid" ] && { echo self; return 5; }

  # Root-owned is refused rather than treated as "surely safe": it is not the authorised
  # owner, and a store that moved to root is a change nobody here made.
  [ "$store_uid" = 0 ] && { echo root; return 6; }

  # Anything other than the one authorised owner stops the run.
  [ "$store_uid" = "$expect_uid" ] || { echo unexpected; return 7; }

  echo ok
  return 0
}
