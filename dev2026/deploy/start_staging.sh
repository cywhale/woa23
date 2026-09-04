#!/usr/bin/env bash
#
# Launch the CANDIDATE api.app for alternate-port PM2 staging on VM24.
#
# This is NOT conf/start_app.sh with the numbers changed, and it must not become that.
# conf/start_app.sh cannot be adapted, for four independent reasons:
#
#   1. it launches `woa23_app:app` — the OLD app. Staging the candidate and
#      accidentally starting the old one would validate nothing and would look like a
#      pass;
#   2. it binds 127.0.0.1:8050 — PRODUCTION's port;
#   3. it passes the reload flag, which is a known production defect and has no place
#      here;
#   4. it sets no WOA23_ZARR_STORE, which api.config REQUIRES at import — the
#      candidate would fail to start with a message about the store, not about the
#      launcher.
#
# It also deliberately does NOT start Dask. The candidate imports no dask and no
# distributed: that is the whole of S1. A staging launcher that started a scheduler
# would be staging something the candidate does not use.
#
# TLS is deliberately absent. Staging is loopback-only and TLS/reverse-proxy behaviour
# belongs to the production cutover validation (spec 010 §5), where the real
# certificates and the real proxy are in play. Staging without TLS is a stated GAP,
# not an oversight.
#
# EVERY REQUIRED VARIABLE IS VALIDATED HERE AND NOTHING IS DEFAULTED. The pm2A run
# failed because the PM2 config carried defaults — an empty store and a spent port —
# which PM2 layered over the environment the command supplied. There are no defaults
# left to layer, and a missing or empty value is a refusal rather than a fallback.
#
#   WOA23_STAGING_PORT=18241 \
#   WOA23_STAGING_STORE=/home/odbadmin/woa23-pm2b/store \
#   WOA23_PRODUCTION_STORE=/home/odbadmin/python/woa23/data \
#     ./deploy/start_staging.sh
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP="api.app:app"
WORKERS="${WOA23_STAGING_WORKERS:-2}"

die() { printf '%s\n' "$@" >&2; exit 2; }

# --------------------------------------------------------------- required, all three
# `:-` rather than `:?` so the message can say WHICH variable and WHY, and so an empty
# value and an unset one get the same explanation. (`:?` would catch both — the colon
# form fires on null as well as unset — but its message is the less useful one.)
PORT="${WOA23_STAGING_PORT:-}"
STORE="${WOA23_STAGING_STORE:-}"
PROD_STORE="${WOA23_PRODUCTION_STORE:-}"

blank() { [ -z "${1//[[:space:]]/}" ]; }

blank "$PORT" && die \
  "WOA23_STAGING_PORT is missing or empty." \
  "  It is REQUIRED and has no default. The PM2 config used to carry one; that is" \
  "  what the pm2A failure was, so nothing here supplies a value you did not."
blank "$STORE" && die \
  "WOA23_STAGING_STORE is missing or empty." \
  "  It is REQUIRED and has no default. Name a staging store that exists, is" \
  "  readable, and carries the anchor group."
blank "$PROD_STORE" && die \
  "WOA23_PRODUCTION_STORE is missing or empty." \
  "  It is REQUIRED — it is what lets this launcher refuse a staging store that" \
  "  resolves inside production's. Without it the guard cannot run, and a guard that" \
  "  silently does not run is worse than none."

# --------------------------------------------------------------------------- the port
case "$PORT" in
  ''|*[!0-9]*) die "WOA23_STAGING_PORT is not a number: '$PORT'" ;;
esac
[ "$PORT" -ge 1024 ] && [ "$PORT" -le 65535 ] \
  || die "WOA23_STAGING_PORT out of range (1024-65535): $PORT"

# Production's ports are refused HERE, in the launcher, not only in a runbook. A
# runbook is a thing someone reads; this is a thing that stops.
case "$PORT" in
  8050|8786|8787) die \
    "refusing to bind $PORT: that is a PRODUCTION port." \
    "  Staging uses an alternate port and never production's." ;;
esac

# SPENT PORTS ARE REFUSED FROM THE LEDGER ITSELF, not from a list kept in this file.
# `scripts/ports_used.tsv` records every port this campaign has allocated, bound or
# not, and its whole purpose is that a run gets ports no earlier run had. Reading it
# here means a port cannot be reused by forgetting to update a hard-coded list — and
# 18221 and 18231, the two this failure has already spent, are in it.
#
# A COROLLARY, learned by breaking it: a port must NOT be written into the ledger
# before the run that uses it. Recording 18241 as "PROPOSED" made this check refuse the
# very port the next authorised run was requested with — the guard would have stopped
# that run at its first step. The ledger records ports a run has TAKEN; a port a request
# merely intends is recorded in the request, and enters the ledger afterwards.
LEDGER="$HERE/scripts/ports_used.tsv"
if [ -r "$LEDGER" ]; then
  if awk -F'\t' -v p="$PORT" '$1 == p { found = 1 } END { exit !found }' "$LEDGER"; then
    die "WOA23_STAGING_PORT $PORT is ALREADY IN scripts/ports_used.tsv." \
        "  Every port in that ledger is spent — allocated by an earlier run whether or" \
        "  not it was ever bound. A staging run takes a first-use port. Refusing."
  fi
else
  die "cannot read the port ledger at $LEDGER" \
      "  It is how a spent port is refused, so its absence is a stop, not a warning."
fi

# -------------------------------------------------------------------------- the store
[ -d "$STORE" ] || die "WOA23_STAGING_STORE is not a directory: $STORE"
[ -r "$STORE" ] || die "WOA23_STAGING_STORE is not readable: $STORE"

# The anchor group is what api.app's lifespan opens before the worker serves anything.
# Checking it HERE turns "the app died at startup" into "the store you named has no
# anchor", which is a different and far more useful failure.
ANCHOR="$STORE/${WOA23_STAGING_ANCHOR_REL:-1_degree/annual/TS}"
[ -r "$ANCHOR/.zgroup" ] || die \
  "no readable anchor group metadata at $ANCHOR/.zgroup" \
  "  api.app's lifespan opens the anchor before serving; a store without it cannot" \
  "  become ready. Name a staging store that has one."

# Production's store is refused by PHYSICAL path, so a symlink cannot slip past. A
# read-only INTENT is not a read-only GUARANTEE.
real_store="$(cd "$STORE" 2>/dev/null && pwd -P)" \
  || die "cannot resolve WOA23_STAGING_STORE: $STORE"
real_prod="$(cd "$PROD_STORE" 2>/dev/null && pwd -P || printf '%s' "$PROD_STORE")"
case "$real_store" in
  "$real_prod"|"$real_prod"/*) die \
    "WOA23_STAGING_STORE resolves inside the production store:" \
    "  staging   : $real_store" \
    "  production: $real_prod" \
    "Refusing. Staging uses its own store, never production's." ;;
esac

# --------------------------------------------------------------------------- the run
export WOA23_ZARR_STORE="$real_store"
# Not pinned: production does not pin it either, and staging should behave as
# production would. The row-order contract does not depend on it — that is what C2
# (`c2g`) established across three independent unpinned starts.
unset PYTHONHASHSEED || true

echo "staging launcher"
echo "  app                   : $APP"
echo "  port                  : $PORT   (first-use; not in the ledger)"
echo "  WOA23_ZARR_STORE      : $WOA23_ZARR_STORE"
echo "  production store       : $real_prod   (guard armed)"
echo "  workers               : $WORKERS"
echo "  reload disabled, TLS absent, Dask absent, not a production port"

# exec: PM2 must track the gunicorn master itself, not a shell that outlives it.
# --graceful-timeout matches the campaign's cleanup budget so a stop is orderly.
#
# The interpreter is named rather than inherited from PATH. `exec gunicorn` resolved
# through PATH and would run whichever gunicorn the operator happened to have first,
# which is not necessarily the pinned one in the verified tree.
PY="${WOA23_STAGING_PYTHON:-$HERE/.venv/bin/python}"
[ -x "$PY" ] || die "no interpreter at $PY" \
  "  Create the environment first (uv sync), or set WOA23_STAGING_PYTHON."
echo "  interpreter           : $PY ($("$PY" --version 2>&1))"

exec "$PY" -m gunicorn "$APP" \
  -w "$WORKERS" \
  -k uvicorn.workers.UvicornWorker \
  -b "127.0.0.1:$PORT" \
  --timeout 120 \
  --graceful-timeout 10
