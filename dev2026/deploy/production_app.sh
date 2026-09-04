#!/usr/bin/env bash
#
# PROPOSED replacement for conf/start_app.sh. NOT INSTALLED, NOT IN USE.
#
# This file lives in dev2026/deploy/ deliberately. Writing it into conf/ would be a
# production change, and a production change needs its own authorisation — so the
# proposal is developed and tested here, and installing it is a separate, later act.
#
# It resolves the five named cutover blockers of spec 010 §5a:
#
#   B2  it launches `api.app:app`, the candidate — not `woa23_app:app`
#   B3  the port comes from WOA23_PORT; there is no port literal in this file
#   B4  it requires and VALIDATES WOA23_ZARR_STORE, which api.config needs at import
#   B5  there is no --reload
#   B1  it `exec`s, so PM2 tracks the gunicorn master itself and the grep-based
#       `pre_stop` that `kill -9`s by command-line string becomes unnecessary —
#       see deploy/ecosystem.production.config.js, which removes it
#
# WHAT IT KEEPS FROM PRODUCTION, ON PURPOSE:
#
#   TLS. Production terminates TLS in gunicorn itself (--keyfile/--certfile), not in a
#   proxy in front of it. Dropping that would turn an HTTPS endpoint into an HTTP one
#   at cutover, which is a silent, externally visible break. The certificate paths stay
#   configurable and are checked for readability before the exec.
#
#   Two workers, and the 120s timeout. Changing concurrency or timeouts at the same
#   time as changing the application would confuse two effects in one deployment.
#
# FAIL-CLOSED, AND WHY THAT IS THE RIGHT TRADE HERE. Every required variable is
# refused by name when missing or empty; nothing is defaulted into place. A launcher
# that refuses to start is visible in `pm2 list` within seconds and is fixed by
# correcting the config. A launcher that starts on a silently-defaulted value serves
# the wrong store, or the wrong port, and looks healthy while doing it. The rollback
# path (spec 011 §7) exists precisely so that a refusal is recoverable.
#
# The one thing NOT defaulted-but-also-not-required is the interpreter, which defaults
# to production's existing pyenv python. That is deliberate: it preserves today's
# behaviour, and spec 011 §4 records that the deployment must first be shown to have
# the candidate's dependencies (blocker B6) before this file can run at all.
set -euo pipefail

APP="api.app:app"

die() { printf '%s\n' "$@" >&2; exit 2; }
blank() { [ -z "${1//[[:space:]]/}" ]; }

# ------------------------------------------------------------------ required values
PORT="${WOA23_PORT:-}"
STORE="${WOA23_ZARR_STORE:-}"

blank "$PORT" && die \
  "WOA23_PORT is missing or empty." \
  "  It is REQUIRED and has no default. The port used to be a literal in this file" \
  "  (blocker B3); it is now configuration, and configuration that is absent is a" \
  "  refusal rather than a guess."
blank "$STORE" && die \
  "WOA23_ZARR_STORE is missing or empty." \
  "  It is REQUIRED. api.config reads it at import, so without it the application" \
  "  fails with a message about the store and the launcher looks innocent — which is" \
  "  blocker B4. It is checked here instead, before anything is started."

# -------------------------------------------------------------------------- the port
case "$PORT" in
  ''|*[!0-9]*) die "WOA23_PORT is not a number: '$PORT'" ;;
esac
[ "$PORT" -ge 1 ] && [ "$PORT" -le 65535 ] \
  || die "WOA23_PORT out of range (1-65535): $PORT"

# ------------------------------------------------------------------------- the store
# Checked HERE so a store problem reads as a store problem. The candidate's lifespan
# opens the anchor group before a worker serves anything; if it is missing, the service
# starts, fails to become ready, and PM2 restarts it in a loop.
[ -d "$STORE" ] || die "WOA23_ZARR_STORE is not a directory: $STORE"
[ -r "$STORE" ] || die "WOA23_ZARR_STORE is not readable: $STORE"

ANCHOR="$STORE/${WOA23_ANCHOR_REL:-1_degree/annual/TS}"
[ -r "$ANCHOR/.zgroup" ] || die \
  "no readable anchor group metadata at $ANCHOR/.zgroup" \
  "  The application opens the anchor at startup; a store without it can never become" \
  "  ready, and PM2 would restart the service indefinitely instead of failing once."

# --------------------------------------------------------------------------- the TLS
# Defaulted to production's current paths so that installing this file does not, by
# itself, change the TLS configuration. Both are checked: gunicorn's own error for an
# unreadable certificate arrives after it has already claimed the port.
# The paths are resolved ONLY when TLS is on. With TLS off nothing is defaulted, read,
# stat'ed or opened -- there is no key or certificate in play at all, so materialising a
# path for one would be inventing state that is not used.
KEYFILE=""; CERTFILE=""
TLS_ARGS=()
if [ "${WOA23_TLS:-on}" = "off" ]; then
  # Only an explicit WOA23_TLS=off disables it, and it is never the default. A typo in
  # a certificate path must not silently downgrade a public HTTPS endpoint to HTTP.
  echo "WARNING: TLS explicitly disabled via WOA23_TLS=off" >&2
else
  # TLS ON is the DEFAULT and is unchanged: `${WOA23_TLS:-on}` above means an unset
  # variable means ON. Both paths are still required, still defaulted to production's
  # own, and still checked for readability before the port is claimed.
  KEYFILE="${WOA23_TLS_KEYFILE:-conf/privkey.pem}"
  CERTFILE="${WOA23_TLS_CERTFILE:-conf/fullchain.pem}"
  [ -r "$KEYFILE" ]  || die "TLS key not readable: $KEYFILE" \
    "  Set WOA23_TLS_KEYFILE, or WOA23_TLS=off to serve plain HTTP deliberately."
  [ -r "$CERTFILE" ] || die "TLS certificate not readable: $CERTFILE" \
    "  Set WOA23_TLS_CERTFILE, or WOA23_TLS=off to serve plain HTTP deliberately."
  TLS_ARGS=(--keyfile "$KEYFILE" --certfile "$CERTFILE")
fi

# ------------------------------------------------------------------- the interpreter
# NAMED, not resolved through PATH. `exec gunicorn` runs whichever gunicorn comes first
# for whoever started PM2, and PM2's environment is not a login shell's.
#
# REQUIRED, and no longer defaulted. It used to fall back to the shared
# `.pyenv/versions/py311` environment when unset, and `pm2G` showed what that costs: the
# run built a per-run isolated venv, verified its interpreter and recorded a 58-package
# manifest for it, and then the service it started mapped 1153 and 451 libraries from the
# SHARED env and exactly ZERO from that venv. polars was loaded from the shared env too.
# The venv was real, correct, measured — and served nothing. Worse, the environment
# contract REQUIRED WOA23_PYTHON absent, so the fallback was not an accident anyone could
# have switched off; it was the only reachable behaviour.
#
# A default that silently substitutes a different dependency set for the one that was
# built and manifested is not a convenience. The manifest then describes something that
# is not serving, which makes the evidence worse than absent: it reads as proof.
#
# So: fail closed. The caller names the interpreter, or nothing starts. The staging
# generator sets it to the staged venv's python; a cutover sets it to whatever
# production's runtime is meant to be — deliberately, in writing, either way.
PY="${WOA23_PYTHON:-}"
blank "$PY" && die "WOA23_PYTHON is not set." \
  "  Name the interpreter that has the application's dependencies. There is no default:" \
  "  a fallback to the shared pyenv environment is how a run can build an isolated venv," \
  "  manifest it, and then serve from somewhere else entirely (pm2G, spec 016)."
[ -x "$PY" ] || die "no interpreter at $PY" \
  "  WOA23_PYTHON must name an executable interpreter that has the application's" \
  "  dependencies."

WORKERS="${WOA23_WORKERS:-2}"

echo "woa23 production launcher"
echo "  app              : $APP        (candidate; woa23_app is not served)"
echo "  port             : $PORT"
echo "  WOA23_ZARR_STORE : $STORE"
echo "  anchor           : $ANCHOR"
if [ "${#TLS_ARGS[@]}" -eq 0 ]; then
  echo "  TLS              : OFF (explicitly)"
else
  echo "  TLS              : on   key=$KEYFILE cert=$CERTFILE"
fi
echo "  workers          : $WORKERS"
echo "  interpreter      : $PY ($("$PY" --version 2>&1))"
echo "  reload           : DISABLED (blocker B5)"

# exec: PM2 must track the gunicorn master itself rather than a shell that outlives it.
# This is what makes the grep-based `pre_stop` unnecessary — PM2's own signal reaches
# the master, and the master stops its workers. Verified in pm2B: stop terminated the
# master and both workers with no pre_stop present at all.
#
# `${TLS_ARGS[@]+"${TLS_ARGS[@]}"}` rather than `"${TLS_ARGS[@]}"`: under `set -u`,
# expanding an EMPTY array is an unbound-variable error on bash 3.2, which is still
# what macOS ships. VM24 runs bash 5, where the plain form is fine — so the bug would
# have stayed invisible until the one moment it mattered, the first time someone set
# WOA23_TLS=off. The offline suite caught it; the `+` form is correct on both.
exec "$PY" -m gunicorn "$APP" \
  -w "$WORKERS" \
  -k uvicorn.workers.UvicornWorker \
  -b "127.0.0.1:$PORT" \
  ${TLS_ARGS[@]+"${TLS_ARGS[@]}"} \
  --timeout 120 \
  --graceful-timeout 10
