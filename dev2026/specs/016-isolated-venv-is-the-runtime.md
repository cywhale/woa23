# 016 — The isolated venv must BE the runtime, and it must fail closed

**Status: SPEC + implemented. Deployment/runtime behaviour change. No VM24 run has
exercised it; `pm2G` is what motivated it and `pm2G` remains NOT A PASS.**

---

## 1. What `pm2G` found

`pm2G` built a per-run isolated venv, verified its interpreter, and recorded a manifest
for it. Then it served from somewhere else.

From `PM2-staging-result-pm2G.md` §4.4:

| worker | total mapped paths | from the pm2G venv | from the **shared py311** env | from production |
|---|---|---|---|---|
| 1456373 | 1565 | **0** | **1153** | 0 |
| 1456374 | 785 | **0** | **451** | 0 |

The only `site-packages` root in use:
`/home/odbadmin/.pyenv/versions/3.11.4/envs/py311/lib/python3.11/site-packages`.
polars was memory-mapped from there too.

**Nothing in the run contradicted anything.** The venv was real (Python 3.11.4, 58
distributions, `manifest_sha256 3835c975…`, polars mainline 1.27.1, recorder stderr 0
bytes). Every environment check passed. The run simply never asked *which libraries were
loaded*, and so reported a carefully measured venv that served nothing.

## 2. The cause, and why it was not reachable by accident

`deploy/production_app.sh` selected the interpreter like this:

```sh
PY="${WOA23_PYTHON:-/home/odbadmin/.pyenv/versions/py311/bin/python3.11}"
```

and the staging environment contract **required `WOA23_PYTHON` ABSENT** — row 8 of the
ten-variable check. So:

- absent was not neutral; **absent selected the shared environment**;
- the fallback was not a switch someone forgot to flip. **It was the only reachable
  behaviour**, because the contract forbade the variable that would have changed it;
- the three claims in the `pm2G` request — "the shared py311 is not the runtime" (§5),
  "maps show the venv and none from py311" (§7 step 10), and "`WOA23_PYTHON` ABSENT"
  (§6.3 row 8) — **could not all be true at once**. Rows 8 and step 10 were mutually
  exclusive and nobody noticed until a run measured it.

**A default that silently substitutes a different dependency set for the one that was
built and manifested is not a convenience.** It makes the manifest worse than missing:
absent evidence reads as absent, but a manifest for the wrong environment reads as proof.

## 3. The decision — fail closed, and name the interpreter

**`WOA23_PYTHON` becomes a required, allowed-present runtime variable. There is no
default. The launcher refuses to start without it.**

| before | after |
|---|---|
| `WOA23_PYTHON` required **ABSENT** | `WOA23_PYTHON` required **present, exact value** |
| unset ⇒ shared `py311` | unset ⇒ **the launcher dies** |
| ten checks: 6 exact, 4 absent | ten checks: **7 exact, 3 absent** |
| five permitted config overrides | **six** permitted config overrides |

The ten-variable allowlist is otherwise unchanged, and `WOA23_PYTHON` was already a
member — only its expected form changes, from `ABSENT` to an exact path.

**`WOA23_PM2C_GRANTED` is unaffected and remains entry-only**: consumed by `unset`
before `pm2 start`, and still required ABSENT from the child. So does
`WOA23_PM2_BIN` — also unset, also never in the child, and deliberately still *not* in
the allowlist so a leak is an `INVALID_ENVIRONMENT`.

## 4. Implementation

### 4.1 `deploy/production_app.sh` — no default

```sh
PY="${WOA23_PYTHON:-}"
blank "$PY" && die "WOA23_PYTHON is not set." …
[ -x "$PY" ] || die "no interpreter at $PY" …
```

Consistent with the launcher's existing fail-closed posture for `WOA23_PORT` and
`WOA23_ZARR_STORE`: every required value is refused by name when missing, and nothing is
defaulted into place. The interpreter was the one exception, and §1 is the bill for it.

**This is a cutover-visible change.** Production's own config must set `WOA23_PYTHON`
before this launcher can run there — that is cutover work, tracked with B1–B5, and it is
not done by this spec.

### 4.2 `deploy/make_staging_override.js` — a sixth permitted override

`--python <abs>` is now required, and `env.WOA23_PYTHON` is the sixth permitted
difference. The generator refuses a value that is:

- not absolute, or not in normal form;
- non-existent, or not executable;
- **inside `/.pyenv/versions/py311/`** — the shared environment, by name. Naming the very
  thing the change exists to stop being used is refused explicitly rather than left to
  the reader.

A **seventh** difference is still a stop.

### 4.3 `deploy/staging_execute.sh` — exact value, then proof

- passes `--python "$VENV"` to the generator, where `$VENV` is the staged tree's own
  `.venv/bin/python`;
- `env_must WOA23_PYTHON "$VENV"` — read back from `/proc/<pid>/environ`, exact;
- **and then proves the venv is what actually serves**, which is the part `pm2G` shows
  cannot be inferred from the environment alone:

| check | why |
|---|---|
| `argv[0] == $VENV` | the interpreter PM2 was told to run, by exact path |
| `/proc/<pid>/exe` **recorded, not asserted equal** | it resolves *through* the venv symlink to the base pyenv binary, so it legitimately differs from `$VENV`; asserting equality would fail every correct run |
| worker maps: `> 0` from `$TREE/.venv/lib` | the venv is loaded |
| worker maps: `== 0` from `/.pyenv/versions/*/envs/py311/` | **the pm2G condition, stated as a refusal** |
| worker maps: `== 0` from `/python/woa23/` | production's tree is not loaded |
| at least one worker's maps readable | otherwise provenance is *unverified*, and unverified is not a pass |

The last row matters: reporting a venv as the runtime on the strength of argv alone is
how `pm2G` produced a confident wrong answer.

## 5. Tests

`scripts/test_staging_entry.sh` and `scripts/test_production_launcher.sh` gain
fail-closed cases for each drift:

| case | required outcome |
|---|---|
| `WOA23_PYTHON` absent from the launcher's environment | launcher **dies**, names the variable |
| `WOA23_PYTHON` empty / whitespace | launcher **dies** |
| `WOA23_PYTHON` naming a non-executable or missing path | launcher **dies** |
| generator invoked without `--python` | **refuses** |
| generator given a relative, non-normal, missing or non-executable path | **refuses** |
| generator given the **shared py311** interpreter | **refuses, by name** |
| generated config omits `env.WOA23_PYTHON` | diff check **refuses** (missing override) |
| a **seventh** difference appears | **refuses** |
| entry's ten-row table | asserts `WOA23_PYTHON` is checked as an **exact value**, not ABSENT |
| entry still `unset`s `WOA23_PM2C_GRANTED` and `WOA23_PM2_BIN` | unchanged, still required ABSENT from the child |

## 6. What this does NOT do

- **Does not close B7.** B7 closes when a *deployment* uses an isolated venv. This makes
  that possible and proves it in staging; it is not a deployment.
- **Does not close B1–B5.**
- **Does not re-classify `pm2G`.** It remains NOT A PASS; its identity and port `18265`
  remain consumed; its state remains retained on VM24.
- **Does not touch `api/`.** The column-order work is [spec 015](015-deterministic-column-order.md)
  and a separate commit.
- **Does not change production.** Production's config does not set `WOA23_PYTHON` today,
  so this launcher would refuse there — deliberately. Setting it is cutover work.

## 7. Consequence: the next staging run

A future staging run needs an entirely new execution identity — `pm2G`'s is consumed and
`18265` is spent. **No such run is proposed here**, and none may be proposed until the PI
has reviewed this spec, the `api/` diff (spec 015) and the re-run C1/C2 evidence.
