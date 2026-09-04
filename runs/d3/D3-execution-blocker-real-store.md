# D-3 — BLOCKED at preflight reading: the subject cannot serve the real store

**No VM24 contact was made this turn. Nothing staged, nothing started, no case issued.
`dep3a` and `19161` remain UNCONSUMED.**

> ## STOP. **D-3 as authorised cannot be executed with subject `6ce915e`.**
>
> The authorisation requires *"real production store through the uid-994 read-only symlink
> path"*. **The subject's only authorised execution entry builds a SYNTHETIC store instead,
> and refuses the real one by design.**
>
> Proceeding would require **editing an executable file**, which the standing rule forbids
> without cutting a new subject first.

---

## 1. Why this stopped before VM24

The D-3 request's own rule (§2):

> *"If anything in preparing or reviewing D-3 requires an executable or harness change, work
> STOPS"* — and a new subject with fresh archive, file-list and three batches is cut first.

**I read the execution path before invoking it**, and it does not support the authorised
configuration. Contacting VM24 first would have created a staging tree and then died at an
assertion, consuming `dep3a` for nothing.

---

## 2. Four obstacles, all in the authorised path

`deploy/staging_execute.sh` is, by its own header, **"THE execution entry for a PM2C-mode
staging validation. Nothing else may start one."**

### 2.1 The store path is hardcoded

```
staging_execute.sh:271   STORE="$ROOT/store"
```

**No flag, no environment override.** The store is always inside the staging root. There is
no `--store` argument; `--store` appears only where the executor *passes* this value to the
config generator.

### 2.2 The executor BUILDS a synthetic store, and asserts its size

```
echo "  store     : $STORE       (synthetic; production data is never copied)"
( cd "$TREE" && ./.venv/bin/python deploy/make_staging_store.py "$STORE" ) …
STORE_FILES="$(find "$STORE" -type f | wc -l | tr -d ' ')"
[ "$STORE_FILES" = 72 ] || die "store has $STORE_FILES files, expected 72"
```

**The real production store holds 123 005 files.** The assertion `= 72` would `die`
immediately.

### 2.3 It would attempt to chmod the production store

```
chmod -R a-w "$STORE"
```

If `$STORE` resolved to the real store, **this is an attempted permission change on
production data**. It would fail — uid 994 does not own it — but **the run would attempt a
write to a production path**, which every D-3 authorisation forbids outright.

### 2.4 The refusal is DELIBERATE, and the subject says so

`deploy/make_staging_store.py`, in its own docstring:

> *"**Why not production's store.** A read-only symlink to it would make the staging store
> resolve inside the production tree, which `deploy/start_staging.sh` **refuses by design**.
> The guard is not disabled for convenience; the fixture exists so it does not have to be."*

And `start_staging.sh` carries a variable whose sole purpose is that refusal:

> *"`WOA23_PRODUCTION_STORE` … is REQUIRED — it is what lets this launcher refuse a staging
> store that resolves inside production's. Without it the guard cannot run, and a guard that
> silently does not run is worse than none."*

**This is not an oversight I can route around. It is a designed guard, with its rationale
recorded, against precisely the thing D-3 was authorised to do.**

**One qualification, stated so the finding is not overstated:** §2.4's guard lives in
`start_staging.sh`, and D-3 uses `production_app.sh` via
`ecosystem.production.config.js` — so **that specific guard would not fire on D-3's path**.
**§2.1–2.3 are the blocking ones**, and they are in `staging_execute.sh`, which runs the store
build **before** config generation regardless of which launcher the config names.

---

## 3. What I did NOT do

| | |
|---|---|
| edit `staging_execute.sh` or any executable | **no** — that changes the subject |
| point `$STORE` at the real store | **no** — §2.3 would attempt a production write |
| hand-roll a launch outside the authorised entry | **no** — *"Nothing else may start one"* |
| contact VM24 | **no contact at all this turn** |
| consume `dep3a` / `19161` | **no** — both remain first-use |

---

## 4. The options, none chosen

| | option | what it costs |
|---|---|---|
| **A** | **Cut a new subject** with a real-store mode in `staging_execute.sh` — symlink instead of synthetic build, skip the `=72` assertion and the `chmod`, add store-boundary checks | a code change to the execution entry, a **fresh archive, file-list and three batches**, and it **reopens the design decision** §2.4 deliberately closed. `dep3a`/`19161` survive only if the identity is not named in the new subject |
| **B** | **Run D-3 on the SYNTHETIC store** — what the machinery actually supports | it is then **not** *"using the real production store"*. The central premise changes, and the result must be renamed accordingly |
| **C** | **Split it**: deployment-shape rehearsal on the synthetic store (B), and treat real-store behaviour as the **C1/C2 track's** territory — which already ran the candidate against the real store, though under the **benchmark harness**, not as a deployment | leaves *"the candidate has never run as a deployment against real data"* open — the exact gap D-3 existed to close |
| **D** | **Defer D-3** | the gap stays open |

**My reading, offered as a recommendation and not a decision:** the conflict is not a bug to
patch. The subject's authors deliberately decided a deployment-shaped staging process must not
open the production store, and wrote the reason down. **Overriding that is a policy decision
about whether a deployment-shaped process may open production data read-only** — and it
deserves to be made explicitly, not implemented as a flag in a hurry.

**If option A is chosen**, the store-boundary work already specified for D-3 preflight (ACLs,
ancestors, symlink target, world-writable, escaping symlinks) is exactly what a real-store
mode would need as its guard — so that design work is not wasted.

---

## 5. Status

| | |
|---|---|
| D-3 | **BLOCKED. Not executed.** |
| subject `6ce915e` | **unchanged** — no executable or harness file touched |
| `dep3a` / `19161` | **unconsumed, still first-use** |
| Phase 1 provisioning | **stands** — QUALIFIED PROVISIONING COMPLETE, unaffected |
| VM24 | **not contacted this turn**; no staging root, workdir, `PM2_HOME`, port or daemon exists |
| C1 / C2 | **not re-run** |

**Nothing here is a candidate deployment rehearsal observation, a deployment PASS, production
equivalence, a data-path correctness PASS, TLS validation or A11 validation.** D-3 produced
no observation of any kind, because it did not run.

**Awaiting the PI's decision between A, B, C and D.**
