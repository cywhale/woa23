# 021 — The subject boundary: what the archive contains, and what it does not

**Status: OFFLINE PROVENANCE CLARIFICATION. No VM24 contact. No code, config or subject
changed by this document.**

Subject `6ce915e72c1be4e1c76e2d1c88e115c2baee1649`.

---

## 1. Is `conf/ecosystem.config.js` in the subject? — **in the COMMIT, NOT in the ARCHIVE**

These are two different things and the distinction is the whole point of this document.

### 1.1 In the commit — YES

```
$ git ls-tree -r --name-only 6ce915e -- conf/
conf/ecosystem.config.js
conf/simu.sh
conf/start_app.sh

$ git ls-tree 6ce915e -- conf/ecosystem.config.js
100644 blob 6e6ddb90b41321949b403bedfdb6ab5fdd7b82eb   conf/ecosystem.config.js

content sha256   ed5dec6ca064bd54cfe4a45f33fd416871859e959671164292b21549b96f2159
VM24 production  ed5dec6ca064bd54cfe4a45f33fd416871859e959671164292b21549b96f2159   ← identical
worktree         ed5dec6ca064bd54cfe4a45f33fd416871859e959671164292b21549b96f2159   ← identical
```

`git show --stat` for the reconciliation commit:

```
08734a5 chore: reconcile repo source to production after Stage B; kill-pattern moves to a fixture
 conf/ecosystem.config.js | 1 -
 1 file changed, 1 deletion(-)
```

### 1.2 In the archive — **NO**

```
$ git archive --format=tar 6ce915e dev2026 | tar -t | grep -c '^conf/'
0
conf/ rows in the 240-file archive listing: 0
```

> **`conf/ecosystem.config.js` is an EXTERNAL INPUT to the subject archive.** It must not be
> described as archive content, and its digest is **not** covered by the archive SHA-256
> `5dc056a4…472e` or the file-list SHA-256 `7821387…b1c6`.

Its provenance is the **commit**, not the archive: blob `6e6ddb90…82eb`, content
`ed5dec6c…2159`.

---

## 2. Why the archive is `dev2026/` only

The archive is built as `git archive --format=tar <sha> dev2026`, and that scope is
deliberate: it is what gets **shipped to VM24 and extracted** for a staging run. `dev2026/`
is the new work — the harness, the deploy scripts, the specs. `conf/`, `api/` and the rest
of the repository are **production's existing tree**, which a staging run must never carry
with it. That is exactly the isolation `bs3v1`, `b35a1` and `b1s1` all depended on.

**The cost of that scope is this document.** An archive that excludes production's config
cannot, by itself, check anything about production's config.

---

## 3. Which suites read the working-tree `conf/`

| suite | reads | references |
|---|---|---|
| `test_production_stop.sh` | `../conf/ecosystem.config.js`, `../conf/simu.sh` | 8 |
| `test_production_launcher.sh` | `../conf/ecosystem.config.js`, `../conf/start_app.sh` | 10 |
| `test_staging_launcher.sh` | `../conf/ecosystem.config.js` | 3 |
| `test_probe_pm2_discovery.sh` | production paths in prose/fixtures | — |

`$HERE` is `dev2026`, so `"$HERE/../conf/…"` resolves **outside the archive root** in every
case.

---

## 4. Can the extracted archive reproduce those assertions? — **NO, demonstrated**

Not argued. The archive was extracted to a directory with **no `conf/` beside it** (240
files, `conf/` absent) and the suites were run there:

```
test_production_stop.sh      5 FAILED, 61 passed
  FAIL the production config is present to be checked at all
  FAIL   it is non-empty
  FAIL   and it still defines the woa23 app (so we are reading the right file)
  FAIL conf/simu.sh is present to be checked
  FAIL conf/simu.sh STILL documents killing ANOTHER project by grep (separate issue)
test_staging_launcher.sh     1 FAILED of 102
  FAIL production's config is present to be checked
test_production_launcher.sh  14 failures
```

**These failures are correct behaviour**, and they exist because of the fix in `6ce915e`.
Before that fix the "config is clean" assertions expected `"no"`, which a **missing file
also produces** — so on a bare archive they would have **passed while reading nothing**.
Now absence fails loudly. The bare-archive run is the proof that the guard works.

### 4.1 Classification — binding

> **Every assertion that reads `../conf/` is EXTERNAL-SOURCE / WORKTREE EVIDENCE.**
> It is **not** archive-self-contained evidence and must never be reported as such.

| claim | status |
|---|---|
| "the offline suites pass at `6ce915e`" | **TRUE of the working tree**, where `conf/` is present |
| "the extracted subject archive passes all suites standalone" | **FALSE.** 20 assertions across three suites require `conf/` |
| "the archive digest covers the production config" | **FALSE.** `conf/` is not in the archive |

The three batches (53 suites, 4382 assertions, 0 non-zero) were run **in the working tree**.
That is the honest scope of that number.

**This does not affect `b1s1`.** That run extracted the archive on VM24 and executed the
*driver*, not the offline suites; no `../conf/` read was involved.

---

## 5. The reconciliation diff — confirmed complete and bounded

| # | change | status |
|---|---|---|
| 1 | `conf/ecosystem.config.js`: **one line removed**, the dead `pre_stop` | ✔ `8db9a6ba…4340` → `ed5dec6c…2159` |
| 2 | four official-config assertions **flipped** to require it clean | ✔ 3 suites, none deleted |
| 3 | kill-pattern coverage moved to an **independent fixture** | ✔ `scripts/fixtures/legacy-pre-stop.fixture.js` (`4d73d0e3…25e9`), load-bearing |
| 4 | `conf/simu.sh` **unmodified** | ✔ `3bc46819…eef9`, byte-identical to Stage A's host reading |

Nothing else: no `api/`, no query logic, no response behaviour, no runtime dependency.

---

## 6. `conf/simu.sh` — separate risk, separate request

**Not modified, and not to be folded into B1.**

It carries the same kill-by-grep technique, including a line that greps **`tide_app`** —
another project — directly. It differs from the config's `pre_stop` in a way that matters:

| | `pre_stop` in the config | `simu.sh` |
|---|---|---|
| would it ever run? | **No.** PM2 5.4.2 has no such hook (Stage A: 0 in source, absent from `schema.json`'s 65 keys) — inert | **Yes.** It is a shell script; if invoked, it runs |

So the thing already fixed was the harmless one, and the live one is still there. It needs
**its own request and its own risk assessment**, and `test_production_stop.sh` still asserts
its presence so it cannot quietly disappear.
