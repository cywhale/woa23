# PM2 discovery re-run `probeC` — execution request, and the `b35a1` usage plan

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. Nothing has been run.**
No VM24 contact since `probeB`.

**`probeB`'s classification stands: `B_NOT_ON_PATH`.** It is not reclassified to
`D_USABLE_FOR_STAGING`. `probeC` re-confirms the same facts with the two evidence
defects fixed; it does not change the verdict, and **it closes no blocker**.

---

## 1. The two evidence defects, fixed offline

### 1.1 A directory was reported as an executable candidate

`find -name pm2` matches **directories**, and on a directory the `x` bit means
**traversable, not runnable** — so `probeB` printed `executable: yes` for the pm2
*package directory*.

**Fixed:** a hit is an `EXECUTABLE CANDIDATE` **only if, after resolving symlinks, it is
a REGULAR FILE that is executable**. `candidate_kind` returns `DIRECTORY`,
`NOT_A_REGULAR_FILE`, `NOT_EXECUTABLE` or `EXECUTABLE`. A directory hit is reported as a
directory, with its `x` bit explained, and is **never offered as something `b35a1` could
run**.

### 1.2 The package path was a guess, and produced a misleading missing-file line

It was `dirname(dirname(realpath))` — right for `<pkg>/bin/pm2`, **wrong** for
`<pkg>/pm2`. It yielded `.../lib` and `.../node_modules` and then reported
*"package.json NOT readable"* about them, which reads like a finding about the host and
was an artefact of my derivation.

**Fixed:** `package_search_dirs` walks ancestors and takes the **nearest one that
actually has a readable `package.json`**, and the package is derived **only after** the
hit is confirmed executable. A directory hit therefore emits **no `package.json` line at
all** — misleading or otherwise.

### 1.3 Regression tests for both — end to end, not by inspection

A test seam (`WOA23_PM2DISC_ROOTS`) points the search at a fixture tree containing
exactly the shapes that caused the confusion: a real `<pkg>/bin/pm2` with its
`package.json` two levels up, the package **directory**, a second executable at
`<pkg>/pm2` **one level in**, and a **mode-644** regular file.

Asserted: the directory is reported non-executable with **no derived `package.json` or
`VERSION` field**; the 644 file is `NOT_EXECUTABLE` with no version; and **both real
executables resolve to the same `package.json`** — which the old derivation could not do.

**`test_probe_pm2_discovery.sh` 71 → 101 assertions.**

**Four of my own assertions were wrong again** and were corrected rather than loosened:
one expected a single non-executable hit when the fixture legitimately has two; one
grepped for `package.json` and matched the probe's own explanatory note about *not*
emitting one; and two source-property checks went stale because the improvements they
describe changed the code.

### 1.4 Search completeness — now reported for EVERY outcome

Untraversable roots are accumulated and listed in their own **`5b. SEARCH COMPLETENESS`**
section with owner and mode, for **every** outcome rather than only `A_ABSENT`, and the
report states that `A_ABSENT` would mean *"not found in what could be searched"*, which
is **not** *"absent from the host"*.

**The search range is NOT widened.** `/home/odbadmin/.local` (mode 700, `odbadmin`)
stays unsearched and is recorded as unsearched.

---

## 2. What `probeC` will re-confirm

Exactly the list you asked for:

1. `/home/odbadmin/.npm-global/bin/pm2` — absolute path, realpath, owner/mode
2. read/write/execute for uid 994 on the **binary**, its **parent directory**, and the
   **package directory**
3. `package.json` **name / version**
4. **SHA-256**
5. whether uid 994 can **only read and execute, and not modify**
6. the **full PATH**, every entry including the last
7. that `PM2_HOME` does **not** fall to production
8. that **`/home/odbadmin/.local` remains an unsearched area**
9. before/after: **no daemon, no file, no listener, no production state change**

---

## 3. Execution subject

```
commit           88364c77d8d684fd980a7fbb65b15918cf722485
subject line     probe: fix probeB's two evidence defects — directory hits and
                 package derivation
archive sha256   552662885e0d51ded24e39a859e76a871b13277d3546ec3462dedb309c3287bd
files            212
file-list sha256 a31b9a443a7f15051950bce17840833857a9a515505a4ca078c2f7e0336baa58
```

`verify_clean_archive.sh` — **all passed (16 assertions)**.

**Offline evidence:** three serial batches at `88364c7`, each recording HEAD itself —
all attest `head=88364c7… dirty=0`. **49 suites, 4121 assertions, 0 non-zero exits,
0 differences** across all three pairings. Roots `Il1620`, `1E5aVg`, `w1MWSW`.
`test_probe_pm2_discovery.sh` **101**, `test_probe_host_capability.sh` 58.

`api/query.py` unchanged at `50907dee…2ca8`.

---

## 4. Execution identity and constraints

| | value |
|---|---|
| **label** | **`probeC`** |
| **run-as account** | `woa23c1ro`, uid 994, gid 993 |
| **connection** | direct SSH, campaign Ed25519 key, `BatchMode=yes`, `IdentitiesOnly=yes`, `RequestTTY=no` |
| **delivery** | `ssh … 'bash -s' < scripts/probe_pm2_discovery.sh` — **stdin only** |
| **written to VM24** | **nothing** — no script, no temp file, no directory |
| **ports / PM2_HOME / tree / store** | **none created** |

**pm2 is never executed** — not the binary, not `--version`, not `-v`, not `jlist`, not
any subcommand. No daemon. No `start`/`stop`/`delete`/`kill`/`save`/`resurrect`. No
privilege escalation. No modification of PATH, ACLs, permissions, production PM2 or
production files. **No identity, port or staging resource is consumed.**

---

## 5. If `probeC` confirms the pm2 is usable — the `b35a1` plan

**Offline planning only. This is not a request to run `b35a1`.**

| constraint | how `b35a1` satisfies it |
|---|---|
| **explicit absolute path** | `WOA23_PM2_BIN=/home/odbadmin/.npm-global/bin/pm2`, which `staging_execute.sh:386` already honours (`PM2="${WOA23_PM2_BIN:-pm2}"`) |
| **do not modify host PATH** | nothing is exported to the host; the value is supplied on the run's own command line, as C1/C2 did for `uv` |
| **uid 994's own staging PM2_HOME** | `/home/woa23c1ro/woa23-b35a1-pm2/` — new, owned by uid 994, asserted absent in pre-flight |
| **no production app name or PM2_HOME** | app `woa23-b35a1-candidate`; `PM2_HOME` never `/home/odbadmin/.pm2`, and the wrapper refuses it |
| **no `pm2 * all`, `kill`, global `save`, `resurrect`** | `production_stop.sh` refuses `all` explicitly; the run uses only a named-app start and stop |
| **re-confirm 18281 and the staging identity** | full pre-flight: port live-unbound **and** absent from the subject's ledger, all identity paths absent, subject hashes verified |
| **record the PM2 version asymmetry** | the result will state **staging used PM2 5.4.2** and **production's PM2 version is UNKNOWN** |
| **report B3 and B5 separately** | two findings, never merged, and **neither described as closing its production blocker** |

### 5.1 The version asymmetry, stated now rather than at the end

Staging would run **PM2 5.4.2** (`/home/odbadmin/.npm-global`). **Production's running
PM2 version is unknown**, and determining it would require executing pm2 or reading its
daemon state — both forbidden. **If they differ, the staging run exercises a different
PM2 than production uses**, and that limitation travels with any B3/B5 finding.

### 5.2 The shared-dependency risk

The pm2 lives under **`odbadmin`'s home**. Using it couples the validation account to a
path another account owns and can change, and to `/home/odbadmin` staying traversable.
uid 994 **cannot modify it** — which is the property that makes it trustworthy to *use* —
but it is not an isolated dependency, and that is the opposite of what the non-owner
discipline was built for. Worth your explicit acceptance rather than my assumption.

---

## 6. What `b35a1` will and will not establish

**Will (staging-only):** that the launcher's argv, as PM2 actually starts it, contains
**no `--reload`** (B5) and binds the port from `WOA23_PORT` with **no `8050` literal**
(B3).

**Will NOT:** close B3 or B5 on production. The defect is live in
`conf/start_app.sh` (`4aaed5b7…`), which `b35a1` never touches. A file that is not
installed cannot close a blocker about the file that is.

---

## 7. Submission

`probeC` is submitted for **explicit authorisation**. It has not been executed and no
VM24 contact has been made. **`b35a1` is not requested here** and will not run until
you review this result and authorise it separately.
