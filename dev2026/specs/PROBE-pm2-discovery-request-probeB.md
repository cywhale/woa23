# PM2 discovery probe `probeB` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. Nothing has been run.**
No VM24 contact.

**This is discovery, not validation.** It closes no blocker. Finding a usable `pm2`
would **not** mean B3 or B5 has passed — the launcher argv that PM2 actually starts is
still unverified and remains `b35a1`'s job.

---

## 1. The question, and the four answers it must tell apart

`probeA` found `pm2` is not on the account's default PATH. **That is not the same as the
host having no pm2**, and the difference decides what happens next:

| | outcome | what it would mean |
|---|---|---|
| **A** | `A_ABSENT` | no `pm2` under any root the account can traverse |
| **B** | `B_NOT_ON_PATH` | exists, uid 994 can execute it, PATH omits it → `b35a1` can use an absolute path or a staging-only PATH |
| **C** | `C_NOT_ACCESSIBLE` | exists, but uid 994 cannot traverse to it, read it, or execute it |
| **D** | `D_USABLE_FOR_STAGING` | readable, executable, outside production state — **and no startup attempted** |

Collapsing any two of these hands back a wrong decision, so each is a distinct
classification with its own test.

**Outcome A is reported with a warning attached**: roots that were **not traversable**
were **not searched**, and "absent from what we could see" is not "absent from the host".
Reporting the first as the second is exactly how A gets returned when the truth is C.

---

## 2. What it will report

### Per candidate

absolute path · realpath · symlink target · owner/mode/size · **sha256** ·
readable by uid 994 · executable by uid 994 · **writable — the binary, its parent
directory (could it be replaced?), and the package** · package directory and its
`package.json` `name` and **VERSION** · whether it lies under production `PM2_HOME` or
other production-owned state · whether its directory is on this account's PATH ·
its individual classification.

### Overall

`node` absolute path, realpath, owner/mode/size, **version**, readability/executability
and prefix · the **full PATH, every entry including the last**, each marked `dir`/`ABSENT`
and whether a `pm2` sits in it · every search root with its depth and **whether it was
traversable** · `PM2_HOME` in the environment, what it would default to, and whether that
exists · each production state path's existence and read/write status for uid 994.

---

## 3. How it stays read-only

**`pm2` is never executed** — not the binary, not `--version`, not `-v`, not `jlist`, not
any subcommand. A pm2 invocation can connect to or spawn the daemon and create
`$PM2_HOME` as a side effect. **Versions come from `package.json` read as text.**
`node --version` **is** run: node is not a daemon.

**Search is bounded.** Every `find` has an explicit `-maxdepth` and `-xdev`; none is
rooted at `/`; an untraversable root is reported and **skipped**, never silently turned
into "no hits".

Search roots (depth): `/usr/local/bin`(1) `/usr/bin`(1) `/bin`(1) `/sbin`(1)
`/usr/local/sbin`(1) `/snap/bin`(1) · `/usr/local/lib/node_modules`(3)
`/usr/lib/node_modules`(3) `/lib/node_modules`(3) · `/opt`(4) `/usr/local/n`(5) ·
`$HOME/.local`(4) `$HOME/.npm-global`(4) `$HOME/node_modules`(3) `$HOME/.nvm`(5) ·
`/home/odbadmin/.nvm`(5) `/home/odbadmin/.npm-global`(4) `/home/odbadmin/.local`(4)
`/home/odbadmin/node_modules`(3) `/home/odbadmin/.config/yarn`(5) · plus node's own
`<prefix>/lib/node_modules`(3).

**No code path exists** for: creating or modifying `PM2_HOME`; `start`/`stop`/`delete`/
`kill`/`save`/`resurrect`; creating a staging tree, workdir, store or **any** temp file;
binding a port; `sudo`/`su`/`setpriv`; modifying `PATH`, ACLs, permissions, production
PM2 or production files. Every filesystem call is `find`/`stat`/`readlink`/`test` or a
read of a regular file.

**Delivery: `ssh … 'bash -s' < scripts/probe_pm2_discovery.sh`** — stdin only.
**Nothing is written to VM24**, not even under the account's own home.

---

## 4. Offline verification already done

**`scripts/test_probe_pm2_discovery.sh` — 71 assertions, passing.**

- `path_is_under` tested for **true path prefixes, not string prefixes** — `/a/bc` is
  **not** under `/a/b`, and `/home/odbadmin/.pm2backup` is **not** production state.
- All four classifications asserted **distinct**, including that a binary sitting *on*
  the PATH but not executable is still **C**, not **D**.
- Prohibitions as **source properties** — a probe that wrongly spawned a daemon would
  already have spawned it.
- Bounded-search properties: every `find` has `-maxdepth`, the candidate search uses
  `-xdev`, none is rooted at `/`, and the untraversable branch `continue`s.

**Four of my own assertions were wrong and were rewritten to test intent rather than
bumped to match the code** — three counted occurrences where the count was incidental,
and one looked for `continue` within one line of a message whose `printf` wraps over
three.

**Rehearsed locally over stdin:** exit 0, `HOME` unchanged at 100 entries, no `~/.pm2`,
no daemon, classification `A_ABSENT` — correct for a machine with no pm2. That rehearsal
is a smoke test only: macOS has no `stat -c`, which worked on odb24 during `probeA`.

**The PATH-listing defect probeA shipped with is fixed** and covered by a regression test
that reproduces its exact shape.

---

## 5. Execution subject

```
commit           0c0d18deb476e718df7ee44330b9a3d4cd9df05a
subject line     probe: read-only PM2 discovery probe — four outcomes, bounded
                 search, no pm2 run
archive sha256   10d2f95ae3db24502f20cc3a73942cc2b315fef1903052e1b2dbd9bc0374db01
files            210
file-list sha256 01f887bd3593d9ba9d5c68553264ed85f561753279eb8813924fd82f6bb3aa89
```

`verify_clean_archive.sh` — **all passed (16 assertions)**.

**Offline evidence:** three serial batches at `0c0d18d`, each recording HEAD itself —
all attest `head=0c0d18d… dirty=0`. **49 suites, 4091 assertions, 0 non-zero exits,
0 differences** across all three pairings. Roots `fSpO2S`, `IC7DtZ`, `iw9mU3`.
`test_probe_pm2_discovery.sh` 71, `test_probe_host_capability.sh` 58.

`api/query.py` unchanged at `50907dee…2ca8`. **This request document is a later commit
and is not part of the subject.**

---

## 6. Execution identity

| | value |
|---|---|
| **label** | **`probeB`** |
| **grant** | *(none — the probe creates nothing and has no grant guard)* |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993** |
| **connection** | direct SSH, campaign Ed25519 key, `BatchMode=yes`, `IdentitiesOnly=yes`, `RequestTTY=no` |
| **delivery** | `bash -s` over **stdin** — no file written to VM24 |
| **ports** | **none** |
| **staging tree / workdir / store / PM2_HOME** | **none created** |

**No identity, port or staging resource is consumed.** `b35a1` remains unaffected and
unauthorised.

---

## 7. Production-impact statement

**Intended impact: NONE.**

| | |
|---|---|
| production files | read-only, and only `stat`/`readlink` on paths that may contain a `pm2`; nothing written |
| production PM2 | **not touched.** `/home/odbadmin/.pm2` is checked for existence and read/write status and **never entered as a search root, never set as `PM2_HOME`** |
| production service | **not contacted.** Zero API requests |
| pm2G / 18265 | **untouched.** This probe checks no listeners at all — it contains no `ss` |
| ports | none bound |
| ACLs, permissions, `.lock`, store | **not touched** |
| PM2 daemon | **none started**, by construction |
| files on VM24 | **none created**, including temp files |

Before/after evidence — HOME entry count and metadata digest, `~/.pm2` existence, PM2
daemon count, process count, listener count, `/tmp` entries, production pids/starttimes,
pm2G pids and boot id — will be captured **around** the run exactly as `probeA`'s was,
and any digest movement will be **explained rather than smoothed over**, as the
`wireplumber` / `snapd-desktop-integration` movement was.

---

## 8. Failure handling

The probe runs every section and reports; it does not stop at the first negative, so one
run gives the whole picture.

**A "not found" is a result, not an error to retry.** If the outcome is `A_ABSENT` or
`C_NOT_ACCESSIBLE`, I will **not** retry, will **not** widen the search on my own
initiative, and will **not** modify the host environment. I will report and put a
host-level installation proposal to you as a separate request.

---

## 9. What happens after — and what does not

**If a suitable `pm2` is found**, I will propose — **offline, for your review** — how
`b35a1` should use it:

- as an **absolute path** via `WOA23_PM2_BIN`, which `staging_execute.sh:386` already
  supports (`PM2="${WOA23_PM2_BIN:-pm2}"`), or
- as a **staging-only `PATH` prefix** supplied on the run's own command line, exactly as
  C1/C2 did for `uv`.

**I will not modify the host environment to make either work.**

**If none is found**, I will propose a host-level installation as a separate request, for
you to decide.

**Either way:** finding `pm2` visible or usable is **not** a B3/B5 pass. The actual PM2
startup and the launcher argv it produces remain **unverified** and are `b35a1`'s job,
under your separate authorisation.

---

## 10. Submission

`probeB` is submitted for **explicit authorisation**. It has not been executed and no
VM24 contact has been made.
