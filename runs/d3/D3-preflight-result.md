# D-3 execution-time preflight — **run 2, corrected: ALL PASS, no mismatch**

**VM24 (`odb24`) as `woa23c1ro`, uid 994. Read-only throughout. `pm2 -v` never invoked.**

> ## STATUS: preflight COMPLETE, no mismatch. **Nothing staged, no `pm2 start`, no case issued.**
>
> Halting here by instruction: staging may not begin until this result is reviewed.

**Nothing was created, started, stopped, killed or modified.** Production `PM2_HOME`, port
8050 and lifecycle observed read-only. All retained daemons untouched.

---

## 1. Store fingerprint — the cross-stage question, ANSWERED

**Three digests exist. They are three different metrics, and the difference is entirely
method — not drift.** Rather than assert that, I reconstructed each method and re-ran it on
today's store.

### 1.1 The three methods differ in three respects

| | **[A]** Stage C / D-1 | **[D]** C1/C2 track | **[E]** my run-1 pass |
|---|---|---|---|
| path format | **absolute** | **relative** (`./…`) | **absolute** |
| entry set | **files only** (`-type f`) — 123 005 | **all entries** — 123 205 (incl. 200 dirs) | **all entries** — 123 205 |
| separator | **TAB** | **TAB** | **SPACE** |
| fields | `%p %s %T@` — path, size, mtime | same | same |
| sort | `LC_ALL=C sort` | `LC_ALL=C sort` | `LC_ALL=C sort` |
| hash | sha256 of the sorted list | same | same |

**Same three metadata fields and the same sort discipline; different path format, different
entry set, different separator.** Any two therefore differ *by construction*, whether or not
the store changed.

### 1.2 What each reconstruction does and does NOT establish

| method | recorded | reproduced today | what that supports |
|---|---|---|---|
| **[A]** | `abe6c212…1806` (Stage C §5.4, D-1) — **truncated; the full value is recorded nowhere** | `abe6c21221b5081eb352a1a549c9d1fd6399c74b1f06b8c2ce09952828c61806` | agreement on **12 of 64 characters** — a **partial check only** |
| **[D]** | `1c89be47344074c87c0d45e1de3e7c6eeca769f74924a3a2fa1c7219205b208f` (C1 `c1m`) — full | identical, **64 of 64** | the **C1/C2 metric** is unchanged since `c1m` |
| **[E]** | `2df3fb98…ca6b` (run 1) | `2df3fb986b2d179e2f25f7a7822d829b14f18c3a22af6adf22353e9c566fca6b` | this run's own value is stable |

> **NOT CLAIMED: that store metadata is proven unchanged since Stage C / D-1.**
>
> **[A]'s check is partial** — 12 of 64 characters against a value that exists only in
> truncated form. **[D]'s full match does not complete it.** [D] is a **different metric** —
> a different track (C1/C2), a different entry set (all 123 205 entries vs [A]'s 123 005
> files), and relative rather than absolute paths. **A different metric verifying itself
> cannot certify [A].**

**An earlier draft of this result said exactly that** — that [D] "corroborates" [A] as "the
same store, verified completely". **That was an invalid inference and is withdrawn.** What
[D] establishes is confined to [D]'s own metric.

File count (123 005), entry count (123 205) and byte total (35 101 630 061) match the values
Stage C recorded; those are **separate, coarser observations**, not a fingerprint match.

### 1.3 The rule that follows

| | |
|---|---|
| **forbidden** | comparing digests **across** methods. A difference so produced is a method artefact, and reporting it as drift would be a fabricated finding — the error Stage C §5.4a already corrected once, and the same shape as my file-list mistake |
| **D-3's use of [A]** | **within-run before/after comparison ONLY.** [A] is the method D-3 fixes for its own before/after; **its agreement with Stage C / D-1 is a partial check and is not carried as a cross-stage conclusion** |
| **also recorded** | **[D]** and **[E]**, each labelled with its method, as observations only |
| **still not claimed** | **content integrity.** All three are **metadata-only**; a same-size, same-mtime content change is invisible to every one of them, within-run comparison included |

---

## 2. Observer-pipeline exclusion — precise, auditable, and it hides nothing

**Exclusion is by PID and PGID identity only — never by command pattern**, exactly as
required. The excluded set is printed in full every time.

```
observing shell pid : 1870643        its process group : 1870643
its sshd parent     : 1870642

EXCLUDED  1870642 1870552 1870552  sshd: woa23c1ro@notty
EXCLUDED  1870643 1870642 1870643  bash -s
EXCLUDED  1870647 1870643 1870643  bash -s
EXCLUDED  1870648 1870647 1870643  ps -o pid=,ppid=,pgid=,args= -u woa23c1ro
```

**Four entries, every one demonstrably the observer.** The safety assertion — *no excluded
entry may match `God Daemon|gunicorn|PM2|api.app|uvicorn`* — returns **0**.

**The proof that it is not over-broad is positive, not rhetorical: both retained PM2 God
Daemons remain in the KEPT set** (`1709473` bs3v1, `1761143` b1s1). An exclusion wide enough
to hide a daemon would have hidden those.

### 2.1 A false REFUSAL, and it was the detector's fault

The first attempt reported **1** daemon-signature match among the excluded and refused. **The
match was the `grep` process itself:** its own command line contained the pattern string
`God Daemon|gunicorn|PM2|…`, and `ps` captured it.

**Fixed by taking the process snapshot into a variable first**, so no matcher exists while
`ps` runs. The assertion then returns 0 correctly.

**This is the third self-observation artefact in this work** — after the `DONE_ALL_THREE`
watcher and the `ps`/`awk` inventory rows. It is precisely the failure the review's "must not
be over-broad" instruction was guarding against, arriving from the opposite direction: not an
exclusion hiding a survivor, but a detector indicting itself.

### 2.2 Self-agreeing retake — PASSES

Three back-to-back inventories, taken before any staging:

```
take 1 / 2 / 3 : 12 rows each
1 vs 2 : IDENTICAL     1 vs 3 : IDENTICAL     2 vs 3 : IDENTICAL
sha256 : ecb0aeba3138fb9bfe03e073707683bdea078ae68ef369cb51154db6e55c6489   (all three)
God Daemon rows kept  : 2
```

**The method does not measure the observer.** The 12 kept rows are 10 desktop-session
processes under `systemd --user` (`1708514`) plus the two retained daemons.

---

## 3. Resolved PM2 — matches the confirmed reference exactly

| | |
|---|---|
| path | `/home/odbadmin/.npm-global/bin/pm2` |
| symlink | → `../lib/node_modules/pm2/bin/pm2` |
| **resolved** | `/home/odbadmin/.npm-global/lib/node_modules/pm2/bin/pm2` |
| owner / mode / **size** | `odbadmin:odbadmin` `775` **56 bytes** (a shim that `require`s `../lib/binaries/CLI.js`) |
| **sha256** | **`bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d`** |
| expected | **identical** |
| **version** | **`5.4.2`**, read from `package.json`. **`pm2 -v` never invoked** |

Supporting digests: `CLI.js` (31 243 bytes) `62a680d4…2d82`; `package.json` `7a4dc970…0a16`.

---

## 4. Store — ACL, unwritability, symlink target

| | |
|---|---|
| target | `/home/odbadmin/python/woa23/data` — **a real directory, not itself a symlink**. The staging `data/` symlink will point here |
| stat | `dev=64512 inode=15732864 odbadmin:odbadmin mode=0775 nlink=5` |

**ACL — the actual mechanism, recorded because mode bits alone would mislead:**

```
store root                  user::rwx  user:woa23c1ro:r-x  group::rwx  mask::rwx  other::r-x
/home/odbadmin/python/woa23 user:woa23c1ro:--x
/home/odbadmin/python       user:woa23c1ro:--x
/home/odbadmin              user:woa23c1ro:--x
/home, /                    no entry for woa23c1ro
```

**Named entries for `woa23c1ro` do exist** — that is how uid 994 reads a store owned
`odbadmin:odbadmin 0775` at all — and **not one grants write**. `[ -w ]` uses `access(2)`,
which honours ACLs, so the negative results below are authoritative, not mode inference.

| check | result |
|---|---|
| store root writable by uid 994 | **no** |
| writable paths beneath | **0** |
| unreadable files / dirs | **0 / 0** |
| untraversable dirs | **0** |
| symlinks inside the store | **0** — escape is impossible, not merely unobserved |
| writable ancestors up to `/` | **0** |

---

## 5. Identity, paths, port

| | |
|---|---|
| account | `uid=994(woa23c1ro) gid=993(woa23c1ro) groups=993` on `odb24` |
| `~/woa23-dep3a` · `-work` · `-pm2` · `~/tmp-dep3a` | **all absent** |
| anything matching `*dep3a*` under `$HOME` | **0** |
| **port 19161** | **0 listeners** |

---

## 6. Node, Python/venv, worker count

### Node — matches probeA/B/C exactly

```
/usr/local/bin/node -> /usr/bin/node   v22.14.0   root:root 755 120177224
sha256 1abce2374a485bddae3c27b17a3e3143e2780232026e627c4fe74ddde3f380a1   (first capture)
```

### Python — the divergence is now CONFIRMED IN ADVANCE, not predicted

| | |
|---|---|
| `uv` | **0.9.22**, `~/.local/bin/uv` (not on `PATH`) |
| subject constraint | `pyproject.toml` **`>=3.11,<3.12`**; `uv.lock` **`==3.11.*`** |
| 3.11 interpreters uv can obtain | **only `cpython-3.11.14`, and only by download** — no 3.11 is installed on the host |
| system `python3` | 3.12.3 — **excluded by the constraint** |
| production interpreter | `/home/odbadmin/.pyenv/versions/py311/bin/python3.11` → **Python 3.11.4** |

**So the venv will be 3.11.14 against production's 3.11.4.** B.1 predicted this; preflight
**confirms it before the run** rather than discovering it after. It is a recorded
**divergence**, not a STOP, and is **not** to be "fixed" by pointing `WOA23_PYTHON` at
production's shared interpreter (spec 016).

**This is now a POLICY DECISION, not an operational note.** `uv` would **download**
cpython-3.11.14, so D-3 as previously written would have reached the network mid-run. The
adopted default is **offline-only**, with a cache gate **before staging** and no fallback of
any kind — see the [network access policy](D3-network-access-policy.md).

**Whether VM24's cache already holds the interpreter and the locked wheels is UNKNOWN**;
checking it needs VM24, and no contact was permitted. The gate obtains the answer before
staging rather than assuming it.

### Worker count

**2**, settled offline by the four-link chain (B.4). The generated config does not exist
before staging, so the **live** assertion belongs after start; nothing about it is claimed
now.

---

## 7. Production and retained state — observed, untouched

```
8050 listeners : 1
3459     God Daemon (/home/odbadmin/.pm2)                        PRODUCTION
1242814/1248938/1438247/1455863   pm2a / pm2b / pm2f / pm2g      retained
1567188  God Daemon (/home/odbadmin/proj/apiverse/.pm2-v331)     another project
1709473  God Daemon (/home/woa23c1ro/woa23-bs3v1-pm2)            retained
1761143  God Daemon (/home/woa23c1ro/woa23-b1s1-pm2)             retained
```

Disk: **94 G free** of 393 G. The 15 local `test_requests.sh` survivors and the ~1065
pre-existing ones are **untouched**.

---

## 8. Result

| item | |
|---|---|
| identity / paths / port | **PASS** |
| store target, ACL, unwritability, symlink | **PASS** |
| store fingerprint comparability | **method difference identified.** [A] agrees with Stage C/D-1 on **12 of 64** characters — a **partial check**; [D] reproduces the **C1/C2** metric in full. **No claim that store metadata is proven unchanged since Stage C/D-1.** [A] is used for **within-run before/after only**, metadata-only |
| PM2 resolved path / size / digest / version | **PASS** |
| Node | **PASS** |
| Python / venv toolchain | **PASS**, with the 3.11.14 vs 3.11.4 divergence confirmed |
| worker count | **2**, offline-settled; live assertion after start |
| observer exclusion + self-agreeing retake | **PASS** |
| **mismatches** | **NONE** |

**Standing limits unchanged.** D-3 is a **candidate deployment rehearsal using the real
production store** — **not** production equivalence, **not** a production PASS, **not** a
cutover, **not** an A11 gate. The interpreter and package-set divergence means **no response
difference may be attributed to the API code alone**.

**Nothing staged. No `pm2 start`. No case issued. Awaiting review of this result.**
