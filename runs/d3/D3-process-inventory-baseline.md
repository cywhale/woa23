# Process inventory — a new, reproducible baseline

**Read-only baseline reconciliation. No staging root, workdir, `PM2_HOME`, tmpdir or store
symlink created. No PM2 started. No case issued. No cleanup, no kills. C1/C2 not re-run.
D-3 NOT resumed.**

**`dep3r` / `19349` is SUPERSEDED, NEVER-BOUND and STRANDED — see §7.1.** It was never
reactivated by editing the request that names it.

**This document lives in `runs/d3/`, outside `git archive <sha> dev2026`.**

---

## 1. The historical digest is retired, not reproduced

```
ecb0aeba3138fb9bfe03e073707683bdea078ae68ef369cb51154db6e55c6489
    method not recorded / not reproducible
```

It was quoted in four documents — `D3-phase0-cache-survey-result.md`,
`D3-phase1-provisioning-result.md`, `D3-preflight-result.md`, `D3-execution-result.md` — as
evidence that no unexpected survivor existed. When D-3 execution required it to be
re-confirmed it could not be. The row **count** (12) and the row **content** still matched;
what could not be recovered was the method. No committed script computed it, and no document
records the field order, separator, sort rule, digest input or algorithm — only the excluded
rows of one run survive, in `D3-preflight-result.md`.

**Five projections were tried and none reproduced it:** full rows; args-only `LC_ALL=C`
sorted; `ps -o args=` over all rows sorted; tab-normalised rows; full rows sorted.

**No further projections were tried, and none will be.** A projection discovered by trying
variants until one matched would manufacture agreement rather than verify it, and the
resulting digest would be evidence of nothing but persistence. It stays in the record with
the label above. It is **not** rewritten into something verifiable, and the new baseline
below is **not** claimed to equal it.

---

## 2. The canonical method

Fixed in `dev2026/scripts/lib_proc_inventory.sh`, and **printed by every run that uses it**,
so the method travels with its own output rather than living in a document a reader may not
have.

| | |
|---|---|
| **field order** | `pid` TAB `starttime` TAB `ppid` TAB `pgid` TAB `ruid` TAB `args` |
| **separator** | a single TAB between fields; exactly one LF after each row |
| **sorting rule** | `LC_ALL=C sort` over the whole canonical row |
| **included rows** | every process of the target user that is not an observer row |
| **excluded rows** | observer rows, by **PID/PGID identity**, never by command-line matching |
| **digest input** | the sorted canonical rows, and nothing else — no header, no host, no timestamp |
| **digest algorithm** | SHA-256, reported as all 64 hex characters |

**Nothing variable may enter the digest input.** A host name or timestamp inside it would
make two honest retakes disagree, and a baseline that cannot agree with itself cannot be
compared with anything.

**Sorting is lexical over the whole row, so the pid *string* orders, not its value.** Said
explicitly because a reader expecting numeric order would recompute the digest wrongly by
hand.

**`starttime` is part of the row, not decoration.** A pid alone is not an identity — pids are
reused — so a baseline keyed on pid alone can report "the same 12 processes" about a
different set. The token comes from `lib_sentinel.sh`'s `proc_starttime`
(`lin-<ticks>` on Linux, `bsd-<epoch>` elsewhere), **reused rather than reimplemented** so
the two cannot drift. VM24 is Linux and takes the `lin-<ticks>` path.

### 2.1 Observer exclusion is an identity test

A row is an observer **iff** its PGID equals the observing script's PGID, **or** its PID
equals the observing script's PPID. That is arithmetic on numbers.

It is deliberately **not** `grep -v` over command text. A command-line exclusion is
over-broad — a real survivor whose command happens to match would vanish from the very
inventory that exists to find it — and it is self-defeating, as §2.2 records.

### 2.2 The script cannot match itself

Two defences, both bought with artefacts this campaign has already paid for:

1. **`ps` runs once, into a variable, before any matcher exists.** The D-3 preflight's first
   inventory refused because its own `grep` carried the signature in `argv` and `ps` captured
   it. A matcher that starts afterwards cannot appear in a snapshot already taken.
2. **The daemon signature is assembled at run time from fragments**, so the literal exists in
   no file, no `argv` and no process listing. It cannot be matched even by something grepping
   the script itself.

The same artefact recurred during D-3 execution when this inventory was passed to `ssh` as a
command **argument**, placing the signature in the remote shell's `argv`. The baseline below
is therefore delivered on **stdin** (`bash -s`), which is why every excluded row reads
`bash -s` rather than the script text.

---

## 3. Provenance of the tool

New subject, per requirement 8:

```
subject   a361f70668f28eaec49fabf078ca4c9c05d5d4ed
archive   92e93bd378af5835ab3d917e6a90ffbfbc40b2632dffbcabee99c70ac6da6ba3
files     261
file-list 699d0f55175db02fe0aa6083c7223685c4082bfe2e9f450968a182e47f7c43db
```

| file | sha256 |
|---|---|
| `scripts/proc_inventory.sh` | `e17944b71034aa53b8e013383f04552352e39a30c8b49f30b4f4c32647fa0351` |
| `scripts/lib_proc_inventory.sh` | `4e5b7106001bbf6ad716db094cb9534985e2c8b51b2b00c8197a17b43acde989` |
| `scripts/lib_sentinel.sh` | `0cea4aa158f431142e4118edd52bea7cc8134292a129c6b9160c7a6b332af679` |
| `scripts/test_proc_inventory.sh` | `6453e7d89b2456047ed7ffee145762079956ce46ec08091dd7e088339a7f74b2` |

**The three D-3 delivery members are byte-for-byte unchanged** from subject `ab670fb`:
`staging_execute.sh` `a69b1265…`, `lib_store_guard.sh` `dc82e80f…`, `staging_bootstrap.sh`
`44ede13c…`. This change adds an inventory tool; it does not touch the reviewed delivery fix.

### 3.1 Three clean batches at this subject

`uv sync` was run and verified (`Python 3.11.14`) **before batch 1**.

| batch | HEAD | tracked dirty | untracked | suites | non-zero | assertions | contract violations | exit |
|---|---|---|---|---|---|---|---|---|
| 1 | `a361f706…` | 0 | 0 | **58** | 0 | **4952** | 0 | 0 |
| 2 | `a361f706…` | 0 | 0 | **58** | 0 | **4952** | 0 | 0 |
| 3 | `a361f706…` | 0 | 0 | **58** | 0 | **4952** | 0 | 0 |

```
batches completed : 3 of 3     non-zero total : 0     postconditions : yes
WOA23_BATCH_COMPLETE subject=a361f70668f28eaec49fabf078ca4c9c05d5d4ed \
  label=d3-a361f70 token=d3-a361f70-52471-bsd-1788267822 batches=3 nonzero=0
```

HEAD checked before and after every batch; per-suite `(suite, exit)` identical across all
three. **4952 = 4883 + 69**, the 69 being `test_proc_inventory.sh`. 4952 is this subject's
total; **4791 remains the historical figure for the 56 suites of `594737c` and is not a
current batch total.**

### 3.2 How the tool reached VM24 without writing to it

The tool is three files that source one another by path. Running it from files would mean
creating files on VM24, which this step does not authorise. It was therefore **composed into
one stream and piped to `bash -s`** — nothing is written on VM24, and the signature stays out
of `argv`.

The composition is mechanical and consists of **exactly five substitutions, all in the
driver**, asserted by the composer:

| # | substitution | why |
|---|---|---|
| 1–2 | the two `. "$_PI_HERE/lib_*.sh"` lines → `:` | the libraries are already above in the stream |
| 3–4 | the two `sha256sum "${BASH_SOURCE[0]}"` report lines → the **archive** digests | a script read from stdin has no file to hash |
| 5 | `_PI_HERE` → empty | it derives from `${BASH_SOURCE[0]}`, which does not exist on stdin |

**The driver's `command -v` guards are untouched**, so if a substitution ever failed to place
a symbol the driver still refuses rather than running without it.

```
composed stream sha256 : b310bfbbd2e45abe9cebf3b577ebd48969d1eec552bcb3fd523bf7c96f229643
```

Substitution 5 was added after the first three takes emitted
`BASH_SOURCE[0]: unbound variable` on stderr. Those runs produced correct output and the
**same digest**, but evidence carrying an unexplained error is not clean evidence. The takes
recorded below have zero bytes on stderr.

---

## 4. The baseline

```
host            : odb24
user            : woa23c1ro   (uid 994)
timestamp UTC   : 2026-09-01 14:35:07
kernel          : Linux 6.8.0-124-generic x86_64
```

```
ROWS=12
PROC_INVENTORY_SHA256=46a044e1a3b33ec66b258528b522534d77892b1cdc32b8d36efa5906cc5e886c
```

### 4.1 Exclusion identity, take 1

```
observer pid    : 2006586
observer pgid   : 2006586
observer ppid   : 2006585
observer start  : lin-158832860
```

### 4.2 Raw excluded rows (4)

```
2006585 2006502 2006502   994 sshd: woa23c1ro@notty
2006586 2006585 2006586   994 bash -s
2006588 2006586 2006586   994 bash -s
2006589 2006588 2006586   994 ps -o pid=,ppid=,pgid=,ruid=,args= -u woa23c1ro
```

Every one is demonstrably the observer, and each is excluded by the **numeric** rule, not
because of what its command says.

### 4.3 Canonical rows, sorted (12) — the digest input, verbatim

```
1708514	lin-114971132	1	1708514	994	/usr/lib/systemd/systemd --user
1708519	lin-114971133	1708514	1708514	994	(sd-pam)
1708530	lin-114971159	1708514	1708530	994	/usr/bin/pipewire
1708531	lin-114971159	1708514	1708531	994	/usr/bin/pipewire -c filter-chain.conf
1708533	lin-114971159	1708514	1708533	994	/snap/snapd-desktop-integration/391/usr/bin/user-session-helper /snap/snapd-desktop-integration/391/usr/bin/snapd-desktop-integration
1708536	lin-114971159	1708514	1708536	994	/usr/bin/wireplumber
1708537	lin-114971159	1708514	1708537	994	/usr/bin/pipewire-pulse
1708562	lin-114971161	1708514	1708562	994	/usr/bin/dbus-daemon --session --address=systemd: --nofork --nopidfile --systemd-activation --syslog-only
1708635	lin-114971170	1708514	1708635	994	/usr/libexec/xdg-document-portal
1708651	lin-114971171	1708514	1708651	994	/usr/libexec/xdg-permission-store
1709473	lin-114971541	1	1709473	994	PM2 v5.4.2: God Daemon (/home/woa23c1ro/woa23-bs3v1-pm2)
1761143	lin-124827409	1	1761143	994	PM2 v5.4.2: God Daemon (/home/woa23c1ro/woa23-b1s1-pm2)
```

**The digest is recomputable from exactly the block above** — that is the property
`ecb0aeba…` lacked, and it is what makes this baseline checkable by anyone:

```
sha256 of the 12 rows above : 46a044e1a3b33ec66b258528b522534d77892b1cdc32b8d36efa5906cc5e886c
reported by the run         : 46a044e1a3b33ec66b258528b522534d77892b1cdc32b8d36efa5906cc5e886c
```

### 4.4 Content requirements — all met

| requirement | result |
|---|---|
| 12 expected kept rows | **12** |
| retained daemon `1709473` (bs3v1) in the kept set | **yes** |
| retained daemon `1761143` (b1s1) in the kept set | **yes** |
| excluded rows carrying a daemon signature | **0** |
| unexpected survivor | **none** — every one of the 12 is a session process or a retained daemon |
| rows whose args contain a TAB | **0** |

The two retained God Daemons being **kept** is the positive proof the exclusion is not
over-broad: an exclusion wide enough to hide a daemon would have hidden those two.

---

## 5. Self-agreeing retakes

Three consecutive retakes, each a separate SSH session.

### 5.1 What must agree, and does

| | take 1 | take 2 | take 3 | |
|---|---|---|---|---|
| **included canonical rows** | `0f1329230d2c…` | `0f1329230d2c…` | `0f1329230d2c…` | **identical** |
| **canonical input** | `46a044e1a3b3…` | `46a044e1a3b3…` | `46a044e1a3b3…` | **identical** |
| **digest** | `46a044e1a3b3…` | `46a044e1a3b3…` | `46a044e1a3b3…` | **identical** |
| exit status | 0 | 0 | 0 | identical |
| stderr bytes | 0 | 0 | 0 | identical |

These three are the baseline. All agree across all three takes.

### 5.2 What is expected to differ, and why

**Excluded observer rows differ in PID and starttime, by construction.** Each retake opens a
new SSH session, so the observer's `sshd`, its `bash -s` shells and its `ps` are different
processes with different pids and different `lin-<ticks>` values every time. Requiring those
rows to be byte-identical would be requiring the observer not to be a new process, which is
not a property the machine can have.

**They are therefore not compared byte-for-byte.** What is verified instead is the observer's
ROLES and COUNT, and that no excluded row carries a retained daemon signature:

| check | take 1 | take 2 | take 3 |
|---|---|---|---|
| excluded row count | **4** | **4** | **4** |
| role: `sshd: woa23c1ro@notty` | 1 | 1 | 1 |
| role: `bash -s` | 2 | 2 | 2 |
| role: `ps -o pid=,ppid=,pgid=,ruid=,args= -u woa23c1ro` | 1 | 1 | 1 |
| ruid on every excluded row | 994 | 994 | 994 |
| **excluded rows matching a retained PM2 daemon signature** | **0** | **0** | **0** |

With the pid/pgid/ppid columns removed, the excluded rows of all three takes are identical:

```
994 bash -s
994 bash -s
994 ps -o pid=,ppid=,pgid=,ruid=,args= -u woa23c1ro
994 sshd: woa23c1ro@notty
```

Observer identity per take, recorded because it is what the exclusion was computed from:

| take | observer pid | pgid | ppid | starttime |
|---|---|---|---|---|
| 1 | 2006586 | 2006586 | 2006585 | `lin-158832860` |
| 2 | 2007082 | 2007082 | 2007081 | `lin-158832925` |
| 3 | 2007576 | 2007576 | 2007575 | `lin-158832991` |

**The zero in the last row of the first table is the load-bearing one.** If a retained PM2
daemon ever appeared among the excluded rows, the exclusion might be hiding a survivor — and
the tool refuses outright rather than reporting a clean baseline, which is exercised
deliberately in `test_proc_inventory.sh` §5.

## 6. This baseline replaces the old one for D-3, and claims nothing about it

The D-3 request will use `46a044e1…cc5e886c` as the **before** value for its before/after
comparison, and the same tool at the same subject will produce the **after** value.

**No claim is made that this equals `ecb0aeba…6489`, or that the machine is unchanged since
that digest was taken.** The two are not comparable: one has a recorded method and one does
not. What can be said is narrower and true — the *content* of the inventory (12 rows, the
same session processes, the same two retained daemons) matches what the historical record
describes in prose.

This mirrors the treatment the store fingerprint already carries: a within-run before/after
metric, with no cross-run equality claim.

---

## 7. Status

| | |
|---|---|
| new baseline | `46a044e1a3b33ec66b258528b522534d77892b1cdc32b8d36efa5906cc5e886c`, 12 rows |
| historical digest | `ecb0aeba…6489` — **retained as record, method not recorded / not reproducible** |
| method | recorded in §2 and printed by every run |
| retakes | 3 consecutive; included canonical rows, canonical input and digest **identical**; excluded observer rows differ in pid/starttime **as expected**, with roles, count and zero daemon-signature matches verified (§5.2) |
| subject | `a361f70668f28eaec49fabf078ca4c9c05d5d4ed`, three clean batches, sentinel written |
| identity | `dep3r` / `19349` **superseded / never-bound / stranded** (§7.1); the current allocation is in the new request (§7.2) |
| VM24 state created | **none** — no staging root, workdir, `PM2_HOME`, tmpdir, bootstrap or store symlink; port `19349` unbound; 0 new files in the home directory |
| VM24 retained state | intact — `d3stage-7015a89`, `d3boot-7015a89`, `woa23-dep3m`, `woa23-dep3m-work`, `woa23-dep3h`, `woa23-dep3h-work` all present |
| production store | `123005` files, mode `775`, owner `1000:1000` — unchanged |
| cleanup / kills | **none** |
| C1 / C2 | **not re-run** |
| D-3 | **not resumed; request prepared offline and awaiting explicit execution authorization** |

### 7.1 `dep3r` / `19349` — superseded, never-bound, stranded, not reusable

| | |
|---|---|
| status | **SUPERSEDED — NEVER-BOUND — STRANDED — NOT REUSABLE** |
| why | the request naming it (`5eb0c13`, corrected in `126918d`) is now an **ancestor** of subject `a361f70`, so it is no longer a post-subject allocation |
| ever bound | **no** — the port was unbound at every check and no staging path was ever created for it |
| reactivated | **no** — the old request was not edited in place to revive it |

**The chronology is what strands it, not any fault of the identity.** An execution identity
is usable only when it is named *after* the subject it will run against. `dep3r` / `19349`
was correctly post-subject for `ab670fb`; cutting `a361f70` on top moved the naming commit
into the new subject's ancestry, and an identity already present in a subject's history is
not a fresh allocation for that subject. It appears in `dep3r`=22 / `19349`=17 places across
the 392 ancestor commits, all of them in `runs/d3/D3-execution-request-ab670fb.md`, and in
**zero** `dev2026` paths.

It joins `dep3a`, `dep3b`, `dep3c`, `dep3d`, `dep3f`, `dep3h`, `dep3j`, `dep3k`, `dep3m`,
`dep3n`, `dep3p`, `dep3q` and their ports as consumed. None is proposed again.

### 7.2 The current allocation

The new identity for the D-3 request is in
[`D3-execution-request-a361f70.md`](D3-execution-request-a361f70.md), committed **after**
subject `a361f70`. It is not named here, because this document is a baseline package rather
than the request, and naming an identity in a second place is how `dep3n` and `dep3p` were
spent.
