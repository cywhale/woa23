# Phase 1 — runtime provisioning: **RESULT**

**Authorised 2026-08-30 (Phase 1 only). Executed on VM24 (`odb24`) as `woa23c1ro`, uid 994.**

> ## PROVISIONING SUCCEEDED. **This is NOT a deployment PASS and D-3 was NOT entered.**
>
> CPython **3.11.14** (build **20251217**) is installed, uv **recognises** it, and the
> authoritative offline resolution **succeeds with no network**.
>
> **Three items need your decision before D-3** (§7). None is a failure of the interpreter;
> two are defects in the request text and one is a permissions observation.

**`dep3a` and `19161` remain unconsumed. No PM2 daemon exists. No staging root, workdir or
`PM2_HOME` was created. No case was issued.**

---

## 1. Archive — verified on both sides

| | |
|---|---|
| source | `astral-sh/python-build-standalone`, build **`20251217`**, at VM24's own metadata URL |
| fetched on | the **operator's machine**. **VM24 made no outbound connection** |
| **size** | **30 507 253 bytes** — the first real figure; uv publishes none |
| **SHA-256, operator side** | `49e99461d9c4ea3ee80ff0e5d00afa197f9d4c00ebf5fab51e70e507f330003a` |
| **SHA-256, on VM24 after transfer** | `49e99461d9c4ea3ee80ff0e5d00afa197f9d4c00ebf5fab51e70e507f330003a` |
| verdict | **MATCH on both sides** — source correct **and** transfer intact |

**Inbound transfer, recorded as required:** one `scp` over the operator's existing SSH
session into `~/uvprov/20251217/`, operator-initiated, VM24 passive. **VM24 outbound egress:
zero.** The local uv 0.9.27 build `20260114` was **never fetched and never used**.

---

## 2. Placement — method 3a, uv's own local file mirror

```
UV_OFFLINE=1  UV_CACHE_DIR=/home/woa23c1ro/.cache/uv
uv python install 3.11.14 --mirror file:///home/woa23c1ro/uvprov --offline
  Downloading cpython-3.11.14-linux-x86_64-gnu (download) (29.1MiB)
  Installed Python 3.11.14 in 889ms
   + cpython-3.11.14-linux-x86_64-gnu (python3.11)
```

**uv performed its own placement**, so the directory name, layout and `BUILD` marker are
uv's, not my reconstruction. **Fallback 3b was not needed.** `--offline` was retained
throughout; the "download" in uv's own wording is a **read of the local file**.

### 2.1 DEVIATION 1 — `UV_PYTHON_DOWNLOADS=never` blocks even an explicit install

The first attempt, with `never` set, refused:

```
Python downloads are not allowed (`python-downloads = "never"`).
Change to `python-downloads = "manual"` to allow explicit installs.
```

**The mirror was never reached — the policy refused before it.** I set
**`UV_PYTHON_DOWNLOADS=manual` for the placement step only**, and it is reported here rather
than buried:

| | |
|---|---|
| why it is not a network exception | `manual` permits an **explicit, user-initiated** install and still blocks automatic ones. `--offline` stayed set, and the source was a **local file** |
| scope | that one command. **`never` was restored** for steps 6 and 7, and both ran under it |
| why I judged it in scope | `UV_PYTHON_DOWNLOADS=never` is listed in your **D-3** rules; the Phase 1 install limits instead say *prefer the uv-supported local file mirror*, which `never` makes impossible |
| verified | no outbound connection; the fallback was **not** used; `--offline` was **never dropped** |

**If you consider this outside the authorisation, the placement can be redone via 3b** —
but 3b is the weaker method, since it requires hand-writing the `BUILD` marker uv wrote
itself.

---

## 3. Installed tree — one archive, thousands of files

| | |
|---|---|
| location | `/home/woa23c1ro/.local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu` |
| **regular files** | **4 238** |
| directories | **281** |
| symlinks | **1 048** |
| **all entries** | **5 567** |
| **total bytes** | **82 320 416** |
| top level | `BUILD`, `bin`, `include`, `lib`, `share` |
| **`BUILD` contents** | **`20251217`** — matches VM24's metadata build tag exactly |
| owner | `woa23c1ro:woa23c1ro` — **one owner, no exceptions** |
| tree fingerprint (`path+size+mode`) | `c3214216a1939da6fda7506e048590a64ddc27ea7dbc7b4fb2984c8a2a876f9b` |

**The before-state was: the directory did not exist at all.** A 30 MB archive became
**5 567 entries** — the distinction §4.1 of the request insisted on, now with real numbers.

### 3.1 DEVIATION 2 — the tree is group-writable, and my own criterion said it should not be

| | |
|---|---|
| **world-writable** | **0 files, 0 directories** |
| **group-writable** | **4 238 files, 281 directories** — i.e. all of them |
| modes | files `664`, dirs `775`, executables `775`, symlinks `777` |
| cause | the account's **`umask 0002`**, applied by uv during extraction |

**My §5.3 acceptance criterion said "nothing group- or world-writable". As written, it is NOT
met**, and I am not redefining it after the fact.

**What the exposure actually is, checked rather than assumed:**

- group `993 (woa23c1ro)` has **no other members** — `getent group 993` → `woa23c1ro:x:993:`;
- **no other user** has 993 as a primary gid;
- **`/home/woa23c1ro` is mode `700`** — no other user can even traverse into the tree.

**So the practical exposure is nil, but the stated criterion still fails and that is your call
to accept or to have corrected** (a `chmod -R g-w` would satisfy it; I have not done it,
because it was not authorised and would alter the tree uv produced).

**My earlier count of "5 567 group- or world-writable" was also my own measurement error** —
`find -perm /022` conflates group- and world-write. Separated properly, world-write is **0**.

---

## 4. Interpreter identity

| | |
|---|---|
| path | `…/cpython-3.11.14-linux-x86_64-gnu/bin/python3.11` |
| **realpath** | identical — **not a symlink** |
| **version** | **`Python 3.11.14`** |
| build stamp | `3.11.14 (main, Dec 17 2025, 21:07:37) [Clang 21.1.4]` — consistent with build `20251217` |
| owner / mode | `woa23c1ro:woa23c1ro` `775` |
| size | **21 333 768 bytes** |
| **SHA-256** | **`96d1b01675f2492922ec6f6ed8445791d2d3231ccae727cda521db30494b751e`** |
| `sys.prefix` | inside the install dir — **not `/usr`, not `/home/odbadmin`** |

**The SHA-256 is a first capture, not a verification.** No published digest exists for the
extracted binary; the *archive* was the verified artifact (§1). This value is a baseline for
future comparison and is not presented as a check that passed.

---

## 5. uv 0.9.22 recognition — **RECOGNISED**

Run with `UV_PYTHON_DOWNLOADS=never` restored:

```
cpython-3.12.3-linux-x86_64-gnu     /usr/bin/python3.12
cpython-3.12.3-linux-x86_64-gnu     /usr/bin/python3 -> python3.12
cpython-3.11.14-linux-x86_64-gnu    .local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu/bin/python3.11
```

**Listed as a filesystem path, not `<download available>`.** **No bypass was used** — not
`WOA23_PYTHON`, not `--python`, not `UV_PYTHON`.

---

## 6. Phase 2 — the authoritative offline resolution: **PASSES**

### 6.1 DEVIATION 3 — the authorised command cannot execute as written

```
$ uv sync --frozen --locked --offline --dry-run
error: the argument '--frozen' cannot be used with '--locked'
```

**`--frozen` and `--locked` are mutually exclusive in uv 0.9.22.** The command string appears
that way in my own request text and was carried into the authorisation verbatim. **It is my
defect.**

**I did not silently pick one.** Both were run separately and both are reported:

| variant | meaning | result |
|---|---|---|
| **`--locked`** | asserts `uv.lock` **must not change** — the stronger assertion, and the one matching "no lock modification" | **exit 0** |
| **`--frozen`** | uses the lock as-is without checking freshness | **exit 0** |

**`--locked` is the one I would carry into D-3**, because it is the flag that enforces the
lock being untouched. **That choice is yours to confirm**, and the request text needs
correcting either way.

### 6.2 The result

```
Using CPython 3.11.14
Resolved 60 packages
Would install 58 packages
```

| | |
|---|---|
| interpreter selected | **CPython 3.11.14** — the one provisioned |
| network | **none.** `UV_OFFLINE=1`, `UV_PYTHON_DOWNLOADS=never` |
| artifacts resolved from | the pinned cache `/home/woa23c1ro/.cache/uv` |
| missing artifacts | **none** |
| `.venv` created | **no** — `--dry-run` honoured |
| digests on VM24 | `pyproject.toml` `aa846b8b…e378e`, `uv.lock` `0d2980a5…cc69` — **match the subject** |

### 6.3 `colorama` — the verdict is uv's, as required

**`colorama` does NOT appear in uv's install list.** Phase 0's `INDICATIVE_MISSING` was
**my derivation over-including it**, exactly as the marker suggested — but **the verdict here
comes from uv's own Linux/cp311 resolution, not from my reading**, which is what was required.

```
60 lock entries − 1 virtual project − 1 win32-only (colorama) = 58 installed
```

**The arithmetic closes exactly**, and no artifact is missing.

---

## 7. What needs your decision

| # | item | my position |
|---|---|---|
| **1** | `UV_PYTHON_DOWNLOADS=manual` used for the placement step (§2.1) | in scope and network-free; **redo via 3b if you disagree** |
| **2** | `--frozen --locked` is not a valid combination (§6.1) | **use `--locked`** for D-3; request text needs correcting |
| **3** | tree is group-writable, failing my §5.3 criterion (§3.1) | practical exposure nil; **accept, or authorise a `chmod -R g-w`** |

---

## 8. Before / after evidence

| | before | after |
|---|---|---|
| `/home/odbadmin/.pyenv` digest | `b8badefd…32b1` | **`b8badefd…32b1` — identical** |
| `.pyenv` entries | 378 | **378** |
| uv cache entries / bytes | 9 969 / 502 146 635 | 9 970 / 502 148 521 |
| **uv cache digest** | `a4c51b82…faca` | `69e3d458…d6f0` — **changed, attributed below** |
| four D-3 identity paths | absent | **absent** |
| `*dep3a*` under `$HOME` | 0 | **0** |
| port **19161** | 0 listeners | **0 listeners** |
| port 8050 | 1 | **1 — untouched** |
| process inventory | 12 rows, `ecb0aeba…6489` | **12 rows, `ecb0aeba…6489` — identical** |
| PM2 daemons owned by uid 994 | 2 (retained `bs3v1`, `b1s1`) | **2 — none created, none touched** |

### 8.1 The cache change, attributed rather than waved through

**Exactly one file was added:**

```
2026-08-30 16:12:15   1886 bytes
/home/woa23c1ro/.cache/uv/interpreter-v4/85046238252ff06c/029587dfd7cc80f4.msgpack
```

**uv's cached probe of the newly installed interpreter**, written when uv first inspected it.
**No package artifact was added** — impossible under `UV_OFFLINE=1`, and the timestamp search
found nothing else. The cache is the pinned, authorised one.

**This is why the digest is reported as changed rather than the change being called
insignificant.** It is expected, it is attributed, and it is not hidden behind an unchanged
entry count.

---

## 9. Status

| | |
|---|---|
| archive verification | **PASS**, both sides |
| placement | **PASS** via 3a, with deviation 1 recorded |
| installed-tree inventory | **RECORDED** — 5 567 entries, 82 320 416 bytes |
| interpreter identity | **PASS** — 3.11.14, uid 994, `sha256 96d1b016…751e` |
| uv recognition | **PASS** — no bypass |
| authoritative offline package check | **PASS** — 58 packages, no missing artifact, no network |
| `PROVISIONING_FAILED` | **not triggered** |
| D-3 | **NOT ENTERED** |

**This is a provisioning result only.** It is **not** a deployment PASS, **not** production
equivalence, and **not** evidence about the candidate's data path.

**The attribution limitation stands unchanged:** D-3 would run **3.11.14** against
production's **3.11.4**, with a resolved package set from this `uv.lock` rather than
production's environment — so **no response difference could be attributed to the API code
alone**.

**Can D-3 proceed?** The runtime blocker is cleared: the interpreter exists, uv sees it, and
all required artifacts resolve offline. **D-3 is not authorised by this result and was not
started.** The three items in §7 are for your decision first.

---

# ADDENDUM — review decisions, and the §6 verification

**Review accepted 2026-08-30. Recorded here so the classification travels with the result.**

## A. Classification, fixed

> # QUALIFIED PROVISIONING COMPLETE
>
> **Not a D-3 result. Not a deployment PASS. Not production equivalence.**
> **D-3 still requires its own explicit authorisation; Phase 1 is not one.**

## B. The decisions

| # | decision |
|---|---|
| 1 | Phase 1 is **QUALIFIED PROVISIONING COMPLETE** — never a D-3 or deployment PASS |
| 2 | the **local file mirror** matched to VM24's uv 0.9.22 is accepted; `UV_PYTHON_DOWNLOADS=manual` is permitted **for the placement step only**. **D-3 restores `never` and stays offline** |
| 3 | the authoritative package check uses **`--locked`**; the mutually exclusive `--frozen` is **not** used |
| 4 | the **group-writable tree is accepted as a recorded exception**. **No `chmod -R g-w`.** D-3 preflight **re-confirms GID, ACL, world-writable and symlink boundaries** |
| 5 | the added uv cache metadata file is an **expected Phase 1 change** |
| 6 | lock, `pyproject`, interpreter tree and production paths verified for unexpected modification — **§C** |
| 7 | **D-3 needs separate explicit authorisation.** Phase 1 is not it |

**Deviations 1–3 from §7 are therefore closed** — accepted as recorded, not withdrawn.

## C. §6 verification — no unexpected modification

### C.1 Lock and `pyproject` — UNCHANGED

```
pyproject.toml  aa846b8be70b0b5d466d0e2a0bbb1f4dfe6ccac5d28f795a57c1c6bbea7e378e
uv.lock         0d2980a5928d4d0964d6cb3b78bffae14aa11a70d3b51ca00f4cf39073dccc69
```

**Both match the subject exactly.** `--locked` would have failed had the lock needed changing,
and it did not. The copies on VM24 are in the bootstrap scratch dir; **the subject's own files
were never placed on VM24 and could not have been touched.**

### C.2 Production paths — UNCHANGED

| | recorded | now |
|---|---|---|
| `/home/odbadmin/.pyenv` digest | `b8badefd…32b1` | **identical** |
| `.pyenv` entries | 378 | **378** |
| **production store** fingerprint (method A) | `abe6c21221b5081eb352a1a549c9d1fd6399c74b1f06b8c2ce09952828c61806` | **identical** |
| store files / bytes | 123 005 / 35 101 630 061 | **identical** |
| anything under `/home/odbadmin` modified in 6 h | — | **none** |

> **WITHDRAWN — this sentence was too broad. See [§E](#e-correction--my-no-production-path-was-written-claim-was-unsupportable).**
>
> uid 994 **cannot enumerate `/home/odbadmin`** (ACL `--x`; `ls` returns *Permission denied*),
> and one leg of the scan above ran over an **unreadable** directory and returned empty for
> that reason — a **vacuous pass**. The supportable claim is narrower: **the two measured
> paths, by the two stated methods, are unchanged.**

### C.3 Interpreter tree — CHANGED, and I can only partly attribute it

**This is reported as a discrepancy, not as a clean pass.**

| | at install (§3) | now |
|---|---|---|
| fingerprint (`path+size+mode`) | `c3214216…876f9b` | **`70e27b40…3243de` — DIFFERENT** |
| `du -sb` | 82 320 416 | **82 320 600 — +184 bytes** |
| files / dirs / links / total | 4238 / 281 / 1048 / 5567 | **4238 / 281 / 1048 / 5567 — identical** |

**What is established:**

- **entry counts are identical by every type** — nothing was added or removed;
- **nothing in the tree has been modified since `16:12:16`** — a timestamp search returns 0;
- the measurement is **stable now** — two consecutive runs give identical values;
- the only post-extraction writes are **4 `.pyc` files and 2 `__pycache__` directories**,
  all stamped **`16:12:15`**, in `encodings/` and `_distutils_hack/`.

**What I cannot establish, and will not paper over:** the exact origin of the **184-byte**
difference. **I recorded a fingerprint and counts but NOT the per-file listing** that §6.1 of
my own request required — so there is nothing to diff against. **That is a shortfall in my
evidence, not a finding about the tree.** The most likely explanation is that the step-4
measurement was taken while uv's post-install interpreter probe was still writing at
`16:12:15`, but **I did not prove that and am not asserting it.**

### C.4 A real finding for D-3, arising from C.3

> **Because the tree is writable, running the interpreter writes `.pyc` into it.** That is
> exactly what produced the four `__pycache__` entries.

**Consequence: the interpreter tree fingerprint WILL drift during D-3**, as the application
imports stdlib modules. **This must be expected and recorded, not treated as tampering.**
D-3's before/after evidence should therefore compare the tree by **entry count, ownership and
mode**, and treat **new `__pycache__` entries as an expected class** — while any change to a
non-`__pycache__` file remains a finding.

**This is a direct consequence of the group-writable exception accepted in decision 4**, and
it is the reason decision 4's re-confirmation at D-3 preflight matters.

## D. Status

| | |
|---|---|
| Phase 1 | **QUALIFIED PROVISIONING COMPLETE** |
| lock / `pyproject` | **unchanged** |
| production paths | **unchanged** |
| interpreter tree | **entry counts unchanged; 184-byte delta unattributed (my evidence gap)** |
| uv cache | one metadata file added — **expected** |
| `dep3a` / `19161` | **unconsumed** |
| **D-3** | **NOT AUTHORISED, NOT STARTED** |

---

# ADDENDUM 2 — required wording and controls before D-3

**Review accepted with corrections. Applied here; nothing re-run, no VM24 state changed.**

## E. CORRECTION — my "no production path was written" claim was unsupportable

**And one leg of its evidence was VACUOUS.** Both are corrected here rather than softened.

### E.1 What uid 994 can actually see under `/home/odbadmin`

```
/home/odbadmin           odbadmin:odbadmin 755   ACL user:woa23c1ro:--x
$ ls /home/odbadmin      ls: cannot open directory '/home/odbadmin': Permission denied
$ find /home/odbadmin    find: '/home/odbadmin': Permission denied
/home/odbadmin/python    readable = NO   (traverse only)
```

**The account cannot enumerate `/home/odbadmin` at all.** A complete manifest of it is
**impossible from this account**, so no statement about "the whole of `/home/odbadmin`" can
ever be supported by this evidence.

### E.2 The vacuous leg

My scan was `find /home/odbadmin/.pyenv /home/odbadmin/python -newermt '-6 hours' 2>/dev/null`
and I reported *"(empty = none)"*.

**`/home/odbadmin/python` is not readable**, so that arm returned `Permission denied` —
**suppressed by `2>/dev/null`** — and contributed **nothing**. **A scan over an unreadable
directory returns empty for the same reason a clean one does.** That is the **vacuous-pass**
shape this campaign has named before, and I produced it again.

### E.3 The narrowed claim — measured paths and methods only

| path | method | result |
|---|---|---|
| `/home/odbadmin/.pyenv` | `find -maxdepth 3 -printf '%p\t%s\t%T@'`, `LC_ALL=C` sorted, sha256 — **378 entries** | `b8badefd…32b1` **before and after — identical** |
| `/home/odbadmin/python/woa23/data` (the store) | full recursive, **method A** (`-type f`, TAB, `LC_ALL=C` sort) | `abe6c212…1806` **identical**; 123 005 files / 35 101 630 061 bytes |

> **The claim is exactly this and no more:** *the two measured paths above, by the two methods
> above, are unchanged.* **Nothing is claimed about `/home/odbadmin` as a whole**, because it
> cannot be enumerated from this account. The earlier sentence "No production path was
> written" is **withdrawn**.

## F. Stabilized D-3 baseline for the interpreter tree

**The per-file manifest I failed to capture at install now exists**, closing the §C.3 evidence
gap for all future comparisons.

| | |
|---|---|
| manifest | [`D3-phase1-interpreter-baseline.tsv`](D3-phase1-interpreter-baseline.tsv) — `type, mode, uid:gid, size, path` for **every entry**, `LC_ALL=C` sorted |
| entries | **5 567** — 4 238 files, 281 directories, 1 048 symlinks |
| **manifest sha256** | **`5e83e03bc24fd105073ec61ae38234c88fb23ad66d6bbfd64ffff317c84a455e`** |
| owner | `994:993` throughout |

### F.1 The ONLY permitted generated files — enumerated, not described

```
d 775 994:993  4096   lib/python3.11/encodings/__pycache__
d 775 994:993  4096   lib/python3.11/site-packages/_distutils_hack/__pycache__
f 664 994:993  6524   lib/python3.11/encodings/__pycache__/__init__.cpython-311.pyc
f 664 994:993 12714   lib/python3.11/encodings/__pycache__/aliases.cpython-311.pyc
f 664 994:993  2383   lib/python3.11/encodings/__pycache__/utf_8.cpython-311.pyc
f 664 994:993 11999   lib/python3.11/site-packages/_distutils_hack/__pycache__/__init__.cpython-311.pyc
```

**Six entries. That is the documented generated set.**

### F.2 The rule for D-3

| observed at D-3 | verdict |
|---|---|
| a **new `__pycache__` directory or `.pyc` file** | **permitted** — recorded as a documented generated cache file |
| **any other content change** — a non-`.pyc` file differing in size | **FINDING** |
| **any symlink** added, removed or re-pointed | **FINDING** |
| **any owner change** away from `994:993` | **FINDING** |
| **any mode change** | **FINDING** |
| **any entry removed** | **FINDING** |

**Compared against the manifest above, not against a whole-tree digest** — a digest is
guaranteed to move and would make every run look like a finding.

### F.3 `PYTHONDONTWRITEBYTECODE=1` — recommended, with its mechanism and its cost

**Recommendation: USE IT**, and it is documented here as required.

| | |
|---|---|
| effect | **no `.pyc` is written at all**, so the tree does not drift and **every** difference becomes a finding — strictly stronger than reasoning about an expected class |
| **how it must be set** | **exported in the spawning shell before `pm2 start`**, inherited by the PM2 daemon and its workers |
| **how it must NOT be set** | **not** in the ecosystem config's `env`. `make_staging_override.js` diffs the generated config against production's and **refuses any key not in `EXPECTED`** — so a config route would require editing the generator, **changing the subject** and forcing a new subject with fresh batches |
| allowlist safety | `staging_execute.sh`'s `ALLOWED_ENV` only scans `^WOA23_`, so this variable **cannot** trip it |
| **the cost, stated** | production does **not** set it. It is therefore **one more documented environment difference**, and it is recorded as such — not hidden |
| why the cost is acceptable | D-3 already **cannot** claim production equivalence, and its interpreter and package set already differ. Turning tree-drift into a hard invariant buys more than one recorded env difference costs |

**If the PI prefers the environment stay closer to production, F.1/F.2 stand on their own** —
the baseline works without this variable; it just leaves an expected-drift class to reason
about.

## G. Restated for the record

| | |
|---|---|
| classification | **QUALIFIED PROVISIONING COMPLETE** — **not** a D-3 or deployment PASS |
| C1 / C2 | **NOT re-run** |
| mirror + `manual` placement | **Phase 1 only.** D-3 restores **`UV_OFFLINE=1`** and **`UV_PYTHON_DOWNLOADS=never`** |
| group-writable tree | **accepted qualified exception. No `chmod -R g-w`.** GID, ACLs, world-writable and symlink boundaries **re-checked at D-3 preflight** (§7z of the request) |
| uv cache metadata file | **expected Phase 1 change** |
| production-path claim | **narrowed to the two measured paths and methods** (§E) |
| **D-3** | **separate action, explicit authorisation required.** It must **report the Python and package divergence** and must **never** claim production equivalence |

---

# ADDENDUM 3 — manifest provenance, symlink semantics, and the tightened cache rule

## H. Item 1 — the baseline WAS a fresh host read

> **`D3-phase1-interpreter-baseline.tsv` was generated by a FRESH read of VM24.**
> **It is not reconstructed from prior output.**

```
ssh … woa23c1ro@192.168.2.24 \
  "find <TREE> -printf '%y\t%m\t%U:%G\t%s\t%p\n' | LC_ALL=C sort"  >  baseline.tsv
```

`find` ran **on VM24**; its stdout was streamed straight to the local file. **No local
reconstruction, no reuse of earlier output, no hand-editing.** It therefore does **not** need
the "local reference evidence" label.

**But it is still a point-in-time read**, taken after Phase 1 had already produced the six
generated entries — which is intended: it captures the **stabilized post-Phase-1 state**.

**A fresh D-3 preflight manifest is required anyway**, and not because provenance is in doubt.
Time passes and the tree is writable, so preflight re-takes both manifests by the same
commands and **compares against these digests**. A difference is adjudicated under §J.

## I. Item 2 — manifest semantics for symlinks

**The size-only record was inadequate**, and this fixes it. In the base manifest a symlink's
`%s` is the **length of its link text** — so two different targets of equal length are
indistinguishable. That is not an acceptable identity for a symlink.

### I.1 The companion manifest

| | |
|---|---|
| file | [`D3-phase1-interpreter-symlinks.tsv`](D3-phase1-interpreter-symlinks.tsv) |
| format | `path` TAB **`link text`** (`find -type l -printf '%p\t%l\n'`, `LC_ALL=C` sorted) |
| entries | **1 048** |
| **sha256** | **`d87444abb43bce878fba979e3e37d53c6af73b274b2b73330443e2e03e69d027`** |
| distinct targets | **772** |

**The link TEXT is hashed, not the size.**

### I.2 Boundary verification — all 1 048 links, measured

| check | result |
|---|---|
| resolve **inside** the interpreter tree | **1 048 of 1 048** |
| **escaping** the tree | **0** |
| **reaching a production path** (`/home/odbadmin*`) | **0** |
| unresolvable / dangling | **0** |
| **absolute** link texts | **0** — every link is relative, so the tree is relocatable and cannot silently follow a path outside itself |

### I.3 The rule for D-3

| observed | verdict |
|---|---|
| link text differs from the manifest | **FINDING** |
| link added or removed | **FINDING** |
| `realpath` resolves **outside** the interpreter tree | **FINDING — REJECT** |
| `realpath` reaches **any production path** | **FINDING — REJECT**, and the run stops |
| an **absolute** link text appears | **FINDING** |

**Boundary checks use `realpath`, not the link text** — a relative text can still escape via
`..`, and only resolution reveals it.

## J. Item 3 — the permitted cache set is SIX EXACT PATHS, not a category

**My earlier rule said "a new `__pycache__` dir or `.pyc` file is permitted". That was too
permissive and is REPLACED.** A category-based rule would let an arbitrary `.pyc` appear
anywhere in the tree and be waved through.

**Permitted — these exact paths, relative to the interpreter tree, and nothing else:**

```
1  lib/python3.11/encodings/__pycache__                                        (dir,  775)
2  lib/python3.11/site-packages/_distutils_hack/__pycache__                    (dir,  775)
3  lib/python3.11/encodings/__pycache__/__init__.cpython-311.pyc               (file, 664,  6524)
4  lib/python3.11/encodings/__pycache__/aliases.cpython-311.pyc                (file, 664, 12714)
5  lib/python3.11/encodings/__pycache__/utf_8.cpython-311.pyc                  (file, 664,  2383)
6  lib/python3.11/site-packages/_distutils_hack/__pycache__/__init__.cpython-311.pyc (file, 664, 11999)
```

| observed | verdict |
|---|---|
| one of the six, unchanged | permitted |
| **a `.pyc` or `__pycache__` at any OTHER path** | **FINDING** — not permitted by category |
| one of the six with a **different size, mode or owner** | **FINDING** |
| any non-enumerated content, symlink, owner or mode change | **FINDING** |

**With `PYTHONDONTWRITEBYTECODE=1` (§K) no new `.pyc` should appear at all**, so the six
should remain exactly six — which is what makes this rule enforceable rather than aspirational.

## K. Item 4 — `PYTHONDONTWRITEBYTECODE=1`, confirmed for D-3

| | |
|---|---|
| **set by** | **export in the spawning shell** before `pm2 start`, inherited by the PM2 daemon and its children |
| **NOT set by** | the ecosystem config. `make_staging_override.js` diffs the generated config against production's and **refuses any key not in `EXPECTED`** — the config route would edit the generator, **change the subject**, and force a new subject with fresh batches |
| allowlist | `staging_execute.sh`'s `ALLOWED_ENV` matches only `^WOA23_`, so this variable **cannot** trip it |
| **verified in** | **the gunicorn master AND every worker**, read from `/proc/<pid>/environ` — not assumed to be inherited |
| acceptance | present and equal to `1` on master and on **all** workers; **absent from any worker is a FINDING** |

> **Recorded as an INTENTIONAL RUNTIME DIFFERENCE.** Production does **not** set it. It joins
> the interpreter and package-set divergence as a documented difference between D-3 and
> production, and it is **never** presented as making the two more alike.

## L. Items 5–7 — restated, unchanged

| | |
|---|---|
| production wording | limited to the **two measured paths and methods** — `.pyenv` at maxdepth 3 (`b8badefd…32b1`) and the store by method A (`abe6c212…1806`). **No claim about `/home/odbadmin` as a whole** — it cannot be enumerated from this account (§E) |
| D-3 environment | **`UV_OFFLINE=1`**, **`UV_PYTHON_DOWNLOADS=never`**. **No fallback, no lock edit, no network access** |
| C1 / C2 | **not re-run** |
| **D-3** | **separately unauthorised.** It must report the **Python and package divergence** and must **never** claim production equivalence |

**Phase 1 classification is unchanged: QUALIFIED PROVISIONING COMPLETE.**
