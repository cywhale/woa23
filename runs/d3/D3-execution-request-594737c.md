# D-3 — candidate deployment rehearsal on the real production store: **execution request**

**Offline only. No VM24 contact since the halt. D-3 NOT retried. No PM2 started, no store
symlink created, nothing killed and nothing cleaned up. Production untouched.**

**This document lives in `runs/d3/`, OUTSIDE `git archive <sha> dev2026`, and is committed
AFTER the subject was cut.** That is what lets it name an execution identity without
burning one.

**Not to be executed until reviewed.**

---

## 1. Why the previous run halted

The D-3 attempt on subject `7015a895` reached the driver and died with:

```
store_owner_verdict: command not found
```

The halt was correct and complete: **no production store was opened, no PM2 was started,
no port was bound, no store symlink was created and no case was requested.**

There were **two independent defects**, and each one alone would have been caught by the
other. That is why neither was caught.

| # | defect | where |
|---|---|---|
| 1 | the bootstrap's manifest was **one member** — the driver — so `lib_store_guard.sh` was never delivered | `staging_bootstrap.sh` |
| 2 | the driver sourced the library **conditionally**, so a missing critical guard was silently tolerated | `staging_execute.sh` |

Defect 2 in full:

```bash
_SE_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -r "$_SE_HERE/lib_store_guard.sh" ]; then
  . "$_SE_HERE/lib_store_guard.sh"
fi
```

`[ -r ]` was false, nothing refused, and the run continued for another several hundred
lines until it reached the call site. **A guard that is absent must stop the run at load,
not produce an undefined function that some later branch may or may not reach.**

### 1.1 Why the tests did not catch it

Every existing test ran the driver **from the checkout**, where `lib_store_guard.sh` is
always sitting next to it. The failure was not merely untested — it was
**unrepresentable**: no test could construct a driver without its library, because no test
ever went through the packaging step that omits it.

`test_staging_bootstrap.sh` did worse than miss it. It asserted, as a property to be
preserved:

```
check "  the bootstrap holds ONLY the driver" 1 "$(find "$BOOT" -type f | wc -l)"
```

The assumption that caused the defect was written down as an invariant and guarded.

### 1.2 The fix — the manifest

`staging_bootstrap.sh` now carries a manifest, named **explicitly** rather than globbed:

```bash
DRIVER_MEMBER="dev2026/deploy/staging_execute.sh"
GUARD_MEMBER="dev2026/deploy/lib_store_guard.sh"

BOOTSTRAP_MEMBERS="$DRIVER_MEMBER
$GUARD_MEMBER"
```

A glob would silently start carrying whatever else appeared in `deploy/`, and *whatever is
there* is not a manifest.

Every member now goes through the same sequence the driver alone used to:

| step | check |
|---|---|
| in the archive | exactly **one** member at that path — a duplicate lets a later copy overwrite the verified first |
| in the archive | a **regular file** — not a directory, symlink or hard link |
| in the archive | **sha256 of the member's bytes**, taken with `tar -xO` |
| on disk | extracted to `$BOOT/deploy/<basename>`, `chmod 700` |
| on disk | **not a symlink**, **is a regular file** |
| on disk | **sha256 re-taken and compared** to the in-archive digest |

and then, after the loop, against the filesystem rather than against an exit code:

```bash
DRIVER_LOCAL="$BOOT/deploy/${DRIVER_MEMBER##*/}"
GUARD_LOCAL="$BOOT/deploy/${GUARD_MEMBER##*/}"
[ -f "$DRIVER_LOCAL" ] || die "the driver was not delivered to $BOOT/deploy"
[ -f "$GUARD_LOCAL" ]  || die "the store guard library was not delivered to $BOOT/deploy" ...
```

The loop body runs in a subshell, so its success is **re-established** here rather than
assumed. That is the same subshell trap that made `test_requests.sh` leak 15 processes
while reporting exit 0.

### 1.3 The fix — a hard dependency, loaded only from the bootstrap

```bash
_SE_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_SE_GUARD="$_SE_HERE/lib_store_guard.sh"
[ -L "$_SE_GUARD" ] && die "REFUSING: the store guard library is a SYMLINK: $_SE_GUARD" ...
[ -e "$_SE_GUARD" ] || die "REFUSING: the store guard library is missing: $_SE_GUARD" ...
[ -f "$_SE_GUARD" ] || die "REFUSING: ... is not a regular file: $_SE_GUARD"
[ -r "$_SE_GUARD" ] || die "REFUSING: ... is not readable: $_SE_GUARD"
. "$_SE_GUARD" || die "REFUSING: the store guard library failed to load: $_SE_GUARD"
command -v store_owner_verdict >/dev/null 2>&1 || die \
  "REFUSING: $_SE_GUARD loaded but does not define store_owner_verdict." ...
```

Three things about this that are not incidental:

1. **The symlink test comes first.** `-e` follows links, so a *broken* symlink fails `-e`
   and would be reported as "missing" — which is not what is on disk. A link is a link
   whether or not it resolves, and the refusal has to say so.
2. **Loading is not the same as being usable.** A truncated or edited library sources
   cleanly and defines nothing, so the symbol itself is checked. `[ -r ]` was true in the
   D-3 case shape too; readability was never the property that mattered.
3. **`$_SE_HERE` only.** No search path and no checkout fallback. If the bootstrap did not
   deliver it, nothing else may satisfy it — including a driver that happens to be run
   while standing inside the source tree.

### 1.4 The refusal happens before anything exists

| line in `staging_execute.sh` | what |
|---|---|
| 47 | the grant check (`WOA23_PM2C_GRANTED`) |
| **60–90** | **the library dependency — the refusal** |
| 217 | the first `mkdir` |
| later | workdir, `PM2_HOME`, port, store symlink, `pm2 start`, case request |

The grant still comes first: sourcing a file is reading code, and it is not done before
the run is authorised. Nothing that creates state comes before either.

### 1.5 A third defect, found while testing the second

`archive_member_problem` matched a member as either `" <name>$"` or `" <name> -> "`. `tar`
prints a **directory with a trailing slash**, so a directory standing where the library
should be matched neither, and was reported as *"the archive contains no member"*.

Fail-closed, so nothing unsafe followed from it — but it **described the archive
incorrectly on the way out**, and a refusal that misreports what is on disk is how the
next investigation starts in the wrong place. It now matches `" <name>/$"` too and says
`DIRECTORY`.

---

## 2. The tests — the real packaging shape, not the checkout

`dev2026/scripts/test_bootstrap_delivery.sh`, **100 assertions**, new. Every case goes
through the shape that actually failed:

```
a tar archive  ->  a clean bootstrap path OUTSIDE the checkout
               ->  extraction by staging_bootstrap.sh itself
               ->  the driver EXECUTED from that bootstrap directory
               ->  with the working directory outside the checkout
```

| group | what it proves |
|---|---|
| **A** (7) | `archive_member_problem` against **real `tar -tvf` listings** of real archives — unique/regular accepted; missing, symlink, duplicate, directory each refused **by name** |
| **B** (23) | delivery end to end: both members present, both **byte-identical to the archive**, neither a symlink; and archives that are missing / symlink / duplicate / directory in the library member are each refused **with no staging root, workdir, `PM2_HOME` or store symlink created** |
| **B′** (7) | the archive-to-disk hash comparison is **not vacuous**, and is made twice |
| **C** (30) | the driver's hard dependency, run from `$BOOT/deploy` |
| **D** (10) | `store_owner_verdict` at every uid combination, from the **delivered** copy |
| **E** (10) | synthetic mode unchanged |
| **F** (13) | the manifest names both members and only those, and **cannot deliver two members to one name** |

The suite reports `all passed (100 assertions)` — the campaign's shape, so its count can be
summed with the other 55. Its `hasnt` helper refuses an **empty** haystack, because
"the refusal did not appear" is trivially true of output that does not exist.

### 2.1 The comparison that could have been permanently vacuous

Nothing in a normal run makes the in-archive digest and the on-disk digest differ, so that
comparison could have been broken since the day it was written and never once shown it. A
`tar` shim earlier on `PATH` corrupts the **second** read of the library member — the
extraction — leaving the first — the expected hash — intact:

```
B24  an extracted member that differs from the archive is refused
B25  ... naming the mismatch
B26  ... and created no staging root
B27  the shim really did reach the extraction and corrupt it
```

**B27 exists because B24 passed vacuously in the first draft.** The case had not created
the bootstrap's parent directory, so the bootstrap refused for *that* instead, returned
non-zero, and B24 was satisfied without the corruption ever being reached. B27 counts the
shim's firings and fails if it did not reach the extraction.

### 2.2 No source tree can satisfy the import

| case | working directory | result |
|---|---|---|
| C1–C8 | outside the checkout | exit 2, `store guard library is missing`, **no** staging root / workdir / store symlink / `PM2_HOME`, never `command not found`, no `pm2 start` |
| C9–C10 | **the checkout root**, where the real library exists | still exit 2, still the same refusal |
| C11–C12 | **`dev2026/deploy/` itself**, where a bare-name source would resolve | still exit 2, still the same refusal |

### 2.3 Every way a delivered library can be wrong

| case | condition | refusal |
|---|---|---|
| C1 | absent | `store guard library is missing` |
| C13 | a symlink — even one pointing at a **good** library | `is a SYMLINK` |
| C13b | a **broken** symlink | `is a SYMLINK`, **not** "missing" |
| C16 | a directory in its place | `not a regular file` |
| C19 | `chmod 000` | `not readable` (skipped when running as root, and said to be) |
| C21 | **modified** — present, readable, regular, sources cleanly, defines nothing | `does not define store_owner_verdict` |
| C24 | intact, valid command line | **no** library refusal, **no** `command not found`, execution continues to a later guard |

### 2.4 What these tests do NOT reach, stated rather than glossed

The ownership verdict is computed inside the stage phase's real-store branch, which sits
**after** the tree is extracted and the venv is built, and whose **first act** is to refuse
a non-GNU `find(1)`/`stat(1)`. This host has neither a staged tree nor GNU coreutils, so
**no local test executes that line, and none claims to.**

What is proved locally is the thing that actually failed — that the library is delivered,
is verified byte-for-byte against the archive, and that its absence or corruption stops
the run at load. The verdict function's own behaviour is exercised in full, from the
delivered copy, in group D:

| store uid | this account | expected | verdict | rc |
|---|---|---|---|---|
| 1000 | 994 | 1000 | `ok` | 0 |
| **994** | 994 | 1000 | **`self`** | 5 |
| 0 | 994 | 1000 | `root` | 6 |
| 1001 | 994 | 1000 | `unexpected` | 7 |
| *(unreadable)* | 994 | 1000 | `unreadable` | 3 |
| `abc` | 994 | 1000 | `nonnumeric` | 4 |
| 0 | **0** | 1000 | **`self`**, not `root` | 5 |
| 994 | 994 | **994** | **`self`**, not `ok` | 5 |

The last two are the ordering: the most dangerous case is never described as merely
surprising, and never as acceptable.

### 2.5 Suites changed rather than added

| suite | change |
|---|---|
| `test_real_store_mode.sh` | its two **source-grep** assertions (`grep -c '\. .*lib_store_guard'`) are replaced by ones that check the dependency has not gone optional again; the behaviour now lives in `test_bootstrap_delivery.sh` |
| `test_staging_bootstrap.sh` | fixtures carry the **whole manifest**; `the bootstrap holds ONLY the driver` → **`exactly the manifest's members`**; the extraction assertions follow the loop rather than one hard-coded member |

---

## 3. Subject and provenance

```
subject   594737c1101e812552bcb940b668b2c483fd94d3
archive   b68c60ec9fba9edd06198d9ec8c0666511b33ef913388d5ac12720e222465eda
files     255
file-list ade819f1a8659febd26b08a2287ab426919f9ad4629558b18fac1382d49860f4
```

The archive is `git archive 594737c1101e812552bcb940b668b2c483fd94d3 dev2026`. The
file-list is computed with the **driver's own `tree_filelist`** — `LC_ALL=C` throughout,
per-file **content** digests as `<sha256>  <path>`, `sort -u`, then sha256 of that list.

**The method was verified against the previous subject before being used here.** Recomputing
`7015a895` with this procedure reproduces its recorded values exactly:

| | recomputed | recorded |
|---|---|---|
| archive | `d577315d2fc53353ac0fd1a73b2086d96b21b96406067053089cc6f2369fc186` | same |
| files | 254 | same |
| file-list | `45b27a6fc3b286655a21a71fde52f7adff6a34f60880e0c83bb0ab7fac017aa4` | same |

An ad-hoc `find | xargs sha256sum` I reached for first gave `a4e2323…` for that same
subject — it differs in path prefix and sort order. It is not the method, and the digest
above is not it.

**Superseded, in order, NOT back-filled:** `ccfca934` → `9269dacb` → `fae5a417` →
`8bb7f2c8` → `19aabf02` → `7015a895` → `4a05f6b7` → `5800a283` → `4a043cd1` →
**`594737c1`**. Their batches are **records, not evidence**, and the void `5ae8f49` set
stays quarantined.

**Three subjects were cut and superseded during this offline pass**, each because the
review that followed found something. None of their numbers appear anywhere in this
document.

| subject | superseded because | its batches |
|---|---|---|
| `4a05f6b7` | `DRIVER_MEMBER` assigned and never read (§3.2) | stopped part-way through batch 1; set aside unread |
| `5800a283` | a manifest could deliver two members to one name (§3.3) | stopped part-way through batch 1; set aside unread |
| `4a043cd1` | the new suite's summary line was unreadable to any tally (§3.4) | **ran to completion, clean**; §3.4 reports what it showed. A separate, earlier run on the same subject **failed** — §3.5 |

I cut before completing the review on the first two, which is the wrong order and cost two
batch cycles. The third was cut only after its own batch output revealed the defect.

### 3.1 What changed from `7015a895`

Exactly five files, and nothing else:

```
M  deploy/staging_bootstrap.sh          the manifest, the loops, DIRECTORY, collisions
M  deploy/staging_execute.sh            the hard dependency, symlink-before-existence
A  scripts/test_bootstrap_delivery.sh   new, 100 assertions
M  scripts/test_real_store_mode.sh      source greps replaced
M  scripts/test_staging_bootstrap.sh    manifest fixtures, "ONLY the driver" retired
```

### 3.2 Why `4a05f6b7` was superseded — a dead constant

The first version of the manifest repeated the two member paths as literals:

```bash
DRIVER_MEMBER="dev2026/deploy/staging_execute.sh"        # assigned, never read
BOOTSTRAP_MEMBERS="dev2026/deploy/staging_execute.sh
dev2026/deploy/lib_store_guard.sh"
```

`DRIVER_MEMBER` was left **assigned and never read** — a constant that looks like it
decides where the driver comes from, while the real decision is made by a literal
elsewhere. The handover `exec` and the post-loop existence assertions used literals too.

That is not a behavioural defect, and it would not have affected a D-3 run. It is,
however, precisely the class this campaign keeps paying for: the inverted ownership guard,
the vacuous BSD fingerprint, the presence-not-behaviour tests and the `check "the bootstrap
holds ONLY the driver"` assertion were all things that **looked authoritative and were
not**. Leaving one in the file that exists to fix that class was not acceptable, so it was
fixed — and because the fix changes bootstrap code, it required a new subject.

`BOOTSTRAP_MEMBERS` is now built from `DRIVER_MEMBER` and a new `GUARD_MEMBER`, and the
delivered paths are derived from them, so those names are the single place the paths are
stated. A sweep for variables assigned and never read across
`staging_bootstrap.sh`, `staging_execute.sh` and `lib_store_guard.sh` now returns **none**.

### 3.3 Why `5800a283` was superseded — a manifest could deliver two files to one name

Members are extracted to `$BOOT/deploy/<basename>`. Two members with the same **basename** —
`dev2026/deploy/x.sh` and `dev2026/scripts/x.sh` — would therefore both be written to
`$BOOT/deploy/x.sh`. The second would **silently overwrite** the first, and the extraction
hash check would **pass anyway**, because it re-reads exactly the bytes it has just written.

`archive_member_problem` cannot see this. Both members are perfectly unique **in the
archive**; the collision is created by the flattening, not by the archive. So it is now
checked where the flattening is decided:

- **step 0**, before any path work: `manifest_basename_collision` refuses a manifest that
  cannot be delivered without one member overwriting another, and an empty manifest;
- **inside the extraction loop**: nothing may be written over, as defence in depth.

The same pass closed a second gap. The post-loop check re-established that both members
were **present** outside the subshell — but not that they were **correct**. "The file is
there" and "the file is the right one" are different claims, and only the first survived.
The parent now re-reads both digests from the archive, so **nothing in the delivery depends
on a subshell's exit status** — the mechanism that leaked 15 processes out of
`test_requests.sh` while it reported exit 0.

Neither defect would have changed the outcome of a D-3 run with today's two-member
manifest. Both were fixed rather than noted, because the whole point of this change is that
delivery stops being something that happens to be right.

Delivered-member digests **in this archive**:

| member | sha256 |
|---|---|
| `deploy/staging_execute.sh` | `a69b12658c9ca6d76f90bb7afc3d46ab0626582534ee670cb92b2c190fc52d2b` |
| `deploy/lib_store_guard.sh` | `dc82e80f62177805d6e64500adebdaf435fca0d1512fd283d264a410ac23d23d` |

The library's digest is **unchanged since `7015a895`**: the ownership decision itself was
already correct and is not being re-litigated by this change. What changed is that it now
arrives, and that the run stops if it does not.

### 3.4 Why `4a043cd1` was superseded — a count nobody could add up

`4a043cd1`'s three batches ran to completion and were **clean**: 3 of 3, 56 suites each,
**0 non-zero**, HEAD checked before and after every batch, `tracked dirty` 0, and the
sentinel written. Reading its own output is what condemned it.

**The new suite printed its result in a shape nothing in this campaign can tally.** Fifty-
five suites end with `all passed (N assertions)` or `N tests, M assertions, K failed`;
`test_bootstrap_delivery.sh` ended with `=== 100 passed, 0 failed ===`. My first tally of
that clean run came to **4667**. The true figure was **4791**. That is the same undercount
that once turned 4528 into 4382, and it was in a file whose whole purpose is stopping
things that look right from being taken as right.

**The batch log's displayed line is not a reliable assertion source for ANY suite**, and
that is worth stating separately, because fixing my file does not fix it. The runner shows
each suite's LAST line: `test_staging_store.py` prints the standard summary and then a
caveat, so the caveat is what is displayed and its 24 assertions are invisible too. Those
24 are the rest of the 4667/4791 gap. **The total must be summed from each suite's own
stdout**, and that is how §3.5's figure is obtained. `test_staging_store.py` is
pre-existing and was not touched.

The same pass found a second defect in the suite: **`hasnt` passed on empty output.** Every
`hasnt` in the file asserts that a refusal did *not* appear in output that certainly
exists — so a command that produced nothing at all would satisfy nine assertions at once
while checking nothing. It now fails an empty haystack, and that guard is itself tested
against an empty input rather than assumed.

### 3.5 A batch run that failed, and why it is reported

`4a043cd1` also carries a **failed** batch set, kept as a record and not merged into
anything:

| batch | exit | non-zero suites |
|---|---|---|
| 1 | **1** | **28** |
| 2 | 0 | 0 |
| 3 | 0 | 0 |

`non-zero total: 29`, and the driver's last line was **`NO SUCCESS SENTINEL — this run is
not complete.`** The sentinel refused, which is what it is for.

**Cause: mine, and it was a setup error, not a defect in the subject.** I created the
worktree and started batches without running `uv sync`. All 27 Python suites exited **127
in 0 seconds** with `run_suites.sh: line 134: .venv/bin/python: No such file or directory`,
and `test_clean_archive.sh` failed 7/21 on the same missing interpreter. A suite inside
batch 1 built the venv as a side effect, which is why batches 2 and 3 were clean — and why
"two clean batches out of three" would have been a completely misleading way to describe
it. HEAD matched the subject at every pre- and post-check and `tracked dirty` was 0
throughout, so nothing about the subject is implicated.

The venv is now provisioned **deliberately, before batch 1, and verified**, and that step
is stated here because it is a precondition of the batch evidence rather than an incidental
detail.

### 3.6 Three serial batches at this subject

The venv was provisioned with `uv sync` **before batch 1** and verified (`Python 3.11.14`).
The worktree is detached at the subject.

| batch | HEAD, as the batch recorded it | tracked dirty | untracked | suites | non-zero | assertions | exit |
|---|---|---|---|---|---|---|---|
| 1 | `594737c1…` | **0** | **0** | **56** | **0** | **4791** | **0** |
| 2 | `594737c1…` | **0** | **0** | **56** | **0** | **4791** | **0** |
| 3 | `594737c1…` | **0** | **0** | **56** | **0** | **4791** | **0** |

```
batches completed : 3 of 3
non-zero total    : 0
postconditions    : yes
WOA23_BATCH_COMPLETE subject=594737c1101e812552bcb940b668b2c483fd94d3 \
  label=d3-594737c token=d3-594737c-10618-bsd-1788167360 batches=3 nonzero=0
```

HEAD was checked **before and after** every batch and matched the subject each time. The
per-suite `(suite, exit)` results are **identical across all three batches**.

**How the assertion figure is obtained.** Summed from **each suite's own stdout**, not from
the runner's displayed line — see §3.4 for why the displayed line undercounts. All 56
suites report a countable total; none is missing. The count that would be read off the
batch log is **4667**, and it is wrong.

**`untracked = 0`, and the `7015a895` request's `untracked = 1` was described wrongly.**
I said there that the 1 was `dev2026/.venv`. It cannot have been: `dev2026/.gitignore:2`
contains `.venv/`, so the venv is *ignored*, and `run_suites.sh:100` counts with
`--untracked-files=all`, which does not list ignored paths. What that 1 actually was, I do
not know, and I am not going to guess it a second time. In this run the count is 0 and
`git status --porcelain --untracked-files=all` in the worktree is empty.

**The sentinel was verified independently**, not taken from the driver's own closing line:

```
sentinel_verify <log> 594737c1… d3-594737c d3-594737c-10618-bsd-1788167360   -> rc=0
sentinel_verify <log> 4a043cd1… d3-594737c d3-594737c-10618-bsd-1788167360   -> rc=4
```

The second call is there because a verifier that accepts everything proves nothing: the
same log, checked against the **superseded** subject, is refused.

**Run identity** is `(pid, starttime)`, recorded as
`pid=10618 starttime=bsd-1788167360`. `bsd-` is this host's path and is a documented local
limitation; **VM24 is Linux and takes the `lin-<ticks>` path** — see §6.

---

## 4. Identity — `dep3q` / `19343`

| | |
|---|---|
| label | **`dep3q`** |
| port | **`19343`** |
| app | `woa23-dep3q-candidate` |
| staging root | `~/woa23-dep3q` |
| workdir | `~/woa23-dep3q-work` |
| `PM2_HOME` | `~/woa23-dep3q-pm2` |
| tmpdir | `~/tmp-dep3q` |
| bootstrap | `~/d3boot-dep3q` *(a bootstrap path is not part of the run's identity)* |

**Selected only after the subject was fixed, and named only here.**

| check | result |
|---|---|
| `dep3q` anywhere in the subject tree (all 255 files, text and binary) | **0** |
| `19343` anywhere in the subject tree, **as a substring**, floats included | **0** |
| `19343` in `scripts/ports_used.tsv` at the subject | **0** (the ledger ends at `19157`) |
| `dep3q` or `19343` in any commit in any branch, ever | **0** |

`19337` was the first candidate and was **rejected**: it occurs inside two files in the
subject tree. That is the same reason `19113` and `19136` were rejected earlier in this
campaign, and the substring test is why it was caught.

### 4.1 `dep3m` / `19301` is PARTIALLY CONSUMED and is not reused

The halted attempt got as far as creating its staging root and workdir before the driver
died. Those exist on VM24 **and are being preserved as they are**:

| path | state |
|---|---|
| `~/d3stage-7015a89` | **present** |
| `~/d3boot-7015a89` | **present** — containing **only `staging_execute.sh`**, which is the physical evidence of defect 1 |
| `~/woa23-dep3m` | **present** |
| `~/woa23-dep3m-work` | **present** |
| `~/woa23-dep3m-pm2` | absent |
| `~/tmp-dep3m` | absent |
| `~/woa23-dep3m/store` | absent — **no store symlink was ever created** |
| port `19301` | **unbound**, and **never bound** — `RETIRED-NEVER-BOUND` |
| dep3m PM2 daemons | **0** |

**Nothing has been deleted, killed or cleaned up, and none of it is reused.** Cleanup
requires separate authorization; this request does not ask for it.

### 4.2 Old identities, none reused

`dep3a`, `dep3b`, `dep3c`, `dep3d`, `dep3f`, `dep3h`, `dep3j`, `dep3k`, `dep3m`, `dep3n`,
`dep3p` and every port they were named with (`19161`, `19187`, `19211`, `19229`, `19259`,
`19301`) are **consumed** and are not proposed again. `dep3n` and `dep3p` were burned
merely by being listed as alternatives, which is why alternatives are no longer listed.

---

## 5. Store — real, read-only, fail closed

Unchanged from the reviewed `7015a895` request, and re-verified by
`test_real_store_mode.sh` (82 assertions) and by group D above.

| | |
|---|---|
| mode | `--store-mode real-readonly --real-store /home/odbadmin/python/woa23/data` — **an explicit named mode**, never a silent reinterpretation of the synthetic path |
| expected owner | uid **1000**; execution account uid **994** |
| the only write | `ln -s "$REAL_STORE" "$STORE"`, and `$STORE` is inside the **staging root** |
| never | `mkdir`, `rm`, `touch`, `chmod`, `chown` — **not even a write probe**, because probing by writing is a write |
| tooling | GNU `find -printf` / `stat -c` are **required**; their absence stops the run rather than returning 0 from a failed command |
| path | one literal, compared **and** `readlink -f`-resolved — every lexical path guard in this campaign has been walked around with a link at least once |

Production store fingerprint at the halt:
`abe6c21221b5081eb352a1a549c9d1fd6399c74b1f06b8c2ce09952828c61806` — **unchanged**.

---

## 6. What this run may be called

**"Candidate deployment rehearsal using the real production store."**

It is **not** production equivalence, **not** a deployment PASS, **not** data-path
correctness PASS, **not** TLS validation, and **not** an A11 gate.

Standing limits, carried forward:

- **Runtime divergence.** Staging builds Python **3.11.14**; production runs **3.11.4**.
  A rehearsal on 3.11.14 does not validate 3.11.4.
- **PID-reuse protection is Linux-only in effect.** The canonical run identity is
  `(pid, starttime)`; on Linux it is `lin-<ticks>` from `/proc/<pid>/stat` field 22. This
  host is BSD and uses `bsd-<epoch>`, a documented **local tooling limitation**. No
  cross-platform full PID-reuse protection is claimed. **VM24 is Linux and must take the
  `lin-<ticks>` path.**
- **Node was recorded** in probes A/B/C — `/usr/local/bin/node` → `/usr/bin/node`,
  v22.14.0, `root:root` 755, 120177224 bytes. It has **not been re-verified in a D-3
  execution**, because no D-3 execution has reached that point.

---

## 7. Status

| | |
|---|---|
| subject | `594737c1101e812552bcb940b668b2c483fd94d3`, three clean batches, sentinel verified |
| identity | `dep3q` / `19343`, absent from the subject, named only here |
| VM24 | **not contacted since the halt** |
| D-3 | **not retried** |
| PM2 | **not started** |
| store symlink | **not created** |
| cleanup / kills | **none** |
| production | **untouched** |
| this request | **awaiting review; not to be executed** |
