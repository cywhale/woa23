# D-3 — candidate deployment rehearsal on the real production store: **execution request**

**Offline only. No VM24 contact. D-3 request prepared offline; D-3 execution not started or
executed. No PM2 started, no store symlink created, no staging, workdir or port created.
Production unchanged. C1/C2 not re-run. No VM24 or production cleanup: the only cleanup
performed was of local batch-tooling processes on the development machine (§7.1).**

**This document lives in `runs/d3/`, OUTSIDE `git archive <sha> dev2026`, and is committed
AFTER the subject was cut.** That is what lets it name an execution identity without
burning one.

**Not to be executed until reviewed.**

**What this round changed.** The review accepted the bootstrap/library delivery fix and
found one provenance defect in the package: the batch log presented **4667** as the
assertion total when the true figure was **4791**. §3A is that fix — a machine-readable
suite summary contract, parsed and fail-closed. §§1–2 are the delivery fix as accepted;
its three files are byte-for-byte unchanged and are reproduced here only so this request
stands alone.

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
subject   ab670fb0c04d447ea084f6da661af1c21e4cb132
archive   39a4f9b67f0e7cf0687fbbf05bcfd3dc932547f5771ed41bd8cf9d8b96834e34
files     258
file-list 86220a49553b1c59d0e9d5791a0474690cc6e76a198c0cd8927251241b50af7b
```

The archive is `git archive ab670fb0c04d447ea084f6da661af1c21e4cb132 dev2026`. The
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
`8bb7f2c8` → `19aabf02` → `7015a895` → `4a05f6b7` → `5800a283` → `4a043cd1` → `594737c1`
→ **`ab670fb0`**. Their batches are **records, not evidence**, and the void `5ae8f49` set
stays quarantined.

`594737c1` was superseded by the review: its three batches were clean and its delivery fix
is accepted, but its batch log reported the wrong assertion total. §3A is that fix.

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

### 3.1 What changed, in two rounds

**Round 1 — the delivery fix, accepted (§§1–2).** Five files:

```
M  deploy/staging_bootstrap.sh          the manifest, the loops, DIRECTORY, collisions
M  deploy/staging_execute.sh            the hard dependency, symlink-before-existence
A  scripts/test_bootstrap_delivery.sh   new, 100 assertions
M  scripts/test_real_store_mode.sh      source greps replaced
M  scripts/test_staging_bootstrap.sh    manifest fixtures, "ONLY the driver" retired
```

**Round 2 — the summary contract (§3A).** Sixty files: three new, and 57 touched only where
they end.

```
A  scripts/lib_suite_summary.sh         the contract: emitter and reader, shell
A  bench/suite_summary.py               the same contract, Python
A  scripts/test_summary_contract.sh     new, 92 assertions
M  scripts/run_suites.sh                parses the contract; fails closed; derives totals
M  scripts/test_tracked.sh              a second exit path that emitted no contract line
M  scripts/test_run_suites.sh           its fixture now obeys the contract
M  bench/test_staging_store.py          the caveat moved BEFORE the summary
M  ... 53 further suites                their ending replaced by one `suite_summary` call
```

**The three delivery files are byte-for-byte unchanged between `594737c` and this subject**,
which is what lets §§1–2 stand as reviewed:

| member | sha256 |
|---|---|
| `deploy/staging_execute.sh` | `a69b12658c9ca6d76f90bb7afc3d46ab0626582534ee670cb92b2c190fc52d2b` |
| `deploy/lib_store_guard.sh` | `dc82e80f62177805d6e64500adebdaf435fca0d1512fd283d264a410ac23d23d` |
| `deploy/staging_bootstrap.sh` | `44ede13cf010411d3d3a1abf50ac9811c4d14cc485d8be50411b217f8a1decee` |

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

| batch | HEAD, as the batch recorded it | tracked dirty | untracked | suites | non-zero | assertions | contract violations | exit |
|---|---|---|---|---|---|---|---|---|
| 1 | `ab670fb0…` | **0** | **0** | **57** | **0** | **4883** | **0** | **0** |
| 2 | `ab670fb0…` | **0** | **0** | **57** | **0** | **4883** | **0** | **0** |
| 3 | `ab670fb0…` | **0** | **0** | **57** | **0** | **4883** | **0** | **0** |

```
batches completed : 3 of 3
non-zero total    : 0
postconditions    : yes
WOA23_BATCH_COMPLETE subject=ab670fb0c04d447ea084f6da661af1c21e4cb132 \
  label=d3-ab670fb token=d3-ab670fb-93411-bsd-1788251833 batches=3 nonzero=0
```

HEAD was checked **before and after** every batch and matched the subject each time. The
per-suite `(suite, exit)` results are **identical across all three batches**.

**The assertion figure now comes from the runner itself**, derived from the suite summaries
— it is no longer something I compute afterwards from per-suite stdout because the log
cannot be trusted. Each batch printed:

```
=== TOTAL: 57 | NON-ZERO: 0 ===
=== ASSERTIONS: 4883 | ASSERTION FAILURES: 0 | CONTRACT-VIOLATIONS: 0 ===
```

It was nevertheless **cross-checked independently**, by summing each of the 57 suites' own
final contract lines from their stdout: **4883 in all three batches, with no suite lacking a
contract line.** Two sources, computed differently, agreeing — which is the thing the old
single source could not offer.

**Assertion accounting, stated without ambiguity:**

| | suites | assertions |
|---|---|---|
| the 56 original suites | 56 | **4791** |
| `test_summary_contract.sh`, new in this subject | 1 | **92** |
| **this subject's total, and the current batch total** | **57** | **4883** |

**All three batches of this subject independently report 4883.** Batch 1, batch 2 and batch
3 each printed `ASSERTIONS: 4883`, and each was separately confirmed at 4883 by summing its
own 57 per-suite contract lines.

**4791 is NOT the current batch total.** It is the historical figure for the 56 suites as
they stood at subject `594737c` — the number the old display under-reported as 4667 — and it
appears in this document only in that historical role and as the target of the positive
control in §3A.5. The current total is **4883**.

The two suites that used to be invisible now appear with their counts:

```
exit=0   test_staging_store.py         1s  all passed (24 assertions)    [24 assertions]
exit=0   test_bootstrap_delivery.sh    2s  all passed (100 assertions)   [100 assertions]
exit=0   test_summary_contract.sh     29s  all passed (92 assertions)    [92 assertions]
```

**The preserved test families all ran and all passed** (requirement 11):

| suite | assertions |
|---|---|
| `test_bootstrap_delivery.sh` | 100 |
| `test_real_store_mode.sh` | 82 |
| `test_staging_bootstrap.sh` | 106 |
| `test_sentinel.sh` | 77 |
| `test_staging_entry.sh` | 148 |
| `test_c2_driver.sh` | 90 |

**The sentinel was verified independently**, not taken from the driver's closing line:

```
sentinel_verify <log> ab670fb0… d3-ab670fb d3-ab670fb-93411-bsd-1788251833   -> rc=0
sentinel_verify <log> 594737c1… d3-ab670fb d3-ab670fb-93411-bsd-1788251833   -> rc=4
```

The second call is there because a verifier that accepts everything proves nothing: the same
log, checked against the **superseded** subject, is refused.

**Run identity** is `(pid, starttime)`: `pid=93411 starttime=bsd-1788251833`. `bsd-` is this
host's path and a documented local limitation; **VM24 is Linux and takes the `lin-<ticks>`
path** — see §6.

**`untracked = 0`, and the open question from the `594737c` request is now answered.** I had
said there that its `untracked = 1` was `dev2026/.venv`, then corrected that to "I do not
know". It is `dev2026/scratchpad/` — a local scratch directory that is untracked and not
ignored. The verification sweep in the main checkout counted **481** untracked files, all of
them under `dev2026/scratchpad/`; in the detached worktree the batches use, the count is
**0**, which is what the table records.

## 3A. The summary contract — the defect this round fixes

### 3A.1 What was wrong

**The batch runner did not parse anything.** It displayed each suite's LAST stdout line, and
the totals were read off that display. That is looking, not parsing, and it is wrong in a
way that cannot announce itself:

| suite | what it printed last | cost |
|---|---|---|
| `test_staging_store.py` | a **caveat**, printed after its summary | **24** assertions invisible |
| `test_bootstrap_delivery.sh` | `=== 100 passed, 0 failed ===`, a private shape | **100** assertions invisible |

The log therefore reported **4667** in the position where a reader expects the total, when
the total was **4791**. A count that is wrong is worse than a count that is absent, because
nobody goes looking for it.

### 3A.2 The contract

```
ASSERTIONS=<n> FAILED=<m>
```

One line, **last**, **exactly once**. Defined in one place per language —
`scripts/lib_suite_summary.sh` and `bench/suite_summary.py` — and the two emitters are
asserted **byte-identical**, because the runner parses one format and does not know which
language produced it. It had become three formats precisely because it lived in 56 places.

The prose line is still printed for a person reading the log. It is **not** what is parsed.
Making one line serve both purposes is the whole of the original defect.

**The exit status is DERIVED from the count**, never passed in beside it. There is no
argument a suite can supply to claim `FAILED=3` and exit 0, so that combination cannot be
produced by accident.

### 3A.3 All 56 suites migrated, and nothing lost

29 shell, 27 Python. **Every suite's emitted count was checked against the per-suite figures
recorded from `594737c`: zero mismatches.** That is what makes "the migration lost no
assertion" a measurement rather than a hope.

`test_staging_store.py`'s caveat now precedes its summary. `test_tracked.sh` had a **second
exit path** — tree-only mode, taken when no git repository sits above the tree — that
printed its own summary and would have emitted no contract line at all.

### 3A.4 The runner fails closed

| condition | result |
|---|---|
| no summary line | refused |
| malformed (`ASSERTIONS=ten`, lowercase, trailing text, negative, comma-separated…) | refused |
| **duplicated** — even two identical lines | refused |
| **summary not last** — a caveat, a note, even a blank line after it | refused |
| `FAILED` > `ASSERTIONS` | refused |
| non-numeric exit status | refused |
| `FAILED=0` but exit ≠ 0 | refused |
| `FAILED=n>0` but exit 0 | refused |

**A refused suite contributes NOTHING to the total and fails the batch.** It is never
skipped with its number quietly absorbed — which is exactly what 4667 was.

```
=== ASSERTIONS: 4883 | ASSERTION FAILURES: 0 | CONTRACT-VIOLATIONS: 0 ===
```

An honestly failing suite is **not** a contract violation: it reports `FAILED=n`, exits
non-zero, and its assertions **are** counted. The two are distinguished, and both are
tested.

### 3A.5 The tests — `test_summary_contract.sh`, 92 assertions

| group | what it proves |
|---|---|
| **1** (16) | the shell emitter: prose then contract, `ASSERTIONS` is the TOTAL, non-numeric and `FAILED>ASSERTIONS` refused, the exit status derived |
| **2** (7) | the Python emitter, and that it is **byte-identical** to the shell one |
| **3** (24) | the reader, against real files: normal, caveat-after, blank-after, missing, **eight** malformed shapes, duplicated ×3, mismatch both ways, over-count, bad rc, missing file |
| **4** (25) | **the REAL runner**, executed against a fixture tree of deliberately broken suites |
| **5** (10) | the positive control: the 56 historical counts must sum to **4791** |
| **6** (7) | every real suite is wired to the contract; the format is defined once |

**Group 4 is the one that matters.** The emitter and the reader can both be perfect while
nothing calls them, so the fixture tree holds the real `run_suites.sh` and the real library
with fake suites beside them; the runner derives its own root from `BASH_SOURCE`, so it
globs the fixture and nothing of the real tree. Each broken kind is run **alone**, and the
assertions check not only that the batch fails but that **the violator's assertions are not
added to the total** — `ASSERTIONS: 0`, not `ASSERTIONS: 24`.

**Group 5 is the positive control you asked for.** The 56 per-suite counts recorded from
`594737c`, summed through the corrected tally, must come to **exactly 4791**; the batch must
exit 0; and `4667` must never appear. Its non-vacuity case displaces **one** summary with a
caveat and requires the total to fall by exactly that suite's count and the batch to fail —
which is precisely what happened silently at 4667, except that nothing failed.

### 3A.6 Three defects of mine, caught while writing this

| # | defect | how it would have shown |
|---|---|---|
| 1 | `set --` in the runner overwrote the **positional parameters** | `run_suites.sh procs cli` would have run **everything** from repeat 2 onward |
| 2 | a test helper set a variable inside `$( )` | the same subshell trap that leaked 15 processes out of `test_requests.sh` |
| 3 | the positive control's **56 numbers were written from memory** | they summed wrong and the assertion caught it |

Defect 3 is the one worth naming plainly: I put invented data into a test. It is now
transcribed from the recorded run, and the file says so in a comment, because invented data
that happens to sum correctly would have been worse than invented data that does not.

### 3A.7 Requirements 9 and 10 cannot both hold literally

Adding tests adds assertions, so a live batch cannot both gain the required tests and still
total exactly 4791. I have read **10 as the arithmetic invariant**: the 56 recorded counts,
summed through the corrected tally, come to exactly 4791 — asserted against fixtures in
group 5, where 4791 is the **expected value of a control**, not a claim about this subject's
batches. The live total is reported separately, with the difference fully attributable:

```
4791   the 56 original suites, unchanged in count
+ 92   test_summary_contract.sh, new in this subject
-----
4883   THIS SUBJECT'S TOTAL — what all three batches report
```

To be explicit, because the whole defect was a wrong number in the position of a total:
**the current batch total is 4883, not 4791.**

If 10 was meant literally, say so and the contract tests fold into an existing suite's
count instead.

### 3A.8 A void sweep, and a self-observing wait loop

Two process failures during this round, neither affecting the subject or the evidence.

**A sweep died mid-run** with `syntax error near unexpected token '('` because I edited
`run_suites.sh` **while bash was executing it** — bash reads scripts incrementally. The file
parses cleanly and the fault was not in it, but **that sweep's numbers are void and none is
quoted anywhere**. Nothing was edited during the sweep and batches reported below.

**Three wait loops span for ~21 hours** doing nothing:

```bash
until ! pgrep -qf 'scripts/run_suites.sh'; do sleep 30; done
```

`pgrep -f` matches the whole command line, and the loop's own command line contains that
string — so it always found itself and the condition was never satisfiable. This is the
**fifth** self-observation artefact in this campaign and the second I have written. It was
confirmed empirically before anything was killed: every PID `pgrep` returned was a
`/bin/zsh -c ... until ! pgrep ...`, none was a runner. The batches below were backgrounded
through the harness instead, with no matcher and so nothing to match.

## 4. Identity — `dep3r` / `19349`

| | |
|---|---|
| label | **`dep3r`** |
| port | **`19349`** |
| app | `woa23-dep3r-candidate` |
| staging root | `~/woa23-dep3r` |
| workdir | `~/woa23-dep3r-work` |
| `PM2_HOME` | `~/woa23-dep3r-pm2` |
| tmpdir | `~/tmp-dep3r` |
| bootstrap | `~/d3boot-dep3r` *(a bootstrap path is not part of the run's identity)* |

**Selected only after the subject was fixed.**

**`dep3r` / `19349` APPEARS IN EXACTLY ONE PLACE: THIS REQUEST**, committed as `5eb0c13`
**after** subject `ab670fb0c04d447ea084f6da661af1c21e4cb132` was cut. That is the whole
mechanism by which an identity can be named without being burned, and it means the identity
is **not** absent from the repository — it is absent from everything that constitutes the
subject.

An earlier draft of this table claimed `dep3r`/`19349` occurred **0** times "in any commit
in any branch, ever". That was false the moment this file was committed, and it was the kind
of over-broad absence claim that makes a real check look like a stronger one. The scope is
now stated exactly:

| scope | `dep3r` | `19349` |
|---|---|---|
| the subject **tree** — all 258 extracted files, text and binary | **0** | **0** |
| the subject **archive** — `git archive ab670fb0… dev2026`, streamed | **0** | **0** |
| the subject **ancestry** — all 389 commits up to and including `ab670fb0…` | **0** | **0** |
| the **ledger**, `scripts/ports_used.tsv` at the subject | **0** | **0** (it ends at `19157`) |
| **all files at HEAD other than this request** | **0** | **0** |
| **this request** (`runs/d3/D3-execution-request-ab670fb.md`, commit `5eb0c13`) | present | present |

The port scope is a **substring** test, not a word-boundary one: `19337` was rejected
earlier in this campaign because it occurs inside a float in the subject tree, as `19113`
and `19136` were before it.

`runs/d3/` is outside `git archive <sha> dev2026`, so this file is not in the subject
archive and naming the identity here does not place it in the subject.

**`dep3q` / `19343` is NOT reused.** It was proposed in the `594737c` request, which you
reviewed and did not authorise. It was never run and never bound — but it was named in a
request, and that is the same footing on which `dep3n` and `dep3p` were treated as spent.
It is retired as `RETIRED-NEVER-BOUND`, and this is why no alternatives are listed here:
listing them spends them.

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

**None of this VM24 state has been deleted, killed or cleaned up, and none of it is
reused.** Cleanup of it requires separate authorization; this request does not ask for it.

(The only cleanup performed anywhere in this round was of local batch-tooling processes on
the development machine — §7.1. It touched nothing on VM24.)

### 4.2 Old identities, none reused

`dep3a`, `dep3b`, `dep3c`, `dep3d`, `dep3f`, `dep3h`, `dep3j`, `dep3k`, `dep3m`, `dep3n`,
`dep3p`, `dep3q` and every port they were named with (`19161`, `19187`, `19211`, `19229`,
`19259`, `19301`, `19343`) are **consumed** and are not proposed again. `dep3n` and `dep3p`
were burned merely by being listed as alternatives, which is why alternatives are no longer
listed.

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
| subject | `ab670fb0c04d447ea084f6da661af1c21e4cb132`, three clean batches, sentinel verified |
| archive | `39a4f9b67f0e7cf0687fbbf05bcfd3dc932547f5771ed41bd8cf9d8b96834e34` |
| file-list | `86220a49553b1c59d0e9d5791a0474690cc6e76a198c0cd8927251241b50af7b`, 258 files |
| assertions | **57 suites / 4883 assertions** per batch, 0 assertion failures, 0 contract violations |
| identity | `dep3r` / `19349`, named only in this request, committed after the subject |
| **D-3** | **request prepared offline; D-3 execution not started or executed** |
| VM24 | **not contacted** |
| PM2 | **not started** — no daemon, no app, no `PM2_HOME` |
| store | **no symlink created**, real production store never opened |
| staging / workdir / tmpdir | **not created** |
| port | **none bound**; `19349` never bound |
| production | **unchanged** |
| C1 / C2 | **not re-run** |
| VM24 retained state | **not cleaned up** — `dep3m`'s staging root, workdir and bootstrap are preserved exactly as §4.1 records |
| cleanup performed | **offline local batch-tool cleanup only** — see §7.1 |
| this request | **awaiting explicit execution authorization; not to be executed** |

### 7.1 The only cleanup performed — offline, local, batch-tooling

**No VM24 process, path or artefact was cleaned up, and no production state was touched.**
The retained `dep3m` state listed in §4.1 is intact and still requires separate
authorization to remove.

What was cleaned up was entirely on the local development machine, and only my own
batch-tooling processes: three shell wait-loops that had been spinning for roughly 21 hours
because `pgrep -f 'scripts/run_suites.sh'` matched **their own command line** (§3A.8), plus
the three `sleep` children they had left.

| PID | what it was | action |
|---|---|---|
| `22453` | local wait-loop, `/bin/zsh -c … until ! pgrep …` | terminated |
| `26725` | local wait-loop, same shape | terminated |
| `79971` | local wait-loop, same shape | terminated |
| `95607` | orphaned `sleep 45` child of the above | terminated |
| `95616` | orphaned `sleep 20` child of the above | terminated |
| `95636` | orphaned `sleep 30` child of the above | terminated |

Each PID was re-read with `ps -o command=` immediately before signalling and terminated
**only** if its command line still matched the wait-loop shape; anything else would have
been refused rather than killed on a stale PID. No `run_suites.sh` or `run_batches.sh`
interpreter was running at the time, and none was killed.

**No local batch evidence was deleted.** The abandoned and failed batch record sets from
superseded subjects are retained as historical records and are not merged with this
subject's set.
