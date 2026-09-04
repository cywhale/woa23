# 020 — Repository source reconciled to production after Stage B

**Status: OFFLINE SOURCE CHANGE, complete. No VM24 contact. Nothing on the host was
touched, read or rolled back by this work.**

Stage B removed a dead `pre_stop` line from production's config on VM24. This brings the
repository's own copy to the same bytes, so the two stop describing different things.

---

## 1. What changed, and what it is

```
conf/ecosystem.config.js
  BEFORE  8db9a6ba1888821c833452ee9602871045e4017500800ae69b00aba9fdde4340
  AFTER   ed5dec6ca064bd54cfe4a45f33fd416871859e959671164292b21549b96f2159
```

**`ed5dec6c…2159` is byte-identical to VM24's production config as of Stage B.** The diff
is one deletion:

```
-    pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
```

**This is an OFFLINE SOURCE CHANGE.** It changed a file in this repository. It did **not**
touch production, run any `pm2` command, or validate anything.

> **The edit was applied by the PI, not by this session.** `conf/ecosystem.config.js` is
> protected by `permissions.deny` (`Edit`/`Write`) **and** by the safety guard's
> `protected_basenames`. Both refused, which is correct — the live production config source
> should not be agent-editable. This session verified the result read-only and went no
> further.

---

## 2. Four things this does NOT mean

| | |
|---|---|
| **Stage B ≠ production B1 validation** | Stage B was a **configuration cleanup with no PM2 lifecycle operation**. It validated nothing |
| **`b1s1` is not production evidence** | it is a **qualified staging-only stop-path PASS** on PM2 5.4.2 under uid 994, and is **not** back-filled |
| **Stage C is not authorised** | not requested, not prepared, not executed. Production B1 remains **OPEN** |
| **A11 is a QUALIFIED PROXY ONLY** | no reliable exact API request count source exists. It stays a **production B1 blocker** if exactness is required, and a marker delta never stands in for it |

---

## 3. The fixture — where the dangerous pattern now lives

`scripts/fixtures/legacy-pre-stop.fixture.js`

Removing the hook from the config would have deleted the only in-repo record of the pattern
B1 exists to prevent. It is preserved in **one** place instead, and that place is
**load-bearing rather than decorative**:

- `test_staging_override.sh` drives the generator's refusal guard (*"a pre_stop key is
  present. B1 removed it; it may not come back"*) **from the fixture's own string**. It was
  previously an inline copy in that one test — which would have become the sole surviving
  record, uncommented, once the config was cleaned.
- `test_production_stop.sh` asserts the fixture still carries the pattern **and** that it is
  **not deployable**: outside `deploy/`, and not named `ecosystem.*.config.js` — so it is
  neither mistaken for a real config nor excluded from the subject's file-list.

The fixture states in its own header that PM2 5.4.2 **never had** a `pre_stop` hook
(Stage A: 0 occurrences in source, absent from `schema.json`'s 65 keys), so the line was
**inert**. It is kept because it *read* as an active SIGKILL safeguard — that is the
regression worth holding on to.

---

## 4. Assertions flipped — four, in three suites

Each previously asserted the production config **contained** the hook (*"the defect is
real"*). That was true and useful while it was there; asserting it now would assert a state
we deliberately left behind.

| file | was | now |
|---|---|---|
| `test_production_stop.sh` | "production's config still HAS the grep pre_stop" | **no `pre_stop`**, **no `kill -9`**, + fixture retained, + fixture not deployable |
| `test_staging_launcher.sh` | "production's config really does grep+kill -9" | **no longer greps+kill -9 either** |
| `test_production_launcher.sh` | "production's config DOES carry pre_stop" | **no longer carries `pre_stop`** |
| `test_production_launcher.sh` | "production's pre_stop DOES use kill -9" | **nor any `kill -9`** |

**Nothing was simply deleted.** Every assertion was replaced by one asserting the opposite
fact, and the knowledge it carried moved to the fixture.

### 4.1 Coverage confirmed still present

`test_stop_jlist_parser.sh` **61** · `test_stop_proc_parsing.sh` **100** ·
`test_production_stop.sh` **62** (grant gate, `(pid, starttime)`, survivor →
`CLEANUP_FAIL`, `INDETERMINATE`/exit 8, no SIGKILL, no `all`/wildcard, no `save`/`resurrect`)
· `test_stop_multiworker.sh` **37** · `test_staging_override.sh` **84** ·
`test_production_launcher.sh` **111** · `test_staging_launcher.sh` **101**.

### 4.2 One of my own constructs was caught by the rule it broke

My first fixture check used `case` **inside a command substitution** — which
`test_production_launcher.sh` asserts no shell file in the tree may do, because it breaks
under bash 3.2. It went red immediately and was replaced with helper functions
(`not_deployable`, `contains`). Recorded because the guard did exactly its job on the person
who added it.

---

## 5. `conf/simu.sh` — UNTOUCHED, and a separate issue

Byte-identical: `3bc46819a11b99eacf464f437034b84b701160cb26aad3fbcdfb67d305cdeef9`, the same
digest Stage A recorded on VM24.

It still contains the same kill-by-grep technique, **including a line that greps `tide_app`
directly** — killing another project by command-line match. Stage B did not address it, this
reconciliation does not address it, and `test_production_stop.sh` **still asserts it is
there**, so it cannot quietly disappear.

**It needs its own decision.** It is not a B1 blocker and should not be bundled into one.

---

## 6. Not changed

No `api/`, no query logic, no response behaviour, no runtime dependency. No VM24 contact, no
production file modified, no `pm2` lifecycle command, no cleanup of `b1s1`, `bs3v1` or any
retained state.
