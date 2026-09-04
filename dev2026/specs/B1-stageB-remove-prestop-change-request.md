# Stage B — remove the dead `pre_stop` key: a **production file change request**

**Status: DRAFT, OFFLINE. Not authorised, not executed. No VM24 contact was made to write
it.** Second of three stages. Its own authorisation, separate from
[A](B1-stageA-production-inventory-request.md) and
[C](B1-stageC-production-b1-validation-request.md).

**Nothing here has been done.** `conf/ecosystem.config.js` is unmodified; no `pm2` reload,
restart, start, stop, delete or kill has been run; no retained state cleaned; `b1s1`,
`bs3v1`, `pm2G` and `18265` untouched.

> **The premise of this stage changed completely after Stage A**, and the earlier draft is
> superseded. It is no longer "remove a live hazard before B1 can run". It is **"delete a
> dead line that reads like a live hazard"**.

---

## 1. What is being changed — a DEAD line in a FILE, not a live definition

**One line**, in one file, on the production host:

```
/home/odbadmin/python/woa23/conf/ecosystem.config.js
  sha256 8db9a6ba1888821c833452ee9602871045e4017500800ae69b00aba9fdde4340
  mtime  2024-06-18 21:37
  line 16: pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
```

**It is dead.** [Stage A §3](B1-stageA-production-inventory-result.md) established, from
four independent daemon-side sources plus PM2's own source:

| | |
|---|---|
| `pm2 jlist` → `pm2_env`, all nine apps | `pre_stop` **absent** |
| `dump.pm2` / `dump.pm2.bak` | **absent** |
| any `PM2_HOME` state file containing the string | **none** |
| PM2 5.4.2 source: occurrences under `pm2/lib` | **0** |
| PM2 5.4.2 `schema.json`, 65 documented app keys | **not present** |

**PM2 5.4.2 has no `pre_stop` hook.** The key is silently ignored. The running app was
started from this very file and the key never entered the daemon's state.

### 1.1 So why change anything

Because **it reads as an active SIGKILL safeguard and is not one.** The next person to open
that file will reasonably believe production kills its workers by grep on every stop. That
belief would be wrong, and acting on it — relying on it, or replicating it into a config
where the key *is* honoured — is the risk this change removes.

**This is documentation hygiene on production, not a functional fix.** Saying so plainly is
the point; overstating it is what the previous draft did.

### 1.2 What this change is NOT

- **Not** a live-behaviour change. The daemon does not carry the key, so removing it from
  the file changes nothing the daemon does.
- **Not** part of B1 validation, and it produces **no** B1 evidence.
- **Not** a fix for `conf/simu.sh`, which documents the same pattern independently and
  deserves its own decision. Bundling it would widen a change that must stay minimal.
- **Not** a change to any other key: `autorestart`, `append_env_to_name`,
  `max_memory_restart`, `script`, `name` and the log paths are all untouched.

---

## 2. Will PM2 notice? — how the answer is established, not assumed

Three questions, each answered by a **read**, before and after:

| | question | how it is settled |
|---|---|---|
| **B1** | does PM2 watch this file and reload on change? | `watch` is **false** for `woa23` in the live definition (Stage A). Re-confirm from `jlist` **immediately before** the edit, and again after |
| **B2** | does the edit alter the running app? | compare `jlist` for all nine apps, `(pid, starttime)` of 4295 / 4296 / 5040 / 5041, and `ss -ltn`, before vs after. **All must be identical** |
| **B3** | does the edit alter the persisted definition? | `dump.pm2` SHA-256 before vs after. **Must be identical** — this change does **not** run `pm2 save` |

**Expected: no reload, no restart, no process change, no listener change** — because the
daemon holds no copy of the key and PM2 does not watch the file.

**If any of B1–B3 shows a change, STOP.** Restore from §4 and report. Do not retry, do not
"apply properly", do not escalate to a reload.

---

## 3. Reload / restart / downtime

**None is required, and none is requested.**

| | |
|---|---|
| reload / restart needed? | **No.** The daemon never adopted the key, so there is nothing to re-adopt |
| downtime | **None expected.** No process is signalled and no listener is touched |
| is a reload *permitted* as a fallback? | **NO.** If the edit appears not to "take", that is a **finding to report**, not a reason to restart production. A restart here would create the very downtime this change does not need |
| API availability during the change | uninterrupted — `8050` is not touched |

**No recovery strategy is proposed, because none should be needed.** If one turns out to be
needed, that is itself the signal to stop: it would mean Stage A's finding was wrong.

---

## 4. Rollback — the exact digest to restore to

```
ORIGINAL  /home/odbadmin/python/woa23/conf/ecosystem.config.js
          sha256 8db9a6ba1888821c833452ee9602871045e4017500800ae69b00aba9fdde4340
```

**Before any edit**, a byte-for-byte copy is written to a retained path alongside the
original, and its SHA-256 verified to equal `8db9a6ba…4340`. **Rollback is restoring that
copy and re-verifying that digest** — nothing more, and no `pm2` command.

The `conf/` file-list digest at Stage A was
`6840b8fc0cc6b106e6abd3314d7e5451ada1b4814f798892b99dee6e47856083` across 5 files; after a
rollback it must return to exactly that.

**Who decides to roll back, and on what signal:** yours. The mechanical trigger is any
B1–B3 discrepancy in §2.

---

## 5. Future `start` / `reload` — does behaviour change?

**No, and this is the one place where removing the line has any effect at all.**

| scenario | before | after |
|---|---|---|
| PM2 5.4.2 starts the app from this file | key parsed, **ignored**, never stored | key absent, nothing to ignore. **Identical behaviour** |
| a future PM2 that *did* honour `pre_stop` | would begin running a grep `kill -9` on every stop — **a silent behaviour change on upgrade** | **cannot happen** |
| someone copies this config to another app or host | carries a hook that looks load-bearing | carries nothing to misread |

**So the change is behaviour-neutral today and removes a latent behaviour change on a future
PM2 upgrade.** That is the honest size of the benefit: small, real, and not urgent.

---

## 6. API request count during this change

**No API call is made, and no request count is claimed.**

Per [Stage A §6.1](B1-stageA-production-inventory-result.md), A11 is a **qualified proxy,
not an exact count**: two markers disagree by 17, the logs are cumulative since 2024 with no
rotation policy found, and their freshness is inconsistent.

For this stage the marker delta is recorded **only** to show whether traffic occurred during
the window. **It is not evidence that the request count is unchanged**, and it must never be
written that way. Since no process and no listener is touched, the count is not expected to
be affected by the change at all — **which is a prediction, not a measurement.**

---

## 7. Procedure — read, copy, edit, verify by reading again

1. **Before**: `jlist` (all nine apps, incl. `watch` for `woa23`); `(pid, starttime)` for
   4295, 4296, 5040, 5041; `ss -ltn`; boot id; SHA-256 of `ecosystem.config.js`;
   SHA-256 of `dump.pm2`; the `conf/` file-list digest; the A11 marker count.
2. **Confirm** the file digest is still `8db9a6ba…4340`. If it is not, **STOP** — the file
   changed since Stage A and this plan is stale.
3. **Preserve** a byte-for-byte copy; verify its digest.
4. **Edit**: delete line 16 only. Nothing else.
5. **Show** the diff and the new digest **for review before proceeding**.
6. **After** (no `pm2` lifecycle command anywhere): repeat every item in step 1 and diff.
   **Required: `jlist` identical, all four `(pid, starttime)` identical, listeners
   identical, `dump.pm2` digest identical, boot id identical.**
7. **Confirm** the daemon still holds no `pre_stop` — unchanged from Stage A, and now
   consistent with the file.

**Any discrepancy at step 6 → STOP, restore per §4, report. No retry, no reload.**

---

## 8. Protected — untouched

`pm2G`/18265 · `pm2A`, `pm2B`, every other historical `PM2_HOME` · `bs3v1` retained daemon
`1709473`, tree, bootstrap, workdir · `b1s1` retained daemon `1761143`, tree, workdir,
bootstrap paths · all non-target PM2 apps · ports 18281, 18283, 19157 · the production
store · `conf/simu.sh`, `start_app.sh`, `privkey.pem`, `fullchain.pem`.

Cleanup of `bs3v1` and `b1s1` remains a **separate** authorisation
([`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md)).

---

## 9. What must NOT be inherited

- **Stage A is an inventory. It validates nothing** and is not B1 evidence.
- **`b1s1` is a qualified staging-only stop-path PASS.** Neither it nor Stage A may be
  back-filled into a production B1 PASS, and **nothing in this stage produces B1 evidence.**
- **Production B1 remains unvalidated and unauthorised.**
- `b1v2` remains **WITHDRAWN**; the staging config digest remains **INCOMPLETE**.

---

## 10. Blockers

| | |
|---|---|
| **B-1** | **your authorisation.** Editing a production file needs it, however inert the line |
| **B-2** | **the file digest must still be `8db9a6ba…4340`** at execution time. Otherwise the file changed after Stage A and this plan is stale |
| **B-3** | **`conf/simu.sh` is deliberately excluded** and still contains the same pattern — including a line that greps `tide_app` directly. It needs its own decision, which this stage does not make |

**Not blocked on Stage C. Stage C is not requested, not scheduled, and not prepared** — it
awaits your review of this plan and of the A11 handling.
