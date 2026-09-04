# Read-only host capability probe `probeA` — result: **REFUSED**

- **Ran:** 2026-08-27 01:18:40 UTC, as `woa23c1ro` (uid 994, gid 993) on `odb24`
- **Delivery:** `ssh … 'bash -s' < scripts/probe_host_capability.sh` — **stdin only, nothing written to VM24**
- **Probe script local SHA-256:** `4566f73abcf5db0f253d8635782563ca31ea1f6435de7c77af04b067342969dc` (9 699 bytes)
- **Exit: 3 — REFUSED.**

**The refusal is the result.** Per the authorisation it was not retried and the
environment was not modified. `b35a1` has **not** been run and remains unauthorised.

---

## 0. The finding

**`pm2` is NOT on the PATH of the validation account.**

```
REFUSE: pm2 is NOT on PATH for this account. staging_execute.sh:387 requires it.
```

This is exactly what the probe existed to catch, and it is why it was worth separating:
**no identity, no port, no staging tree and no `PM2_HOME` were spent finding it out.**

`node` **is** present. A second refusal — `uv` — is a PATH question, not an absence, and
is explained precisely in §3.

**Consequence:** the staging harness cannot run as `woa23c1ro` as things stand.
`staging_execute.sh:387` does `command -v "$PM2"` and would `die "no pm2 binary"`.
**This is a decision for you, not something for me to work around.**

---

## 1. Identity, HOME and PATH, as recorded

```
id      : uid=994(woa23c1ro) gid=993(woa23c1ro) groups=993(woa23c1ro)
uid     : 994   gid: 993
user    : woa23c1ro
HOME    : /home/woa23c1ro
PATH    : /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:
          /usr/games:/usr/local/games:/snap/bin
```

`HOME` is **not** on `PATH`, and neither is `~/.local/bin`. That single fact explains
both refusals.

---

## 2. `node` — present

| | |
|---|---|
| resolved | `/usr/local/bin/node` |
| realpath | `/usr/bin/node` |
| owner / mode / size | `root:root` `755` `120177224` |
| **version** | **v22.14.0** |

System-wide, root-owned, world-executable. Reachable by uid 994 with no PATH change.

## 3. `pm2` — NOT on PATH; and `uv` — present but not on PATH

**`pm2`:** `command -v pm2` found nothing. Because the probe **never executes `pm2`**,
no daemon was contacted or spawned; the absence is a PATH fact only. The probe did not
go looking for it elsewhere — searching other accounts' trees was not authorised, and
would not have changed the finding that it is not reachable *as configured*.

**`uv`:** reported as not on PATH — **and this is not a contradiction of earlier runs.**
Every C1/C2 run set `PATH=/home/woa23c1ro/.local/bin:$PATH` explicitly on the command
line, and `uv` resolved to `/home/woa23c1ro/.local/bin/uv`, version 0.9.22, sha256
`1f95b3affb7fd478f068f62b80e374b84bf46764c37e9f35d2a648e5b9aa0036` — verified in the
`c1r`, `c2j` and `c2k` pre-flights. **The probe deliberately did not prepend that path**,
so it reports the *default* environment. `uv` exists and is usable; it is simply not on
the login PATH.

**The same may be true of `pm2`** — it could exist somewhere uid 994 can execute, off the
default PATH. **The probe cannot say, and I am not guessing.** §6 puts that to you.

## 4. `PM2_HOME` — the guard is satisfied

```
PM2_HOME in environment        : <unset>
effective (what pm2 would use) : /home/woa23c1ro/.pm2
not a production PM2 path (checked against /home/odbadmin/.pm2, .pm2/, /root/.pm2)
under this account's own home
production /home/odbadmin/.pm2  : exists, readable=yes, writable=NO
production /home/odbadmin/.pm2/ : exists, readable=yes, writable=NO
production /root/.pm2           : not present or not visible
this probe sets no PM2_HOME, runs no pm2, and creates no PM2 directory
```

**The account does not fall back to production's `PM2_HOME`** — with `PM2_HOME` unset it
would use `/home/woa23c1ro/.pm2`, which does not exist and was not created.

**Production's `/home/odbadmin/.pm2` is readable but NOT writable by uid 994.** Readable
is worth stating plainly rather than glossing: the account can *see* production's PM2
state. It cannot modify it, which is the property the guard requires — and the probe
never entered it, read from it, or set it as `PM2_HOME`.

## 5. Host state, observed read-only

```
boot id : 0b513a75-213b-40bf-8219-1c7cbc51a085
8050 listening  : 1        18265 (pm2G) listening : 1
production pid=4296 starttime=14214
production pid=5040 starttime=15825
production pid=5041 starttime=15829
pm2G pid=1456369 / 1456373 / 1456374  RUNNING (observed, untouched)
would-be staging paths: all four absent, and NOT created
```

---

## 6. Before / after — nothing added, no daemon, no listener

| | BEFORE 01:18:29 | AFTER 01:19:19 | |
|---|---|---|---|
| entries under `HOME` (recursive) | 70122 | 70122 | **UNCHANGED** |
| listeners (`ss -ltn`) | 45 | 45 | **UNCHANGED** |
| `/tmp` entries owned by the account | 309 | 309 | **UNCHANGED** |
| PM2 daemon processes (this account) | 0 | 0 | **none started** |
| processes (this account) | 16 | 16 | **UNCHANGED** |
| `~/.pm2` | absent | absent | **not created** |
| any `*probe*` file under `HOME` or `/tmp` | — | **none found** | **nothing left behind** |
| production pids + starttimes | 4296/14214, 5040/15825, 5041/15829 | identical | **unchanged** |
| pm2G pids | three RUNNING | three RUNNING | **untouched** |
| boot id | `0b513a75…` | `0b513a75…` | **unchanged** |

### 6.1 The HOME digest DID change — and it was not the probe

The metadata fingerprint over `HOME` differed between snapshots. Reported rather than
smoothed over, and then explained:

```
what changed mtime in the 15 minutes around the run — total: 4
  1787793580  l  /home/woa23c1ro/snap/snapd-desktop-integration/391/.config/ibus/bus
  1787793580  d  /home/woa23c1ro/snap/snapd-desktop-integration/391/.config/ibus
  1787793560  d  /home/woa23c1ro/.local/state/wireplumber
  1787793560  f  /home/woa23c1ro/.local/state/wireplumber/restore-stream
```

All four belong to the **account's own systemd user session** — `wireplumber` and
`snapd-desktop-integration`, both visible in the account's process list before the probe
ran, and both of which write their own state on their own schedule. **None is touched by
the probe**, which reads nothing under `snap/` or `.local/state/`.

Corroborating: the **entry count is identical**, so nothing was added; **no `*probe*`
file exists**; and the digest is **stable across back-to-back takes**
(`9d76cd8f…` twice), so the measure is not inherently noisy — it moved because those
daemons wrote, not because the metric drifts.

**Conclusion: the probe wrote nothing to VM24.**

---

## 7. A defect in the probe itself, found by running it

**The PATH listing dropped its last entry.** `/snap/bin` is in `PATH` and was never
printed, because

```
printf '%s' "${PATH:-}" | tr ':' '\n' | while IFS= read -r d; do …
```

`printf '%s'` emits no trailing newline, so the final field is unterminated and `read`
returns non-zero on it — the loop body never runs for the last element.

**It does not change the finding.** `command -v pm2` searches the real `PATH`, including
`/snap/bin`, and found nothing. But the *report* was incomplete, and a report that
silently omits one PATH entry is exactly the kind of thing that later gets cited as
"we checked".

**Fixed offline** (`printf '%s\n'`), with a regression test. **The fix has NOT been
exercised on the host** — fixing it does not entitle me to re-run, and I have not.

---

## 8. What was NOT done

- **`pm2` was never executed** — no subcommand, no `start`/`stop`/`delete`/`kill`/`save`/`resurrect`/`jlist`, no daemon.
- **Nothing written to VM24** — no script, no temp file, no directory. Delivery was `bash -s` over stdin.
- No `PM2_HOME` created or modified; none exported.
- No staging tree, workdir or store.
- No port bound.
- No `sudo`, `su`, `setpriv` or any privilege escalation.
- No production API request; production PM2 state, ACLs, permissions and data untouched.
- **No retry, and no modification of the host environment** after the refusal.

---

## 9. What this does and does not establish

**Establishes:** as configured today, the validation account has `node` v22.14.0, does
**not** have `pm2` on its PATH, does **not** fall back to production's `PM2_HOME`, and
cannot write production's PM2 directory.

**Does not establish:** anything about B1–B5. **No blocker is closed, or moved.** This
was a capability check, and `b35a1` has not run.

---

## 10. For your decision — I am not acting on any of these

`b35a1` cannot proceed as written. The options, with what each costs:

1. **Put `pm2` (and `uv`) on the validation account's PATH** — e.g. the run supplies
   `PATH=/home/woa23c1ro/.local/bin:…` as C1/C2 already did, *if* a `pm2` exists that
   uid 994 can execute. **Unknown whether one does**; answering it needs another
   read-only probe, which is another authorisation.
2. **Install `pm2` for the validation account** — changes the host, so it is a change
   request in its own right, not part of a validation run.
3. **Run the staging validation as `odbadmin`**, as `pm2B`/`pm2F`/`pm2G` did — but that
   discards the non-owner discipline the C1 sequence was rebuilt around, and `odbadmin`
   *can* write production's PM2 state. I would want that decided deliberately, not by
   default.
4. **Re-scope B3/B5** to something that does not need PM2 at all — a launcher-argv check
   under a plain shell rather than under PM2. It would establish less: the argv would be
   the launcher's, not the argv PM2 actually starts.

I have no recommendation I am confident in without knowing whether an executable `pm2`
exists for uid 994 anywhere — which is precisely what option 1 would settle. Say which
you want and I will prepare it offline for review.
