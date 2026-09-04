# PM2 discovery probe `probeB` — result: **`B_NOT_ON_PATH`** (search INCOMPLETE)

- **Ran:** 2026-08-27 02:56:45 UTC as `woa23c1ro` (uid 994, gid 993) on `odb24`
- **Delivery:** `ssh … 'bash -s' < scripts/probe_pm2_discovery.sh` — **stdin only, nothing written to VM24**
- **Probe local SHA-256:** `03be930df33aea0b72f891bcd3839ceab810da4996c54c2f5dfac7b96505fafa` (11 208 bytes)
- **Exit: 0.**

**This is a capability finding only. It is NOT a B3/B5 pass.** No pm2 was started, no
`PM2_HOME` exists, and the launcher argv PM2 actually produces remains unverified —
that is `b35a1`'s job, still unauthorised.

Per the authorisation, outcome **B** means **stop and report**. I have not widened the
search, installed anything, retried, or modified the host.

---

## 1. Identity, and the full PATH

```
uid  : 994   gid : 993   user : woa23c1ro
HOME : /home/woa23c1ro
PATH : /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:
       /usr/games:/usr/local/games:/snap/bin        (9 entries)
```

All nine entries listed, **including the last** — the defect `probeA` shipped with is
fixed. **None of the nine contains a `pm2`.**

| # | entry | dir? | pm2 here? |
|---|---|---|---|
| 1 | `/usr/local/sbin` | yes | no |
| 2 | `/usr/local/bin` | yes | no |
| 3 | `/usr/sbin` | yes | no |
| 4 | `/usr/bin` | yes | no |
| 5 | `/sbin` | yes | no |
| 6 | `/bin` | yes | no |
| 7 | `/usr/games` | yes | no |
| 8 | `/usr/local/games` | yes | no |
| 9 | `/snap/bin` | yes | no |

## 2. `node`

```
resolved : /usr/local/bin/node       realpath : /usr/bin/node
stat     : root:root mode=755 size=120177224
version  : v22.14.0
readable : yes    executable : yes
prefix   : /usr
```

## 3. Search roots — and the one that was NOT traversable

| root | depth | outcome |
|---|--:|---|
| `/usr/local/bin`, `/usr/bin`, `/bin`, `/sbin`, `/usr/local/sbin`, `/snap/bin` | 1 | searched — 0 hits each |
| `/usr/lib/node_modules`, `/lib/node_modules` | 3 | searched — 0 hits |
| `/opt` | 4 | searched — 0 hits |
| `/usr/local/n` | 5 | searched — 0 hits |
| `/home/woa23c1ro/.local` | 4 | searched — 0 hits |
| `/home/odbadmin/.nvm` | 5 | searched — 0 hits |
| **`/home/odbadmin/.npm-global`** | 4 | **searched — 3 hits** |
| **`/home/odbadmin/.local`** | 4 | **NOT TRAVERSABLE** — `r=n x=n owner=odbadmin:odbadmin mode=700` |
| `/usr/local/lib/node_modules`, `/home/woa23c1ro/.npm-global`, `/home/woa23c1ro/node_modules`, `/home/woa23c1ro/.nvm`, `/home/odbadmin/node_modules`, `/home/odbadmin/.config/yarn` | — | ABSENT |

### 3.1 THE SEARCH IS INCOMPLETE — stated explicitly

**`/home/odbadmin/.local` is mode 700 and owned by `odbadmin`. uid 994 cannot traverse
it, so it was NOT searched.** Any `pm2` inside it is invisible to this probe and to the
validation account.

This does not change the outcome — a usable candidate *was* found elsewhere — but the
finding is **"a pm2 exists at the paths below"**, not **"these are all the pm2 on the
host"**. Per the authorisation's item 9, incompleteness is marked rather than reported
as `A_ABSENT`.

A cosmetic note: `/usr/lib/node_modules` was searched twice — once from the fixed list
and once as node's own `<prefix>/lib/node_modules`. Harmless duplication, 0 hits both
times.

---

## 4. Candidates

### 4.1 The real one — `/home/odbadmin/.npm-global/bin/pm2`

| | |
|---|---|
| absolute path | `/home/odbadmin/.npm-global/bin/pm2` |
| realpath | `/home/odbadmin/.npm-global/lib/node_modules/pm2/bin/pm2` |
| type | symlink → `../lib/node_modules/pm2/bin/pm2` |
| owner / mode / size | **`odbadmin:odbadmin` `775` `56`** |
| **sha256** | **`bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d`** |
| **package** | `/home/odbadmin/.npm-global/lib/node_modules/pm2` — `odbadmin:odbadmin` `775` |
| **`package.json`** | readable — **name `pm2`, VERSION `5.4.2`** |
| production state | **not** under any known production path |
| on this account's PATH | **no** |
| **classification** | **`B_NOT_ON_PATH`** |

**Read / write / execute for uid 994:**

| target | read | write | execute |
|---|---|---|---|
| the binary | **yes** | **no** | **yes** |
| its parent directory (`.../bin`) | — | **no** — cannot be replaced | — |
| the package directory | — | **no** | — |
| `package.json` | yes | **no** | — |

**Not writable at any level by uid 994** — the validation account cannot modify or
replace it, which is the property that matters for trusting it.

### 4.2 Two further hits — and a reporting imprecision I introduced

`find -name pm2` matches **directories** too, so two of the three "candidates" are not
executables:

| path | what it actually is |
|---|---|
| `/home/odbadmin/.npm-global/lib/node_modules/pm2` | the **package directory** (`size=4096`) |
| `/home/odbadmin/.npm-global/lib/node_modules/pm2/pm2` | an 87-byte file inside the package, sha256 `2c88ef6dfbc924be2f9a3e3e8487f6d62427ffc44088d57b37f77e50da2c4aee` |

**Two flaws in how the probe reported these, and neither is hidden:**

1. **`executable: yes` for the package directory is misleading.** On a directory the
   `x` bit means *traversable*, not *runnable*. The probe applied a binary's test to a
   directory.
2. **Their `package`/`package.json` derivation is wrong.** The probe computes the package
   as `dirname(dirname(realpath))`, which is right for `<pkg>/bin/pm2` and wrong for a
   directory or a file one level in — hence `.../lib/package.json NOT readable` and
   `.../lib/node_modules/package.json NOT readable`. Those two "not readable" lines are
   artefacts of my derivation, **not** evidence about the host.

Neither flaw affects §4.1, which is the real executable and whose package.json resolved
correctly to pm2 5.4.2. Both are recorded because a probe whose stray rows look like
findings is how a wrong conclusion later gets cited as evidence.

---

## 5. `PM2_HOME` — unchanged and uncreated

```
PM2_HOME in environment : <unset>
would default to        : /home/woa23c1ro/.pm2      exists? no — and NOT created
production /home/odbadmin/.pm2          : exists  r=yes  w=NO
production /root/.pm2                   : not present or not visible
production /home/odbadmin/python/woa23  : exists  r=NO   w=NO
this probe set no PM2_HOME, ran no pm2, and created no directory
```

`/home/odbadmin/.pm2`'s mtime is `1786685045`, unchanged and untouched — it was never
entered as a search root and never set as `PM2_HOME`.

---

## 6. Before / after — nothing added, no daemon, no listener

| | BEFORE 02:56:37 | AFTER 02:57:27 | |
|---|---|---|---|
| HOME entries (recursive) | 70122 | 70122 | **UNCHANGED** |
| `~/.pm2` | absent | absent | **not created** |
| PM2 daemons (this account) | 0 | 0 | **none started** |
| processes (this account) | 16 | 16 | **UNCHANGED** |
| listeners | 45 | 45 | **UNCHANGED** |
| `/tmp` entries owned by account | 309 | 309 | **UNCHANGED** |
| production pids : starttimes | 4296:14214, 5040:15825, 5041:15829 | identical | **unchanged** |
| pm2G pids | three up | three up | **untouched** |
| boot id | `0b513a75…` | `0b513a75…` | **UNCHANGED** |
| probe / temp file left behind | — | **none found** | **nothing written** |

**HOME metadata digest moved**, and the cause is the same as `probeA`'s and again not the
probe — four paths, all the account's own systemd user session:

```
d  /home/woa23c1ro/.local/state/wireplumber
f  /home/woa23c1ro/.local/state/wireplumber/restore-stream
d  /home/woa23c1ro/snap/snapd-desktop-integration/391/.config/ibus
l  /home/woa23c1ro/snap/snapd-desktop-integration/391/.config/ibus/bus
```

Entry count identical, no probe file anywhere. **The probe wrote nothing to VM24.**

---

## 7. Classification — and a definitional tension I am not resolving on my own

**The probe returned `B_NOT_ON_PATH`**, by its own rule: `D_USABLE_FOR_STAGING` requires
the candidate's directory to be **on the account's PATH**, and this one is not.

**But the substance may meet what you meant by D.** Your wording was *"pm2 存在但不在
PATH"* for B and *"pm2 可用於 staging，但尚未做實際啟動驗證"* for D — and this pm2 **is**
readable, executable, outside production state, and reachable by absolute path. So it
is simultaneously "not on PATH" (B, literally) and arguably "usable for staging" (D, in
substance).

**I am reporting it as B and not silently reclassifying it to D**, because reclassifying
would be me unlocking the next step by redefining the finding — and B's instruction is
to stop. **Which of the two you consider it is your call.**

### 7.1 What is established, and what is not

**Established:** a `pm2` **5.4.2** exists at
`/home/odbadmin/.npm-global/bin/pm2`, uid 994 can read and execute it by permission bits,
it is not writable by uid 994 at any level, and it is not under production PM2 state.

**NOT established:**

- that it **runs** — the probe never executed it, so "executable" is a permission-bit
  finding, not proof;
- that the search was exhaustive — `/home/odbadmin/.local` was unreadable (§3.1);
- **anything at all about B3 or B5.**

### 7.2 Two things worth your attention before any decision

1. **It lives under `odbadmin`'s home.** Using it makes the validation account depend on
   a path another account owns and can change at any time, and on
   `/home/odbadmin` staying traversable. That is a shared dependency, not an isolated
   one — the same category of coupling the non-owner discipline was meant to reduce.
2. **pm2 `5.4.2` here vs whatever production's PM2 daemon is running.** The probe did not
   and could not determine production's running PM2 version, because that would require
   executing pm2 or reading its daemon state. If they differ, a staging run would exercise
   a different PM2 than production uses.

---

## 8. What was NOT done

- **`pm2` never executed** — not the binary, not `--version`, not `-v`, not `jlist`, not
  any subcommand. No daemon contacted or spawned.
- **Nothing written to VM24** — no script, no temp file, no directory. Stdin delivery.
- No `PM2_HOME` created or modified; none set or exported.
- No `start`/`stop`/`delete`/`kill`/`save`/`resurrect`.
- No staging tree, workdir, store or listener.
- No privilege escalation of any kind.
- No modification of PATH, ACLs, permissions, production PM2 or production files.
- **No search widening, no installation, no retry** after the outcome.

## 9. Evidence

| file | contents |
|---|---|
| `scratchpad/probeB/01-before.txt` | BEFORE snapshot |
| `scratchpad/probeB/02-probe.txt` | the probe's full output |
| `scratchpad/probeB/03-after.txt` | AFTER snapshot, digest-delta explanation |

---

## 10. Stopping here

Outcome **B** — stopped and reported, as instructed. `b35a1` is **not** run and remains
unauthorised, and I have **not** prepared its usage plan, since that was conditioned on
**D**.

**If you judge this to be D in substance**, say so and I will submit — offline, for your
review — how `b35a1` would use this pm2 by absolute path via `WOA23_PM2_BIN` (which
`staging_execute.sh:386` already supports) or via a staging-only `PATH` prefix on the
run's own command line, together with the shared-dependency risk in §7.2.

**If you judge it B and want the search completed**, `/home/odbadmin/.local` needs
either a permission change or a look by someone who can read it — both host-level acts,
and neither is mine to take.
