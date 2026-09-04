# PM2 discovery re-run `probeC` — result: **`B_NOT_ON_PATH`** (search INCOMPLETE)

- **Ran:** 2026-08-27 04:51:29 UTC as `woa23c1ro` (uid 994, gid 993) on `odb24`
- **Delivery:** `ssh … 'bash -s' < scripts/probe_pm2_discovery.sh` — **stdin only, nothing written to VM24**
- **Probe local SHA-256:** `b1acce36acb872713d19f167855fcbd3eb2172b43ca3ab339396bf9296b14d58` (16 097 bytes) — the **fixed** probe (`probeB` ran `03be930d…`)
- **Exit: 0.**

**Classification retained: `B_NOT_ON_PATH`.** Not reclassified to `D`.
**This closes no blocker** and says nothing about B3 or B5.

---

## 1. Identity and the full PATH

```
uid  : 994   gid : 993   user : woa23c1ro
HOME : /home/woa23c1ro
PATH : /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:
       /usr/games:/usr/local/games:/snap/bin          (9 entries)
```

All nine listed, **including the last**. **None contains a `pm2`:**

| # | entry | pm2 here? | | # | entry | pm2 here? |
|---|---|---|---|---|---|---|
| 1 | `/usr/local/sbin` | no | | 6 | `/bin` | no |
| 2 | `/usr/local/bin` | no | | 7 | `/usr/games` | no |
| 3 | `/usr/sbin` | no | | 8 | `/usr/local/games` | no |
| 4 | `/usr/bin` | no | | 9 | `/snap/bin` | no |
| 5 | `/sbin` | no | | | | |

## 2. `node`

```
resolved : /usr/local/bin/node        realpath : /usr/bin/node
stat     : root:root mode=755 size=120177224
version  : v22.14.0                   readable : yes   executable : yes
```

---

## 3. Search scope and traversability

| root | depth | outcome |
|---|--:|---|
| `/usr/local/bin`, `/usr/bin`, `/bin`, `/sbin`, `/usr/local/sbin`, `/snap/bin` | 1 | searched — 0 hits |
| `/usr/lib/node_modules`, `/lib/node_modules` | 3 | searched — 0 hits |
| `/opt` | 4 | searched — 0 hits |
| `/usr/local/n` | 5 | searched — 0 hits |
| `/home/woa23c1ro/.local` | 4 | searched — 0 hits |
| `/home/odbadmin/.nvm` | 5 | searched — 0 hits |
| **`/home/odbadmin/.npm-global`** | 4 | **searched — 3 hits** |
| **`/home/odbadmin/.local`** | 4 | **NOT TRAVERSABLE — `r=n x=n owner=odbadmin:odbadmin mode=700`** |
| `/usr/local/lib/node_modules`, `/home/woa23c1ro/.npm-global`, `/home/woa23c1ro/node_modules`, `/home/woa23c1ro/.nvm`, `/home/odbadmin/node_modules`, `/home/odbadmin/.config/yarn` | — | ABSENT |

### 3.1 The search remains INCOMPLETE — reported for this B outcome, not only for A

```
== 5b. SEARCH COMPLETENESS ==
  SEARCH IS INCOMPLETE. These roots could NOT be traversed by uid 994 and
  were NOT searched. Any pm2 inside them is invisible to this probe:
      /home/odbadmin/.local   odbadmin:odbadmin mode=700
```

**The range was not widened.** `/home/odbadmin/.local` stays unsearched, and the caveat
is repeated beside the verdict itself.

---

## 4. Executable candidates — both fixes confirmed on the host

### 4.1 `/home/odbadmin/.npm-global/bin/pm2` — the one that matters

| | |
|---|---|
| absolute path | `/home/odbadmin/.npm-global/bin/pm2` |
| realpath | `/home/odbadmin/.npm-global/lib/node_modules/pm2/bin/pm2` |
| type | symlink → `../lib/node_modules/pm2/bin/pm2` |
| owner / mode / size | **`odbadmin:odbadmin` `775` `56`** |
| **SHA-256** | **`bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d`** |
| **`package.json`** | `.../lib/node_modules/pm2/package.json` — **name `pm2`, version `5.4.2`** |
| production state | **not** under any known production path |
| on PATH | **no** |
| **classification** | **`B_NOT_ON_PATH`** |

**read / write / execute for uid 994:**

| target | rwx |
|---|---|
| the binary | **`r-x`** |
| parent directory `.../pm2/bin` | **`r-x`** |
| package directory `.../node_modules/pm2` | **`r-x`** |
| `package.json` | readable, **not writable** |

**uid 994 can read and execute it, and cannot modify it** — not the binary, not its
parent directory, not the package, not `package.json`. That is exactly the asymmetry
that makes it safe to *use* and impossible to *tamper with* from the validation account.

### 4.2 The second executable

`/home/odbadmin/.npm-global/lib/node_modules/pm2/pm2` — regular file, `775`, 87 bytes,
sha256 `2c88ef6dfbc924be2f9a3e3e8487f6d62427ffc44088d57b37f77e50da2c4aee`, `r-x`,
not writable. **It resolves to the same package** — `pm2` `5.4.2`. It is **not** the
path proposed for use; §4.1's `bin/pm2` is.

### 4.3 The directory hit — defect 1 fixed, verified on the host

```
--- hit (NOT an executable candidate): /home/odbadmin/.npm-global/lib/node_modules/pm2
kind      : DIRECTORY
NOTE      : this is a DIRECTORY. Its x bit means TRAVERSABLE, not runnable.
            No package.json is derived for a directory hit, and none is
            reported missing -- probeB emitted exactly such a misleading line.
```

**No `package.json` or `VERSION` field is emitted for it**, and no "NOT readable" line.

**Defect 2 fixed, verified:** both real executables — one at `<pkg>/bin/pm2`, one at
`<pkg>/pm2` — resolved to the **same** `package.json` at `5.4.2`. `probeB`'s
`dirname(dirname())` derivation produced `.../lib` and `.../node_modules` for these and
then reported them "NOT readable"; those two misleading lines are gone.

---

## 5. `PM2_HOME` does not fall to production

```
PM2_HOME in environment : <unset>
would default to        : /home/woa23c1ro/.pm2      exists? no — and NOT created
production /home/odbadmin/.pm2          : exists  r=yes  w=NO
production /root/.pm2                   : not present or not visible
production /home/odbadmin/python/woa23  : exists  r=NO   w=NO
this probe set no PM2_HOME, ran no pm2, and created no directory
```

---

## 6. Before / after — nothing changed

| | BEFORE 04:51:23 | AFTER 04:51:50 | |
|---|---|---|---|
| HOME entries | 70122 | 70122 | **UNCHANGED** |
| `~/.pm2` | absent | absent | **not created** |
| PM2 daemons | 0 | 0 | **none started** |
| processes | 16 | 16 | **UNCHANGED** |
| listeners | 45 | 45 | **UNCHANGED** |
| `/tmp` entries | 309 | 309 | **UNCHANGED** |
| production pids : starttimes | 4296:14214, 5040:15825, 5041:15829 | identical | **UNCHANGED** |
| pm2G pids | three up | three up | **UNCHANGED** |
| **production `.pm2` mtime** | 1786685045 | 1786685045 | **UNCHANGED** |
| **`npm-global/bin/pm2` mtime** | 1728136844 | 1728136844 | **UNCHANGED** |
| boot id | `0b513a75…` | `0b513a75…` | **UNCHANGED** |
| probe / temp file | — | **none found** | **nothing written** |

The only HOME movement is the account's own session daemons — `wireplumber` and
`snapd-desktop-integration` — as in `probeA` and `probeB`. **The probe wrote nothing.**

Note the pm2 binary's own mtime is unchanged: reading and hashing it did not touch it.

---

## 7. What this establishes, and what it does not

**Establishes:** `pm2` **5.4.2** exists at `/home/odbadmin/.npm-global/bin/pm2`; uid 994
can **read and execute** it and **cannot modify** it at any level; it is **not** under
production PM2 state; and it is **not on the account's PATH**.

**Does NOT establish:**

- that it **runs** — the probe never executed it, so `r-x` is a permission fact, not proof;
- that the search was exhaustive — `/home/odbadmin/.local` was unreadable;
- **anything about B3 or B5**. The launcher argv PM2 actually starts is unverified and
  remains `b35a1`'s job.

**Classification stays `B_NOT_ON_PATH`.**

## 8. Evidence

| file | contents |
|---|---|
| `scratchpad/probeC/01-before.txt` | BEFORE snapshot |
| `scratchpad/probeC/02-probe.txt` | full probe output |
| `scratchpad/probeC/03-after.txt` | AFTER snapshot and digest-delta explanation |
