# `b35a1` usage plan — how the discovered pm2 would be used

**Status: OFFLINE PLAN. NOT a request to run. `b35a1` is NOT authorised.**
No VM24 contact. Submitted for review; it will not run until you authorise it separately.

**`probeC` classified the pm2 `B_NOT_ON_PATH`, and that stands.** This plan does not
depend on reclassifying it — using an absolute path is precisely how a `B` candidate is
used without touching PATH.

---

## 1. The binary this plan pins

```
WOA23_PM2_BIN = /home/odbadmin/.npm-global/bin/pm2
realpath        /home/odbadmin/.npm-global/lib/node_modules/pm2/bin/pm2
sha256          bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d
package         /home/odbadmin/.npm-global/lib/node_modules/pm2   (pm2 5.4.2)
uid 994         r-x on binary, parent dir and package; writable nowhere
```

`staging_execute.sh:386` already honours it: `PM2="${WOA23_PM2_BIN:-pm2}"`. **No code
change is needed to use an absolute path**, and none is proposed.

---

## 2. The constraints, and how each is met

| constraint | how |
|---|---|
| **absolute path via `WOA23_PM2_BIN`** | supplied on the run's own command line; the wrapper's existing default is not relied on |
| **do not modify host PATH** | nothing exported to the host, no profile touched, no symlink created. PATH stays exactly the nine entries `probeC` recorded |
| **re-check realpath, SHA-256, version and permissions before starting** | a pre-start gate, §3 |
| **stop on any digest or realpath change** | the gate **refuses**, before any PM2 invocation |
| **uid 994's own staging `PM2_HOME`** | `/home/woa23c1ro/woa23-b35a1-pm2/`, new, asserted absent first |
| **no production app name or `PM2_HOME`** | app `woa23-b35a1-candidate`; `PM2_HOME` never `/home/odbadmin/.pm2` — `production_stop.sh` requires `WOA23_PM2_HOME` explicitly and never defaults it |
| **no `pm2 * all`, `kill`, global `save`, `resurrect`** | §4 |
| **re-confirm 18281 and the staging identity** | full pre-flight, §5 |
| **record the PM2 version asymmetry** | §6 |
| **report B3 and B5 separately** | §7 |
| **no staging PASS described as closing a production blocker** | §7.1 |

---

## 3. The pre-start gate — re-verify, then refuse on any drift

Run **before** any pm2 invocation, and **before** any tree, workdir or `PM2_HOME` is
created. Every value is compared against what `probeC` recorded:

| check | expected |
|---|---|
| `readlink -f /home/odbadmin/.npm-global/bin/pm2` | `/home/odbadmin/.npm-global/lib/node_modules/pm2/bin/pm2` |
| `sha256sum` of that realpath | `bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d` |
| `package.json` name / version | `pm2` / **`5.4.2`** |
| binary rwx for uid 994 | `r-x` — **readable, executable, NOT writable** |
| parent dir rwx | `r-x`, not writable |
| package dir rwx | `r-x`, not writable |
| owner / mode | `odbadmin:odbadmin` / `775` |
| production state | **not** under `/home/odbadmin/.pm2`, `/root/.pm2`, or the production tree |

**Any mismatch — realpath, digest, version, or the binary becoming writable — is a
STOP.** Not a warning, not a re-hash, not a retry: the run ends before it starts, and
nothing has been created.

**Why this gate exists:** the binary lives under **another account's home**. `odbadmin`
can replace it between `probeC` and `b35a1`, and the validation account would have no
way to notice. Re-verifying at the moment of use is what turns "we checked once" into
"we checked now". A digest change would mean the pm2 being run is not the pm2 that was
reviewed.

---

## 4. What `b35a1` will and will not invoke

**Will:** a named-app `start` against its own `PM2_HOME`, argv capture, and a named-app
`stop`, followed by release verification.

**Will NOT, under any outcome:**

- `pm2 * all` in any form — `production_stop.sh` refuses `all` explicitly, because it
  would reach every app under a `PM2_HOME`, and production's holds `dask-scheduler`,
  `dask-worker`, `ghrsst`, `mhwapi` and the WOA23 app
- `pm2 kill` — it would take the daemon down, not an app
- `pm2 save` — it writes a resurrect list, which is persistent state
- `pm2 resurrect` — it would start apps from a saved list
- anything at all against `/home/odbadmin/.pm2`
- `pm2G`, port 18265, its app entry or retained state — untouched, not cleaned, not
  inspected destructively

---

## 5. Pre-flight, re-confirmed rather than carried over

Nothing from an earlier run is assumed. In this order, and **all before any arm starts**:

1. identity `uid=994(woa23c1ro) gid=993`, not `odbadmin`, `HOME=/home/woa23c1ro`;
2. **the §3 pm2 gate** — first, because it is the cheapest refusal;
3. `node` present and executable — `probeC` recorded `/usr/bin/node` v22.14.0;
4. **18281 live-unbound**, and **absent from the subject's own ledger**;
5. staging paths absent: `woa23-b35a1`, `-work`, `-pm2`, `tmp-b35a1`;
6. `PM2_HOME` resolves to `/home/woa23c1ro/woa23-b35a1-pm2/` and is asserted **not**
   production's;
7. subject archive, file count, file-list and per-file hashes;
8. production PID/starttime identity, listeners via `ss -ltn` only, boot id;
9. **pm2G** in its expected retained state;
10. store read-only scan as uid 994.

**Abort before creating anything** on any mismatch.

---

## 6. The PM2 version asymmetry — recorded now, and again in the result

**Staging will run PM2 `5.4.2`** (`/home/odbadmin/.npm-global`, sha256 `bbb58671…`).

**Production's PM2 version is UNVERIFIED.** Determining it would require executing pm2
or reading its daemon state, both forbidden — so it was not determined, and this is
**not** an omission to be filled in later by inference.

**If the two differ, `b35a1` exercises a different PM2 than production runs**, and every
finding inherits that limitation. This sentence will appear in the `b35a1` result, not
only here.

### 6.1 The shared-dependency risk, restated for the record

The pm2 is under `odbadmin`'s home. Using it couples the validation account to a path
another account owns and can change, and to `/home/odbadmin` staying traversable. uid 994
**cannot modify it**, which is what makes it trustworthy to use; but the coupling is the
opposite of what the non-owner discipline was built for, and the §3 gate is the
mitigation, not a cure. **This warrants your explicit acceptance rather than my
assumption.**

---

## 7. How the result will be reported

**B3 and B5 as two separate findings**, never merged:

| finding | what staging can show |
|---|---|
| **B3** | the argv binds the port from `WOA23_PORT`; **no `8050` literal** is reachable |
| **B5** | the argv contains **no `--reload`** |

Plus, kept distinct from both: the **tool capability** result (`probeA`/`probeB`/`probeC`),
and the **remaining production/cutover validation**.

### 7.1 What will NOT be said

**No staging PASS will be described as closing B3, B5, or any B1–B5 blocker on
production.** The defect is live in `conf/start_app.sh` (`4aaed5b7…`), which `b35a1`
never touches. **A file that is not installed cannot close a blocker about the file that
is.** The result will state, per blocker, what only a cutover can establish.

---

## 8. Submission

This plan is submitted for **review only**. `b35a1` has not been run, no VM24 contact has
been made for it, and it will not run until you authorise it separately.
