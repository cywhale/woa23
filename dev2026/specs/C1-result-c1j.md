# C1 `c1j` — result: `INCOMPLETE_VALIDATION` (aborted at step 1, no execution)

**Classification: `INCOMPLETE_VALIDATION`. NOT a PASS. NOT a candidate failure. NOT
quotable as any C1 result.**

The run aborted at pre-flight **step 1 — connection identity**. **Neither campaign SSH key
authenticates as `woa23c1ro`**, so the orchestration could not be started as uid 994, and
the only permitted alternative — launching from `odbadmin` or escalating — is forbidden.

Attempted 2026-08-25 under the C1 `c1j` authorisation of the same date.

---

## 1. What this is

| | |
|---|---|
| **classification** | **`INCOMPLETE_VALIDATION`** — aborted at pre-flight step 1 |
| **quotable?** | **NO.** Not as a PASS, not as a FAIL, not as evidence about the candidate |
| **a candidate failure?** | **NO.** The candidate was never exercised |
| **any arm started?** | **NO** |
| **any port bound?** | **NO** — 18341, 18342, 18979 all still unbound |
| **staging / workdir created?** | **NO** — `/home/woa23c1ro/woa23-c1j/` and `-work/` absent |
| **`run_controlled.sh` invoked?** | **NO** |
| **production API requests** | **ZERO** |
| **production store modified?** | **NO** — mtime unchanged |
| **pm2G** | **untouched** |

## 2. Where it stopped

**Step 1 of §5: "connection identity".** The authorisation requires the orchestration to
run as `woa23c1ro` over direct SSH with the campaign Ed25519 key and `BatchMode=yes`, and
explicitly forbids `sshpass`, `sudo`, privilege escalation, or launching from `odbadmin`.

Both campaign keys were offered and both were refused:

```
debug1: Offering public key: id_ed25519_odb ED25519
        SHA256:qPsalgToID/4ZurXwbT4z1eeZS30K9lbE84nJhxUXVI explicit
debug1: Authentications that can continue: publickey,password
woa23c1ro@192.168.2.24: Permission denied (publickey,password).
```

| key | fingerprint | result |
|---|---|---|
| `id_ed25519_odb` (`mac-claude`) | `SHA256:qPsalgToID/4ZurXwbT4z1eeZS30K9lbE84nJhxUXVI` | **denied** |
| `odbclaw_ed25519` (`odbclaw-audit`) | `SHA256:zME8cGMyxPjuKs4HfsqlmbnrQ4vNbOuQMVORcUxXBRI` | **denied** |

**No password was attempted and none will be.** `BatchMode=yes` was set throughout.

## 3. What the account looks like from outside

Read-only diagnosis via `odbadmin` — **inspection only, nothing launched, nothing
written**:

| | |
|---|---|
| `getent passwd` | `woa23c1ro:x:994:993::/home/woa23c1ro:/bin/bash` |
| home directory | **exists** — `drwx------ 6 woa23c1ro woa23c1ro`, mtime `8月 25 13:47` |
| login shell | **`/bin/bash`** — the earlier `/usr/sbin/nologin` has been fixed |
| uid / gid | **994 / 993**, private group only |
| `sshd_config` | `PubkeyAuthentication yes`; the only `Match` block is `Match User odbclaw_audit`, unrelated |

**The account is set up correctly in every respect I can observe.** Home, shell, uid, gid
and group are all exactly as the authorisation describes.

**What I cannot observe:** `/home/woa23c1ro/.ssh/authorized_keys`. The home is mode `700`
and owned by 994, so `odbadmin` cannot read it — **correctly**, and I did not attempt to
escalate to find out. So I can say the key is not being accepted; I cannot say why.

**The three candidates, and I cannot distinguish them:**

1. `authorized_keys` is absent or empty;
2. it holds a **different** public key than either of the two above;
3. permissions inside `.ssh` are such that sshd refuses the file — sshd ignores an
   `authorized_keys` that is group- or world-writable, and ignores `.ssh` itself if its
   permissions are too open.

## 4. What is needed

**One of the two public keys below installed in `/home/woa23c1ro/.ssh/authorized_keys`**,
with `.ssh` mode `700` and `authorized_keys` mode `600`, both owned by
`woa23c1ro:woa23c1ro`:

```
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGD0QR3e79MxD94OEfHEz775NN4LapfONCLjfXDgjfJZ mac-claude
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIMd7ByVpcLAiDY6l2C+e8NhtpVcuGSEE5Uvs3Yue0zQ7 odbclaw-audit
```

`mac-claude` (`id_ed25519_odb`) is the key this campaign uses for `vm24` and is the natural
choice.

**This campaign will not install it.** Writing into another account's home, or changing its
permissions, is host administration and is outside every authorisation given — the same
boundary that kept `chmod`, `chown`, ACL and account changes off the table throughout.

## 5. State on VM24 — nothing changed

**The production store, verified after the abort:**

| | |
|---|---|
| directory mtime | **`1787622836`** (`2026-08-25 09:53:56 +0800`) — **unchanged**, still the value `c1h`'s probe left |
| modifications | **none.** No write, no `chmod`, no `chown`, no ACL change, no `.lock` change |

**`pm2G`, untouched:**

| | |
|---|---|
| port `18265` | **still BOUND** |
| gunicorn 1456369, workers 1456373 / 1456374 | **still RUNNING** |
| PM2 entry, tree, workdir, store, logs, uv cache | **all retained** |

**No `pm2` command was issued, no process signalled, no port released, no cleanup.**

**`c1j`'s own identity — nothing created:**

| | |
|---|---|
| `/home/woa23c1ro/woa23-c1j/` | **absent** |
| `/home/woa23c1ro/woa23-c1j-work/` | **absent** |
| ports 18341 / 18342 / 18979 | **all unbound** |

## 6. Gate results

**None. No gate ran.**

| gate | result |
|---|---|
| canonical values and column sequence | **not run** |
| candidate column-order contract | **not run** |
| row-order contract `(time_period, depth, lat, lon)` | **not run** |
| JSON / CSV fields, values, status | **not run** |
| reconstruction rules | **not run** |
| **UID 994 evidence** | **none — no process was ever started to check** |
| **request count** | **0** — to the arms and to production alike |
| **cleanup** | **not applicable; nothing to clean.** No cleanup was performed |

## 7. The `c1j` identity

**Treated as CONSUMED, conservatively.** The campaign's own rule is that an identity is
consumed "once a run has been authorised and started, whether or not it reached the point
of binding anything", and an authenticated-connection attempt was made against VM24 under
this label.

**Nothing was created and no port was bound**, so ports `18341`, `18342`, `18979` are
**RETIRED-NEVER-BOUND**, not spent.

**A retry should therefore take a new label and new first-use ports.** If the PI judges
that a refused SSH handshake does not "start" a run, `c1j` could be reused — but that is
the PI's call, not mine, and reuse is the reading I will not take unilaterally.

## 8. Standing limits, unchanged

**B1–B5 remain open. B7 remains open.** **`pm2G` remains NOT A PASS.** **C2 remains
blocked** and was not run. **`c1f`, `c2g`, `s2pB`, `pm2G`, `c1h` and `c1i` are not
back-filled** into this run's evidence, and nothing here confirms any of them.

**The candidate `832e767` is neither validated nor invalidated.** Its contract remains
untested on VM24; the 132-run offline evidence stands on its own and is not a substitute.

## 9. Evidence

`scratchpad/c1ro/07-ssh-diag.txt` — the read-only diagnosis, the post-abort store mtime,
the pm2G state and the untouched `c1j` paths and ports.

**Nothing was created on VM24 by this attempt.**
