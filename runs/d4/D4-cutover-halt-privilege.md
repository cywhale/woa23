# D-4 cutover: **HALTED at the privilege preflight. Nothing was changed.**

**No cutover step was executed.** Production was not stopped, no PM2 operation was run, no
nginx file was read for modification or edited, no `nginx -t`, no reload, no app started, no
API request, no cache purge, no cleanup. **The window was not opened.**

---

## 1. Credentials — declined, and not needed

**I did not read `~/proj/woa23/.env`, and did not handle `VM24_USER` / `VM24_PASS`.**
Reading `.env*` and handling plaintext passwords are both outside what I may do, and the
project's own `safety_guard.py` hook denies `.env` reads independently.

**It made no difference to the work:** every VM24 command in this campaign, including the
preflight below, ran over **existing SSH key-based access** (`ssh -o BatchMode=yes vm24`).
No password was required, requested, displayed, logged or committed. No private-key material
was read or printed.

## 2. The privilege preflight — measured, read-only

| check | result |
|---|---|
| account | `odbadmin`, **uid 1000, euid 1000 — not root** |
| groups | `odbadmin root adm cdrom sudo dip plugdev staff lpadmin sambashare docker shiny-apps` |
| `/etc/nginx/conf2.d/routes-vm124.conf` | `-rw-r--r-- root root` — **NOT writable by `odbadmin`** |
| `/etc/nginx/conf2.d/` | `drwxr-xr-x root root` — **NOT writable** (so no create-and-replace either) |
| nginx master | pid **1088337, running as root** |
| `/run/nginx.pid` | **NOT writable** — a reload signal cannot be delivered |
| `/usr/sbin/nginx` | `-rwxr-xr-x root root` — no setuid bit |

**Conclusion: as `odbadmin`, and without privilege escalation, the runbook's steps 5, 6 and
their rollback equivalents are not performable.** Editing the route file, running `nginx -t`
and reloading nginx all require root.

### 2.1 The `sudo` group is not the answer, and I did not test it

`odbadmin` is a member of the **`sudo`** group. **That is suggestive, not proof** — group
membership does not by itself establish what `sudoers` permits, and confirming it requires
*invoking* `sudo`.

**I did not invoke `sudo`, and will not.** Privilege escalation is outside what I may do, the
project's safety hook denies it, and your own instruction says not to self-`sudo`, not to
change permissions, and not to look for an alternative path. So the capability remains
**unverified by me, by design**.

**As you noted: `odbadmin` being able to read the TLS private key is a consequence of its
`root`-group membership and directory modes — it is not root, and it does not confer the
ability to write `/etc/nginx` or signal the nginx master.** The preflight above confirms
exactly that: readable, not writable.

## 3. Why I stopped BEFORE step 2, not part-way through

This is the operational reason, not only the rule.

The window's first production action is `pm2 stop woa23`. Had I taken it and then met this
wall at step 5, **`/api/woa23` would be down** with the window unable to advance — service
restored only by an app-side rollback, having taken production down for a step that was never
reachable. **Stopping while production is still untouched is the only version of this halt
that costs nothing.**

Nothing is half-done: the old app is still serving, nginx is unmodified, and the f66
provisioning is intact and idle.

## 4. Everything else is ready and unchanged

| | |
|---|---|
| artifact identity | **intact** — `APP_ROOT=/home/odbadmin/python/woa23-f66ddd8` present, `uv.lock` digest `0d2980a5…dccc69`, `WOA23_PYTHON` present and executable |
| `143bf8c` tree/venv | **not used, not touched** |
| owner decisions 2–5 | **recorded and accepted**: S2 risk accepted (nothing modified, chmod-ed, chown-ed, moved, deleted or renewed; **S2 is NOT claimed resolved**); the three config deltas authorised with **placement 3a**; the two-line nginx change authorised for in-window use; the full window including in-window B1 authorised |
| the runbook | unchanged and ready to execute from step 1 |

## 5. What is needed to proceed

**One thing: an operator who actually holds root on VM24 for the privileged steps.** Either

- **(a)** a named human operator with root runs steps 5–6 and the nginx half of rollback,
  while I run the `[app]` steps as `odbadmin` and hold every gate — the two-operator model the
  runbook was written for; **or**
- **(b)** you confirm, having checked it yourself, exactly what `sudo` capability `odbadmin`
  has for these specific commands — and note that even then, **executing privilege escalation
  is not something I can do**; that half of the window needs a human.

**Until then D-4 remains NOT EXECUTION-READY**, now for a single, concrete reason rather than
five: the privileged half of the window has no operator who can perform it.

**No evidence claim is affected by this halt.** The D-3 observation, the synthetic harness
timings and the batch sentinel remain what they were — validation, not production equivalence
and not performance evidence. The AVX2 masking remains accepted, unresolved residual risk
under B6; store content integrity remains unproven; the old certificate and key are untouched.
