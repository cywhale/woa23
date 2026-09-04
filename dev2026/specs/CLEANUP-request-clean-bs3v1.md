# `CLEAN-bs3v1` — cleanup request for the state `bs3v1` and `b35a1` left on VM24

**Status: DRAFT REQUEST. Not authorised, not executed. Deliberately SEPARATE from
[`b1v1`](B1-host-validation-request-b1v1.md) and from every validation run.**

Under spec 014 §2, cleanup that follows a preserved state needs its **own** authorisation
and is never bundled into a validation run. Under the same policy, **daemon removal is
its own authorisation** and is not implied by stopping an app. This request keeps those
separations visible rather than collapsing them for convenience.

---

## 1. What is outstanding, and why each item is still there

| # | item | why it survives |
|---|---|---|
| 1 | staging **PM2 daemon, pid `1709473`**, `PM2_HOME=/home/woa23c1ro/woa23-bs3v1-pm2` | `bs3v1`'s cleanup wrapper failed; under 014 §2 the state was preserved. Not mine to remove. |
| 2 | the `bs3v1` **staging tree, bootstrap and workdir** | same preservation |
| 3 | `~/woa23-b35a1/` and `b35a1-archive.tar` | `b35a1` ended `INVALID_PRE_START`; nothing was started, and the tree was left as evidence of the refusal |

The `bs3v1` **app** is already stopped and port `18283` released — by the manual named-app
stop that 014 §2 now forbids. That act is recorded, not hidden, and is part of why the
policy changed.

**Nothing here is production.** No production path, port, daemon, config or data appears
in this request.

---

## 2. What this request is for

Removal of items 1–3 above, **after** their evidentiary value has been taken, and in an
order that keeps each step independently verifiable.

**It is explicitly not** a validation run, and produces no B-series evidence.

---

## 3. Capture before removal — this must happen first

Deleting a preserved failure destroys the record of the failure. Before anything is
removed:

1. **The daemon's own view** — `pm2 jlist` and `pm2 list` under the `bs3v1` `PM2_HOME`,
   captured verbatim. This is the same daemon whose field order broke the old parser, so
   its raw output is worth keeping even after the fix.
2. **The staging logs** — `staging.log`, `staging.outerr.log` and the PM2 daemon log,
   copied out.
3. **A file listing with digests** of the staging tree and of `~/woa23-b35a1/`, so what
   was removed is stated precisely rather than approximately.
4. **The process table** for pid `1709473` and any children, with `starttime`s, so the
   identity being removed is pinned and PID reuse cannot confuse the record.

If any capture fails, **the run stops and removes nothing.**

---

## 4. Removal — ordered, and each step verified

Each step is verified against the process table or the filesystem **before** the next
begins. A discrepancy between a command's exit code and the observed state stops the run.

1. **Confirm no app is running** under the `bs3v1` daemon. If one is, **STOP** — that is a
   different situation than this request describes, and it needs its own decision.
2. **Stop the daemon** — `pm2 kill` scoped to the `bs3v1` `PM2_HOME` only. Verify pid
   `1709473` is gone by `(pid, starttime)`, not by name.
3. **Remove the `bs3v1` staging tree, bootstrap and workdir**, then verify absence.
4. **Remove `~/woa23-b35a1/` and `b35a1-archive.tar`**, then verify absence.
5. **Final capture** — `ss -ltn` showing the relevant ports unbound, and the account's
   remaining directories.

**Constraints in force throughout:** `PM2_HOME` is explicit and has no default; nothing
outside the unprivileged staging account is touched; no wildcard removal; no `pm2 kill`
without a verified `PM2_HOME`; SIGKILL is not used to make a step succeed.

---

## 5. Ledger consequences

- `18283` — already **SPENT**; unchanged by this request.
- `18281` — **RETIRED-NEVER-BOUND**; unchanged, and specifically **not** reclassified as
  SPENT by anything here.
- No port is allocated, bound or released by this request beyond confirming that what is
  already released stays released.

---

## 6. What authorising this does and does not grant

Authorising this grants removal of **items 1–3 only**, on the accounts and paths named
above. It grants nothing on production, does not authorise `b1v1`, and does not carry
forward to any future cleanup — each preserved state, if there is another, needs its own
request.

---

## 7. Open decision, unrelated to VM24

Still awaiting your keep-or-remove decision, and deliberately **not** bundled into this
request: the ghrsst assessor-approval record at
`~/.config/ghrsst/p5_assessor_approval_7126c7b.json`. It is on the local machine, not
VM24, and is mentioned here only so it does not get lost.
