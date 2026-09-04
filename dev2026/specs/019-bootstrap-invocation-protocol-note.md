# 019 — Bootstrap invocation: the two corrections every future request must carry

**Status: PROTOCOL NOTE, binding on FUTURE requests. It changes no code and no subject.**

`b1s1` hit two pre-start refusals before it ran. Neither damaged anything, and both are
recorded in [the `b1s1` result §1.1 and §9](B1-staging-stop-path-result-b1s1.md). One was a
wrapper mistake; the other is a **defect in the subject's own usage example**, which is
still there and is deliberately left there.

This note exists so the next request does not rediscover either. **It is not a fix.**
Changing `staging_bootstrap.sh` would change the subject, and `b1s1` is **not** to be re-run
for this.

---

## 1. `--pm2-home` must be given TWICE

### 1.1 What the subject's example says, and why it cannot work

`deploy/staging_bootstrap.sh` at subject `787a72d` documents:

```
#   ./deploy/staging_bootstrap.sh \
#       --archive   /path/to/subject.tar \
#       --bootstrap /home/woa23c1ro/b35b1-bootstrap \
#       --root      /home/woa23c1ro/woa23-b35b1 \
#       --workdir   /home/woa23c1ro/woa23-b35b1-work \
#       --tmpdir    /home/woa23c1ro/tmp-b35b1 \
#       --pm2-home  /home/woa23c1ro/woa23-b35b1-pm2 \
#       -- --phase stage --label b35b1 --port 18291 --app woa23-b35b1-candidate \
#          --files 203 --filelist <sha256>
```

The handover is `staging_bootstrap.sh:332`:

```bash
exec "$BOOT/deploy/staging_execute.sh" --root "$ROOT" --archive "$ARCHIVE" "$@"
```

Only `--root` and `--archive` are supplied automatically — which is exactly what line 42
of the same file says. But `staging_execute.sh:131` makes `--pm2-home` **required in the
stage phase**, and the example never puts it after the `--`.

**So the documented example, followed verbatim, dies at the driver's argument check.** The
bootstrap's `--pm2-home` is consumed by the bootstrap's *own* path guards and is never
passed on.

### 1.2 The corrected form — use this

```
WOA23_PM2C_GRANTED=yes bash <bootstrap-script-path>/staging_bootstrap.sh \
  --archive        <archive> \
  --archive-sha256 <sha256> \
  --bootstrap      <FRESH path that does NOT exist> \
  --root           <root> \
  --workdir        <workdir> \
  --tmpdir         <tmpdir> \
  --pm2-home       <pm2home> \
  -- --phase stage --label <label> \
     --pm2-home <pm2home> \        # <-- REQUIRED AGAIN, for the driver
     --port <port> --app <app> --files <n> --filelist <sha256>
```

**The repetition is not redundancy.** The first `--pm2-home` tells the *bootstrap* which
path to treat as an identity path when checking that the bootstrap sits outside all of
them. The second tells the *driver* where to put the daemon. They are read by two different
programs for two different reasons, and neither can be inferred from the other.

---

## 2. The bootstrap script must NOT live in the `--bootstrap` directory

The `--bootstrap` path **must not exist** when the tool runs — the guard refuses a
pre-existing one, because it may hold a driver from another archive. The tool creates it.

`b1s1`'s first refusal was caused by extracting the bootstrap *script* into the very
directory then passed as `--bootstrap`.

### 2.1 The corrected form

```
LAUNCH=<somewhere else entirely>      # holds the bootstrap SCRIPT
BOOT=<fresh, non-existent>            # passed as --bootstrap; the TOOL creates it

mkdir -p "$LAUNCH"
tar -xO -f "$ARCHIVE" dev2026/deploy/staging_bootstrap.sh > "$LAUNCH/staging_bootstrap.sh"
# hash it from the archive AND on disk, and compare, before running it
chmod +x "$LAUNCH/staging_bootstrap.sh"
bash "$LAUNCH/staging_bootstrap.sh" --bootstrap "$BOOT" ...
```

**A retry needs a NEW `--bootstrap` path.** Each attempt consumes one, because the tool
creates the directory and will refuse it next time. That is correct: a bootstrap directory
is where a driver from *some* archive already lives. Bootstrap paths are **not** part of
the run identity ([spec 013](013-b1-b5-host-validation-matrix.md)), so consuming several is
not consuming identities — but every one must be **retained, not deleted**, and listed in
the result.

---

## 3. What a future request must state

| | requirement |
|---|---|
| 1 | the `--pm2-home` **passthrough** value, shown explicitly in the request's own command block |
| 2 | the **bootstrap script path**, distinct from the `--bootstrap` path |
| 3 | the `--bootstrap` path, and that it **does not exist** |
| 4 | that additional bootstrap paths may be consumed by a refusal, will be **retained**, and will be listed |

---

## 4. What this note does NOT do

- **It does not modify `staging_bootstrap.sh`**, or any file in subject `787a72d`.
- **It does not require `b1s1` to be re-run.** `b1s1`'s classification is unchanged:
  **qualified staging-only stop-path PASS**, with its six limitations intact.
- **It does not close production B1**, which remains unvalidated and unauthorised.
- **It authorises no cleanup.** `bs3v1`'s and `b1s1`'s retained daemons and trees stay
  exactly as they are.

Whenever `staging_bootstrap.sh` is next changed for some other reason, the usage example
should be corrected then — as part of a new subject, with its own batches. Until that
happens, **this note is the correction**, and the defect stays in the subject where the
`b1s1` record can point at it.
