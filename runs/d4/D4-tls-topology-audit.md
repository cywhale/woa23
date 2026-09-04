# D-4 — read-only TLS topology audit

**Read-only. Nothing was modified.** No production stop, start, restart, reload or
reconfigure. **No API request was issued.** No TLS file was modified, `chmod`-ed,
`chown`-ed, renewed, copied or deleted. No private-key material was printed.

**Headline: the live architecture is NEITHER branch A nor branch B. It is BOTH — public TLS
at nginx, re-encrypted to the application over HTTPS on loopback.** The application's expired
certificate is **in active use**, not stale configuration.

---

## 1. Where TLS terminates — evidenced

```
client ──TLS──▶ nginx  0.0.0.0:443        public terminator, VALID certificate
                  │
                  │  location /api/woa23 { proxy_pass https://woa23api; }
                  │  upstream woa23api { server 127.0.0.1:8050; }
                  ▼
              gunicorn 127.0.0.1:8050     SECOND TLS, EXPIRED certificate, unverified
```

| | |
|---|---|
| public terminator | **nginx**, master pid 1088337, `root`, `-c /etc/nginx/nginx.conf` |
| public listeners | `0.0.0.0:443`, `0.0.0.0:80` |
| active site | `/etc/nginx/sites-enabled/vm124.conf` → `/etc/nginx/sites-available/vm124-final.conf` |
| `server_name` | **`eco.odb.ntu.edu.tw`** |
| include chain | `nginx.conf:137` → `conf2.d/upstreams-vm124.conf`; `nginx.conf:139` → `sites-enabled/vm124.conf` → `conf2.d/routes-vm124.conf` |
| WOA23 routes | `location /api/woa23` and `location /api/swagger/woa23`, both `proxy_pass https://woa23api` |
| upstream | `upstream woa23api { server 127.0.0.1:8050; }` |

**apache2 is also running** (pid 1022196) but **contains no reference to 8050** — it does not
serve the WOA23 API.

`/etc/nginx/conf2.d/upstream01.conf` also defines a `woa23api` upstream on 8050 but **is not
included by any active config** — inactive file, recorded so it is not mistaken for live.

## 2. The application terminates TLS too — and its certificate is ACTIVE

Live argv of the serving master (pid 1828352):

```
… gunicorn woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8050 \
  --keyfile conf/privkey.pem --certfile conf/fullchain.pem --timeout 120 --reload
```

`--keyfile`/`--certfile` are present, so **gunicorn speaks TLS on 8050**, and nginx reaches it
with `proxy_pass **https**://woa23api`.

**Therefore the expired application certificate is neither unused nor stale — it is serving
the nginx→app hop right now.**

**Why an expired certificate works there:** nginx does not verify it. There is **no
`proxy_ssl_verify` directive in scope for the WOA23 locations** — the one at
`routes-vm124.conf:36` sits inside `location @tilesfallback`, a different block — so nginx's
**default `proxy_ssl_verify off`** applies. The internal hop is encrypted but unauthenticated.

## 3. Certificate inventory — safe metadata only

### 3.1 Public terminator (nginx, port 443) — **VALID**

| | |
|---|---|
| certificate | `/etc/letsencrypt/live/eco.odb.ntu.edu.tw/fullchain.pem` |
| realpath | `/etc/letsencrypt/archive/eco.odb.ntu.edu.tw/fullchain40.pem` (symlink) |
| private key | `/etc/letsencrypt/live/eco.odb.ntu.edu.tw/privkey.pem` → `…/archive/…/privkey40.pem` |
| owner / mode (both symlinks) | `root:root` **777** |
| subject | `CN = eco.odb.ntu.edu.tw` |
| issuer | `C = US, O = Let's Encrypt, CN = YR2` |
| validity | `notBefore Jul 13 23:49:43 2026 GMT` · `notAfter Oct 11 23:49:42 2026 GMT` |
| **currently valid** | **YES** (and still valid 30 days hence) |
| SAN | `DNS:eco.odb.ntu.edu.tw` |
| cert public-key sha256 | `eb7dc653e2bbb2ef209a38d45b65bcad5e978122942ce4dd586cc5ddcc1a7c19` |
| readable by the production account | **YES — see §4.2** |

### 3.2 Application (gunicorn, loopback 8050) — **EXPIRED, but ACTIVE**

| | |
|---|---|
| certificate | `/home/odbadmin/python/woa23/conf/fullchain.pem` (realpath same, not a symlink) |
| private key | `/home/odbadmin/python/woa23/conf/privkey.pem` (realpath same, not a symlink) |
| owner / mode | `odbadmin:odbadmin` **644** — both |
| subject | `CN = eco.odb.ntu.edu.tw` |
| issuer | `C = US, O = Let's Encrypt, CN = R3` |
| validity | `notBefore May 27 23:22:04 2023` · `notAfter Aug 25 23:22:03 2023 GMT` |
| **currently valid** | **NO — expired over three years ago** |
| SAN | `DNS:eco.odb.ntu.edu.tw` |
| cert/key pair match | **YES** — public-key sha256 `373aeb238f8c398c7ff81b7ce62415fa5b55d9fc9de14f2510365f29f7f782a1` on both |
| readable by the production account | yes |

**The two are different key pairs** (`eb7dc653…` vs `373aeb23…`), so renewing one does not
touch the other.

## 4. Security findings

### 4.1 The application private key is mode 644 — world-readable, and ACTIVE

`/home/odbadmin/python/woa23/conf/privkey.pem` is `644`. **Any local account can read it.**

**It is used by the live application, but it is NOT the public terminator's key** — so it
fits neither of the two categories the review anticipated. Stating it exactly:

- it is **not stale configuration**, so it cannot be set aside as an unused finding;
- it is **not the public TLS key**, so a client-facing compromise does not follow directly;
- it **is** the key authenticating the nginx→app hop, on a hop nginx does not verify.

**Classification: an ACTIVE security finding requiring separate permission-remediation
authorization.** It is not fixed here, and it is kept out of the cutover change.

### 4.2 The PUBLIC terminator's private key is readable by the production account

`/etc/letsencrypt/live` and `/etc/letsencrypt/archive` are `root:root` **710** — but the
production account is a member of the **`root` group**:

```
odbadmin groups: odbadmin root adm cdrom sudo dip plugdev staff lpadmin sambashare docker shiny-apps
```

so the group bits satisfy traversal, and `test -r` on `privkey.pem` returns **YES**. The
`live/` symlinks are additionally mode **777**.

**This is a more serious finding than 4.1**: it is the key for public TLS. It is **recorded,
not changed**, and it is **out of scope for the cutover** — remediation needs its own
authorization and its own thought about what else depends on that group membership.

## 5. Branch determination

| branch | description | evidenced? |
|---|---|---|
| **A** | reverse proxy terminates TLS; app runs loopback HTTP with TLS off | **NO** — nginx uses `proxy_pass https://`, so a TLS-off app would break the route |
| **B** | the app terminates TLS and needs a valid, correctly protected cert/key | **PARTLY** — the app does terminate TLS, but its certificate is expired and its key is 644 |

**Neither branch describes the live system on its own. The evidenced architecture is a
hybrid: public TLS terminated at nginx with a valid certificate, then re-encrypted to the
application over an unverified internal TLS hop using an expired certificate.**

This is not reported as unresolved — the configuration is explicit and was read directly. It
is reported as *a third shape the plan did not anticipate*.

### 5.1 What this means for the cutover — a real constraint

**The new application cannot simply run with `WOA23_TLS=off`.** Every candidate run in this
campaign used TLS off; if the cutover keeps that, `proxy_pass https://woa23api` would attempt
TLS against a plaintext socket and `/api/woa23` would fail.

Two coherent options, neither chosen here:

| option | change | consequence |
|---|---|---|
| **B-keep** | new app terminates TLS on 8050 as today | smallest change; **perpetuates the expired certificate and the 644 key** |
| **A-move** | new app runs plaintext on 8050 **and** nginx changes to `proxy_pass http://woa23api` | removes the second TLS layer and both findings from the app; **but changes nginx**, which is outside the cutover's current scope and needs its own authorization and rollback plan |

**This is an owner decision.** The D-4 plan currently assumes the app's TLS variables are set
from the proposed config, which corresponds to **B-keep**.

## 6. The hostname question — candidates only, no selection

**I am not choosing, and I have not chosen.**

| candidate | evidence |
|---|---|
| **`eco.odb.ntu.edu.tw`** | the `server_name` of the **live active** nginx server block that carries `location /api/woa23`; the SAN of the valid public certificate; the SAN of the app certificate |
| `api.odb.ntu.edu.tw` | appears 5× in the repository; **not** a `server_name` in the active nginx config |
| `www.odb.ntu.edu.tw` | appears 1× in the repository; **not** a `server_name` in the active nginx config |

The live configuration points at one name. **That is evidence, not a decision** — whether
`eco.odb.ntu.edu.tw` is the *official* production hostname for the WOA23 API, or whether the
service is meant to move to another name, is yours to state explicitly.

**The SAN-coverage check remains unestablished until you state the hostname.**

---

## 7. Status

| | |
|---|---|
| audit | **read-only, complete**; nothing modified, no API request issued |
| TLS termination | **nginx (public, valid cert) AND the app (loopback, expired cert)** — §1, §2 |
| public listener serving the API | nginx `0.0.0.0:443`, `server_name eco.odb.ntu.edu.tw` |
| upstream to 8050 | `upstream woa23api`, reached by `proxy_pass https://woa23api` |
| app certificate | **ACTIVE, not stale** — and expired |
| finding 4.1 | app private key `644`, active — **separate permission-remediation authorization** |
| finding 4.2 | **public** TLS private key readable by the production account via `root` group — **separate authorization**, more serious |
| branch | **hybrid** — neither A nor B alone; §5.1 sets out B-keep and A-move |
| hostname | **candidates reported, selection NOT made** — §6 |
| cutover | **not authorized, not prepared, not performed** |
