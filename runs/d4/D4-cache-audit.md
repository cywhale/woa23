# D-4 — read-only nginx response-cache audit for the WOA23 locations

**Read-only. Nothing was modified.** No nginx, TLS, PM2 or production configuration was
changed. **No reload, no purge, no cache entry opened, deleted or modified. No API request
was issued.** Run as `odbadmin` (uid 1000).

**Headline: WOA23 IS cached, and a 400 response IS explicitly cacheable.** The cache key does
not contain the upstream scheme, so **the A-move change neither invalidates nor re-keys any
existing entry** — every cached WOA23 response survives the cutover addressable exactly as it
was. A stale pre-cutover 400 can still be served afterwards, by a mechanism that is now
identified precisely. There is an intended way to defeat it that requires **no purge and no
configuration change** — but **that bypass is UNPROVEN and is recorded as a blocker**
(§4.6), not as a solution. **No cache purge is planned at any point.**

Evidence: [`cacheaudit-1.log`](cacheaudit-1.log), [`cacheaudit-2.log`](cacheaudit-2.log).

---

## 1. Is proxy cache actually enabled for WOA23? — **YES, for both locations**

The chain is textual and complete, read from the files:

```
conf2.d/routes-vm124.conf:151  location /api/woa23 {
                        :152      proxy_pass https://woa23api;
                        :153      include conf2.d/aio_cache_proxy.conf;   ─┐
                        :156  location /api/swagger/woa23 {                │
                        :157      proxy_pass https://woa23api;             │
                        :158      include conf2.d/aio_cache_proxy.conf;   ─┤
                                                                          ▼
conf2.d/aio_cache_proxy.conf   include conf2.d/proxy_pass_snippet.conf;
                               include conf2.d/aio.conf;
                               include conf2.d/api_cache_proxy.conf;     ─┐
                                                                          ▼
conf2.d/api_cache_proxy.conf:1  proxy_cache api_proxy;                    ◀── caching is ON
```

**Neither WOA23 location contains any cache directive of its own** — caching arrives solely
through the include on lines 153 and 158.

**The scope of `proxy_cache off` was checked rather than assumed.** `routes-vm124.conf:170`
does contain `proxy_cache off;`, but reading the enclosing block shows it is inside
`location /mcp/metocean` (lines 162–172), whose own comment says *"MCP streaming must not use
the API response cache."* **It does not apply to WOA23.** (The same class of scope error
produced a wrong reading of `proxy_ssl_verify` earlier in this campaign; it was checked here
for that reason.)

### 1.1 One limit, stated rather than papered over

**`nginx -T` — the resolved runtime view — is UNMEASURED.** It requires root and `odbadmin`
cannot run it. **Everything above is established from the static configuration files**, which
is a complete and unambiguous include chain, but it is not the same as reading the
configuration the running master actually holds. Confirming that is a `[root]` action and
belongs with the nginx step of the window (§10A.5 check 3 of the plan already requires the
`nginx -T` dump).

## 2. Cache zone, storage and key

| | |
|---|---|
| zone | **`api_proxy`**, `keys_zone=api_proxy:8m` |
| storage path | **`/tmp/nginx-api-cache`** |
| layout | `levels=1:2` |
| max size | `max_size=1000m` |
| inactive | `inactive=600m` — an entry not *accessed* for 10 hours is evicted regardless of validity |
| temp path | `proxy_temp_path /tmp` |
| declared in | `conf.d/api_proxy_cache.conf:13` — glob-included by `nginx.conf:138` |
| cached methods | `proxy_cache_methods GET POST` |

**The cache key is `"$http_host$request_uri"`.**

| component | in the key? | consequence |
|---|---|---|
| **host** | **YES** — `$http_host` is the request's `Host` header | entries are per-hostname; `eco.odb.ntu.edu.tw` has its own |
| **URI path** | **YES** | `/api/woa23` and `/api/woa23/csv` are **separate entries** — JSON and CSV cannot collide |
| **query string** | **YES** — `$request_uri` is the full original request URI including `?…` | every distinct query is its own entry |
| **scheme** | **NO** | an `http://` and an `https://` request for the same host+URI share one entry |
| **upstream scheme** | **NO** | **this is the one that matters for A-move — see §4** |
| **request body** | **NO** | see §2.1 |
| `Accept` / any `Vary` | **NO** | not applicable here, because format is selected by URI, not by header |

### 2.1 The body-independent key is NOT a WOA23 hazard

`proxy_cache_methods GET POST` combined with a key that omits the request body would let two
POSTs with different bodies share one cached response. **The WOA23 API is GET-only** — its
four routes are `@app.get` (`/api/woa23`, `/api/woa23/csv`, `/api/swagger/woa23`,
`/api/swagger/woa23/openapi.json`). **So this does not affect WOA23**, and it is recorded
here only so the reading is not left ambiguous. Whether it affects other APIs sharing this
snippet is outside this audit's scope and is **not** asserted either way.

## 3. Can a 400 response be cached? — **YES, explicitly, for 1 minute**

```
conf2.d/api_cache_proxy.conf:4   proxy_cache_valid 200 302 10d;
conf2.d/api_cache_proxy.conf:5   proxy_cache_valid 400 404 1m;      ◀── 400 IS cached
```

**A 400 is stored with a 1-minute freshness lifetime.** This is not incidental — it is
configured deliberately and explicitly.

**Upstream `Cache-Control` / `Expires` headers from the application are UNMEASURED**, because
measuring them would require issuing an API request, which this audit is forbidden to do.
There is **no `proxy_ignore_headers`** in scope for WOA23, so upstream cache headers, if the
application sends any, would still be honoured. Whether it sends any is not established.

## 4. Could the existing cache hold an old WOA23 400? — **possible; currently UNMEASURED**

### 4.1 Whether one is there right now — **UNMEASURED, and not inferred**

```
/tmp/nginx-api-cache   drwx------  nginx:root  uid 121  mode 700
readable by odbadmin    : NO
traversable by odbadmin : NO
```

**The directory is mode 700 and owned by `nginx`. `odbadmin` cannot list or traverse it, so
the presence or absence of any specific cached entry cannot be established.** Recorded as
**UNMEASURED**. No inference is drawn from the directory's own mtime, and **no cache entry
was listed, opened, read, purged or modified.**

### 4.2 Whether the cache survives the maintenance window — **YES**

| | |
|---|---|
| `/tmp` | not a separate mount; `systemd-tmpfiles`: `D /tmp 1777 root root 30d` — emptied at boot, aged at 30d |
| host uptime | 2 weeks 5 days |
| eviction | `inactive=600m` — 10 hours without an access |

**Nothing about the cutover clears this cache.** Stopping and starting the application does
not; changing the nginx upstream scheme does not; an nginx **reload** does not.

### 4.3 The key does not change — so entries carry straight across the cutover

**`$http_host$request_uri` contains nothing about the upstream.** Changing
`proxy_pass https://woa23api` to `proxy_pass http://woa23api` therefore **does not alter a
single cache key**. Every pre-cutover entry, including any cached 400, remains addressable by
exactly the same key after the change.

**This is the mechanism by which a pre-cutover 400 can be served after cutover.** It is not
speculative — it follows directly from the configured key.

### 4.4 How a STALE 400 can still be served, even though it is fresh for only 1 minute

```
conf2.d/api_cache_proxy.conf:12  proxy_cache_use_stale error timeout updating
                                                     http_500 http_502 http_503 http_504;
conf2.d/api_cache_proxy.conf:13  proxy_cache_background_update on;
conf2.d/api_cache_proxy.conf:11  proxy_cache_revalidate on;
conf2.d/api_cache_proxy.conf:14  proxy_cache_lock on;
```

Two distinct paths, both live during this window:

| # | when | what happens |
|---|---|---|
| **1** | **during the outage** (§10 steps 3a–5), upstream down | `use_stale error timeout` lets nginx serve a **stale cached response instead of 502** — including a stale **400** |
| **2** | **on the first request after cutover** for a previously-cached URI | `use_stale updating` + `background_update on` serve the **stale entry to that client** while nginx fetches the new response in the background. The **second** request gets the new 200 |

**Path 2 is the concrete way a post-cutover smoke check could see the old 400 and read as a
failure of the deployment.** It is a caching artefact, and the deployment would be correct.

### 4.5 The fix — no purge, no configuration change, and it is observable

Measured from `conf.d/api_proxy_cache.conf` and `conf2.d/api_cache_proxy.conf`:

```
map $request_method    $not_post          { default 1; POST 0; }
map $http_cache_control $api_cache_bypass { default $not_post; "" 0; }

proxy_cache_bypass $request_body_file $api_cache_bypass;
proxy_no_cache     $request_body_file;
add_header X-api-cache $upstream_cache_status;
```

**A GET carrying any non-empty `Cache-Control` request header sets `$api_cache_bypass = 1`,
so nginx BYPASSES the cache and fetches from the upstream.** And because
`$request_body_file` is empty for a GET, `proxy_no_cache` is 0, so **the fresh response is
also stored** — replacing the stale entry for every subsequent client.

### 4.6 THE BYPASS IS NOT PROVEN — `CACHE-BYPASS-UNPROVEN`, a BLOCKER

**§4.5 is a reading of nginx directive semantics from static configuration files. It is not a
measurement, and this audit does not present it as one.**

| # | why it is unproven | what would close it |
|---|---|---|
| 1 | the **resolved running configuration is UNMEASURED** — `nginx -T` needs root (§1.1). What the running master actually holds has not been seen | the `[root]` operator's `nginx -T` dump |
| 2 | **the behaviour has never been observed.** No API request may be issued, so no response carrying `X-api-cache: BYPASS` has ever been seen. map -> variable -> directive is an **inference** | one observed request, inside the cutover window |

**Recorded as a BLOCKER: `CACHE-BYPASS-UNPROVEN`. The cutover plan may not be marked
execution-ready while it stands, and the bypass must not be described as established.**

### 4.7 `add_header X-api-cache` has no `always` — a 400 carries NO cache header

```
conf2.d/api_cache_proxy.conf:15   add_header X-api-cache $upstream_cache_status;
```

**There is no `always` parameter.** nginx adds an `add_header` field only for responses with
status 200, 201, 204, 206, 301, 302, 303, 304, 307 or 308. **400 is not among them.**

**A stale-400 response will therefore arrive with no `X-api-cache` header at all.** Any
procedure that identifies a cache result by reading `HIT` or `STALE` off a returned 400 is
reading a field that is not present.

**WITHDRAWN — an earlier revision of this document proposed the following discriminator:**
that a `Cache-Control: no-cache` request returning **200** followed by a plain request
returning **400** *is* a cache result and not an application failure. **That does not follow
and is withdrawn.** The same observation is equally consistent with a routing or upstream
fault that differs between the two requests, and `X-api-cache` cannot arbitrate because **the
400 may carry no such header**. The two are **not distinguishable** from that evidence.

**The cutover plan therefore classifies it as a single, undivided
`CACHE_OR_ROUTING_FAILURE` and stops** — see §13.2 of [`D4-cutover-plan.md`](D4-cutover-plan.md).
Separating the two requires the `[root]` operator's `nginx -T` dump and the `X-api-cache`
values from responses that *do* carry the header; that is a separate diagnostic step, not an
inference to make inside a gate.

**What the request ordering does establish**, and all it establishes: the loopback request is
authoritative for application behaviour because it never traverses nginx; and an
`X-api-cache: BYPASS` on the `no-cache` public request — present, because that request is
expected to be a **200** — is the single observation that closes `CACHE-BYPASS-UNPROVEN`.
**If it does not report `BYPASS`, §4.5's reading is wrong: stop, and do not improvise a purge
or a cache-directive change.**

**Nothing here purges anything.** Request 1 replaces one entry by ordinary cache behaviour,
which is not a purge; no cache file is touched by hand, and no configuration changes.

---

## 5. Answers to the six audit questions

| # | question | answer |
|---|---|---|
| 1 | is proxy cache really enabled for WOA23? | **YES** — `proxy_cache api_proxy`, via the include at `routes-vm124.conf:153` and `:158`, for **both** locations. `proxy_cache off` at line 170 belongs to `location /mcp/metocean` and does not apply |
| 2 | zone / storage / key composition | zone **`api_proxy`** (8m); storage **`/tmp/nginx-api-cache`**, `levels=1:2`, `max_size=1000m`, `inactive=600m`. Key **`"$http_host$request_uri"`** — **host YES, URI YES, query string YES, scheme NO**, upstream scheme NO, body NO |
| 3 | can a 400 be cached? | **YES** — `proxy_cache_valid 400 404 1m`, explicitly, 1-minute freshness |
| 4 | could the cache hold an old WOA23 400? | **Possible, and it would survive the cutover unchanged** because the key omits the upstream scheme. **Whether one exists right now is UNMEASURED** — §4.1. A stale one can still be served, by two identified paths — §4.4 |
| 5 | unreadable directories | **`/tmp/nginx-api-cache` is `nginx:root` mode 700 — UNMEASURED, not inferred.** `nginx -T` also UNMEASURED (needs root) |
| 6 | no cache entry cleared, modified or touched | **Confirmed.** No entry was listed, opened, read, purged or modified; no reload; no purge; no API request. **No purge is planned for the cutover either** |

**Beyond the six questions, two things that change the procedure:**

| | |
|---|---|
| **`CACHE-BYPASS-UNPROVEN`** | the `Cache-Control` bypass is read from static files only. `nginx -T` is root-only and no request has been observed. **BLOCKER** — §4.6 |
| **`add_header X-api-cache` has no `always`** | a **400 carries no `X-api-cache` header**, so cache status cannot be read off a failing response. The working discriminator is the request *pair* — §4.7 |

## 6. What this audit did NOT do

- did **not** open, read, list, purge, delete or modify any cache **entry**;
- did **not** reload, restart or reconfigure nginx;
- did **not** issue any API request — so the application's own response headers are
  **UNMEASURED**;
- did **not** touch TLS, PM2, or any production configuration;
- did **not** run `nginx -T`, which needs root — the resolved runtime view is **UNMEASURED**.
