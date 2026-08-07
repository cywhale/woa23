"""Black-box benchmark: measures the WOA23 API end-to-end over HTTP.

This needs no access to the server -- it is the user's-eye view. Run it against
production to get a baseline, then against a candidate build to compare.

Deliberately gentle on the target: requests are sequential with a pause between
them, so this does not act as a load test. Concurrency probing is a separate,
opt-in thing (--concurrency) that you should only point at a non-production host.

Usage:
    uv run python -m bench.http_bench --repeat 3 --out results/http_prod.json

Response bodies are measured, never printed.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bench.queries import Query, select  # noqa: E402

DEFAULT_BASE_URL = "https://eco.odb.ntu.edu.tw"
ENDPOINT = "/api/woa23"


def git_commit() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
            cwd=Path(__file__).resolve().parent.parent.parent,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def time_one(client: httpx.Client, url: str, params: dict, timeout: float,
             cache_bust: bool = True) -> dict:
    """One request. Returns timings in milliseconds plus payload facts.

    There is an nginx proxy cache in front of production (it reports
    `x-api-cache: HIT|MISS`). Repeating an identical URL therefore measures
    nginx, not the application -- warm numbers collected that way came back
    around 3 ms and meant nothing. `cache_bust` appends an ignored parameter so
    every request gets a distinct cache key and actually reaches the app.
    """
    params = dict(params)
    if cache_bust:
        params["_cb"] = uuid.uuid4().hex

    t0 = time.perf_counter()
    ttfb = None
    nbytes = 0
    chunks: list[bytes] = []
    with client.stream("GET", url, params=params, timeout=timeout) as resp:
        status = resp.status_code
        cache_state = resp.headers.get("x-api-cache")
        for chunk in resp.iter_bytes():
            if ttfb is None:
                ttfb = (time.perf_counter() - t0) * 1000
            nbytes += len(chunk)
            chunks.append(chunk)
        total = (time.perf_counter() - t0) * 1000

    rows = None
    if status == 200:
        try:
            rows = len(json.loads(b"".join(chunks)))
        except Exception:
            rows = None

    return {
        "status": status,
        "cache": cache_state,
        "ttfb_ms": round(ttfb, 2) if ttfb is not None else None,
        "total_ms": round(total, 2),
        "bytes": nbytes,
        "rows": rows,
    }


def summarise(runs: list[dict]) -> dict:
    ok = [r for r in runs if r["status"] == 200]
    if not ok:
        return {"ok": 0, "failed": len(runs)}
    totals = [r["total_ms"] for r in ok]
    ttfbs = [r["ttfb_ms"] for r in ok if r["ttfb_ms"] is not None]
    return {
        "ok": len(ok),
        "failed": len(runs) - len(ok),
        "cache_states": sorted({r.get("cache") for r in ok if r.get("cache")}),
        # Cold (first) call separated out: it is usually the one that pays for
        # opening the Zarr store, and averaging it away hides the real problem.
        "cold_ms": ok[0]["total_ms"],
        "warm_median_ms": round(statistics.median(totals[1:] or totals), 2),
        "min_ms": round(min(totals), 2),
        "max_ms": round(max(totals), 2),
        "ttfb_median_ms": round(statistics.median(ttfbs), 2) if ttfbs else None,
        "bytes": ok[0]["bytes"],
        "rows": ok[0]["rows"],
        "ms_per_1k_rows": (
            round(statistics.median(totals) / max(ok[0]["rows"], 1) * 1000, 2)
            if ok[0]["rows"] else None
        ),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL)
    ap.add_argument("--repeat", type=int, default=3,
                    help="requests per query (first one is reported as cold)")
    ap.add_argument("--pause", type=float, default=0.5,
                    help="seconds between requests, to stay polite to production")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--query", action="append", dest="queries",
                    help="run only this query id (repeatable)")
    ap.add_argument("--include-heavy", action="store_true",
                    help="also run the cases marked heavy (global surface, 0.25-degree region)")
    ap.add_argument("--no-cache-bust", action="store_true",
                    help="send identical URLs, which the nginx proxy cache will serve "
                         "as HITs -- use this only to measure the cache itself")
    ap.add_argument("--out", type=Path, default=None, help="write JSON results here")
    args = ap.parse_args()
    cache_bust = not args.no_cache_bust

    queries: list[Query] = select(args.queries, args.include_heavy)
    url = args.base_url.rstrip("/") + ENDPOINT

    print(f"target : {url}")
    print(f"queries: {len(queries)}  repeat: {args.repeat}  "
          f"cache-bust: {'on' if cache_bust else 'OFF (measuring nginx cache)'}\n")

    results = []
    with httpx.Client(follow_redirects=True) as client:
        for q in queries:
            runs = []
            for i in range(args.repeat):
                if i or results:
                    time.sleep(args.pause)
                try:
                    runs.append(time_one(client, url, q.params(), args.timeout, cache_bust))
                except Exception as exc:
                    runs.append({"status": -1, "error": repr(exc),
                                 "ttfb_ms": None, "total_ms": None, "bytes": 0, "rows": None})
            s = summarise(runs)
            results.append({"id": q.id, "intent": q.intent,
                            "params": q.params(), "runs": runs, "summary": s})
            if s.get("ok"):
                cache_txt = ",".join(s["cache_states"]) or "-"
                print(f"{q.id:26s} 1st {s['cold_ms']:8.1f} ms | med {s['warm_median_ms']:8.1f} ms "
                      f"| {s['rows'] or 0:7d} rows | {s['bytes'] / 1024:8.1f} KiB | {cache_txt}")
            else:
                print(f"{q.id:26s} FAILED ({runs[0].get('error') or runs[0]['status']})")

    payload = {
        "kind": "http",
        "base_url": args.base_url,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "client_platform": platform.platform(),
        "git_commit": git_commit(),
        "cache_bust": cache_bust,
        "repeat": args.repeat,
        "results": results,
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
