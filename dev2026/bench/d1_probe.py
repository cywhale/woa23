"""Issue the D1 characterization requests to both arms and record what came back.

This is Python and not four lines of `curl` in the runner for the reason the S2
campaign already learned twice: logic inside a shell heredoc is logic no test can
reach, and both C1 attempts that died before the contract gate died on exactly that
kind of wiring. Everything here is exercised by `bench/test_d1_probe.py` and end to
end by `bench/test_d1_integration.py`.

**It records; it does not judge.** No status is compared with an expectation and no
body is checked for shape. The bytes are hashed, the first 512 are kept verbatim,
and `bench.d1_evidence` decides the three narrow things that are decided.

**Both arms are driven from one process, and that is what makes the counterbalancing
real.** Spec 005 section 8.2 requires each case to be issued once per arm — ten
countable requests per arm, not twenty — in counterbalanced order. Two independent
per-arm probes could not counterbalance anything, because neither would know which
arm went first. So the order alternates **per case**: the first case is asked
reference-then-candidate (`RC`), the second candidate-then-reference (`CR`), and so
on. Each arm still sees each case exactly once.

**The recovery probe follows each case, before the next case begins**, and its status
is what sets that case's `request_level`. Batching the probes at the end would only
show that the process survived all four together (spec 005 section 6.2).

    uv run python -m bench.d1_probe \\
        --candidate-url http://127.0.0.1:18141 --reference-url http://127.0.0.1:18142 \\
        --candidate-pid 1234 --reference-pid 1235 \\
        --survey results/<label>_store_survey.json \\
        --out-candidate results/<label>_d1_candidate.jsonl \\
        --out-reference results/<label>_d1_reference.jsonl
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.d1_cases import (                                  # noqa: E402
    REQUIRED_SEASONAL_PERIOD, RECOVERY, depth_cases,
)

HEAD_BYTES = 512


def alive(pids: list[int]) -> bool | None:
    """Is every pid still there? `None` when it cannot be determined.

    None is not False. A pid we cannot ask about has not been shown to have died,
    and `d1_evidence` refuses to read either as "still serving".
    """
    if not pids:
        return None
    for pid in pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            continue                     # it exists; it is simply not ours to signal
        except OSError:
            return None
    return True


def fetch(base_url: str, path: str, params: dict, timeout: float = 60.0) -> dict:
    """One request. Returns the raw observation; never raises on an HTTP status."""
    query = urllib.parse.urlencode(dict(params))
    url = f"{base_url}{path}?{query}" if query else f"{base_url}{path}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return {"http_status": resp.status,
                    "content_type": resp.headers.get("Content-Type"),
                    "content_disposition": resp.headers.get("Content-Disposition"),
                    "body": resp.read(), "transport_error": None}
    except urllib.error.HTTPError as exc:
        headers = exc.headers
        return {"http_status": exc.code,
                "content_type": headers.get("Content-Type") if headers else None,
                "content_disposition": (headers.get("Content-Disposition")
                                        if headers else None),
                "body": exc.read(), "transport_error": None}
    except Exception as exc:                       # noqa: BLE001
        # A transport failure is an observation, not a crash. Losing it would lose
        # the case, and "the request never completed" is exactly the kind of result
        # this run exists to record.
        return {"http_status": 0, "content_type": None, "content_disposition": None,
                "body": b"", "transport_error": f"{type(exc).__name__}: {exc}"}


def error_text(body: bytes, content_type: str | None) -> str | None:
    """FastAPI's `detail`, verbatim, when the body is a JSON object carrying one."""
    if not body or not (content_type or "").startswith("application/json"):
        return None
    try:
        obj = json.loads(body)
    except Exception:                              # noqa: BLE001
        return None
    if isinstance(obj, dict) and "detail" in obj:
        detail = obj["detail"]
        return detail if isinstance(detail, str) else json.dumps(detail)
    return None


def row_count(body: bytes, content_type: str | None) -> int | None:
    """Rows, only when the body really is a JSON array. Otherwise None, never 0."""
    if not (content_type or "").startswith("application/json"):
        return None
    try:
        obj = json.loads(body)
    except Exception:                              # noqa: BLE001
        return None
    return len(obj) if isinstance(obj, list) else None


def observe(case, base_url: str, arm: str, request_order: str, *,
            depth_isolated: bool, pids: list[int]) -> dict:
    raw = fetch(base_url, case.path, case.params)
    body = raw["body"]
    return {
        "case_id": case.id,
        "arm": arm,
        "endpoint": case.path,
        "params": dict(case.params),
        "group": case.group,
        # Carried per record so no reader has to infer the scope from the case id.
        # "the seasonal nitrate result" is exactly the generalisation these prevent.
        "variable": case.variable,
        "climatology": case.climatology,
        "season": case.season,
        "time_period": case.params.get("time_period"),
        "scope": case.scope,
        "request_order": request_order,
        "http_status": raw["http_status"],
        "content_type": raw["content_type"],
        "content_disposition": raw["content_disposition"],
        "transport_error": raw["transport_error"],
        "body_bytes": len(body),
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "body_head_b64": base64.b64encode(body[:HEAD_BYTES]).decode("ascii"),
        "error_text": error_text(body, raw["content_type"]),
        "row_count": row_count(body, raw["content_type"]),
        # Filled in from the recovery probe that follows this case, on this arm.
        "request_level": None,
        "process_alive": alive(pids),
        "depth_isolated": bool(depth_isolated),
    }


def run(*, candidate_url: str, reference_url: str, survey: dict,
        candidate_pids: list[int], reference_pids: list[int]) -> dict[str, list[dict]]:
    """Every scheduled request, counterbalanced per case.

    If the survey did not establish its preconditions, **nothing is issued**.
    Running the depth cases anyway would produce a record that looks like a depth
    result and is not one (spec 005 section 5).
    """
    out: dict[str, list[dict]] = {"candidate": [], "reference": []}
    if not survey.get("preconditions_met"):
        return out

    isolated = bool(survey.get("depth_isolated"))
    # The REQUIRED code, never one the survey happened to find. A survey that could
    # not confirm 13 does not reach here at all — `preconditions_met` is false — and
    # a run that quietly used 14 would still have been reported as the
    # `time_period=13` case. Testing another season needs its own case id.
    period = REQUIRED_SEASONAL_PERIOD
    arms = {"candidate": (candidate_url, candidate_pids),
            "reference": (reference_url, reference_pids)}

    for i, case in enumerate(depth_cases(period)):
        order = "RC" if i % 2 == 0 else "CR"
        sequence = (("reference", "candidate") if order == "RC"
                    else ("candidate", "reference"))
        pending: dict[str, dict] = {}
        for arm in sequence:
            url, pids = arms[arm]
            rec = observe(case, url, arm, order, depth_isolated=isolated, pids=pids)
            out[arm].append(rec)
            pending[arm] = rec
        # The recovery probes follow this case and precede the next one, in the same
        # order, so each case's request_level comes from a probe issued after that
        # case and before anything else touched the arm.
        for arm in sequence:
            url, pids = arms[arm]
            rec = observe(RECOVERY, url, arm, order, depth_isolated=isolated,
                          pids=pids)
            rec["request_level"] = rec["http_status"] == 200
            out[arm].append(rec)
            pending[arm]["request_level"] = rec["http_status"] == 200
            pending[arm]["process_alive"] = rec["process_alive"]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--candidate-url", required=True)
    ap.add_argument("--reference-url", required=True)
    ap.add_argument("--candidate-pid", type=int, action="append", default=[])
    ap.add_argument("--reference-pid", type=int, action="append", default=[])
    ap.add_argument("--survey", type=Path, required=True)
    ap.add_argument("--out-candidate", type=Path, required=True)
    ap.add_argument("--out-reference", type=Path, required=True)
    args = ap.parse_args()

    try:
        survey = json.loads(args.survey.read_text())
    except Exception as exc:                       # noqa: BLE001
        print(f"cannot read the store survey {args.survey}: {exc!r}", file=sys.stderr)
        return 2
    if not survey.get("preconditions_met"):
        print("the store survey did not establish its preconditions; no "
              "characterization request is issued", file=sys.stderr)
        return 3

    records = run(candidate_url=args.candidate_url, reference_url=args.reference_url,
                  survey=survey, candidate_pids=args.candidate_pid,
                  reference_pids=args.reference_pid)
    for arm, path in (("candidate", args.out_candidate),
                      ("reference", args.out_reference)):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as fh:
            for r in records[arm]:
                fh.write(json.dumps(r) + "\n")
        print(f"{arm}: {len(records[arm])} requests recorded to {path}")

    for r in records["candidate"]:
        if r["case_id"] == RECOVERY.id:
            continue
        print(f"   {r['case_id']:18s} {r['request_order']} HTTP {r['http_status']} "
              f"{r['content_type']!r} {r['body_bytes']}B rows={r['row_count']!r} "
              f"request_level={r['request_level']!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
