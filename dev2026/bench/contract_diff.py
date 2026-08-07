"""The contract gate: does the candidate return what the reference returns?

Spec 001 section 5.2 defines two variants and this implements both.

**5.2A — byte-exact.** Candidate against a controlled reference, both under a pinned
`PYTHONHASHSEED`. Response bodies are compared byte for byte, errors included. This
is the strong form and it needs D2b.

**5.2B — semantic.** Candidate against live production, whose hash seed cannot be
aligned because we may not restart it. Rows are compared as a multiset keyed on
`(lon, lat, depth, time_period)`, columns as a set, values exactly. It is called
semantic comparison because that is what it is: a float-formatting or key-ordering
regression would pass, and that is the price of not running a reference process.

Parsing is only ever used to **localise** a failure — "row 412, key salinity_an"
instead of "byte 91,244". Under 5.2A it is never the pass criterion.

    uv run python -m bench.contract_diff \\
        --candidate http://127.0.0.1:8051 \\
        --reference http://127.0.0.1:8052 --variant 5.2A \\
        --candidate-meta results/meta_candidate.json \\
        --reference-meta results/meta_reference.json \\
        --out results/contract_s1.json
"""

import argparse
import csv
import io
import json
import platform
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bench.contract_cases import Case, all_cases  # noqa: E402
from bench.provenance import load_meta, validate_meta  # noqa: E402

INDEX = ("lon", "lat", "depth", "time_period")


def fetch(client: httpx.Client, base: str, case: Case, timeout: float) -> dict:
    r = client.get(base.rstrip("/") + case.path, params=case.params, timeout=timeout)
    body = r.read()
    return {"status": r.status_code, "body": body,
            "content_type": r.headers.get("content-type", "").split(";")[0]}


def _rows(body: bytes, is_csv: bool) -> list[dict] | None:
    try:
        if is_csv:
            return list(csv.DictReader(io.StringIO(body.decode())))
        parsed = json.loads(body)
        return parsed if isinstance(parsed, list) else None
    except Exception:
        return None


def _sort_key(row: dict):
    return tuple(str(row.get(k)) for k in INDEX)


def compare_semantic(a: dict, b: dict, is_csv: bool) -> list[str]:
    """Order-insensitive comparison for 5.2B. Values still compared exactly."""
    if a["status"] != b["status"]:
        return [f"status {a['status']} vs {b['status']}"]

    if a["status"] != 200:
        return _compare_error(a["body"], b["body"])

    ra, rb = _rows(a["body"], is_csv), _rows(b["body"], is_csv)
    if ra is None or rb is None:
        # Not a row payload at all — the OpenAPI document is a JSON object and the
        # Swagger page is HTML. There is no row or column ordering to be insensitive
        # to, so the honest comparison is the strict one. An earlier version called
        # these "unparseable" and failed them, which is how two responses of 8,597
        # and 8,597 identical bytes were reported as differing.
        if a["body"] == b["body"]:
            return []
        return [f"non-row payload differs ({len(a['body'])} vs {len(b['body'])} "
                f"bytes); compared as raw bytes because there is no row structure "
                f"to compare semantically"]
    if len(ra) != len(rb):
        return [f"row count {len(ra)} vs {len(rb)}"]
    if ra and set(ra[0]) != set(rb[0]):
        only_a = sorted(set(ra[0]) - set(rb[0]))
        only_b = sorted(set(rb[0]) - set(ra[0]))
        return [f"column sets differ: reference-only {only_a}, candidate-only {only_b}"]

    problems = []
    for i, (x, y) in enumerate(zip(sorted(ra, key=_sort_key), sorted(rb, key=_sort_key))):
        if x != y:
            for k in sorted(set(x) | set(y)):
                if x.get(k) != y.get(k):
                    problems.append(f"row {i} key {k!r}: {x.get(k)!r} vs {y.get(k)!r}")
            if len(problems) >= 5:
                problems.append("… further differences suppressed")
                break
    return problems


def _compare_error(a: bytes, b: bytes) -> list[str]:
    """Error bodies, compared as whole objects — never on `detail` alone.

    An earlier version pulled `detail` out of each side and compared those. Anything
    that failed to parse, or parsed without a `detail` key, yielded `None` on both
    sides and was scored a match:

        400 b"x" vs b"y"             -> []      (both unparseable)
        400 {"foo":1} vs {"foo":2}   -> []      (neither has `detail`)

    So an error path could change shape entirely and pass. Now the bodies must both
    be JSON objects carrying `detail`, and the whole object is compared.
    """
    if a == b:
        return []
    parsed = []
    for label, body in (("reference", a), ("candidate", b)):
        try:
            obj = json.loads(body)
        except Exception:
            return [f"{label} error body is not JSON ({body[:60]!r})"]
        if not isinstance(obj, dict):
            return [f"{label} error body is {type(obj).__name__}, expected an object"]
        if "detail" not in obj:
            return [f"{label} error body has no 'detail' key (keys: {sorted(obj)})"]
        parsed.append(obj)

    ref, cand = parsed
    if ref != cand:
        return [f"error object key {k!r}: {ref.get(k)!r} vs {cand.get(k)!r}"
                for k in sorted(set(ref) | set(cand)) if ref.get(k) != cand.get(k)]
    # Same object, different bytes: a formatting difference. Semantic comparison
    # accepts it and says so; the byte gate would not.
    return []


def localise(a: dict, b: dict, is_csv: bool) -> list[str]:
    """Why the bytes differ, for a human. Never the verdict under 5.2A."""
    notes = compare_semantic(a, b, is_csv)
    if notes:
        return notes
    return ["bodies differ but parse identically — a formatting, ordering or "
            "whitespace difference, which is exactly what the byte gate exists to "
            "catch"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--reference", required=True)
    ap.add_argument("--variant", required=True, choices=("5.2A", "5.2B"))
    ap.add_argument("--candidate-meta", type=Path, default=None)
    ap.add_argument("--reference-meta", type=Path, default=None)
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--pause", type=float, default=0.1,
                    help="seconds between cases (each case is one request per arm)")
    ap.add_argument("--insecure", action="store_true",
                    help="skip TLS verification — needed under 5.2B, where the "
                         "reference is production's TLS listener on loopback and its "
                         "certificate names the public host")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    # Provenance first, before any request: the same rule the latency gate follows.
    problems = []
    metas = {}
    for label, path in (("candidate", args.candidate_meta),
                        ("reference", args.reference_meta)):
        # The reference's provenance is required under both variants. Revision 1
        # let it be None under 5.2B, which meant a carried-over contract result had
        # nothing recorded about the backend it was compared against — so a later
        # run could not tell whether that backend had since changed.
        pinned = not (args.variant == "5.2B" and label == "reference")
        meta, errs = load_meta(path, label)
        metas[label] = meta
        problems.extend(
            errs if errs else validate_meta(meta, label, require_pinned_seed=pinned))
    if problems:
        print("INVALID_METADATA — refusing to compare:")
        for m in problems:
            print(f"  - {m}")
        return 1

    cases = all_cases()
    print(f"variant   : {args.variant} "
          f"({'byte-exact' if args.variant == '5.2A' else 'semantic'})")
    print(f"candidate : {args.candidate}")
    print(f"reference : {args.reference}")
    print(f"cases     : {len(cases)}\n")

    results, failed = [], []
    with httpx.Client(verify=not args.insecure, follow_redirects=True) as client:
        for case in cases:
            is_csv = case.path.endswith("/csv")
            try:
                ref = fetch(client, args.reference, case, args.timeout)
                cand = fetch(client, args.candidate, case, args.timeout)
            except Exception as exc:
                results.append({"id": case.id, "verdict": "ERROR", "error": repr(exc)})
                failed.append(case.id)
                print(f"{case.id:10s} ERROR {exc!r}")
                continue
            time.sleep(args.pause)

            status_ok = ref["status"] == cand["status"] == case.expect_status
            if args.variant == "5.2A":
                same = ref["status"] == cand["status"] and ref["body"] == cand["body"]
                notes = [] if same else localise(ref, cand, is_csv)
            else:
                notes = compare_semantic(ref, cand, is_csv)
                same = not notes

            verdict = "MATCH" if same else "DIFFER"
            if not status_ok:
                notes.append(f"status {ref['status']}/{cand['status']} but the case "
                             f"expects {case.expect_status} — the recorded behaviour "
                             f"has changed on both sides")
            results.append({
                "id": case.id, "intent": case.intent, "path": case.path,
                "params": case.params, "expect_status": case.expect_status,
                "reference_status": ref["status"], "candidate_status": cand["status"],
                "reference_bytes": len(ref["body"]),
                "candidate_bytes": len(cand["body"]),
                "verdict": verdict, "notes": notes,
                "status_as_recorded": status_ok,
            })
            if not same or not status_ok:
                failed.append(case.id)
            flag = "" if same else "  <-- " + (notes[0] if notes else "")
            print(f"{case.id:10s} {verdict:6s} {ref['status']}/{cand['status']} "
                  f"{len(ref['body']):8d}/{len(cand['body']):<8d}{flag}")

    gate = "PASS" if not failed else "FAIL"
    print(f"\ncontract gate: {gate}")
    if failed:
        print(f"  differing: {', '.join(failed)}")

    payload = {
        "kind": "contract_diff",
        "gate": gate,
        "variant": args.variant,
        "candidate_url": args.candidate,
        "reference_url": args.reference,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "host": platform.node(),
        "python": sys.version,
        "harness_invocation": sys.argv,
        "candidate_meta": metas.get("candidate"),
        "reference_meta": metas.get("reference"),
        "results": results,
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2, default=str))
        print(f"wrote {args.out}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
