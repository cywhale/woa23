"""The D1 characterization evidence: what was recorded, and the little that is judged.

Spec 005 sections 7 and 10. Two jobs, kept apart because they have different
standing:

**Recording.** Every case, per arm, per endpoint, with the raw bytes' digest and the
first 512 bytes kept verbatim. **No expected status or body appears anywhere**, and
JSON and CSV are recorded as separate observations that are never compared with each
other — `app.get_woa23_csv` raises 400 on an empty frame and `app.get_woa23` has no
such branch, so assuming they agree would be assuming away the thing being observed.

**Judging.** Exactly three conditions, and nothing about whether a status code is the
*right* one:

1. the candidate and the reference returned the same bytes for every case;
2. the anchor recovery probe that followed each case returned 200, and every process
   in the arm's tree was still alive;
3. a case that needs depth isolation was only issued when the survey established it.

A status code being *correct* is a contract question. It belongs to spec 003 or to
S2b, and answering it here would turn an observation into an assertion nobody
authorised.

    uv run python -m bench.d1_evidence --out results/<label>_d1.json \\
        --candidate results/<label>_d1_candidate.jsonl \\
        --reference results/<label>_d1_reference.jsonl \\
        --survey results/<label>_store_survey.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

#: Every field a recorded response must carry. A record missing one is INVALID, not
#: "mostly fine": the point of characterization is that the record is the result, so
#: an incomplete record has nothing to fall back on.
REQUIRED_FIELDS: tuple[tuple[str, type | tuple[type, ...]], ...] = (
    ("case_id", str), ("arm", str), ("endpoint", str), ("params", dict),
    ("request_order", str),
    ("http_status", int), ("content_type", (str, type(None))),
    ("content_disposition", (str, type(None))),
    ("body_bytes", int), ("body_sha256", str), ("body_head_b64", str),
    ("error_text", (str, type(None))), ("row_count", (int, type(None))),
    ("request_level", (bool, type(None))), ("process_alive", (bool, type(None))),
    ("depth_isolated", bool),
)

OUTCOMES = {
    "CHARACTERIZATION_RECORDED": 0,
    "PRECONDITION_UNMET": 3,
    "DIVERGENCE": 1,
    "REQUEST_LEVEL_VIOLATION": 1,
    "INVALID_EVIDENCE": 1,
}


def body_digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def load_records(path: Path) -> tuple[list[dict], list[str]]:
    """One JSON object per line. A broken line is a problem, never a skipped line."""
    records, problems = [], []
    try:
        text = path.read_text()
    except OSError as exc:
        return [], [f"cannot read {path}: {exc.__class__.__name__}"]
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            problems.append(f"{path}:{n}: not valid JSON ({exc.msg})")
            continue
        if not isinstance(obj, dict):
            problems.append(f"{path}:{n}: {type(obj).__name__}, expected an object")
            continue
        records.append(obj)
    return records, problems


def validate(records: list[dict], arm: str) -> list[str]:
    problems = []
    for i, r in enumerate(records):
        where = f"{arm}[{i}] {r.get('case_id', '<no case_id>')}"
        for field, typ in REQUIRED_FIELDS:
            if field not in r:
                problems.append(f"{where}: missing {field!r}")
            elif not isinstance(r[field], typ) or (
                    typ is not bool and isinstance(r[field], bool)
                    and field != "request_level" and field != "process_alive"):
                problems.append(
                    f"{where}: {field!r} is {type(r[field]).__name__}")
        if r.get("arm") not in (None, arm):
            problems.append(f"{where}: labelled arm {r.get('arm')!r}, expected {arm!r}")
    return problems


def compare(candidate: list[dict], reference: list[dict]) -> dict:
    """Byte equality, arm against arm, position by position.

    Position, not `case_id`: the anchor recovery probe is issued four times per arm
    — once after each case — so a case id is not a key. Both arms are driven from
    one process in lockstep, so the Nth record of one arm is the Nth of the other by
    construction, and a mismatch in what those two records are is itself a problem
    worth naming rather than a lookup to work around.

    Equality is over the bytes, the status and the content type. A body that differs
    only in a header would still be a difference between the arms.
    """
    problems, per_case = [], []
    if len(candidate) != len(reference):
        problems.append(
            f"the arms recorded different numbers of requests: candidate "
            f"{len(candidate)}, reference {len(reference)}. They are issued in "
            f"lockstep, so a difference means one arm did not receive what the "
            f"other did")
    for n, (c, r) in enumerate(zip(candidate, reference)):
        cid, order = c.get("case_id"), c.get("request_order")
        if cid != r.get("case_id") or order != r.get("request_order"):
            problems.append(
                f"position {n}: candidate has {cid!r}/{order!r} and reference has "
                f"{r.get('case_id')!r}/{r.get('request_order')!r} — the arms were "
                f"not asked the same thing in the same order")
            per_case.append({"position": n, "case_id": cid, "request_order": order,
                             "same": None})
            continue
        same = (c.get("body_sha256") == r.get("body_sha256")
                and c.get("http_status") == r.get("http_status")
                and c.get("content_type") == r.get("content_type"))
        per_case.append({
            "position": n, "case_id": cid, "request_order": order, "same": same,
            "candidate": {k: c.get(k) for k in
                          ("http_status", "content_type", "body_bytes", "body_sha256")},
            "reference": {k: r.get(k) for k in
                          ("http_status", "content_type", "body_bytes", "body_sha256")},
        })
        if not same:
            problems.append(
                f"{cid} ({order}, position {n}): candidate {c.get('http_status')} "
                f"{c.get('content_type')!r} {str(c.get('body_sha256'))[:12]} vs "
                f"reference {r.get('http_status')} {r.get('content_type')!r} "
                f"{str(r.get('body_sha256'))[:12]}")
    return {"ok": not problems, "problems": problems, "per_case": per_case}


def request_level(records: list[dict], arm: str) -> list[str]:
    """Did each case leave the process serving?

    `request_level` is recorded per case from the anchor probe issued immediately
    after it. `None` means it was never determined, which is not the same as false
    and is not allowed to pass as true.
    """
    problems = []
    for r in records:
        cid = r.get("case_id")
        if cid == "D1-ANCHOR-RECOVER":
            continue
        if r.get("request_level") is not True:
            problems.append(
                f"{arm} {cid}: request_level is {r.get('request_level')!r} — the "
                f"anchor probe after this case did not return 200, so the failure "
                f"was not confined to the request")
        if r.get("process_alive") is not True:
            problems.append(
                f"{arm} {cid}: process_alive is {r.get('process_alive')!r} — a "
                f"process in this arm's tree did not survive the case")
    return problems


def isolation(records: list[dict], survey: dict) -> list[str]:
    """A case needing depth isolation must not have been issued without it."""
    isolated = bool(survey.get("depth_isolated"))
    problems = []
    for r in records:
        if r.get("depth_isolated") and not isolated:
            problems.append(
                f"{r.get('case_id')}: recorded as depth-isolated while the survey "
                f"says otherwise")
    if not isolated:
        issued = sorted({r.get("case_id") for r in records
                         if str(r.get("case_id", "")).startswith("D1-DEPTH-OOR")})
        if issued:
            problems.append(
                f"the survey did not establish depth isolation, yet {issued} were "
                f"issued. Their result cannot be reported as depth characterization")
    return problems


def decide(*, evidence_problems, comparison, level_problems, isolation_problems,
           survey) -> dict:
    """One named outcome. Ordered so the weaker claim always wins."""
    if not survey.get("preconditions_met"):
        return {"outcome": "PRECONDITION_UNMET", "because": survey.get(
            "verdict_note", "the store survey did not establish its preconditions")}
    if evidence_problems:
        return {"outcome": "INVALID_EVIDENCE",
                "because": "the recorded evidence is incomplete or unreadable, so "
                           "there is nothing to characterize from"}
    if isolation_problems:
        return {"outcome": "INVALID_EVIDENCE",
                "because": "a case was issued or labelled outside what the survey "
                           "established"}
    if not comparison["ok"]:
        return {"outcome": "DIVERGENCE",
                "because": "the candidate and the reference did not return the same "
                           "bytes; characterization stops here"}
    if level_problems:
        return {"outcome": "REQUEST_LEVEL_VIOLATION",
                "because": "a case was not confined to its own request"}
    return {"outcome": "CHARACTERIZATION_RECORDED",
            "because": "both arms returned identical bytes for every case, each case "
                       "left the process serving, and the survey established that "
                       "depth was the isolated variable. What the responses ARE is "
                       "recorded, not judged."}


def build(candidate_path: Path, reference_path: Path, survey_path: Path) -> dict:
    survey_problems: list[str] = []
    try:
        survey = json.loads(survey_path.read_text())
    except Exception as exc:                       # noqa: BLE001
        survey = {"preconditions_met": False,
                  "verdict_note": f"the store survey is unreadable: {exc!r}"}
        survey_problems.append(f"cannot read {survey_path}: {exc!r}")

    cand, cp = load_records(candidate_path)
    ref, rp = load_records(reference_path)
    evidence_problems = (cp + rp + survey_problems
                         + validate(cand, "candidate") + validate(ref, "reference"))
    comparison = compare(cand, ref)
    level_problems = request_level(cand, "candidate") + request_level(ref, "reference")
    isolation_problems = isolation(cand, survey) + isolation(ref, survey)

    decision = decide(evidence_problems=evidence_problems, comparison=comparison,
                      level_problems=level_problems,
                      isolation_problems=isolation_problems, survey=survey)
    return {
        "kind": "d1_characterization",
        "outcome": decision["outcome"],
        "exit_code": OUTCOMES[decision["outcome"]],
        "because": decision["because"],
        "n_candidate_records": len(cand),
        "n_reference_records": len(ref),
        "evidence_problems": evidence_problems,
        "comparison": comparison,
        "request_level_problems": level_problems,
        "isolation_problems": isolation_problems,
        "survey": {k: survey.get(k) for k in
                   ("verdict", "preconditions_met", "depth_isolated",
                    "required_seasonal_period", "seasonal_periods_present",
                    "seasonal_depth", "seasonal_depth_vs_p12",
                    "annual_depth", "annual_depth_vs_p12",
                    "selectable_levels_in_requested_range",
                    "store_schema_mismatch", "depth_invariant_note",
                    "non_anchor_characterization", "missing_reachable_groups",
                    "reads_coordinate_chunks")},
        "observations": [
            {k: r.get(k) for k in
             ("case_id", "arm", "endpoint", "variable", "climatology", "season",
              "time_period", "scope", "group", "request_order", "http_status",
              "content_type", "content_disposition", "body_bytes", "body_sha256",
              "error_text", "row_count", "request_level", "process_alive",
              "depth_isolated")}
            for r in cand + ref],
        "scope": (
            "annual nitrate 0-800 m (D1-DEPTH-SUP), and WINTER nitrate "
            "(D1-DEPTH-OOR-tp13, time_period=13) at "
            "3000-4000 m. Nothing here characterizes time_period 14, 15 or 16, "
            "monthly nitrate, phosphate, silicate, TS or oxygen — the Nutrients "
            "group holds three variables and this run requests one of them."),
        "not_judged": (
            "No status code or body here is judged right or wrong. JSON and CSV are "
            "recorded separately and are never compared with each other. Whether "
            "they ought to agree is a contract question for spec 003 or S2b."),
        "limitations": [
            "The store survey read coordinate chunks. The lifespan anchor check's "
            "zero-chunk property is a separate matter and remains offline-audited "
            "and implementation-supported, not observed on this host.",
            "Process readiness requests are not counted by the harness, so the "
            "run's total request count is a range and must be reported as one.",
            "Nothing here measures latency, throughput or resource use.",
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--candidate", type=Path, required=True)
    ap.add_argument("--reference", type=Path, required=True)
    ap.add_argument("--survey", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    result = build(args.candidate, args.reference, args.survey)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2))

    print(f"== D1 characterization: {result['outcome']} ==")
    print(f"   {result['because']}")
    for group in ("evidence_problems", "request_level_problems", "isolation_problems"):
        for p in result[group]:
            print(f"   - {p}")
    for p in result["comparison"]["problems"]:
        print(f"   - {p}")
    print("   what was observed (not judged):")
    for o in result["observations"]:
        if o["case_id"] == "D1-ANCHOR-RECOVER":
            continue
        print(f"     {o['arm']:10s} {o['case_id']:18s} {o['request_order']} "
              f"HTTP {o['http_status']} {o['content_type']!r} "
              f"{o['body_bytes']}B sha {str(o['body_sha256'])[:12]} "
              f"rows={o['row_count']!r} err={o['error_text']!r}")
    print(f"wrote {args.out}")
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
