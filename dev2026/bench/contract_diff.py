"""The contract gate: does the candidate return what the reference returns?

Spec 001 section 5.2 defines two variants and spec 008 §7a adds a third. All three
are implemented here.

**5.2A — byte-exact.** Candidate against a controlled reference, both under a pinned
`PYTHONHASHSEED`. Response bodies are compared byte for byte, errors included. This
is the strong form and it needs D2b.

**5.2B — semantic.** Candidate against live production, whose hash seed cannot be
aligned because we may not restart it. Rows are compared as a multiset keyed on
`(lon, lat, depth, time_period)`, columns as a set, values exactly. It is called
semantic comparison because that is what it is: a float-formatting or key-ordering
regression would pass, and that is the price of not running a reference process.

**5.2C — canonical row order + candidate order contract.** C1's arrangement, both
arms ours and both pinned, but for a candidate that **deliberately reorders rows**
(spec 008). Raw byte equality is no longer the verdict, because the contract now says
the bytes must differ. Three findings are reported separately and PASS needs all
three: values and column sequence compared after normalising **both** arms to
`(time_period, depth, lat, lon)`; the **candidate's own** conformance to that order;
and each raw-order difference recorded as the decided change, proved by permuting the
reference's own rows into contract order and reproducing the candidate's bytes. Where
a case's shape admits no ordering difference — an error body, a non-row payload, a
single row, a reference already ordered correctly — bytes must still match exactly,
and a difference there is still a regression.

Parsing is only ever used to **localise** a failure — "row 412, key salinity_an"
instead of "byte 91,244". Under 5.2A it is never the pass criterion. Under 5.2C it
is, necessarily: a gate that may not compare bytes has to compare something else.

    uv run python -m bench.contract_diff \\
        --candidate http://127.0.0.1:8051 \\
        --reference http://127.0.0.1:8052 --variant 5.2A \\
        --candidate-meta results/meta_candidate.json \\
        --reference-meta results/meta_reference.json \\
        --out results/contract_s1.json
"""

import argparse
import csv
import hashlib
import io
import json
import platform
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bench.contract_cases import Case, all_cases  # noqa: E402
from bench.provenance import (  # noqa: E402
    SEED_POLICY_MEANING, load_meta, seed_requirement_for, validate_meta,
    validate_store_agreement, verify_group_path_agreement)

INDEX = ("lon", "lat", "depth", "time_period")

#: Spec 008's row-order contract key, in the contract's own order. Deliberately NOT
#: `INDEX`: that tuple is `(lon, lat, depth, time_period)` and is used, as strings, to
#: PAIR rows for the semantic comparison — sound for pairing, wrong for the contract.
#: See `contract_key` for why the difference is not cosmetic.
CONTRACT_KEY = ("time_period", "depth", "lat", "lon")


def contract_key(row: dict):
    """Spec 008's key, NUMERIC in every component.

    `_sort_key` below stringifies, which is correct where it is used — both sides get
    the same treatment, the keys are unique, so it is a bijection and the comparison
    comes out order-insensitive either way. It would be **wrong here**: as text,
    `'13'` sorts before `'2'` and `'-10'` before `'-9'`, and the contract is about
    numbers. Two functions, because they answer two questions.

    Raises if a component is missing or unparseable; the caller decides what an
    unusable row means, and guessing a key would silently invent an order.
    """
    return (int(row["time_period"]), float(row["depth"]),
            float(row["lat"]), float(row["lon"]))


def row_order_contract(resp: dict, is_csv: bool) -> dict:
    """Does THIS response satisfy spec 008's row order? Asked of the candidate only.

    The reference is not held to this: it does not implement the contract, and under
    an unpinned seed its order is a property of its process. Holding it to a contract
    it never adopted would manufacture failures.

    `applies` separates "conforms" from "there was nothing to conform to". A 400 body,
    the OpenAPI document and a zero-row result have no row order, and scoring them as
    passes would let a run where every case errored report a perfect order gate.
    """
    out = {"applies": False, "ok": None, "n_rows": None, "violation": None}
    if resp.get("status") != 200:
        return out
    rows = _rows(resp["body"], is_csv)
    if rows is None:
        return out
    out["n_rows"] = len(rows)
    if not rows:
        return out
    try:
        keys = [contract_key(r) for r in rows]
    except (KeyError, TypeError, ValueError) as exc:
        out["applies"] = True
        out["ok"] = False
        out["violation"] = f"row key unreadable: {exc!r}"
        return out
    out["applies"] = True
    for i, (a, b) in enumerate(zip(keys, keys[1:])):
        if a > b:
            out["ok"] = False
            out["violation"] = (f"row {i} key {a} precedes row {i + 1} key {b} — "
                                f"the order must ascend by "
                                f"(time_period, depth, lat, lon), numerically")
            return out
        if a == b:
            out["ok"] = False
            out["violation"] = (f"rows {i} and {i + 1} share the key {a}; spec 008 "
                                f"allows at most one row per key")
            return out
    out["ok"] = True
    return out


def canonicalise(rows: list[dict]) -> list[dict]:
    """Both arms put into contract order, so values can be compared without order."""
    return sorted(rows, key=contract_key)


def compare_canonical(a: dict, b: dict, is_csv: bool) -> list[str]:
    """C1's correctness verdict once row order is deliberately different.

    Like `compare_semantic`, but stricter in the one place C1 cares about: the column
    **sequence** must match, not merely the column set. Column order is out of spec
    008's scope, so it must not move — and 5.2B's set comparison would not notice if
    it did.
    """
    if a["status"] != b["status"]:
        return [f"status {a['status']} vs {b['status']}"]
    if a["status"] != 200:
        return _compare_error(a["body"], b["body"])

    ra, rb = _rows(a["body"], is_csv), _rows(b["body"], is_csv)
    if ra is None or rb is None:
        if a["body"] == b["body"]:
            return []
        return [f"non-row payload differs ({len(a['body'])} vs {len(b['body'])} "
                f"bytes); compared as raw bytes because there is no row structure"]
    if len(ra) != len(rb):
        return [f"row count {len(ra)} vs {len(rb)}"]
    if ra and list(ra[0]) != list(rb[0]):
        # A column SET difference is a real defect. A column SEQUENCE difference over the
        # same set is spec 015's decided change, classified by classify_raw_difference and
        # reported as its own finding -- not folded in here as a canonical mismatch.
        if sorted(ra[0]) != sorted(rb[0]):
            return [f"column SET differs: {sorted(set(ra[0]) ^ set(rb[0]))}"]
    try:
        ca, cb = canonicalise(ra), canonicalise(rb)
    except (KeyError, TypeError, ValueError) as exc:
        return [f"cannot canonicalise: {exc!r}"]

    problems = []
    for i, (x, y) in enumerate(zip(ca, cb)):
        if x != y:
            for k in sorted(set(x) | set(y)):
                if x.get(k) != y.get(k):
                    problems.append(f"canonical row {i} key {k!r}: "
                                    f"{x.get(k)!r} vs {y.get(k)!r}")
            if len(problems) >= 5:
                problems.append("… further differences suppressed")
                break
    return problems


def _json_elements(body: bytes):
    """The top-level array's elements as (parsed, raw bytes), or None.

    Raw slices, not re-serialised objects: re-serialising would prove this harness's
    formatting rather than the API's, and the whole point of the reconstruction below
    is to carry the candidate's own bytes across untouched.
    """
    text = body.decode()
    i = text.find("[")
    if i < 0:
        return None
    dec = json.JSONDecoder()
    out, pos = [], i + 1
    while True:
        while pos < len(text) and text[pos] in " \t\r\n":
            pos += 1
        if pos >= len(text):
            return None
        if text[pos] == "]":
            return out
        try:
            obj, end = dec.raw_decode(text, pos)
        except ValueError:
            return None
        out.append((obj, text[pos:end]))
        pos = end
        while pos < len(text) and text[pos] in " \t\r\n":
            pos += 1
        if pos < len(text) and text[pos] == ",":
            pos += 1


def reconstruct_from_reference(ref: dict, cand: dict, is_csv: bool) -> dict:
    """Permute the REFERENCE's own rows into contract order; do they become the
    candidate's bytes exactly?

    This is what turns "only the row order changed" from an inference into a
    demonstration: every byte of every row is the reference's, only the sequence is
    the candidate's.

    It refuses rather than guesses. Reassembly is first checked against the
    reference's ORIGINAL order — if the pieces do not put the original body back
    together byte for byte, the splitting is not faithful and no conclusion may be
    drawn from a rearranged version of it. `available: False` is then honest, and
    spec 008 §7a.4 says such a run is INCOMPLETE_VALIDATION, not a pass.
    """
    out = {"available": False, "matches": None, "why": None}
    if ref.get("status") != 200 or cand.get("status") != 200:
        out["why"] = "not a 200 pair; nothing to reorder"
        return out

    if is_csv:
        text = ref["body"].decode()
        newline = "\r\n" if "\r\n" in text else "\n"
        parts = text.split(newline)
        trailing = ""
        if parts and parts[-1] == "":
            parts, trailing = parts[:-1], newline
        if len(parts) < 2:
            out["why"] = "fewer than two CSV lines; no data rows to reorder"
            return out
        header, lines = parts[0], parts[1:]
        rows = _rows(ref["body"], is_csv)
        if rows is None or len(rows) != len(lines):
            out["why"] = (f"{len(lines)} CSV data lines but "
                          f"{0 if rows is None else len(rows)} parsed rows — an "
                          f"embedded newline or a quoting rule this cannot honour")
            return out
        rebuilt = newline.join([header] + lines) + trailing
        if rebuilt.encode() != ref["body"]:
            out["why"] = "reassembling the reference's own order did not reproduce it"
            return out
        try:
            order = sorted(range(len(rows)), key=lambda i: contract_key(rows[i]))
        except (KeyError, TypeError, ValueError) as exc:
            out["why"] = f"reference rows have no readable contract key: {exc!r}"
            return out
        permuted = newline.join([header] + [lines[i] for i in order]) + trailing
        out["available"] = True
        out["matches"] = permuted.encode() == cand["body"]
        return out

    elements = _json_elements(ref["body"])
    if elements is None:
        out["why"] = "reference body is not a top-level JSON array"
        return out
    if not elements:
        out["why"] = "empty array; no rows to reorder"
        return out
    text = ref["body"].decode()
    prefix = text[:text.find("[") + 1]
    suffix = text[text.rfind("]"):]
    rebuilt = prefix + ",".join(raw for _, raw in elements) + suffix
    if rebuilt.encode() != ref["body"]:
        out["why"] = ("reassembling the reference's own order did not reproduce it "
                      "byte for byte — the array carries whitespace or separators "
                      "this splice does not model")
        return out
    try:
        order = sorted(range(len(elements)),
                       key=lambda i: contract_key(elements[i][0]))
    except (KeyError, TypeError, ValueError) as exc:
        out["why"] = f"reference rows have no readable contract key: {exc!r}"
        return out
    permuted = prefix + ",".join(elements[i][1] for i in order) + suffix
    out["available"] = True
    out["matches"] = permuted.encode() == cand["body"]
    return out


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


def order_fingerprint(resp: dict, is_csv: bool) -> dict:
    """Order, recorded separately from the verdict.

    Under 5.2B the row order is deliberately not part of pass/fail: with no pinned
    seed the arms iterate `zarr_group_paths` in whatever order their own process
    hashes it, so a row-order difference is a property of the process rather than a
    defect. That does not make it uninteresting — it is the observable the C2
    seed-diversity question is actually about — so it is measured and carried
    alongside the result, never folded into it.

    Three separate things, because they can move independently:

    - `body_sha256` — the raw bytes. Changes if anything at all changes.
    - `row_order_sha256` — the ordered sequence of row *keys* only, values excluded.
      This isolates order: identical rows in a different sequence change this and
      nothing else.
    - `columns` — the column sequence, which is where a pivot's group order surfaces.

    A response with no row structure (the OpenAPI document, an error body) gets a
    body digest and nulls, rather than a fabricated ordering.
    """
    out = {"body_sha256": hashlib.sha256(resp["body"]).hexdigest(),
           "row_order_sha256": None, "columns": None, "n_rows": None}
    if resp.get("status") != 200:
        return out
    rows = _rows(resp["body"], is_csv)
    if rows is None:
        return out
    out["n_rows"] = len(rows)
    if rows:
        out["columns"] = list(rows[0])
    keys = "\n".join("".join(str(r.get(k)) for k in INDEX) for r in rows)
    out["row_order_sha256"] = hashlib.sha256(keys.encode()).hexdigest()
    return out


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


# --------------------------------------------- the DECIDED differences, spec 015 and 008
# Two changes are decided and documented, and neither may be counted as a regression --
# nor waved through. c1p failed 5 of 64 on exactly these and could return no verdict.
#
#   spec 015  the candidate orders columns parameter-major and deterministically; the
#             reference is unmodified woa23_app and still emits hash-seeded order.
#   spec 008  revision 6 published API 1.1.0: the OpenAPI info.version and description
#             gained the row-order statement. Spec 008 said in terms that applying it
#             "changes api/ and would require a new C1/C2". c1f passed BEFORE it.
#
# The permission for each is narrow, because a blanket "bytes may differ" throws away
# the sensitivity C1 exists for.

def describe_exception(exc: BaseException) -> dict:
    """The full cause of a failure, for the artefact.

    c1q recorded only "not importable" and threw the reason away, so the actual
    `KeyError: 'WOA23_ZARR_STORE'` had to be recovered afterwards by reproducing the
    import by hand. A guard that fails closed must still say WHY it closed, and the
    artefact must carry enough to diagnose it without going back to the machine.
    """
    import traceback as _tb
    out = {"type": type(exc).__name__,
           "module": type(exc).__module__,
           "message": str(exc),
           "repr": repr(exc),
           "traceback": _tb.format_exception(type(exc), exc, exc.__traceback__)}
    cause = exc.__cause__ or exc.__context__
    if cause is not None:
        out["cause"] = {"type": type(cause).__name__,
                        "module": type(cause).__module__,
                        "message": str(cause),
                        "repr": repr(cause)}
    return out


def canonical_column_rule():
    """The spec 015 conformance comparator, as `(comparator, failure)`.

    Exactly one of the two is None. `failure` is a `describe_exception` dict, never a
    bare None that discards the reason -- see c1q, where the reason was thrown away.

    This now imports `bench.column_contract`, which is pure and stdlib-only. It does
    NOT import `api.query`: that pulls in `api.config`, whose module-level
    `os.environ["WOA23_ZARR_STORE"]` (api/config.py:33) is unset in the harness
    process and made conformance permanently UNVERIFIED in c1q.

    The guard STAYS regardless. If this import ever fails, the caller reports the
    conformance finding UNVERIFIED and the run becomes INCOMPLETE_VALIDATION rather
    than passing on a check it could not make.
    """
    try:
        from bench.column_contract import column_order_conformance
        return column_order_conformance, None
    except Exception as exc:                                  # noqa: BLE001
        return None, describe_exception(exc)


def column_order_difference(ref: dict, cand: dict, is_csv: bool) -> dict:
    """Is this difference EXACTLY the decided column-order change, and nothing else?

    Proven, not assumed. The reference's own rows, with their columns permuted into the
    candidate's sequence, must reproduce the candidate's bytes EXACTLY. If they do, the
    only thing that moved is column order; if they do not, something else moved too and
    that is a regression.
    """
    out = {"applies": False, "reconstructed": None, "why": None,
           "ref_columns": None, "cand_columns": None}
    ra, rb = _rows(ref["body"], is_csv), _rows(cand["body"], is_csv)
    if ra is None or rb is None or not ra or not rb:
        out["why"] = "not a row payload on both sides"
        return out
    ca, cb = list(ra[0]), list(rb[0])
    out["ref_columns"], out["cand_columns"] = ca, cb
    if ca == cb:
        out["why"] = "column sequences are identical; this is not a column-order difference"
        return out
    if sorted(ca) != sorted(cb):
        out["why"] = (f"column SETS differ, not merely their order: "
                      f"{sorted(set(ca) ^ set(cb))}")
        return out
    out["applies"] = True
    # RECONSTRUCTION: permute the reference into the candidate's column sequence and
    # rebuild its bytes. Equality proves nothing but the order moved.
    try:
        permuted = [{k: r[k] for k in cb} for r in ra]
        rebuilt = _render(permuted, is_csv, cand["body"])
    except Exception as exc:                                  # noqa: BLE001
        out["reconstructed"] = False
        out["why"] = f"cannot rebuild the reference in the candidate's order: {exc!r}"
        return out
    if rebuilt is None:
        out["reconstructed"] = False
        out["why"] = "no renderer for this payload; reconstruction not attempted"
        return out
    out["reconstructed"] = (rebuilt == cand["body"])
    out["why"] = ("the reference, permuted into the candidate's column order, reproduces "
                  "the candidate's bytes exactly"
                  if out["reconstructed"] else
                  "permuting the reference's columns does NOT reproduce the candidate's "
                  "bytes, so something other than column order also moved")
    return out


def _render(rows, is_csv, model: bytes):
    """Rebuild a payload from rows, matching the model's shape. None if unsupported."""
    import csv as _csv
    import io
    import json as _json
    if is_csv:
        text = model.decode("utf-8", "replace")
        nl = "\r\n" if "\r\n" in text else "\n"
        buf = io.StringIO()
        w = _csv.DictWriter(buf, fieldnames=list(rows[0]), lineterminator=nl)
        w.writeheader()
        for r in rows:
            w.writerow(r)
        return buf.getvalue().encode("utf-8")
    try:
        _json.loads(model)
    except Exception:                                         # noqa: BLE001
        return None
    sep = (",", ":") if b", " not in model[:200] else (", ", ": ")
    return _json.dumps(rows, separators=sep).encode("utf-8")


def openapi_docs_only_difference(ref: dict, cand: dict) -> dict:
    """Is an OpenAPI difference confined to the version and description strings?

    Spec 008 rev 6 published 1.1.0 by changing `info.version` and adding the row-order
    statement to descriptions. NOTHING else may differ: a route, parameter, schema or
    response that moved survives this normalisation and is still a regression. That is
    the whole point -- "it is the documentation surface, so it is fine" is not a check.
    """
    import json as _json
    out = {"applies": False, "docs_only": None, "why": None,
           "ref_version": None, "cand_version": None}

    def strip(o):
        if isinstance(o, dict):
            return {k: strip(v) for k, v in sorted(o.items())
                    if k not in ("description", "version", "summary")}
        if isinstance(o, list):
            return [strip(v) for v in o]
        return o

    try:
        a, b = _json.loads(ref["body"]), _json.loads(cand["body"])
    except Exception as exc:                                  # noqa: BLE001
        out["why"] = f"not both JSON: {exc!r}"
        return out
    if not (isinstance(a, dict) and isinstance(b, dict)):
        out["why"] = "not both JSON objects"
        return out
    if "openapi" not in a or "openapi" not in b:
        out["why"] = "not both OpenAPI documents"
        return out
    out["applies"] = True
    out["ref_version"] = (a.get("info") or {}).get("version")
    out["cand_version"] = (b.get("info") or {}).get("version")
    out["docs_only"] = strip(a) == strip(b)
    out["why"] = ("identical once version, summary and description strings are removed: "
                  "no route, parameter, schema or response moved"
                  if out["docs_only"] else
                  "the documents differ in more than version/summary/description")
    return out


def column_order_conformance_result(cand_columns, params: dict) -> dict:
    """Candidate column-order conformance -- a SEPARATE question from reconstruction.

    Reconstruction proves no value moved and is satisfied by ANY permutation. This
    proves the candidate's order is the one spec 015 mandates. c1q had the first and
    not the second, which is exactly why it could not be a PASS.

    Fail-closed: if the comparator cannot be reached or cannot be applied, the result
    is UNVERIFIED with the cause recorded. UNVERIFIED is never a pass.
    """
    out = {"verified": False, "conformant": None, "why": None, "setup_error": None}
    # No request at all is NOT the same as a request that omits `parameter`/`append`.
    # The latter has real defaults (api/query.py:171,185: `mn` and `temperature`); the
    # former means the caller never told us what was asked for. Ruling on invented
    # defaults would report a CONFORMANT candidate as a regression, so this fails
    # closed to UNVERIFIED instead.
    if params is None:
        out["why"] = ("no request parameters were supplied for this case, so the spec "
                      "015 rule cannot be applied: conformance is UNVERIFIED rather "
                      "than ruled against assumed defaults")
        out["setup_error"] = {"type": "MissingRequest",
                              "message": "params is None; the case's request is unknown"}
        return out
    comparator, failure = canonical_column_rule()
    if comparator is None:
        out["setup_error"] = failure
        out["why"] = ("the spec 015 conformance comparator could not be loaded, so "
                      "conformance is UNVERIFIED rather than assumed: "
                      f"{(failure or {}).get('repr')}")
        return out
    try:
        finding = comparator(cand_columns, params)
    except Exception as exc:                                  # noqa: BLE001
        out["setup_error"] = describe_exception(exc)
        out["why"] = ("the spec 015 conformance comparator raised, so conformance is "
                      f"UNVERIFIED rather than assumed: {exc!r}")
        return out
    out.update(finding)
    return out


#: The ONLY version pair C2 may classify as an expected documentation change. Spec 008
#: rev 6 published API 1.1.0 and said in terms that applying it "changes api/ and would
#: require a new C1/C2". Any other pair -- 1.1.0 -> 1.2.0, or a downgrade -- is NOT this
#: known change and stays a regression until it is separately decided and authorised.
C2_EXPECTED_DOC_VERSIONS = ("1.0.0", "1.1.0")

#: The one note `compare_semantic` emits for a non-row payload whose bytes differ. A
#: case carrying ANY other note has a second problem and is never reclassified.
_NON_ROW_NOTE_PREFIX = "non-row payload differs"


def c2_expected_documentation(ref: dict, cand: dict, notes: list) -> dict:
    """Is this 5.2B difference EXACTLY the decided 1.0.0 -> 1.1.0 documentation change?

    A CLASSIFICATION LAYER, sitting above `compare_semantic`, which is unchanged and
    still produces the notes this reads. c2j failed because 5.2B had no way to say "this
    difference was decided": `compare_semantic` falls back to raw bytes for a non-row
    payload and never classifies. Rather than teach the comparator to forgive bytes --
    which would blunt it for every case -- the comparator keeps saying exactly what it
    said, and this decides, separately, whether what it said is the one known change.

    The permission is deliberately narrower than C1's. C1 accepted any version pair that
    was docs-only; here the pair must be EXACTLY 1.0.0 -> 1.1.0. Everything else -- a
    value, route, parameter, schema or response change, a different version pair, a
    second note, a non-200 -- stays a regression.
    """
    out = {"applies": False, "expected": False, "why": None, "docs": None}

    # A case with more than the one non-row note has a second problem; a case whose note
    # is anything else was never the documentation surface to begin with.
    real = [n for n in (notes or []) if n]
    if len(real) != 1 or not real[0].startswith(_NON_ROW_NOTE_PREFIX):
        out["why"] = ("not the decided documentation difference: expected exactly one "
                      f"non-row-payload note, got {real!r}")
        return out
    if ref.get("status") != 200 or cand.get("status") != 200:
        out["why"] = "a non-200 response is not the documentation surface"
        return out

    doc = openapi_docs_only_difference(ref, cand)      # the c1r-proven comparison
    out["docs"] = doc
    if not doc.get("applies"):
        out["why"] = f"not both OpenAPI documents: {doc.get('why')}"
        return out
    out["applies"] = True
    pair = (doc.get("ref_version"), doc.get("cand_version"))
    if pair != C2_EXPECTED_DOC_VERSIONS:
        out["why"] = (f"OpenAPI {pair[0]} -> {pair[1]} is not the decided change "
                      f"{C2_EXPECTED_DOC_VERSIONS[0]} -> {C2_EXPECTED_DOC_VERSIONS[1]}")
        return out
    if not doc.get("docs_only"):
        out["why"] = (f"the documents differ beyond version, summary and description: "
                      f"{doc.get('why')}")
        return out
    out["expected"] = True
    out["why"] = (f"OpenAPI {pair[0]} vs {pair[1]}: {doc.get('why')} "
                  f"(spec 008 rev 6 published API 1.1.0)")
    return out


def classify_raw_difference(ref: dict, cand: dict, is_csv: bool,
                            order_contract: dict, params: dict | None = None) -> dict:
    """Is this raw-byte difference the decided contract change, or a regression?

    Spec 008 §7a.4. A blanket "bytes may differ now" would throw away the sensitivity
    C1 exists for, so the permission is narrow: **only** a 200 row payload whose
    reference was not already in contract order may differ, and only if the reference's
    own rows, permuted into that order, reproduce the candidate's bytes exactly.

    Everything else — error bodies, the OpenAPI document, single-row results, and
    multi-row results the reference already ordered correctly — must still be
    byte-identical, and a difference there is still a regression.
    """
    identical = ref["body"] == cand["body"]
    out = {"bytes_identical": identical, "classification": None, "why": None,
           "reconstruction": None}
    if identical:
        out["classification"] = "BYTE_IDENTICAL"
        return out

    if ref.get("status") != 200 or cand.get("status") != 200:
        out["classification"] = "REGRESSION_BYTES_MUST_MATCH"
        out["why"] = "a non-200 response carries no row order to change"
        return out
    rows = _rows(ref["body"], is_csv)
    if rows is None:
        # The documentation surface is the one non-row payload with a DECIDED change:
        # spec 008 rev 6 published 1.1.0. Allowed only if nothing but version/summary/
        # description moved -- a route or schema change still fails here.
        doc = openapi_docs_only_difference(ref, cand)
        out["docs"] = doc
        if doc.get("applies") and doc.get("docs_only"):
            out["classification"] = "EXPECTED_DOCUMENTATION_CHANGE"
            out["why"] = (f"OpenAPI {doc['ref_version']} vs {doc['cand_version']}: "
                          f"{doc['why']} (spec 008 rev 6 published API 1.1.0)")
            return out
        out["classification"] = "REGRESSION_BYTES_MUST_MATCH"
        out["why"] = ("a non-row payload carries no row order to change"
                      if not doc.get("applies") else
                      f"OpenAPI documents differ beyond documentation: {doc['why']}")
        return out
    # COLUMN order first: it is a different question from row order, and c1p's four
    # regressions were all this. Proven by reconstruction, never assumed.
    col = column_order_difference(ref, cand, is_csv)
    # Recorded whether or not it applies: "the column SETS differ" is exactly the
    # diagnosis a reader needs when the case is later classified a regression for a
    # reason that sounds unrelated, and c1q had to be re-queried by hand to get it.
    out["columns"] = col
    if col.get("applies"):
        # The two questions, asked and recorded APART. A regression is decided on
        # reconstruction; conformance is decided on the spec 015 rule; and the change
        # is EXPECTED only when BOTH hold.
        # `params` passed through as-is: `None` (no request supplied) must NOT collapse
        # into `{}` (a request that omits them and takes the documented defaults).
        conf = column_order_conformance_result(col.get("cand_columns"), params)
        out["conformance"] = conf
        if not col.get("reconstructed"):
            # Something other than order moved. This is a regression whatever the
            # conformance comparator says, so it is decided first.
            out["classification"] = "REGRESSION_BYTES_MUST_MATCH"
            out["why"] = col.get("why")
            return out
        if not conf.get("verified"):
            out["classification"] = "UNRECONSTRUCTED"
            out["why"] = ("the reference reconstructs exactly in the candidate's column "
                          "order, so no value moved -- but the candidate's order could "
                          "NOT be checked against the spec 015 rule, so conformance is "
                          f"UNVERIFIED rather than assumed: {conf.get('why')}")
            return out
        if not conf.get("conformant"):
            out["classification"] = "REGRESSION_BYTES_MUST_MATCH"
            out["why"] = ("the reference reconstructs exactly, so no value moved, but "
                          "the candidate's column order is NOT the order spec 015 "
                          f"mandates: {conf.get('why')}")
            return out
        out["classification"] = "EXPECTED_COLUMN_ORDER_CHANGE"
        out["why"] = (f"decided spec 015 column order; {col['why']}; and the candidate's "
                      f"order conforms to the spec 015 rule")
        return out

    if len(rows) < 2:
        out["classification"] = "REGRESSION_BYTES_MUST_MATCH"
        out["why"] = (f"{len(rows)} row(s): there is only one possible order, so a "
                      f"byte difference is not an ordering difference")
        return out
    try:
        already = [contract_key(r) for r in rows]
    except (KeyError, TypeError, ValueError) as exc:
        out["classification"] = "REGRESSION_BYTES_MUST_MATCH"
        out["why"] = f"reference rows have no readable contract key: {exc!r}"
        return out
    if already == sorted(already):
        out["classification"] = "REGRESSION_BYTES_MUST_MATCH"
        out["why"] = ("the reference was ALREADY in contract order, so the sort "
                      "cannot explain a byte difference")
        return out

    rec = reconstruct_from_reference(ref, cand, is_csv)
    out["reconstruction"] = rec
    if not rec["available"]:
        # Not a pass and not a regression: the evidence for "only the order changed"
        # could not be produced. Spec 008 §7a.4 makes the run INCOMPLETE_VALIDATION.
        out["classification"] = "UNRECONSTRUCTED"
        out["why"] = rec["why"]
        return out
    if rec["matches"]:
        out["classification"] = "EXPECTED_ROW_ORDER_CHANGE"
        out["why"] = ("the reference's own rows, permuted into contract order, are "
                      "byte-identical to the candidate's response")
        return out
    out["classification"] = "REGRESSION_BYTES_MUST_MATCH"
    out["why"] = ("reordering the reference's rows does NOT reproduce the "
                  "candidate's bytes, so something other than the order changed")
    return out


#: Classifications the dispatch has ALREADY placed, which the descriptive-note fallback
#: must therefore never re-bucket as regressions. `UNRECONSTRUCTED` is here too: it is
#: its own outcome (INCOMPLETE_VALIDATION), not a regression, and counting it as both
#: would turn "could not check" into "candidate failed" -- the exact conflation c1q's
#: report had to unpick by hand.
CLASSIFIED_NOT_A_REGRESSION = frozenset({
    "BYTE_IDENTICAL",
    "EXPECTED_ROW_ORDER_CHANGE",
    "EXPECTED_COLUMN_ORDER_CHANGE",
    "EXPECTED_DOCUMENTATION_CHANGE",
    "UNRECONSTRUCTED",
})


def _conformance_row(cid: str, rd: dict) -> dict:
    """One case's column findings, with reconstruction and conformance kept APART.

    They answer different questions -- see `bench/column_contract`. Reporting them in
    one field would let "the bytes reconstruct" read as "the order is right", which is
    exactly the conflation c1q's report had to spell out by hand.
    """
    col = rd.get("columns") or {}
    conf = rd.get("conformance") or {}
    return {"id": cid,
            "reference": col.get("ref_columns"),
            "candidate": col.get("cand_columns"),
            # necessary, not sufficient: satisfied by ANY permutation
            "reconstructed": col.get("reconstructed"),
            # the spec 015 question: is the candidate's own order the mandated one
            "conformance_verified": conf.get("verified"),
            "conformant": conf.get("conformant"),
            "expected_order": conf.get("expected"),
            "first_divergence": conf.get("first_divergence"),
            "conformance_setup_error": conf.get("setup_error"),
            "why": conf.get("why")}


def summarise_5_2C(results: list[dict]) -> dict:
    """The three findings, counted, and the one sentence the run may be quoted as.

    Spec 008 §7a.4a forbids reporting this as "5.2A 64/64 raw byte-exact PASS": once
    the bytes deliberately differ, that sentence is false in its plain reading. So the
    headline names all three findings, and **PASS requires all three**.

    `INCOMPLETE_VALIDATION` is its own outcome, not a soft pass: it means the evidence
    for "only the order changed" could not be produced for some case. It exits
    non-zero and §7a.4 bars it from any deployment claim.
    """
    n = len(results)
    regressions, unreconstructed, order_failures = [], [], []
    canonical_match = byte_identical = expected = 0
    # The findings the C1/C2 request promised and c1p did not have: the decided column
    # order and the decided documentation change, counted APART from regressions so a
    # reader can see which differences were expected and which were not.
    expected_columns, expected_docs = [], []
    column_conformance = []
    order_ok = order_applicable = order_inapplicable = 0
    for r in results:
        cid = r.get("id", "?")
        if r.get("verdict") == "ERROR":
            regressions.append(cid)
            continue
        oc = r.get("candidate_row_order_contract") or {}
        if oc.get("applies"):
            order_applicable += 1
            if oc.get("ok"):
                order_ok += 1
            else:
                order_failures.append(cid)
        else:
            order_inapplicable += 1
        rd = r.get("raw_difference") or {}
        cls = rd.get("classification")
        if cls == "BYTE_IDENTICAL":
            byte_identical += 1
        elif cls == "EXPECTED_ROW_ORDER_CHANGE":
            expected += 1
        elif cls == "EXPECTED_COLUMN_ORDER_CHANGE":
            expected_columns.append(cid)
            column_conformance.append(_conformance_row(cid, rd))
        elif cls == "EXPECTED_DOCUMENTATION_CHANGE":
            expected_docs.append(cid)
        elif cls == "UNRECONSTRUCTED":
            unreconstructed.append(cid)
            # An unverified conformance check is itself a finding and must be visible,
            # not merely implied by the case's absence from the conformant list. Only
            # for cases that ARE column-order differences: `columns` is now recorded
            # for every row payload, including ones where it does not apply.
            if (rd.get("columns") or {}).get("applies"):
                column_conformance.append(_conformance_row(cid, rd))
        elif cls == "REGRESSION_BYTES_MUST_MATCH":
            regressions.append(cid)
        # A case whose only problem is the order gate is not also a byte regression;
        # `notes` already carries ROW_ORDER_CONTRACT_FAILURE and it is counted above.
        #
        # The classification above is AUTHORITATIVE. c1q's gate was wrong because this
        # fallback ran unconditionally afterwards and re-added C20a -- already classified
        # EXPECTED_DOCUMENTATION_CHANGE -- to `regressions`, on the strength of a note
        # that was merely DESCRIPTIVE ("non-row payload differs ... no row structure").
        # C20a then appeared in both `expected_documentation_diffs` and `regressions`.
        # A case the dispatch has already placed is never re-bucketed here; the fallback
        # keeps its sensitivity for cases the dispatch did NOT classify.
        if not [x for x in (r.get("notes") or [])
                if not x.startswith("ROW_ORDER_CONTRACT_FAILURE")
                and not x.startswith("bytes differ where they must not")]:
            canonical_match += 1
        elif cls not in CLASSIFIED_NOT_A_REGRESSION and cid not in regressions:
            regressions.append(cid)

    if regressions or order_failures:
        gate = "FAIL"
    elif unreconstructed:
        gate = "INCOMPLETE_VALIDATION"
    else:
        gate = "PASS"

    # Two different facts, kept apart. "expected raw-order differences: 0" says no
    # case's bytes differed; it does NOT say a reconstruction succeeded, and the first
    # version of this line read "0 (reconstructed from the reference's own rows)" —
    # which described a proof that never ran. When the count is zero the
    # reconstruction path is N/A and the headline says so, so a reader cannot take
    # zero differences as evidence that the machinery works.
    if expected == 0:
        recon_phrase = ("expected raw-order differences: 0; raw-order reconstruction: "
                        "N/A, NOT EXERCISED")
        recon_line = ("N/A — not exercised: no raw-order difference occurred, so "
                      "nothing was reconstructed and this run is no evidence that "
                      "the reconstruction path works")
    else:
        recon_phrase = (f"expected raw-order differences: {expected}, each "
                        f"reconstructed from the reference's own rows")
        recon_line = (f"{expected}/{expected} reconstructed from the reference's own "
                      f"rows")

    if gate == "PASS":
        headline = (f"C1 contract validation — canonical values/columns match "
                    f"({canonical_match}/{n}), candidate row-order contract PASS "
                    f"({order_ok}/{order_applicable} applicable), required byte checks "
                    f"pass ({byte_identical} byte-identical), and no unexpected "
                    f"differences. {recon_phrase}.")
    elif gate == "INCOMPLETE_VALIDATION":
        headline = (f"C1 contract validation INCOMPLETE — {len(unreconstructed)} "
                    f"case(s) could not be reconstructed from the reference's own "
                    f"rows, so 'only the order changed' is NOT demonstrated for them. "
                    f"This is not a PASS and may not be carried into a deployment "
                    f"claim (spec 008 §7a.4).")
    elif order_failures:
        headline = (f"ROW_ORDER_CONTRACT_FAILURE on {len(order_failures)} case(s): "
                    f"the candidate does not implement the decided order. This is a "
                    f"contract failure, not a semantic divergence.")
    else:
        headline = (f"C1 contract validation FAILED — {len(regressions)} case(s) "
                    f"differ for a reason the row-order change does not explain.")

    return {"gate": gate, "headline": headline, "n_cases": n,
            "canonical_match": canonical_match, "byte_identical": byte_identical,
            "expected_diffs": expected, "order_ok": order_ok,
            "order_applicable": order_applicable,
            "order_inapplicable": order_inapplicable,
            "order_failures": order_failures,
            "unreconstructed": unreconstructed,
            # Separate from `expected_diffs` on purpose: a count of zero and a
            # successful proof are different findings (spec 008 §7c.4, §7c.6).
            "reconstruction": ("N/A_NOT_EXERCISED" if expected == 0
                               else "RECONSTRUCTED"),
            "reconstruction_note": recon_line,
            "regressions": sorted(set(regressions)),
            # THE FIVE FINDINGS the C1/C2 request specified, reported apart from one
            # another. c1p counted all of these as regressions and could return no
            # verdict; conflating "expected and proven" with "unexplained" is what made
            # that run unreadable.
            "expected_column_order_diffs": sorted(set(expected_columns)),
            "column_order_conformance": column_conformance,
            "expected_documentation_diffs": sorted(set(expected_docs)),
            "column_reconstruction": (
                "N/A_NOT_EXERCISED" if not expected_columns
                else ("RECONSTRUCTED" if all(c.get("reconstructed")
                                             for c in column_conformance)
                      else "NOT_RECONSTRUCTED")),
            "reporting_rule": ("PASS requires all three findings. This run may NOT be "
                               "reported as '5.2A byte-exact PASS': the raw bytes "
                               "differ by design (spec 008 §7a.4a).")}


def _encode_body(body: bytes) -> dict:
    """One body, exactly, in a form JSON can carry and a reader can re-audit.

    Never truncated. `bytes` is recorded alongside so any silent loss is detectable by
    comparing it with the length of what came back.
    """
    import base64 as _b64
    import hashlib as _hl
    out = {"bytes": len(body), "sha256": _hl.sha256(body).hexdigest()}
    try:
        out["text"] = body.decode("utf-8")
        out["encoding"] = "utf-8"
    except UnicodeDecodeError:
        out["base64"] = _b64.b64encode(body).decode("ascii")
        out["encoding"] = "base64"
    return out


def retained_evidence(ref: dict, cand: dict, raw_difference: dict | None) -> dict | None:
    """The compared bodies themselves, for every case a reader may need to re-audit.

    c1q retained the documentation VERDICT and both body digests but not the bodies, so
    C20a's structural claim -- "no route, parameter, schema or response moved" -- could
    be re-derived offline against those digests but not re-audited from the artefact.
    A digest proves WHICH documents were compared; it does not let anyone check the
    comparison. Both are now kept.

    Retained whenever the case is not byte-identical, or the comparison could not be
    completed. Byte-identical cases are skipped: their bodies are equal, both digests
    are recorded, and keeping two copies of every matching payload would bury the
    evidence that matters under the evidence that does not.
    """
    rd = raw_difference or {}
    cls = rd.get("classification")
    conf = (rd.get("conformance") or {})
    # Variants other than 5.2C do not classify at all, so `raw_difference` is None and
    # the bodies themselves are the only thing to go on. Without this fallback every
    # matching case in 5.2A/5.2B would retain two identical copies of its payload.
    identical = rd.get("bytes_identical")
    if identical is None:
        identical = (ref["body"] == cand["body"])
    needs = (identical is False
             or cls in ("UNRECONSTRUCTED", "REGRESSION_BYTES_MUST_MATCH")
             or conf.get("verified") is False)
    if not needs:
        return None
    return {"why_retained": ("not byte-identical, or the comparison could not be "
                             "completed: retained so the classification can be "
                             "independently re-audited, not merely re-derived"),
            "classification": cls,
            "reference": _encode_body(ref["body"]),
            "candidate": _encode_body(cand["body"])}


def request_order(index: int) -> str:
    """Which arm is requested first for case `index`: "RC" reference, "CR" candidate.

    Every case used to fetch the reference first and the candidate second. For a
    gate that compares *bodies* that is not obviously wrong, but it makes one arm
    systematically the cold one and the other systematically the warm one for all 64
    cases, and any order-dependent state — a connection pool, a store handle, the
    page cache under a shared store — is then always applied in the same direction.
    A gate is not the place to leave a systematic asymmetry lying around.

    Alternating by index counterbalances it exactly for an even case count and to
    within one case otherwise. The chosen order is recorded per case in the
    artefact, so the balance is auditable rather than asserted.
    """
    return "RC" if index % 2 == 0 else "CR"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--reference", required=True)
    ap.add_argument("--variant", required=True, choices=("5.2A", "5.2B", "5.2C"))
    ap.add_argument("--candidate-meta", type=Path, default=None)
    ap.add_argument("--reference-meta", type=Path, default=None)
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--pause", type=float, default=0.1,
                    help="seconds between cases (each case is one request per arm)")
    ap.add_argument("--insecure", action="store_true",
                    help="skip TLS verification — needed under 5.2B, where the "
                         "reference is production's TLS listener on loopback and its "
                         "certificate names the public host")
    ap.add_argument("--seed-policy", default=None,
                    choices=("both-pinned", "reference-unpinned", "both-unpinned"),
                    help="which arms must have a pinned PYTHONHASHSEED. Defaults to "
                         "both-pinned under 5.2A and reference-unpinned under 5.2B — "
                         "the latter being 5.2B's original arrangement, a pinned "
                         "candidate against live production. C2 is neither: both "
                         "arms are ours and both are deliberately unpinned, which "
                         "needs both-unpinned stated explicitly.")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    # Explicit when given; otherwise the variant's historical default. That default
    # is kept so 5.2A and the original 5.2B behave as they did — but it is only a
    # default: C2 is a third arrangement (both arms ours, both unpinned) that neither
    # covers, and it must state `both-unpinned` rather than inherit either.
    policy = args.seed_policy or (
        "reference-unpinned" if args.variant == "5.2B" else "both-pinned")
    print(f"seed policy: {policy}")
    print(f"  {SEED_POLICY_MEANING[policy]}")

    # Provenance first, before any request: the same rule the latency gate follows.
    problems = []
    metas = {}
    for label, path in (("candidate", args.candidate_meta),
                        ("reference", args.reference_meta)):
        # The reference's provenance is required under both variants. Revision 1
        # let it be None under 5.2B, which meant a carried-over contract result had
        # nothing recorded about the backend it was compared against — so a later
        # run could not tell whether that backend had since changed.
        req = seed_requirement_for(policy, label)
        meta, errs = load_meta(path, label)
        metas[label] = meta
        problems.extend(
            errs if errs else validate_meta(meta, label, seed_requirement=req))
    # Both arms must be reading the same store, and — under 5.2A — building their
    # zarr_group_paths from the same string. These live here, in the gate, and not
    # only in the runner: the gate is what publishes a MATCH, so it is the gate that
    # has to be unable to publish one it cannot justify. The 2026-08-08 run had both
    # checks in the runner alone and still spent 128 requests establishing that two
    # arms disagreed about something knowable before the first request.
    if not problems:
        problems.extend(validate_store_agreement(metas.get("candidate"),
                                                 metas.get("reference")))
        if args.variant in ("5.2A", "5.2C"):
            # Not 5.2B. There the reference is live production, whose store literal
            # is whatever it is and cannot be aligned — that is the reason 5.2B
            # compares semantically in the first place. 5.2C is C1's arrangement with
            # a different comparison, so it keeps C1's preconditions.
            problems.extend(verify_group_path_agreement(metas.get("candidate"),
                                                        metas.get("reference")))
    if problems:
        print("INVALID_METADATA — refusing to compare:")
        for m in problems:
            print(f"  - {m}")
        return 1

    cases = all_cases()
    variant_name = {"5.2A": "byte-exact", "5.2B": "semantic",
                    "5.2C": "canonical row order + candidate order contract"}
    print(f"variant   : {args.variant} ({variant_name[args.variant]})")
    if args.variant == "5.2C":
        print("            spec 008 §7a: the candidate deliberately reorders rows, so")
        print("            raw byte equality is NOT the verdict. Three findings are")
        print("            reported separately and PASS needs all three.")
    print(f"candidate : {args.candidate}")
    print(f"reference : {args.reference}")
    print(f"cases     : {len(cases)}\n")

    results, failed = [], []
    # 5.2B: cases classified as the decided documentation change. Kept apart
    # from `failed` and from the MATCH count, so all three read separately.
    expected_doc_ids = []
    with httpx.Client(verify=not args.insecure, follow_redirects=True) as client:
        for index, case in enumerate(cases):
            is_csv = case.path.endswith("/csv")
            order = request_order(index)
            try:
                if order == "RC":
                    ref = fetch(client, args.reference, case, args.timeout)
                    cand = fetch(client, args.candidate, case, args.timeout)
                else:
                    cand = fetch(client, args.candidate, case, args.timeout)
                    ref = fetch(client, args.reference, case, args.timeout)
            except Exception as exc:
                results.append({"id": case.id, "verdict": "ERROR", "error": repr(exc),
                                "request_order": order})
                failed.append(case.id)
                print(f"{case.id:10s} ERROR {exc!r}")
                continue
            time.sleep(args.pause)

            status_ok = ref["status"] == cand["status"] == case.expect_status
            order_contract = None
            expected_diff = None
            expected_doc = None
            if args.variant == "5.2A":
                same = ref["status"] == cand["status"] and ref["body"] == cand["body"]
                notes = [] if same else localise(ref, cand, is_csv)
            elif args.variant == "5.2C":
                # Three findings, kept apart. Spec 008 §7a.3: the canonical
                # comparison is the correctness verdict, the candidate order gate is
                # the new-contract verdict, and a raw-order difference is the decided
                # change — recorded, not counted as a regression.
                notes = compare_canonical(ref, cand, is_csv)
                order_contract = row_order_contract(cand, is_csv)
                if order_contract["ok"] is False:
                    notes.append(f"ROW_ORDER_CONTRACT_FAILURE: "
                                 f"{order_contract['violation']}")
                expected_diff = classify_raw_difference(ref, cand, is_csv,
                                                        order_contract, case.params)
                if expected_diff["classification"] == "REGRESSION_BYTES_MUST_MATCH":
                    notes.append(f"bytes differ where they must not: "
                                 f"{expected_diff['why']}")
                same = not notes
            else:
                # 5.2B. `compare_semantic` is UNCHANGED and still says exactly what it
                # said before; the classification layer sits above it and decides,
                # separately, whether what it said is the one decided change.
                notes = compare_semantic(ref, cand, is_csv)
                expected_doc = c2_expected_documentation(ref, cand, notes)
                if expected_doc["expected"]:
                    # NOT a semantic MATCH and NOT a regression -- its own class. The
                    # notes are kept verbatim so the record still shows what differed.
                    expected_doc_ids.append(case.id)
                    same = False
                else:
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
                "request_order": order,
                # Recorded for every variant, used as a verdict by none. Under
                # 5.2B this is the only place row order is visible at all.
                "reference_order": order_fingerprint(ref, is_csv),
                "candidate_order": order_fingerprint(cand, is_csv),
                # Spec 008 §7b.2: recorded for EVERY variant, from the response this
                # case already fetched, so C2 can gate on it without a single extra
                # request. It is a verdict only where the variant says so — 5.2C here,
                # and `c2_summary` for the C2 cycles.
                "candidate_row_order_contract": row_order_contract(cand, is_csv),
                "raw_difference": expected_diff,
                # Spec: the artefact must be independently RE-AUDITABLE, not merely
                # re-derivable from a digest. None for byte-identical cases.
                "retained_bodies": retained_evidence(ref, cand, expected_diff),
                # 5.2B only: the decided-documentation classification, kept apart from
                # both `verdict` (which stays DIFFER) and `failed`.
                "expected_documentation": expected_doc,
            })
            # An expected documentation difference is NOT a failure. It is also not a
            # MATCH: `verdict` stays DIFFER and the notes are kept verbatim, so the
            # record shows what differed and why it was allowed. Status still gates --
            # a documentation change cannot excuse a status change.
            if (expected_doc or {}).get("expected") and status_ok:
                pass
            elif not same or not status_ok:
                failed.append(case.id)
            flag = "" if same else "  <-- " + (notes[0] if notes else "")
            print(f"{case.id:10s} {verdict:6s} {ref['status']}/{cand['status']} "
                  f"{len(ref['body']):8d}/{len(cand['body']):<8d}{flag}")

    gate = "PASS" if not failed else "FAIL"
    findings = None
    if args.variant == "5.2C":
        findings = summarise_5_2C(results)
        gate = findings["gate"]
        print()
        print("== C1 contract validation (spec 008 §7a.4a) ==")
        print(f"   canonical values/columns match : {findings['canonical_match']}"
              f"/{findings['n_cases']}")
        print(f"   candidate row-order contract   : "
              f"{findings['order_ok']}/{findings['order_applicable']} applicable "
              f"({findings['order_inapplicable']} carry no row order)")
        print(f"   byte-identical, as required    : {findings['byte_identical']}")
        print(f"   expected raw-order differences : {findings['expected_diffs']}")
        print(f"   raw-order reconstruction       : "
              f"{findings['reconstruction_note']}")
        if findings["unreconstructed"]:
            print(f"   NOT reconstructed              : "
                  f"{findings['unreconstructed']}  <-- INCOMPLETE_VALIDATION")
        if findings["regressions"]:
            print(f"   REGRESSIONS                    : "
                  f"{', '.join(findings['regressions'])}")
        print(f"   result: {gate}")
        print("   " + findings["headline"])
    else:
        print(f"\ncontract gate: {gate}")
    if expected_doc_ids:
        print(f"  expected documentation differences (not regressions): "
              f"{', '.join(expected_doc_ids)}")
    if failed:
        print(f"  differing: {', '.join(failed)}")

    payload = {
        "kind": "contract_diff",
        "gate": gate,
        "variant": args.variant,
        "findings_5_2C": findings,
        # 5.2B: the decided documentation change, reported apart from semantic MATCH
        # and apart from regressions. Empty for every other variant.
        "expected_documentation_diffs": sorted(expected_doc_ids),
        "seed_policy": policy,
        "candidate_url": args.candidate,
        "reference_url": args.reference,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "host": platform.node(),
        "python": sys.version,
        "harness_invocation": sys.argv,
        "request_order_counts": {
            o: sum(1 for r in results if r.get("request_order") == o)
            for o in ("RC", "CR")},
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
