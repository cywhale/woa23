"""Offline tests for the contract comparison. No network.

The error-body path had a false pass that only a test would have caught: comparing
`detail` alone scored two unparseable bodies, and two objects with no `detail` at
all, as matches. The reviewer found it by hand; these keep it found.

    uv run python -m bench.test_contract
"""

import json

from bench.contract_cases import CASES, all_cases, csv_cases
from bench.contract_diff import compare_semantic

failures: list[str] = []
passed: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    (passed if cond else failures).append(name)
    print(f"  {'ok  ' if cond else 'FAIL'} {name}" + (f"  {detail}" if not cond else ""))


def cmp(sa, ba, sb, bb, is_csv=False):
    return compare_semantic({"status": sa, "body": ba}, {"status": sb, "body": bb}, is_csv)


def test_error_bodies() -> None:
    """The exact false passes from the Rev30 review."""
    check("two unparseable error bodies are not a match",
          cmp(400, b"x", 400, b"y") != [], str(cmp(400, b"x", 400, b"y")))
    check("error objects without 'detail' are not a match",
          cmp(400, b'{"foo":1}', 400, b'{"foo":2}') != [])
    check("a missing 'detail' is named as such",
          "no 'detail' key" in "".join(cmp(400, b'{"foo":1}', 400, b'{"foo":2}')))
    # Identical bytes are a match whatever shape they have. The gate detects
    # divergence between the two arms; it does not adjudicate whether production's
    # error format is sensible.
    check("identical bodies match even without 'detail'",
          cmp(400, b'{"foo":1}', 400, b'{"foo":1}') == [])
    check("a non-object error body is rejected",
          "expected an object" in "".join(cmp(400, b'[1]', 400, b'[2]')))

    d = b'{"detail":"No data found for the specified query parameters"}'
    check("identical error bodies match", cmp(404, d, 404, d) == [])
    check("differing detail is caught",
          cmp(400, b'{"detail":"a"}', 400, b'{"detail":"b"}') != [])
    check("whitespace-only difference passes semantic comparison",
          cmp(400, b'{"detail":"a"}', 400, b'{"detail": "a"}') == [])
    check("an extra key is caught",
          cmp(400, b'{"detail":"a"}', 400, b'{"detail":"a","x":1}') != [])
    check("differing status is caught", cmp(400, d, 404, d) != [])


def test_success_bodies() -> None:
    rows = [{"lon": 1.5, "lat": 2.5, "depth": 0.0, "time_period": "0", "t": 1.0},
            {"lon": 2.5, "lat": 2.5, "depth": 0.0, "time_period": "0", "t": 2.0}]
    a = json.dumps(rows).encode()
    check("identical payloads match", cmp(200, a, 200, a) == [])

    shuffled = json.dumps(list(reversed(rows))).encode()
    check("row order is not a difference under 5.2B",
          cmp(200, a, 200, shuffled) == [], str(cmp(200, a, 200, shuffled)))

    reordered_keys = json.dumps(
        [{"t": r["t"], "time_period": r["time_period"], "depth": r["depth"],
          "lat": r["lat"], "lon": r["lon"]} for r in rows]).encode()
    check("column order is not a difference under 5.2B",
          cmp(200, a, 200, reordered_keys) == [])

    changed = json.loads(a); changed[0]["t"] = 1.0000001
    check("a value change is caught",
          cmp(200, a, 200, json.dumps(changed).encode()) != [])

    dropped = json.dumps([{k: v for k, v in r.items() if k != "t"} for r in rows]).encode()
    check("a missing column is caught",
          "column sets differ" in "".join(cmp(200, a, 200, dropped)))

    check("a row-count change is caught",
          "row count" in "".join(cmp(200, a, 200, json.dumps(rows[:1]).encode())))

    nulls = json.dumps([{**r, "t": None} for r in rows]).encode()
    check("null versus a value is caught", cmp(200, a, 200, nulls) != [])


def test_csv_bodies() -> None:
    head = "lon,lat,depth,time_period,t\n"
    a = (head + "1.5,2.5,0.0,0,1.0\n2.5,2.5,0.0,0,\n").encode()
    check("identical CSV matches", cmp(200, a, 200, a, is_csv=True) == [])
    # An empty field is how a land cell reaches CSV; the literal NaN is what a
    # non-pandas rewrite would emit. They must not compare equal.
    b = (head + "1.5,2.5,0.0,0,1.0\n2.5,2.5,0.0,0,NaN\n").encode()
    check("empty field versus literal NaN is caught",
          cmp(200, a, 200, b, is_csv=True) != [],
          str(cmp(200, a, 200, b, is_csv=True)))


def test_non_row_payloads() -> None:
    """The documentation routes are not row payloads and must not be called unparseable.

    The first campaign failed C20a/C20b with "unparseable body (8597 vs 8597 bytes)"
    — two identical responses, reported as differing, because the comparator assumed
    every 200 body was a list of rows.
    """
    doc = b'{"openapi":"3.1.0","info":{"title":"ODB WOA23 API"}}'
    check("an identical JSON object matches", cmp(200, doc, 200, doc) == [])
    check("a differing JSON object is caught",
          "non-row payload differs" in "".join(
              cmp(200, doc, 200, doc.replace(b"3.1.0", b"3.0.0"))))
    html = b"<!DOCTYPE html><html><body>swagger</body></html>"
    check("identical HTML matches", cmp(200, html, 200, html) == [])
    check("differing HTML is caught",
          cmp(200, html, 200, html.replace(b"swagger", b"other")) != [])
    check("an empty row list is still handled as rows",
          cmp(200, b"[]", 200, b"[]") == [])


def test_case_list() -> None:
    ids = [c.id for c in all_cases()]
    check("case ids are unique", len(ids) == len(set(ids)))
    check("every data case is replayed as CSV",
          len(csv_cases()) == sum(1 for c in CASES if c.csv_replay))
    check("the Swagger cases are not replayed as CSV",
          not any(c.id.startswith("C20") for c in csv_cases()))
    check("the CSV/JSON status divergence is encoded",
          all(c.csv_status == 400 for c in CASES if c.id in ("C5d", "C5e", "C18")))
    check("the 404 cases are recorded as 404",
          all(c.expect_status == 404 for c in CASES if c.id in ("C5a", "C5b")))
    check("C5c records the silent-drop 200",
          next(c for c in CASES if c.id == "C5c").expect_status == 200)


def main() -> int:
    for fn in (test_error_bodies, test_success_bodies, test_csv_bodies,
               test_non_row_payloads, test_case_list):
        print(f"\n{fn.__name__}")
        fn()
    total = len(passed) + len(failures)
    if failures:
        print(f"\nFAILED {len(failures)}/{total}: {', '.join(failures)}")
    else:
        print(f"\nall passed ({total} assertions)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
