"""Offline tests for the contract comparison. No network.

The error-body path had a false pass that only a test would have caught: comparing
`detail` alone scored two unparseable bodies, and two objects with no `detail` at
all, as matches. The reviewer found it by hand; these keep it found.

    uv run python -m bench.test_contract
"""

import json

from bench.contract_cases import CASES, all_cases, csv_cases
from bench.contract_diff import (compare_semantic, order_fingerprint,
                                 request_order)

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


def test_order_fingerprint_isolates_order() -> None:
    """Order, recorded apart from the verdict, and apart from the values.

    Under 5.2B a row-order difference is not a defect — it is the consequence of the
    unpinned seed C2 exists to observe. So it must be *visible* without being part of
    pass/fail, and it must not move when something that is not order moves.
    """
    rows = [{"lon": 1.5, "lat": 2.5, "depth": 0.0, "time_period": "0", "t": 1.0},
            {"lon": 2.5, "lat": 2.5, "depth": 0.0, "time_period": "0", "t": 2.0}]
    a = {"status": 200, "body": json.dumps(rows).encode()}
    same = {"status": 200, "body": json.dumps(rows).encode()}
    reversed_rows = {"status": 200, "body": json.dumps(list(reversed(rows))).encode()}
    changed_value = {"status": 200, "body": json.dumps(
        [rows[0], {**rows[1], "t": 99.0}]).encode()}

    fa = order_fingerprint(a, False)
    check("the same payload fingerprints the same",
          fa == order_fingerprint(same, False))
    check("a reversed payload has a different row-order digest",
          fa["row_order_sha256"] != order_fingerprint(reversed_rows, False)["row_order_sha256"])
    check("and 5.2B still calls it a match — order is not the verdict",
          cmp(200, a["body"], 200, reversed_rows["body"]) == [])

    # The isolation that makes the fingerprint mean "order": a value change moves the
    # body digest and leaves the row-order digest alone. Without this, every
    # cross-cycle value difference would be reported as an ordering change.
    fv = order_fingerprint(changed_value, False)
    check("a changed value moves the body digest",
          fa["body_sha256"] != fv["body_sha256"])
    check("but not the row-order digest",
          fa["row_order_sha256"] == fv["row_order_sha256"])

    check("the column sequence is recorded", fa["columns"] == list(rows[0]))
    check("so is the row count", fa["n_rows"] == 2)

    # No row structure: no fabricated ordering.
    doc = {"status": 200, "body": json.dumps({"openapi": "3.1.0"}).encode()}
    fd = order_fingerprint(doc, False)
    check("an object payload has no row order", fd["row_order_sha256"] is None)
    check("and no columns", fd["columns"] is None)
    check("but still has a body digest", len(fd["body_sha256"]) == 64)

    err = {"status": 400, "body": b'{"detail":"bad"}'}
    fe = order_fingerprint(err, False)
    check("an error body has no row order", fe["row_order_sha256"] is None)
    check("and is still fingerprinted", len(fe["body_sha256"]) == 64)

    empty = {"status": 200, "body": b"[]"}
    fem = order_fingerprint(empty, False)
    check("an empty row list has a digest rather than None",
          fem["row_order_sha256"] is not None and fem["n_rows"] == 0)
    check("and no columns to report", fem["columns"] is None)

    csv_body = {"status": 200,
                "body": b"lon,lat,depth,time_period,t\n1.5,2.5,0.0,0,1.0\n"}
    fc = order_fingerprint(csv_body, True)
    check("CSV rows are fingerprinted too", fc["n_rows"] == 1)


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


def test_request_order_is_counterbalanced() -> None:
    """Which arm goes first must not be the same arm every time.

    Every case used to fetch the reference first and the candidate second. For all
    64 cases that makes one arm systematically cold and the other systematically
    warm, so any order-dependent state — connection pool, store handle, page cache
    over a shared store — is always applied in the same direction.
    """
    n = len(all_cases())
    seq = [request_order(i) for i in range(n)]
    check("only the two orders are produced", set(seq) <= {"RC", "CR"}, str(set(seq)))
    check(f"the {n} cases split evenly", seq.count("RC") == seq.count("CR") == n // 2,
          f"RC={seq.count('RC')} CR={seq.count('CR')}")
    check("the order alternates rather than blocking",
          all(a != b for a, b in zip(seq, seq[1:])), str(seq[:6]))
    check("it is a pure function of the index",
          [request_order(i) for i in range(n)] == seq)

    # The property that actually matters: no arm is first for a majority of cases,
    # and it holds for an odd count too, to within the one case that cannot balance.
    for size in (1, 2, 3, 63, 64, 65):
        s2 = [request_order(i) for i in range(size)]
        check(f"n={size}: neither arm leads by more than one",
              abs(s2.count("RC") - s2.count("CR")) <= 1,
              f"RC={s2.count('RC')} CR={s2.count('CR')}")


def main() -> int:
    for fn in (test_error_bodies, test_success_bodies, test_csv_bodies,
               test_non_row_payloads,
               test_order_fingerprint_isolates_order, test_case_list,
               test_request_order_is_counterbalanced):
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
