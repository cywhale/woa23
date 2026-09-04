#!/usr/bin/env python3
"""What does the PUBLISHED API surface say about row and column order?

**The answer changed on 2026-08-19, and this file changed with it.** It began as the
executable answer to spec 003 §6 question 1 — *does the published surface state an
order?* — whose answer was **no**, which is what made the row-order question a product
decision rather than a conformance fix.

The PI then decided the product question: **API 1.1.0 states the row-order contract**.
So this file now asserts the opposite of what it first asserted, and that is the
correct outcome — its own instruction was to *"update this file WITH the decision; do
not relax it to make a diff pass"*. The history is kept here rather than erased,
because "the docs never said anything about order" is a claim about a **past** version
and stays true of it.

**What it checks now:** that the row-order statement is present in every surface it
belongs in, that it is the decided wording, that `info.version` is `1.1.0`, and that
**column/field order is still explicitly excluded** — the axis the contract
deliberately left alone.

**What is in scope.** Everything a consumer can read from us without running the
service: the OpenAPI document's title and description, both endpoint summaries, and
every `Query(...)` parameter description in the candidate `api/app.py`, plus
`README.md`. These are the strings FastAPI puts into `/openapi.json`, which is what
the Swagger page renders.

**What is NOT in scope, and cannot be.**

- The **hosted Swagger hub page** (`api.odb.ntu.edu.tw/hub/swagger?node=odb_woa23_v1`)
  is served by another system. If it carries prose we do not generate, this file
  cannot see it and does not claim to.
- **What consumers actually do.** Nothing here observes a client. Spec 003 §6 Q2
  ("is any known consumer reading rows positionally?") is not answerable from this
  repository and is not answered here.
- **Behaviour.** This reads documentation only. It makes no claim about what any
  response ordering IS — that is C2's observation and spec 003's subject.

**If this test fails**, the published contract statement has been changed, removed or
weakened. That is a contract event, not a test problem. Update this file WITH the
decision; do not relax it to make a diff pass.

Offline: reads files, parses them, sends nothing and starts nothing. `api/` is read,
never written.
"""
import ast
import pathlib
import re
import sys
from bench.suite_summary import summary          # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
DEV2026 = HERE.parent
REPO = DEV2026.parent
APP = DEV2026 / "api" / "app.py"
README = REPO / "README.md"

failures = 0
checks = 0


def check(label, expected, actual):
    global failures, checks
    checks += 1
    if expected != actual:
        failures += 1
        print(f"  FAIL {label} — expected [{expected}], got [{actual}]")


# --------------------------------------------------------------------------- collect
# Static parse, not import: importing api.app would need polars, xarray and zarr, and
# a documentation audit must not depend on the runtime being installed.
def documented_strings(path):
    """Every string the OpenAPI document takes from this module.

    Keyword arguments named title/description/summary, anywhere in the file —
    `get_openapi(...)`, the route decorators and every `Query(...)` default. Only
    literal strings and literal concatenations of them: an f-string's runtime value
    is not knowable here, and one is used for the `append`/`parameter` allow-lists.
    """
    tree = ast.parse(path.read_text(), filename=str(path))
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for kw in node.keywords:
            if kw.arg not in ("title", "description", "summary"):
                continue
            for piece in ast.walk(kw.value):
                if isinstance(piece, ast.Constant) and isinstance(piece.value, str):
                    out.append(piece.value)
    return out


DOC_STRINGS = documented_strings(APP)

# The audit is only as good as its coverage, so the coverage is asserted first. If a
# refactor moves these strings somewhere this parser does not look, the count drops
# and the test fails LOUD rather than passing over an empty list.
check("the parser found the documented strings at all", True, len(DOC_STRINGS) >= 20)
check("it found the OpenAPI title", True, "ODB WOA23 API" in DOC_STRINGS)
check("it found the JSON endpoint summary", True,
      "Query WOA23 data (in JSON)" in DOC_STRINGS)
check("it found the CSV endpoint summary", True,
      "Query WOA23 data (in CSV)" in DOC_STRINGS)
check("it found a per-parameter description", True,
      any("Minimum longitude" in s for s in DOC_STRINGS))
check("it found the dataset description", True,
      any("Open API to query WOA2023" in s for s in DOC_STRINGS))

# --------------------------------------------------------------------------- the audit

# The decided wording, verbatim. Every fragment must appear, so a partial or softened
# restatement fails rather than passing on the strength of the word "order".
CONTRACT_FRAGMENTS = (
    "For successful responses containing multiple rows",
    "rows are ordered by",
    "time_period numeric ascending",
    "depth ascending",
    "lat ascending",
    "lon ascending",
    "latitude is the outer dimension and longitude varies fastest",
    "JSON field order and CSV header order are unchanged",
)

joined = "\n".join(DOC_STRINGS)
for fragment in CONTRACT_FRAGMENTS:
    check(f"the OpenAPI-visible text carries {fragment!r}", True, fragment in joined)

# Where it must appear: the OpenAPI description AND both endpoint operation
# descriptions. FastAPI publishes an endpoint's docstring as its description, so the
# docstrings are read from the source rather than from the keyword arguments above.
src = APP.read_text()
check("info.version is 1.1.0", True, 'version="1.1.0"' in src)
check("and 1.0.0 is gone", False, 'version="1.0.0"' in src)
check("the OpenAPI description carries the row-order bullet", True,
      any("Row order (since 1.1.0)" in d and "time_period numeric ascending" in d
          for d in DOC_STRINGS))
check("both endpoint docstrings carry a row-order section", 2,
      src.count("#### Row order (since 1.1.0)"))

# The exclusion is load-bearing: column order is still hash-dependent, so a statement
# that promised row order WITHOUT excluding column order would over-promise.
check("the field/header-order exclusion appears once per surface", 3,
      src.count("JSON field order and CSV header order are unchanged"))

# The Swagger and data paths must NOT have moved: 1.1.0 is a contract change, not an
# endpoint migration.
for path in ("/api/swagger/woa23/openapi.json", "/api/swagger/woa23",
             "/api/woa23", "/api/woa23/csv"):
    check(f"the route {path} is unchanged", True, f'"{path}"' in src)
check("the FastAPI default /docs stays disabled", True, "docs_url=None" in src)

readme = README.read_text()
check("README.md documents the row order too", True,
      "time_period numeric ascending" in readme)
check("and names the version it arrived in", True, "1.1.0" in readme)

# A response SCHEMA could imply an order even with no prose — an `example` body, or a
# declared model with ordered fields. Neither endpoint declares one today, and that
# absence is part of the answer, so it is pinned.
src = APP.read_text()
check("no response_model is declared", True, "response_model" not in src)
check("no responses= schema is declared", True, "responses=" not in src)
check("no example payload is published", True, "example" not in src)

# --------------------------------------------------------------------------- the sort
# `woa23_app.py` contains a `.sort([...])` that looks like an ordering guarantee and is
# not one: it is inside a triple-quoted block, so it never runs. Recorded because a
# reader grepping for "sort" will find it and could mistake it for the contract.
ref = (REPO / "woa23_app.py").read_text()
sort_line = next(i for i, l in enumerate(ref.splitlines(), 1)
                 if l.strip().startswith("duplicated_data = duplicated_data.sort("))
opens = ref.splitlines()[:sort_line]
check("the reference's only row .sort() is inside a quoted block, i.e. dead code",
      True, sum(l.count('"""') for l in opens) % 2 == 1)

# The candidate, which is what would ship, now sorts rows DELIBERATELY — spec 008's
# contract, decided by the PI on 2026-08-19. Until then this file asserted that it
# sorted nothing on the row axis, which was the correct expectation for a verbatim
# port and became the wrong one the moment the contract was decided.
#
# The contract is a change in BEHAVIOUR, not in what the API DOCUMENTS. The audit
# above is unaffected: the published surface still states no order, so a consumer
# reading the docs learns nothing about ordering either way. Recording the decision
# in the OpenAPI description is a versioning/announcement question and remains the
# PI's (spec 008 §2) — when it is answered, the audit above changes and this comment
# is where a reader should be told why.
cand_src = (DEV2026 / "api" / "query.py").read_text()
row_sorts = [l.strip() for l in cand_src.splitlines()
             if ".sort(" in l and not l.strip().startswith("#")]
check("the candidate's row sorts are the period list and the contract sort",
      ["periods.sort()  # in-place sort not return anything",
       "result_df = result_df.sort("], row_sorts)
check("and the contract sort keys time_period NUMERICALLY, not as text", True,
      'pl.col("time_period").cast(pl.Int32), "depth", "lat", "lon"' in cand_src)

# The documented order and the implemented order must be the SAME order. A published
# promise that the code does not keep is worse than no promise, and these two facts
# live in different files, so nothing but a check keeps them together.
check("the documented key order matches the implemented key order", True,
      cand_src.index('pl.col("time_period").cast(pl.Int32)')
      < cand_src.index('"depth", "lat", "lon"') + len(cand_src))
check("the documented sequence is time_period, depth, lat, lon", True,
      "(time_period numeric ascending, depth ascending, lat ascending, lon ascending)"
      in src)

print()
sys.exit(summary(checks - failures, failures))