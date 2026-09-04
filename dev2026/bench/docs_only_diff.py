#!/usr/bin/env python3
"""Is an `api/` change documentation-only? Decided by machine, not by assertion.

A C1/C2 re-run costs a controlled VM24 execution. A change that only edits published
prose should not cost one — but "it's only docs" is exactly the claim nobody should be
allowed to make about their own diff. So this decides it structurally.

**The allowlist. These, and nothing else:**

1. the OpenAPI `info.version` — the `version=` keyword of `get_openapi(...)`;
2. the OpenAPI `description` — the `description=` keyword of `get_openapi(...)`;
3. **endpoint operation docstrings** — and any other docstring, since a docstring is by
   definition not executed;
4. comments and blank lines.

**Forbidden, and each one fails the check:** routes and paths, request parameters
(names, defaults, types **and their `description=` strings**, which are request
documentation and not in the allowlist), response schemas, handler logic, query logic,
serialisation, runtime configuration, and any change to API data behaviour.

**How it decides.** Both revisions are parsed to an AST. Every docstring is removed, and
the two allowed `get_openapi` keyword strings are replaced by a placeholder. If the
resulting trees are **identical**, nothing outside the allowlist moved — because
everything that executes is still in the tree and still identical. If they differ, the
check fails and prints where.

That is stronger than a textual diff review: a reviewer skims, and an AST comparison
does not. It is also narrower than "the tests pass" — tests can pass while behaviour
changes.

**What a PASS does and does not license.** It licenses skipping a C1/C2 re-run for
*this* diff. It does **not** make the commit new C1/C2 evidence, and it does not extend
to any other file: `api/query.py`, `api/config.py` and `api/store_paths.py` must be
**byte-identical**, and that is checked separately below.

    uv run python -m bench.docs_only_diff <old-rev> <new-rev>
    uv run python -m bench.docs_only_diff 1439194 HEAD
"""

import argparse
import ast
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

#: The only file the allowlist covers. Every other `api/` file must be byte-identical:
#: prose lives in `app.py`, so a "docs-only" change to `query.py` is a contradiction.
DOCS_FILE = "api/app.py"
OTHER_API_FILES = ("api/__init__.py", "api/config.py", "api/query.py",
                   "api/store_paths.py")

#: The `get_openapi(...)` keywords the allowlist covers.
ALLOWED_OPENAPI_KEYWORDS = ("version", "description")

PLACEHOLDER = "<<ALLOWLISTED-OPENAPI-STRING>>"


def git_show(rev: str, path: str) -> str:
    r = subprocess.run(["git", "-C", str(REPO.parent), "show",
                        f"{rev}:dev2026/{path}"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"cannot read {path} at {rev}: {r.stderr.strip()}")
    return r.stdout


class Normalise(ast.NodeTransformer):
    """Strip what the allowlist permits, keep everything that executes.

    Docstrings are removed rather than blanked, so a docstring's *presence* is not
    load-bearing either — adding one to a function that had none is still docs-only.

    The two allowed `get_openapi` keyword strings become a placeholder. Only inside a
    call named `get_openapi`: a `description=` anywhere else — a `Query(...)`, say — is
    request-parameter documentation, is NOT in the allowlist, and stays in the tree
    where a difference will be caught.
    """

    def _strip_doc(self, node):
        self.generic_visit(node)
        body = node.body
        if (body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            node.body = body[1:] or [ast.Pass()]
        return node

    visit_Module = _strip_doc
    visit_FunctionDef = _strip_doc
    visit_AsyncFunctionDef = _strip_doc
    visit_ClassDef = _strip_doc

    def visit_Call(self, node):
        self.generic_visit(node)
        func = node.func
        name = getattr(func, "id", None) or getattr(func, "attr", None)
        if name == "get_openapi":
            for kw in node.keywords:
                if kw.arg in ALLOWED_OPENAPI_KEYWORDS:
                    kw.value = ast.Constant(value=PLACEHOLDER)
        return node


def normalised_dump(src: str) -> str:
    tree = Normalise().visit(ast.parse(src))
    ast.fix_missing_locations(tree)
    return ast.dump(tree, annotate_fields=True, include_attributes=False, indent=1)


def first_difference(a: str, b: str) -> str:
    la, lb = a.splitlines(), b.splitlines()
    for i, (x, y) in enumerate(zip(la, lb)):
        if x != y:
            return (f"first structural difference at normalised node line {i + 1}:\n"
                    f"       old: {x.strip()[:160]}\n       new: {y.strip()[:160]}")
    if len(la) != len(lb):
        return (f"the trees have different sizes: {len(la)} vs {len(lb)} normalised "
                f"nodes — something was added or removed outside the allowlist")
    return "trees differ but no line differs (should not happen)"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("old", help="revision BEFORE the change (e.g. the C1/C2 subject)")
    ap.add_argument("new", help="revision after (e.g. HEAD)")
    args = ap.parse_args()

    print(f"docs-only allowlist check: {args.old} -> {args.new}\n")
    failures = []

    # 1. Every other api/ file must be byte-identical. Prose lives in app.py.
    for path in OTHER_API_FILES:
        old, new = git_show(args.old, path), git_show(args.new, path)
        if old == new:
            print(f"  ok   {path} byte-identical")
        else:
            failures.append(f"{path} CHANGED — outside the allowlist, which covers "
                            f"{DOCS_FILE} only")
            print(f"  FAIL {path} changed")

    # 2. app.py: identical once docstrings and the two allowed strings are removed.
    old_src, new_src = git_show(args.old, DOCS_FILE), git_show(args.new, DOCS_FILE)
    if old_src == new_src:
        print(f"  ok   {DOCS_FILE} byte-identical (nothing to allowlist)")
    else:
        try:
            old_n, new_n = normalised_dump(old_src), normalised_dump(new_src)
        except SyntaxError as exc:
            failures.append(f"{DOCS_FILE} does not parse: {exc!r}")
            old_n = new_n = None
        if old_n is not None:
            if old_n == new_n:
                print(f"  ok   {DOCS_FILE} differs ONLY in docstrings and the "
                      f"allowlisted get_openapi version/description")
            else:
                failures.append(f"{DOCS_FILE} changed OUTSIDE the allowlist")
                print(f"  FAIL {DOCS_FILE} changed outside the allowlist")
                print("       " + first_difference(old_n, new_n))

    print()
    if failures:
        print("DOCS-ONLY: NO — this diff is NOT documentation-only.")
        for f in failures:
            print(f"  - {f}")
        print("\nIt may not be classified as docs-only, and it requires a NEW C1 and C2")
        print("under a new execution identity before any deployment or performance")
        print("claim. Stop and report the diff rather than re-classifying it.")
        return 1

    print("DOCS-ONLY: YES — within the allowlist.")
    print("  This licenses skipping a C1/C2 re-run FOR THIS DIFF ONLY.")
    print("  It does NOT make the commit C1/C2 evidence: c1f and c2g remain the")
    print("  evidence, and they describe the data path, which this diff does not touch.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
