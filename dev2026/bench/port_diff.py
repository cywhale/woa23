"""Show every executable difference between the original function and its port.

Spec 001 claims the candidate is the original "lifted verbatim" apart from a named
set of changes. That claim needs to be checkable by someone who did not write it,
and a textual diff will not do it: `woa23_app.py` carries dead code inside
triple-quoted string literals — a pandas implementation and a duplicate-check block,
both inert — which a text diff reports as removals and which inflate the apparent
change.

So this compares the **statements that actually execute**: parse both, drop bare
string expressions, unparse, diff. Anything it prints is a real difference and
belongs in the spec's inventory or should not be there.

    uv run python -m bench.port_diff

Exit code is 0 whether or not differences exist — the point is to show them, not to
pass or fail. Reading the list is the review step.
"""

from __future__ import annotations

import ast
import difflib
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent

ORIGINAL = "woa23_app.py"
CANDIDATE_MODULES = ("dev2026/api/config.py", "dev2026/api/query.py",
                     "dev2026/api/app.py")

# Every function defined in the original, and where it landed. The list is checked
# against the original at run time, so a function nobody ported is reported rather
# than quietly omitted — the earlier version covered six of ten and its output was
# still called a complete inventory.
PAIRS = (
    ("process_woa23_data", "dev2026/api/query.py"),
    ("to_lowest_grid_point", "dev2026/api/query.py"),
    ("determine_subgroup", "dev2026/api/query.py"),
    ("custom_json_serializer", "dev2026/api/query.py"),
    ("generate_custom_openapi", "dev2026/api/app.py"),
    ("lifespan", "dev2026/api/app.py"),
    ("custom_openapi", "dev2026/api/app.py"),
    ("custom_swagger_ui_html", "dev2026/api/app.py"),
    ("get_woa23", "dev2026/api/app.py"),
    ("get_woa23_csv", "dev2026/api/app.py"),
)


def _is_inert_string(stmt: ast.stmt) -> bool:
    return (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str))


def _strip(node: ast.AST) -> None:
    """Drop bare string statements everywhere, not only at the top of a function.

    The original's commented-out-by-string blocks sit inside `for` and `if` bodies,
    so a top-level-only filter leaves them in and they show up as spurious
    removals.
    """
    for field, value in ast.iter_fields(node):
        if isinstance(value, list):
            kept = [v for v in value
                    if not (isinstance(v, ast.stmt) and _is_inert_string(v))]
            setattr(node, field, kept)
            for v in kept:
                if isinstance(v, ast.AST):
                    _strip(v)
        elif isinstance(value, ast.AST):
            _strip(value)


def executable_body(path: Path, name: str) -> list[str] | None:
    """The function's statements, with docstrings and commented-out-by-string code removed."""
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            _strip(node)
            return ast.unparse(node).splitlines()
    return None


def top_level(path: Path) -> tuple[dict[str, str], list[str], list[str]]:
    """Module-level assignments, imports, and anything else that runs at import."""
    tree = ast.parse(path.read_text())
    assigns: dict[str, str] = {}
    imports: list[str] = []
    other: list[str] = []
    for stmt in tree.body:
        if isinstance(stmt, (ast.Import, ast.ImportFrom)):
            imports.append(ast.unparse(stmt))
        elif isinstance(stmt, ast.Assign):
            for tgt in stmt.targets:
                if isinstance(tgt, ast.Name):
                    assigns[tgt.id] = ast.unparse(stmt.value)
        elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        elif _is_inert_string(stmt):
            continue
        else:
            other.append(ast.unparse(stmt))
    return assigns, imports, other


def module_level_report() -> int:
    """Differences that live outside any function — where the Dask client lived."""
    o_assign, o_imports, o_other = top_level(REPO / ORIGINAL)
    c_assign: dict[str, str] = {}
    c_imports: list[str] = []
    c_other: list[str] = []
    for rel in CANDIDATE_MODULES:
        a, i, x = top_level(REPO / rel)
        c_assign.update(a)
        c_imports += i
        c_other += x

    changed = 0
    print("\nmodule level")
    for name in sorted(set(o_assign) - set(c_assign)):
        print(f"   - {name} = {o_assign[name]}")
        changed += 1
    for name in sorted(set(c_assign) - set(o_assign)):
        print(f"   + {name} = {c_assign[name]}")
        changed += 1
    for name in sorted(set(o_assign) & set(c_assign)):
        if o_assign[name] != c_assign[name]:
            print(f"   ~ {name}: {o_assign[name]}  ->  {c_assign[name]}")
            changed += 1
    for stmt in o_other:
        if stmt not in c_other:
            print(f"   - {stmt}")
            changed += 1
    for stmt in c_other:
        if stmt not in o_other:
            print(f"   + {stmt}")
            changed += 1
    if not changed:
        print("   identical")

    # Import lines are reported but not counted: the candidate is three modules
    # where the original was one, so they cannot match and a difference is not
    # evidence by itself. `__future__` imports are the exception — they change how
    # annotations exist at runtime, and FastAPI builds the OpenAPI document by
    # introspecting exactly those — so those ARE counted.
    for imp in c_imports:
        if imp.startswith("from __future__") and imp not in o_imports:
            print(f"   + {imp}  <-- changes annotation semantics; not informational")
            changed += 1
    only_orig = [i for i in o_imports if i not in c_imports]
    only_cand = [i for i in c_imports if i not in o_imports]
    if only_orig or only_cand:
        print("   imports (informational — a 3-module split cannot match a 1-module file):")
        for i in only_orig:
            print(f"      - {i}")
        for i in only_cand:
            print(f"      + {i}")
    return changed


def main() -> int:
    total = 0

    defined = {n.name for n in ast.parse((REPO / ORIGINAL).read_text()).body
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    covered = {name for name, _ in PAIRS}
    if defined - covered:
        print(f"NOT COVERED by this tool: {sorted(defined - covered)}")
        total += len(defined - covered)

    for label, cand_rel in PAIRS:
        fn = label
        orig = executable_body(REPO / ORIGINAL, fn)
        cand = executable_body(REPO / cand_rel, fn)
        if orig is None or cand is None:
            print(f"\n{label}: NOT FOUND "
                  f"({'original' if orig is None else 'candidate'})")
            continue
        diff = [l for l in difflib.unified_diff(orig, cand, "original", "candidate",
                                                lineterm="", n=1)]
        changed = sum(1 for l in diff if l[:1] in "+-" and l[:3] not in ("+++", "---"))
        total += changed
        if not diff:
            print(f"\n{label}: identical ({len(orig)} statements)")
            continue
        print(f"\n{label}: {changed} changed lines "
              f"(original {len(orig)}, candidate {len(cand)})")
        for line in diff:
            print("   " + line)

    total += module_level_report()
    print(f"\ntotal AST changed lines, functions and module level: {total}")
    print("These lines group into a smaller number of conceptual changes; spec 001")
    print("section 4.6 lists both figures so neither can be mistaken for the other.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
