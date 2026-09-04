"""The two dist-info-keyed digests of a package tree, and the name-keyed one.

Three digests exist over the same tree and they answer different questions. Two of
them have been confused for each other once already, which is why each now carries
its canonicalization in the record rather than only a value.

**`package_tree_digest`** — every `*.dist-info` directory in the tree, including the
four with no `METADATA`. Keyed on the directory name, which is unique within a tree.

**`runtime_distribution_digest`** — the same, restricted to directories that have a
`METADATA` file, i.e. the distributions an interpreter can actually see.

Both use one row per directory::

    <dist-info directory>\\t<Name>\\t<Version>\\t<PEP 503 name>\\tMETADATA=<0|1>\\tRECORD=<0|1>

sorted by directory name, joined with ``\\n``, UTF-8, SHA-256. This is spec 002
section 4.1.2b's definition.

**`name_version_set_sha256`** — a *different* canonicalization, computed elsewhere
(`collect_backend_meta.dependencies`) from inside a running interpreter::

    sorted({f"{Name}=={Version}"}) joined with "\\n", UTF-8, SHA-256

It is keyed on name and version, not on directory, so two dist-info directories
claiming the same name and version collapse into one entry. That is exactly the
weakness section 4.1.2b replaced, and the value it produces for the clone —
``60236d72…`` — is the **superseded rev 1–5 runtime-distribution digest**, not the
current one. It is still recorded, because it is the only one of the three that can
be computed from inside an arm's own interpreter and is therefore what proves the
two arms see the same packages. It is no longer *called* the runtime distribution
digest.

    uv run python -m bench.package_digests --root /path/to/clone/dist
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from email.parser import BytesParser
from pathlib import Path

#: What each digest hashes, recorded beside the value so a reader never has to guess.
CANONICALIZATION = {
    "package_tree_digest": (
        "one row per *.dist-info directory in the tree (all of them, including "
        "those without METADATA): "
        "'<dir>\\t<Name>\\t<Version>\\t<PEP503 name>\\tMETADATA=<0|1>\\tRECORD=<0|1>', "
        "sorted by directory name, joined with newlines, UTF-8, SHA-256. "
        "Spec 002 section 4.1.2b."),
    "runtime_distribution_digest": (
        "the same rows, restricted to directories that have a METADATA file. "
        "Spec 002 section 4.1.2b."),
    "name_version_set_sha256": (
        "sorted set of '<Name>==<Version>' over importlib.metadata.distributions() "
        "with a usable Name, joined with newlines, UTF-8, SHA-256. Keyed on name and "
        "version, NOT on dist-info directory, so two directories claiming the same "
        "name and version collapse to one entry. This is the SUPERSEDED rev 1-5 "
        "canonicalization; it is not the runtime_distribution_digest."),
}


def normalise(name: str) -> str:
    """PEP 503 name normalisation."""
    return re.sub(r"[-_.]+", "-", name).lower()


def dist_info_rows(root: str | Path) -> tuple[list[str], list[str]]:
    """(all rows, rows with METADATA), in the section 4.1.2b format.

    Reads the tree directly. No interpreter is started and no package is imported:
    the question is what the directory claims, and importing would answer a
    different one.
    """
    root = Path(root)
    rows_all: list[str] = []
    rows_runtime: list[str] = []
    for entry in sorted(p.name for p in root.iterdir() if p.name.endswith(".dist-info")):
        path = root / entry
        has_md = int((path / "METADATA").exists())
        has_rec = int((path / "RECORD").exists())
        name = version = ""
        if has_md:
            with open(path / "METADATA", "rb") as fh:
                meta = BytesParser().parse(fh, headersonly=True)
            name = meta.get("Name") or ""
            version = meta.get("Version") or ""
        row = (f"{entry}\t{name}\t{version}\t{normalise(name)}"
               f"\tMETADATA={has_md}\tRECORD={has_rec}")
        rows_all.append(row)
        if has_md:
            rows_runtime.append(row)
    return rows_all, rows_runtime


def _digest(rows: list[str]) -> str:
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()


def digests(root: str | Path) -> dict:
    """Both dist-info-keyed digests, with their counts and canonicalization."""
    rows_all, rows_runtime = dist_info_rows(root)
    return {
        "root": str(root),
        "n_dist_info_directories": len(rows_all),
        "n_runtime_distributions": len(rows_runtime),
        "package_tree_digest": _digest(rows_all),
        "runtime_distribution_digest": _digest(rows_runtime),
        "canonicalization": {
            k: CANONICALIZATION[k]
            for k in ("package_tree_digest", "runtime_distribution_digest")},
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", required=True, help="the package tree (…/dist)")
    ap.add_argument("--expect-package-tree", default=None)
    ap.add_argument("--expect-runtime-distribution", default=None)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    d = digests(args.root)
    print(f"  {d['n_dist_info_directories']} dist-info directories, "
          f"{d['n_runtime_distributions']} with METADATA")
    print(f"  package_tree_digest         {d['package_tree_digest']}")
    print(f"  runtime_distribution_digest {d['runtime_distribution_digest']}")

    problems = []
    for flag, key in (("expect_package_tree", "package_tree_digest"),
                      ("expect_runtime_distribution", "runtime_distribution_digest")):
        want = getattr(args, flag)
        if want and want != d[key]:
            problems.append(f"{key} is {d[key]}, expected {want}")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(d, indent=2))
    if problems:
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
