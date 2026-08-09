"""Interpreter and import-path evidence for the S2 C1/C2 modes.

The S2 arms do not run out of `dev2026/.venv`. They run production's own Python
binary against a read-only copy of production's package tree, and the whole point
of that arrangement is that **nothing** is imported from production's live
`site-packages` or from `~/python/woa23/src`. That is a claim about what a
process actually loaded, so it is checked against two kinds of evidence, not one:

1. **An identically-launched interpreter.** Same binary, same flags, same
   `PYTHONPATH`, same cwd, same environment — asked to report `sys.executable`,
   `sys.prefix`, `sys.base_prefix`, `sys.flags`, the full ordered `sys.path`, and
   `__file__` for every module that matters. This is exact about the launch
   procedure and is **not** the gunicorn worker: it is a sibling process started
   the same way. It cannot prove what the worker imported.

2. **The running arm's own `/proc/<pid>/maps`.** Every file the process has
   actually mapped, which for this stack is every native extension it loaded —
   polars, numpy, zarr's codecs, h5py, netCDF4. If one of those came from
   production's site-packages, it appears here and nowhere else. This *is* the
   arm, and it is the stronger of the two, but it only sees files with native
   code: a pure-Python module imported from the wrong place leaves no trace in
   `maps`.

Neither alone is sufficient and the pair is not equivalent to a proof. What can
be said is what each one establishes, which is why they are recorded separately.

The `-S` limitation applies to everything here and is carried in the output:
`site.py` never runs, so no `.pth` file is processed. See spec 002 section 4.3.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

# Modules whose provenance is worth confirming: the web stack the arms serve
# through, and every library on the read path. If one of these resolves into
# production, the isolation claim is false regardless of what the rest do.
DEFAULT_MODULES = (
    "fastapi", "starlette", "uvicorn", "gunicorn", "pydantic",
    "polars", "numpy", "pandas", "xarray", "zarr", "numcodecs",
    "httpx", "dask", "distributed",
)

# A fixed set, used only to observe whether independent starts hash differently.
# Fixed because the point is to compare cycles against each other: a set that
# varied per cycle would make identical seeds look different.
FIXED_STRINGS = (
    "1_degree", "0.25_degree", "annual", "seasonal", "monthly",
    "temperature", "salinity", "TS", "data/", "mn", "an",
)

_PROBE = textwrap.dedent(
    """
    import json, os, site, sys

    def _expand(entry):
        # An empty sys.path entry means the current directory. Recorded expanded
        # as well as raw: "" tells a reader nothing about which directory was on
        # the path, and that is the entry a cwd-relative import comes from.
        raw = entry
        p = os.getcwd() if entry == "" else entry
        p = os.path.abspath(p)
        return {"raw": raw, "abs": p, "real": os.path.realpath(p)}

    out = {
        "executable": sys.executable,
        "prefix": sys.prefix,
        "base_prefix": sys.base_prefix,
        "exec_prefix": sys.exec_prefix,
        "version": sys.version.split()[0],
        "cwd": os.getcwd(),
        "argv0": sys.argv[0],
        "flags": {
            "no_site": int(sys.flags.no_site),
            "ignore_environment": int(sys.flags.ignore_environment),
            "dont_write_bytecode": int(sys.flags.dont_write_bytecode),
            "no_user_site": int(sys.flags.no_user_site),
            "hash_randomization": int(sys.flags.hash_randomization),
            "isolated": int(sys.flags.isolated),
        },
        "dont_write_bytecode": bool(sys.dont_write_bytecode),
        "hashseed_env": os.environ.get("PYTHONHASHSEED"),
        "pythonpath_env": os.environ.get("PYTHONPATH"),
        "sys_path": [_expand(e) for e in sys.path],
        # site.py did not run under -S, so these are what an unrun site would
        # have consulted. Recorded so the gap is visible rather than implied.
        "site_enabled": not sys.flags.no_site,
        "user_site": getattr(site, "ENABLE_USER_SITE", None),
    }

    mods = {}
    for name in __MODULES__:
        try:
            m = __import__(name)
            f = getattr(m, "__file__", None)
            mods[name] = {
                "file": f,
                "real": os.path.realpath(f) if f else None,
                "version": getattr(m, "__version__", None),
            }
        except Exception as exc:
            mods[name] = {"error": f"{type(exc).__name__}: {exc}"}
    out["modules"] = mods

    strings = __STRINGS__
    out["hash_probe"] = {
        "strings": list(strings),
        "hashes": [hash(s) for s in strings],
        "set_order": list(set(strings)),
    }
    print(json.dumps(out))
    """
)


def probe_source(modules=DEFAULT_MODULES, strings=FIXED_STRINGS) -> str:
    return (_PROBE.replace("__MODULES__", repr(list(modules)))
                  .replace("__STRINGS__", repr(tuple(strings))))


def launch_env(clone: str, *, hashseed: str | None, base: dict | None = None) -> dict:
    """The environment an S2 arm is started with.

    Built here rather than in the shell so the probe and the arms cannot drift:
    the runner starts both from this same function's answer.

    `PYTHONHOME` and `VIRTUAL_ENV` are removed rather than left alone. Either one
    inherited from the invoking shell would redirect the interpreter's idea of
    where its packages live, which is the single thing this whole arrangement is
    controlling.
    """
    env = dict(base if base is not None else os.environ)
    for drop in ("PYTHONHOME", "VIRTUAL_ENV", "PYTHONSTARTUP"):
        env.pop(drop, None)
    env["PYTHONPATH"] = clone
    env["PYTHONNOUSERSITE"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    if hashseed is None:
        # C2 observes the unpinned behaviour, so the variable must be absent —
        # not empty. PYTHONHASHSEED="" is not "unset": CPython rejects it.
        env.pop("PYTHONHASHSEED", None)
    else:
        env["PYTHONHASHSEED"] = hashseed
    return env


def interpreter_facts(binary: str, clone: str, cwd: str, *,
                      hashseed: str | None,
                      modules=DEFAULT_MODULES,
                      strings=FIXED_STRINGS,
                      timeout: float = 180.0) -> dict:
    """Run the probe under the arms' exact launch procedure and return its report."""
    env = launch_env(clone, hashseed=hashseed)
    argv = [binary, "-S", "-c", probe_source(modules, strings)]
    r = subprocess.run(argv, cwd=cwd, env=env, capture_output=True,
                       text=True, timeout=timeout)
    if r.returncode != 0:
        return {"probe_error": r.stderr.strip()[-2000:], "argv": argv[:2] + ["-c", "<probe>"]}
    try:
        facts = json.loads(r.stdout)
    except json.JSONDecodeError as exc:
        return {"probe_error": f"probe output is not JSON: {exc}",
                "stdout_head": r.stdout[:400]}
    facts["launch"] = {
        "binary": binary,
        "argv": [binary, "-S", "-c", "<probe>"],
        "cwd": cwd,
        "package_clone": clone,
        "pythonhashseed": hashseed,
        "env_overrides": {k: env.get(k) for k in
                          ("PYTHONPATH", "PYTHONNOUSERSITE",
                           "PYTHONDONTWRITEBYTECODE", "PYTHONHASHSEED")},
    }
    facts["site_limitation"] = (
        "-S: site.py did not run, so no .pth file in the package clone was "
        "processed. distutils-precedence.pth and the basemap nspkg .pth are "
        "present in the clone and did not execute. This is import correctness "
        "for the package tree, not production's site/.pth startup semantics."
    )
    return facts


def sha256_file(path: str) -> str | None:
    h = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
    except OSError:
        return None
    return h.hexdigest()


def binary_identity(binary: str) -> dict:
    """What the interpreter argument actually names.

    `stat` without `-L` reported 52 bytes for this binary once, because that is
    the size of the symlink and not of the ELF behind it. Both are recorded.
    """
    p = Path(binary)
    out: dict = {"given": binary, "is_symlink": p.is_symlink()}
    try:
        out["real"] = os.path.realpath(binary)
        out["size"] = os.stat(binary).st_size          # follows symlinks
        out["lstat_size"] = os.lstat(binary).st_size   # does not
        out["sha256"] = sha256_file(binary)
    except OSError as exc:
        out["error"] = repr(exc)
    return out


# ------------------------------------------------------------------ checking ---

def _under(path: str, root: str) -> bool:
    """Path containment on component boundaries.

    `startswith` alone makes /a/woa23-staging look like it is inside /a/woa23,
    which is the same class of mistake that made port 8050 match 18050.
    """
    if not root:
        return False
    root = root.rstrip("/") or "/"
    return path == root or path.startswith(root + "/")


def expand_roots(roots: list[str]) -> list[str]:
    """Each root plus its realpath, deduplicated and order-preserving.

    A root and a resolved path have to be compared in the same terms or the
    comparison silently answers about nothing. Production's site-packages is
    reached as `~/.pyenv/versions/py311/lib/python3.11/site-packages` and *is*
    `~/.pyenv/versions/3.11.4/envs/py311/lib/python3.11/site-packages`; a
    forbidden root recorded only in the first form would not match a `sys.path`
    entry recorded in the second, and the check would report "under no allowed
    root" — fail-closed, but describing the wrong problem — or, for a root that
    is also allowed by another rule, nothing at all.

    Touches the filesystem, which is why it is separate from the pure checks:
    callers resolve their roots once, up front, and the checking stays testable
    on literal values.
    """
    out: list[str] = []
    for r in roots:
        for form in (r, os.path.realpath(r)):
            if form and form not in out:
                out.append(form)
    return out


def check_import_paths(facts: dict, *, allowed: list[str],
                       forbidden: list[str]) -> list[str]:
    """Every entry on `sys.path` must be an allowed root, and none may be production.

    Fails closed twice over: an entry under a forbidden root is a problem, and so
    is an entry under no allowed root at all. The second half is what catches the
    path nobody thought to forbid.
    """
    problems: list[str] = []
    if "probe_error" in facts:
        return [f"the interpreter probe did not run: {facts['probe_error']}"]
    entries = facts.get("sys_path")
    if not entries:
        return ["the probe reported no sys.path at all"]

    for e in entries:
        for key in ("abs", "real"):
            p = e.get(key)
            if not p:
                problems.append(f"sys.path entry {e.get('raw')!r} has no {key} form")
                continue
            hit = next((f for f in forbidden if _under(p, f)), None)
            if hit:
                problems.append(
                    f"sys.path entry {e['raw']!r} resolves ({key}) to {p}, which is "
                    f"inside production at {hit}")
                continue
            if not any(_under(p, a) for a in allowed):
                problems.append(
                    f"sys.path entry {e['raw']!r} resolves ({key}) to {p}, which is "
                    f"under none of the allowed roots {allowed}")

    for name, info in (facts.get("modules") or {}).items():
        if "error" in info:
            problems.append(f"module {name} did not import: {info['error']}")
            continue
        for key in ("file", "real"):
            p = info.get(key)
            if not p:
                problems.append(f"module {name} has no {key}")
                continue
            hit = next((f for f in forbidden if _under(p, f)), None)
            if hit:
                problems.append(
                    f"module {name}.__file__ ({key}) is {p}, inside production at {hit}")
            elif not any(_under(p, a) for a in allowed):
                problems.append(
                    f"module {name}.__file__ ({key}) is {p}, under none of the "
                    f"allowed roots {allowed}")

    flags = facts.get("flags") or {}
    if flags.get("ignore_environment"):
        problems.append(
            "sys.flags.ignore_environment is set: -E makes the interpreter ignore "
            "every PYTHON* variable while they remain visible in os.environ, so "
            "this record cannot be cited (spec 002 section 4.2.0a)")
    if not flags.get("no_site"):
        problems.append("expected -S (sys.flags.no_site) for the C1 launch procedure")
    if not facts.get("dont_write_bytecode"):
        problems.append("PYTHONDONTWRITEBYTECODE did not take effect: the clone is "
                        "read-only and must not be written to")
    return problems


def parse_maps(text: str) -> list[str]:
    """File paths from a `/proc/<pid>/maps` dump, deduplicated and sorted."""
    seen = set()
    for line in text.splitlines():
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        path = parts[5].strip()
        if path.startswith("/"):
            seen.add(path)
    return sorted(seen)


def check_maps(paths: list[str], *, forbidden: list[str]) -> list[str]:
    """Mapped files that came from production.

    Only the forbidden half is checked here, deliberately. A process maps plenty
    of legitimate things this harness has no list for — the C library, locale
    archives, the store's own files — so requiring an allow-list would produce
    noise, not safety. What matters is that nothing came from production.
    """
    problems = []
    for p in paths:
        hit = next((f for f in forbidden if _under(p, f)), None)
        if hit:
            problems.append(f"the running process has mapped {p}, inside production at {hit}")
    return problems


def read_maps(pid: int) -> tuple[list[str] | None, str | None]:
    try:
        text = Path(f"/proc/{pid}/maps").read_text()
    except OSError as exc:
        return None, repr(exc)
    return parse_maps(text), None


def seed_digest(facts: dict) -> str | None:
    """A stable identifier for one process's hash seed, from the fixed probe.

    Two starts with the same digest hashed the fixed strings identically. That is
    what "the seed did not vary" means here, and it is all it means.
    """
    probe = facts.get("hash_probe")
    if not probe or "hashes" not in probe:
        return None
    payload = json.dumps({"strings": probe["strings"], "hashes": probe["hashes"]},
                         sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------- CLI ---

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--python-binary", required=True)
    ap.add_argument("--package-clone", required=True)
    ap.add_argument("--cwd", required=True, help="the arm's working directory")
    ap.add_argument("--label", required=True)
    ap.add_argument("--hashseed", default=None,
                    help="pinned value for C1; omit entirely for C2")
    ap.add_argument("--allow", action="append", default=[],
                    help="a root sys.path and module files may live under; repeatable")
    ap.add_argument("--forbid", action="append", default=[],
                    help="a production root nothing may resolve into; repeatable")
    ap.add_argument("--pid", action="append", type=int, default=[],
                    help="a running arm process whose /proc/<pid>/maps is checked")
    ap.add_argument("--module", action="append", default=[],
                    help="override the default module list")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    modules = tuple(args.module) if args.module else DEFAULT_MODULES
    facts = interpreter_facts(args.python_binary, args.package_clone, args.cwd,
                              hashseed=args.hashseed, modules=modules)
    facts["binary"] = binary_identity(args.python_binary)
    facts["label"] = args.label

    # The interpreter's own stdlib is allowed without being named on the command
    # line: base_prefix is discovered, not configured, and hard-coding it in the
    # runner would be a second place for it to be wrong.
    allowed = list(args.allow)
    base = facts.get("base_prefix")
    if base:
        allowed.append(str(Path(base) / "lib"))
    allowed = expand_roots(allowed)
    forbidden = expand_roots(args.forbid)
    facts["allowed_roots"] = allowed
    facts["forbidden_roots"] = forbidden

    problems = check_import_paths(facts, allowed=allowed, forbidden=forbidden)

    maps_record = {}
    for pid in args.pid:
        paths, err = read_maps(pid)
        if err is not None:
            problems.append(f"cannot read /proc/{pid}/maps ({err}); the running "
                            f"process's loaded files cannot be checked")
            maps_record[str(pid)] = {"error": err}
            continue
        hits = check_maps(paths, forbidden=args.forbid)
        maps_record[str(pid)] = {"n_mapped_files": len(paths),
                                 "production_hits": hits}
        problems.extend(hits)
    facts["maps"] = maps_record
    facts["problems"] = problems
    facts["seed_digest"] = seed_digest(facts)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(facts, indent=2, sort_keys=True))

    print(f"  {args.label}: {facts.get('executable')}")
    print(f"    prefix {facts.get('prefix')}  base_prefix {facts.get('base_prefix')}")
    print(f"    flags {facts.get('flags')}")
    print(f"    sys.path ({len(facts.get('sys_path') or [])} entries):")
    for e in facts.get("sys_path") or []:
        print(f"      {e['raw']!r} -> {e['real']}")
    for name, info in sorted((facts.get("modules") or {}).items()):
        print(f"    {name:12s} {info.get('real') or info.get('error')}")
    for pid, rec in sorted(maps_record.items()):
        print(f"    /proc/{pid}/maps: {rec.get('n_mapped_files', '?')} mapped files, "
              f"{len(rec.get('production_hits') or [])} in production")
    print(f"    LIMITATION {facts.get('site_limitation')}")

    if problems:
        print(f"{args.label}: import isolation is not established:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
