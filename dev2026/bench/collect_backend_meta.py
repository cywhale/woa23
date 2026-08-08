"""Collect provenance for a running backend. Runs on the host, beside the process.

`paired_bench.py` only speaks HTTP, so it cannot see the thing it is measuring: the
source files actually loaded, the hash seed in force, the interpreter, the resolved
dependencies. Those have to be read from the process itself, which means a sidecar
on the same host rather than a flag the harness invents.

    uv run python -m bench.collect_backend_meta --port 8051 \
        --manifest candidate --out results/meta_candidate.json

The result is passed to `paired_bench.py --candidate-meta / --reference-meta` and
embedded verbatim in the benchmark record.

**Environment variables are whitelisted, never dumped.** A process environment
routinely holds credentials, and a benchmark artefact is something we commit and
share. Only the keys named in ENV_WHITELIST are read.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bench.manifests import MANIFESTS, expand  # noqa: E402
from bench.provenance import load_meta, validate_meta  # noqa: E402

# Everything that can change a measurement or a result. Nothing else is read.
ENV_WHITELIST = (
    "PYTHONHASHSEED",
    "VIRTUAL_ENV",
    "WOA23_ZARR_STORE",
    "DASK_SCHEDULER_ADDRESS",
    "PYTHONPATH",
    "OMP_NUM_THREADS",
    "POLARS_SKIP_CPU_CHECK",
)


def local_port_of(field: str) -> int | None:
    """The port from an `ss` address column, or None if the column has no port.

    `ss` writes the local address as `addr:port`, and the address half may itself
    contain colons (`[::]:8050`, `[fe80::8050]:9000`) or a zone suffix
    (`[fe80::1%eth0]:8050`). Only the text after the **last** colon is the port, and
    only if it is entirely digits — `0.0.0.0:*` has none.
    """
    host, sep, port = field.rpartition(":")
    if not sep or not port.isdigit():
        return None
    return int(port)


def parse_ss_listeners(output: str, port: int) -> list[int]:
    """Every PID holding a *listening* socket on `port`, across **all** matching rows.

    A service usually appears more than once — a separate row per address family, so
    `0.0.0.0:5433` and `[::]:5433` are two lines for one server. An earlier version
    returned at the first matching row, which silently dropped whichever family came
    second and could therefore miss the master entirely.

    Two things this must not do, both of which earlier forms did:

    - **Match a port as a substring.** An IPv6 address may contain the port's digits
      as a hextet, so `[fe80::8050]:9000` is a different service that a substring
      test reads as a listener on 8050. The port is parsed from the local-address
      column and compared as an integer.
    - **Look outside the local-address column.** Scanning the first five fields also
      scans the *peer* column, so a client connected to `127.0.0.1:8050` was reported
      as holding that port. `-l` output makes peers `0.0.0.0:*` and hides this, which
      is exactly why it should not be relied on.

    Rows are taken only in state LISTEN, which also skips the header line.

    Pure so it can be tested against captured `ss` output rather than a live host.
    """
    pids: set[int] = set()
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 4 or fields[0] != "LISTEN":
            continue
        if local_port_of(fields[3]) != port:
            continue
        pids.update(int(m) for m in re.findall(r"pid=(\d+)", line))
    return sorted(pids)


def pids_on_port(port: int) -> list[int]:
    """Every PID holding the listening socket, via `ss`.

    A forking server has several. gunicorn's master creates the socket and each
    worker inherits it, so `ss -lntp` reports all of them and the order is not
    meaningful. An earlier version took the first `pid=` match and called it the
    master; on the live production port that returned a **worker** (4366) while the
    master was 3960, so every identity field in the record would have described the
    wrong process.
    """
    try:
        out = subprocess.run(["ss", "-lntp"], capture_output=True, text=True,
                             timeout=10).stdout
    except Exception:
        return []
    return parse_ss_listeners(out, port)


def ppid_of(pid: int) -> int | None:
    """Field 4 of /proc/<pid>/stat, parsed past the parenthesised comm field."""
    raw = proc_field(pid, "stat")
    if not raw:
        return None
    try:
        return int(raw[raw.rindex(")") + 1:].split()[1])
    except (ValueError, IndexError):
        return None


def master_of(pids: list[int], ppid=ppid_of) -> int | None:
    """The one process in the set that is not a child of another in the set.

    Returns None when the answer is not unique — no root, more than one, or any PID
    whose parent could not be read at all. That last case is the one that bit:
    `master_of([7000], ppid=lambda _: None)` used to return 7000, because a `None`
    parent is trivially "not in the set" and so looked like a root. An unreadable
    parent is not evidence of being a root; it is evidence of knowing nothing.
    Every caller must treat None as a hard failure rather than falling back to a
    guess: recording the wrong process as the master is worse than recording
    nothing, because the record looks authoritative either way.

    `ppid` is injectable so the selection logic can be tested without live processes.
    """
    if not pids:
        return None
    parents = {p: ppid(p) for p in pids}
    if any(v is None for v in parents.values()):
        # An unreadable /proc/<pid>/stat means the process cannot be classified at
        # all. Treating "no parent found" as "therefore a root" is how a single
        # unreadable PID used to be returned as the master.
        return None
    roots = [p for p, parent in parents.items() if parent not in pids]
    return roots[0] if len(roots) == 1 else None


def proc_field(pid: int, name: str) -> str | None:
    try:
        return Path(f"/proc/{pid}/{name}").read_text()
    except Exception:
        return None


def cmdline(pid: int) -> tuple[list[str] | None, str | None]:
    """(argv array, display string).

    `/proc/<pid>/cmdline` is NUL-separated argv. Joining it with spaces is lossy —
    an argument containing a space becomes indistinguishable from two arguments —
    so the array is recorded as the authoritative form and the joined string is
    kept only for human reading.
    """
    raw = proc_field(pid, "cmdline")
    if not raw:
        return None, None
    argv = [a for a in raw.split("\0") if a]
    return argv, " ".join(argv)


def whitelisted_env(pid: int) -> dict:
    raw = proc_field(pid, "environ")
    if not raw:
        return {}
    out = {}
    for item in raw.split("\0"):
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        if k in ENV_WHITELIST:
            out[k] = v
    # An unset PYTHONHASHSEED is itself the finding: the backend is running with a
    # random seed, so its output ordering is not reproducible.
    out.setdefault("PYTHONHASHSEED", "<unset — randomised>")
    return out


def proc_starttime(pid: int) -> int | None:
    """Field 22 of /proc/<pid>/stat: process start time, in clock ticks since boot.

    PID plus argv is not an identity. PIDs are recycled, and a restarted backend
    launched by the same command line has the same argv — so a comparison on those
    two alone can call a different process the same one. The start time is what
    distinguishes them.

    The `comm` field can contain spaces and parentheses, so the parse anchors on the
    last `)` rather than splitting the whole line.
    """
    raw = proc_field(pid, "stat")
    if not raw:
        return None
    try:
        return int(raw[raw.rindex(")") + 1:].split()[19])
    except (ValueError, IndexError):
        return None


def boot_id() -> str | None:
    """Identifies this boot, so a start time from before a reboot is not comparable."""
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return None


def workers(pid: int) -> list[int]:
    try:
        r = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True,
                           timeout=10)
        return [int(x) for x in r.stdout.split()]
    except Exception:
        return []


def hash_manifest(cwd: Path, label: str) -> dict:
    """Full SHA-256 of every file in the manifest, resolved against the process cwd.

    An unreadable file is recorded as such and fails the INVALID_METADATA gate; it
    is never silently skipped, because a missing hash is indistinguishable from a
    file that never existed.
    """
    out = {}
    for p in expand(label, cwd):
        rel = str(p.relative_to(cwd))
        try:
            out[rel] = hashlib.sha256(p.read_bytes()).hexdigest()
        except Exception as exc:
            out[rel] = f"<unreadable: {exc.__class__.__name__}>"
    return out


def _canonical_store(cwd: Path, literal: str) -> str:
    """The store the backend actually reads: absolute, symlinks followed.

    Two different things are recorded about the store and they must not be
    conflated. `store_path_literal` is the raw string the process interpolates into
    the group paths — it decides set iteration order and is kept exactly as
    configured, trailing slash and all. `store_path` is *where that resolves to*,
    and it has to be absolute for two reasons:

    - it is relative to the **backend's** cwd, not the collector's, and the two
      differ: the collector runs from the repository while each arm runs from its
      own staging directory. `zmetadata_fingerprints()` walks this path, so a
      relative value made it fingerprint whatever happened to sit under the
      collector's cwd — silently, since a missing directory just yields nothing.
    - each arm reaches the store through its own symlink, so only the resolved
      path can show that both land on the same store.

    Resolution is non-strict: a path that does not exist still comes back absolute,
    and the emptiness is then reported by the fingerprint check rather than raising
    here.
    """
    path = Path(literal)
    if not path.is_absolute():
        path = cwd / path
    return str(path.resolve())


def resolve_store(label: str, cwd: Path, env: dict) -> dict:
    """Where the backend's Zarr store is, decided by *which backend this is*.

    The two are not symmetric and must not be treated as such:

    * **reference** — the unmodified `woa23_app.py`, which hard-codes the relative
      `data/` at line 63 and never consults the environment. Its store is `cwd/data`
      whatever `WOA23_ZARR_STORE` happens to say, so reading that variable here
      would record a path the process does not use.
    * **candidate** — takes `WOA23_ZARR_STORE` explicitly and fails at import
      without it (spec 001 section 4.2), so an unset variable is an error rather
      than a fallback.

    An earlier version branched on whether the variable was set, which for the
    reference would have silently reported a stray environment value as the store
    in use.
    """
    if label == "reference":
        # `store_path_literal` is the string the process interpolates into
        # f"{zarr_store_path}/{grid_path}/{subgroup}" — NOT the resolved directory.
        # It is what decides the iteration order of the `zarr_group_paths` set, and
        # the two arms differing here is what produced the C16 byte difference in
        # the 2026-08-08 run: the reference builds "data//1_degree/..." while the
        # candidate built an absolute path. The literal is pinned for the reference
        # by the runner's SHA-256 check on woa23_app.py, where line 63 sets it.
        return {"store_path": _canonical_store(cwd, "data/"),
                "store_source": "hardcoded_relative",
                "store_path_literal": "data/"}
    explicit = env.get("WOA23_ZARR_STORE")
    if not explicit:
        raise SystemExit(
            "candidate has no WOA23_ZARR_STORE; it cannot have started successfully "
            "(spec 001 section 4.2 makes it mandatory with no fallback). Refusing to "
            "guess a store path.")
    # The literal is the *unmodified* environment value, not a normalised Path:
    # normalising would erase exactly the difference that matters here, since
    # "data/" and "data" hash differently and therefore order differently.
    return {"store_path": _canonical_store(cwd, explicit), "store_source": "env",
            "store_path_literal": explicit}


def zmetadata_fingerprints(store: str | None) -> dict | None:
    """Identity of every group's consolidated metadata.

    **Consolidated metadata, not data.** `.zmetadata` records array shapes, chunk
    grids, compressors and attributes; it says nothing about the bytes inside the
    chunks, so two stores agreeing here can still hold different values. A match is
    evidence that two collections saw the same store *configuration*, not that they
    read identical data — the contract gate (spec 001 section 5) is what compares
    the data itself.

    Recorded per group: nanosecond mtime **and** a SHA-256 of the file. The mtime
    alone is weak — second resolution loses same-second edits, and any `touch`
    changes it without changing a byte — so the digest is the authoritative field
    and the timestamp is corroboration.

    The scan is recursive. An earlier version globbed `*/*/*/.zmetadata`, hard-coding
    the grid/period/parameter-group depth; a store reorganised to any other depth
    would have produced an empty, confidently wrong fingerprint.
    """
    if not store:
        return None
    root = Path(store)
    if not root.is_dir():
        return {"error": f"{store} is not a directory"}
    out: dict = {}
    for p in sorted(root.glob("**/.zmetadata")):
        key = str(p.relative_to(root))
        try:
            st = p.stat()
            out[key] = {
                "mtime_ns": st.st_mtime_ns,
                "size": st.st_size,
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            }
        except OSError as exc:
            out[key] = {"error": f"<unreadable: {exc.__class__.__name__}>"}
    if not out:
        return {"error": f"no .zmetadata found anywhere under {store}"}
    return out


def resolve_env_python(cwd: Path, argv: list[str], env: dict) -> dict:
    """The interpreter whose *packages* the process is using — not `/proc/<pid>/exe`.

    `/proc/<pid>/exe` follows the venv's symlink to the base binary, so it names a
    real executable whose site-packages belong to something else entirely. The first
    campaign recorded fastapi 0.115.2 / polars 1.10.0 / xarray 2024.9.0 for both arms
    from the pyenv base install, while both were actually running 0.115.12 / 1.27.1 /
    2025.3.1. The record described neither backend, and it looked plausible.

    So the environment is derived from what the process was told to use, in order of
    directness, and the method is recorded alongside the answer.
    """
    venv = env.get("VIRTUAL_ENV")
    if venv and (Path(venv) / "bin" / "python").exists():
        return {"env_python": str(Path(venv) / "bin" / "python"),
                "env_python_source": "VIRTUAL_ENV"}
    if argv:
        sibling = Path(argv[0]).parent / "python"
        if sibling.exists():
            return {"env_python": str(sibling), "env_python_source": "argv0_sibling"}
    return {"env_python": None, "env_python_source": "unresolved"}


def dependencies(env_python: str | None, lockfile: Path | None) -> dict:
    """Installed distributions of the environment actually in use.

    `importlib.metadata` rather than `pip freeze`: a uv-created venv has no pip, so
    the pip call would fail or — worse, as it did — silently answer for a different
    interpreter.
    """
    out: dict = {}
    if lockfile and lockfile.exists():
        out["lockfile"] = str(lockfile)
        out["lockfile_sha256"] = hashlib.sha256(lockfile.read_bytes()).hexdigest()
    if not env_python:
        out["distributions_error"] = "could not resolve the environment interpreter"
        return out
    # Kept as a plain block rather than a one-liner: the first attempt was a
    # semicolon-chained string whose quoting collapsed, and it returned an empty
    # list rather than an error — the same failure shape as the bug it replaced.
    code = textwrap.dedent(
        """
        import importlib.metadata as md, json, sys
        seen = set()
        for dist in md.distributions():
            name = dist.metadata["Name"]
            if name:
                seen.add(f"{name}=={dist.version}")
        print(json.dumps({"version": sys.version.split()[0],
                          "dists": sorted(seen)}))
        """
    )
    try:
        r = subprocess.run([env_python, "-c", code],
                           capture_output=True, text=True, timeout=60)
        if r.returncode != 0:
            out["distributions_error"] = r.stderr.strip()[:300]
            return out
        payload = json.loads(r.stdout)
        listed = "\n".join(payload["dists"])
        out["python_version"] = payload["version"]
        out["distributions_sha256"] = hashlib.sha256(listed.encode()).hexdigest()
        out["distributions"] = payload["dists"]
    except Exception as exc:
        out["distributions_error"] = repr(exc)
    return out


def compare_identity(before: dict, after: dict) -> list[str]:
    """Confirm the two records describe the same backend, not two different ones.

    Without this, `--against` would happily compare a candidate record with a
    reference one, or a record whose sources were swapped underneath it, and report
    "store unchanged" about two unrelated things.
    """
    out = []
    for field, note in (("label", "arm"),
                        ("port", "port"),
                        ("manifest_patterns", "manifest"),
                        ("cwd", "working directory"),
                        ("master_pid", "process"),
                        ("proc_starttime", "process start time"),
                        ("boot_id", "boot"),
                        ("launch_argv", "process argv"),
                        ("source_sha256", "source files")):
        if before.get(field) != after.get(field):
            out.append(f"{note} changed since the earlier collection "
                       f"({field}: {before.get(field)!r} -> {after.get(field)!r})")
    return out


def compare_store(before: dict, after: dict) -> list[str]:
    """Differences in store identity between two collections of the same backend.

    Run after the gate with `--against` the pre-run file, this covers the sampling
    window — which the two pre-run collections, taken back to back, cannot.
    """
    out = []
    if before.get("store_path") != after.get("store_path"):
        out.append(f"store_path changed: {before.get('store_path')!r} -> "
                   f"{after.get('store_path')!r}")
    b = before.get("zmetadata_fingerprints") or {}
    a = after.get("zmetadata_fingerprints") or {}
    for gone in sorted(set(b) - set(a)):
        out.append(f"group disappeared: {gone}")
    for new in sorted(set(a) - set(b)):
        out.append(f"group appeared: {new}")
    for g in sorted(set(a) & set(b)):
        if not isinstance(a[g], dict) or not isinstance(b[g], dict):
            continue
        if a[g].get("sha256") != b[g].get("sha256"):
            out.append(f"consolidated metadata changed: {g}")
        elif a[g].get("mtime_ns") != b[g].get("mtime_ns"):
            out.append(f"touched without a metadata change: {g}")
    return out


def build_meta(*, manifest: str, port: int, pid: int, listeners: list[int],
               port_verified: bool, cwd: Path, exe: str | None,
               argv: list[str], argv_str: str | None, env: dict, store: dict,
               lockfile: Path | None, expect_argv: list[str]) -> dict:
    env_py = resolve_env_python(cwd, argv, env)
    deps = dependencies(env_py["env_python"], lockfile)
    """Assemble the provenance record.

    Split out of `main()` so a test can build one without a live process and assert
    that its key set equals `provenance.REQUIRED_META_FIELDS`. Revision 16 claimed
    that reconciliation was programmatic when it had only been done once by hand in
    a shell; this is what makes the claim true, and what will fail if either list
    grows a field the other does not know about.
    """
    return {
        "kind": "backend_meta",
        "label": manifest,
        "manifest_patterns": list(MANIFESTS[manifest]),
        "collected_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "host": platform.node(),
        "kernel": platform.release(),
        "port": port,
        "master_pid": pid,
        "listener_pids": listeners,
        "port_verified": port_verified,
        "proc_starttime": proc_starttime(pid),
        "boot_id": boot_id(),
        "worker_pids": workers(pid),
        "cwd": str(cwd),
        # Both are recorded. `executable` is what the kernel reports; `env_python`
        # is what owns the packages. On a venv they differ, and the difference is
        # exactly what made the first campaign's dependency record wrong.
        "executable": exe,
        **env_py,
        "launch_argv": argv,
        "launch_command": argv_str,
        "expect_argv_contains": expect_argv,
        "env": env,
        "env_whitelist": list(ENV_WHITELIST),
        **store,
        "zmetadata_fingerprints": zmetadata_fingerprints(store["store_path"]),
        "source_sha256": hash_manifest(cwd, manifest),
        "dependencies": deps,
        # The version of the interpreter that owns the packages, not the collector's
        # and not /proc/<pid>/exe's. Two arms must agree on this before either is
        # asked a question about latency.
        "env_python_version": deps.get("python_version"),
        "collector_python": sys.version,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, required=True,
                    help="the backend's listening port. Required even when --pid is "
                         "given: the port is part of what identifies which arm this "
                         "record describes, and a record without it cannot be "
                         "checked against the run it belongs to.")
    ap.add_argument("--pid", type=int,
                    help="assert the master PID; it must equal the master derived "
                         "from the port's listener set, and a worker is refused")
    ap.add_argument("--manifest", required=True, choices=sorted(MANIFESTS),
                    help="which fixed source manifest to hash (see bench/manifests.py); "
                         "patterns are globs, so a new module is covered without "
                         "anyone updating a flag")
    ap.add_argument("--lockfile", type=Path, default=None)
    ap.add_argument("--expect-argv-contains", action="append", required=True,
                    help="REQUIRED. Substring that must appear in the process argv "
                         "(repeatable). A port can be held by anything, and an "
                         "optional identity check is one nobody runs — so provenance "
                         "is not collected at all without it. For the candidate: "
                         "--expect-argv-contains api.app:app; for the reference: "
                         "--expect-argv-contains woa23_app:app")
    ap.add_argument("--against", type=Path, default=None,
                    help="a backend_meta file collected earlier. The freshly read "
                         "store fingerprints are compared against it and any drift "
                         "is reported non-zero. Run this AFTER the gate to cover the "
                         "sampling window, which the pre-run collections cannot.")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    if args.against and args.out and args.against.resolve() == args.out.resolve():
        raise SystemExit(
            "--out and --against name the same file; the comparison would overwrite "
            "its own baseline and then find nothing changed.")

    listeners = pids_on_port(args.port)
    if not listeners:
        raise SystemExit(f"nothing is listening on port {args.port}")
    master = master_of(listeners)
    if master is None:
        # Fail closed. Guessing which of several roots is the server would produce a
        # record that looks authoritative and names the wrong process.
        raise SystemExit(
            f"cannot uniquely identify the master among the listeners on port "
            f"{args.port} ({listeners}) — no single process in that set is the "
            f"parent of the rest. Refusing to guess.")
    if args.pid and args.pid != master:
        # Being *in* the listener set is not enough: every gunicorn worker is, and
        # a worker recorded as master_pid poisons the start time, the worker list
        # and every post-run identity comparison.
        raise SystemExit(
            f"--pid {args.pid} is not the master on port {args.port}; {master} is "
            f"(listeners: {listeners}). A worker holds the inherited socket too, so "
            f"membership alone does not make it the master.")
    pid = master
    # Always true by this point: every path that could not confirm the master has
    # already raised. The field is still emitted and still checked by the gate,
    # which is defence against a record that was hand-edited or produced by an
    # older collector.
    port_verified = True

    argv, argv_str = cmdline(pid)
    if argv is None:
        raise SystemExit(f"cannot read argv of pid {pid}; are you the owning user?")
    missing = [s for s in args.expect_argv_contains if not any(s in a for a in argv)]
    if missing:
        raise SystemExit(
            f"pid {pid} on port {args.port} does not look like the intended backend: "
            f"argv is missing {missing}. argv = {argv}. Refusing to record provenance "
            f"for a process that may simply be occupying the port."
        )

    cwd_link = Path(f"/proc/{pid}/cwd")
    try:
        cwd = cwd_link.resolve()
    except Exception:
        raise SystemExit(f"cannot read cwd of pid {pid}; are you the owning user?")
    exe = None
    try:
        exe = str(Path(f"/proc/{pid}/exe").resolve())
    except Exception:
        pass

    env = whitelisted_env(pid)
    store = resolve_store(args.manifest, cwd, env)

    meta = build_meta(manifest=args.manifest, port=args.port, pid=pid,
                      listeners=listeners, port_verified=port_verified, cwd=cwd,
                      exe=exe, argv=argv, argv_str=argv_str, env=env, store=store,
                      lockfile=args.lockfile,
                      expect_argv=list(args.expect_argv_contains))

    seed = meta["env"].get("PYTHONHASHSEED")
    print(f"pid {pid} on port {args.port}  cwd {cwd}")
    print(f"  PYTHONHASHSEED = {seed}")
    if seed == "<unset — randomised>":
        print("  WARNING: output ordering from this backend is not reproducible "
              "(see spec 001 section 5.1)")
    print(f"  workers: {meta['worker_pids']}")
    for k, v in meta["source_sha256"].items():
        print(f"  {k}  {v}")

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(meta, indent=2))
        print(f"wrote {args.out}")
    if args.against:
        before, errs = load_meta(args.against, args.manifest)
        problems = [f"earlier record: {m}" for m in
                    (errs or validate_meta(before, args.manifest))]
        # The record just collected must be valid too. Comparing a fresh record with
        # a bad hash seed, an incomplete source manifest or an unreadable store
        # against a good baseline and reporting "store unchanged" would be a
        # reassurance about something that is not fit to be compared at all.
        problems += [f"current record: {m}" for m in validate_meta(meta, args.manifest)]
        if problems:
            print(f"\nnot comparable — a record must be valid before an unchanged "
                  f"verdict about it means anything:")
            for m in problems:
                print(f"  - {m}")
            return 2
        drift = compare_identity(before, meta) + compare_store(before, meta)
        if drift:
            print(f"\nSTORE DRIFT since {args.against}:")
            for d in drift:
                print(f"  - {d}")
            return 2
        print(f"\nstore unchanged since {args.against}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
