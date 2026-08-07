"""Tests for the provenance gate: schema validation and source manifests.

Everything here was verified by hand while it was written, which is exactly why it
needs to be a file: a manual check proves the code worked once, on one machine, and
leaves nothing behind that fails when someone loosens a rule later.

    uv run python -m bench.test_provenance
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path

from bench.manifests import MANIFESTS, expand
from bench.collect_backend_meta import (
    build_meta, compare_identity, compare_store, local_port_of, master_of,
    parse_ss_listeners, resolve_store)
from bench.provenance import (
    REQUIRED_META_FIELDS, load_meta, validate_meta, validate_store_agreement,
    verify_environment_match, verify_environment_record, verify_prior_contract,
    verify_prior_rung)

failures: list[str] = []

# Syntactically valid, semantically meaningless. Only ever used where the fixture's
# cwd does not exist, which fails validation before digests are reached.
STUB_DIGEST = "0" * 64


passed: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        passed.append(name)
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        failures.append(name)


def make_cwd(tmp: Path, label: str = "candidate") -> Path:
    """A working directory whose contents match the manifest exactly."""
    if label == "candidate":
        (tmp / "api").mkdir(parents=True, exist_ok=True)
        for name in ("__init__.py", "app.py", "config.py", "query.py"):
            (tmp / "api" / name).write_text("x")
    else:
        (tmp / "src").mkdir(parents=True, exist_ok=True)
        (tmp / "woa23_app.py").write_text("x")
        for name in ("__init__.py", "config.py", "dask_client_manager.py",
                     "woa23_utils.py"):
            (tmp / "src" / name).write_text("x")
    return tmp


def good_meta(label: str = "candidate", cwd: Path | None = None) -> dict:
    from bench.manifests import expand as _expand
    root = cwd or Path("/nonexistent")
    try:
        # Real digests of the fixture files. Until revision 21 these were "a" * 64
        # and validation accepted them, which is exactly the hole being closed.
        sources = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                   for p in _expand(label, root)}
    except SystemExit:
        # No real cwd was supplied, so the manifest cannot be expanded. These
        # fixtures are for tests that assert on some *other* field; validation stops
        # at "cwd is not reachable" long before any digest is examined, so this is a
        # deliberate placeholder and is never compared against a file.
        sources = {"api/app.py": STUB_DIGEST}
    return {
        "kind": "backend_meta",
        "label": label,
        "manifest_patterns": list(MANIFESTS[label]),
        "collected_at": "2026-08-06T10:00:00+0800",
        "host": "odb24",
        "kernel": "6.8.0-124-generic",
        "port": 8051,
        "master_pid": 12345,
        "port": 8051 if label == "candidate" else 8052,
        "listener_pids": [12345, 12346],
        "port_verified": True,
        "proc_starttime": 987654,
        "boot_id": "0f9c2d18-1a2b-4c3d-8e5f-6a7b8c9d0e1f",
        "expect_argv_contains": ["api.app:app" if label == "candidate"
                                 else "woa23_app:app"],
        "worker_pids": [12346],
        "cwd": str(root),
        "executable": "/home/odbadmin/woa23-dev2026/dev2026/.venv/bin/python",
        "launch_argv": ["gunicorn", "api.app:app", "-w", "1"],
        "launch_command": "gunicorn api.app:app -w 1",
        "env": {"PYTHONHASHSEED": "0", "WOA23_ZARR_STORE": "/home/odbadmin/python/woa23/data"},
        "env_whitelist": ["PYTHONHASHSEED", "WOA23_ZARR_STORE"],
        "collector_python": "3.11.9 (main)",
        "source_sha256": sources,
        "env_python": "/home/odbadmin/woa23-dev2026/dev2026/.venv/bin/python",
        "env_python_source": "VIRTUAL_ENV",
        "env_python_version": "3.11.4",
        "store_path": "/home/odbadmin/python/woa23/data",
        "store_source": "hardcoded_relative" if label == "reference" else "env",
        "zmetadata_fingerprints": {
            "1_degree/annual/TS/.zmetadata": {
                "mtime_ns": 1754400000123456789, "size": 4096, "sha256": "c" * 64},
        },
        "dependencies": {"lockfile_sha256": "b" * 64,
                         "distributions_sha256": "d" * 64,
                         "python_version": "3.11.4"},
    }


def has(problems: list[str], needle: str) -> bool:
    return any(needle in p for p in problems)


def test_good_meta_passes() -> None:
    with tempfile.TemporaryDirectory() as d:
        m = good_meta("candidate", make_cwd(Path(d)))
        problems = validate_meta(m, "candidate")
        check("a complete record passes", problems == [], str(problems))


def test_source_set_must_match_manifest() -> None:
    """Well-formed digests naming the wrong files must not pass."""
    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d))

        m = good_meta("candidate", cwd)
        m["source_sha256"] = {"api/invented.py": "c" * 64}
        p = validate_meta(m, "candidate")
        check("a fabricated filename is rejected", has(p, "fabricated or stale"))
        check("the omitted real files are reported", has(p, "omits"))

        m = good_meta("candidate", cwd)
        m["source_sha256"].pop("api/query.py")
        check("a subset of the manifest is rejected",
              has(validate_meta(m, "candidate"), "omits 'api/query.py'"))

        m = good_meta("candidate", cwd)
        m["cwd"] = "/definitely/not/here"
        check("an unreachable cwd is reported, not skipped",
              has(validate_meta(m, "candidate"), "not reachable"))


def test_source_digests_are_recomputed() -> None:
    """A well-formed digest of the wrong content used to pass every check."""
    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d))

        m = good_meta("candidate", cwd)
        check("digests matching the files on disk pass",
              validate_meta(m, "candidate") == [], str(validate_meta(m, "candidate")))

        m = good_meta("candidate", cwd)
        m["source_sha256"]["api/app.py"] = "a" * 64
        check("a well-formed digest of the wrong content is rejected",
              has(validate_meta(m, "candidate"), "does not match its recorded digest"))

        # The realistic case: nobody tampered, someone edited the file after the
        # sidecar ran. Same symptom, same verdict.
        m = good_meta("candidate", cwd)
        (cwd / "api" / "query.py").write_text("edited after collection")
        check("a source edited after collection is rejected",
              has(validate_meta(m, "candidate"), "api/query.py"))


def test_kernel_and_store_fields_required() -> None:
    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d))
        for field in ("kernel", "store_path", "store_source",
                      "zmetadata_fingerprints"):
            m = good_meta("candidate", cwd); del m[field]
            check(f"{field} is required",
                  has(validate_meta(m, "candidate"), repr(field)))

        m = good_meta("candidate", cwd)
        m["store_source"] = "guessed"
        check("an unknown store_source is rejected",
              has(validate_meta(m, "candidate"), "store_source"))
        m = good_meta("candidate", cwd)
        m["store_source"] = "hardcoded_relative"
        check("the candidate may not use the reference's resolution",
              has(validate_meta(m, "candidate"), "expected 'env'"))
        r = good_meta("reference", make_cwd(Path(d), "reference"))
        r["store_source"] = "env"
        check("the reference may not use the candidate's resolution",
              has(validate_meta(r, "reference"), "expected 'hardcoded_relative'"))
        m = good_meta("candidate", cwd)
        m["zmetadata_fingerprints"] = {"error": "no .zmetadata found anywhere under /x"}
        check("an empty store scan is reported",
              has(validate_meta(m, "candidate"), "store scan reports"))
        m = good_meta("candidate", cwd)
        m["zmetadata_fingerprints"] = {"g": {"error": "<unreadable: OSError>"}}
        check("an unreadable group fingerprint is rejected",
              has(validate_meta(m, "candidate"), "zmetadata for g"))
        m = good_meta("candidate", cwd)
        m["zmetadata_fingerprints"] = {"g": {"sha256": "short", "mtime_ns": 1}}
        check("a malformed store digest is rejected",
              has(validate_meta(m, "candidate"), "64 hex"))
        m = good_meta("candidate", cwd)
        m["zmetadata_fingerprints"] = {"g": {"sha256": "c" * 64, "mtime_ns": "1"}}
        check("a non-integer mtime_ns is rejected",
              has(validate_meta(m, "candidate"), "mtime_ns"))


def test_resolve_store_is_label_driven() -> None:
    """The reference ignores the environment; reading it would record a lie."""
    r = resolve_store("reference", Path("/srv/app"), {"WOA23_ZARR_STORE": "/wrong"})
    check("reference ignores a stray WOA23_ZARR_STORE",
          r == {"store_path": "/srv/app/data", "store_source": "hardcoded_relative"},
          str(r))
    c = resolve_store("candidate", Path("/x"), {"WOA23_ZARR_STORE": "/data"})
    check("candidate uses its explicit store",
          c == {"store_path": "/data", "store_source": "env"}, str(c))
    try:
        resolve_store("candidate", Path("/x"), {})
        check("candidate without the variable is refused", False, "no SystemExit")
    except SystemExit:
        check("candidate without the variable is refused", True)


def test_post_run_drift() -> None:
    """`--against` covers the sampling window the pre-run collections cannot."""
    base = {"store_path": "/d",
            "zmetadata_fingerprints": {"g": {"sha256": "a" * 64, "mtime_ns": 1}}}
    check("no drift when nothing moved", compare_store(base, base) == [])
    changed = {"store_path": "/d",
               "zmetadata_fingerprints": {"g": {"sha256": "b" * 64, "mtime_ns": 2}}}
    check("metadata change is reported",
          has(compare_store(base, changed), "consolidated metadata changed"))
    touched = {"store_path": "/d",
               "zmetadata_fingerprints": {"g": {"sha256": "a" * 64, "mtime_ns": 9}}}
    check("a touch without a content change is reported separately",
          has(compare_store(base, touched), "touched without a metadata change"))
    moved = {"store_path": "/elsewhere", "zmetadata_fingerprints": base["zmetadata_fingerprints"]}
    check("a moved store is reported", has(compare_store(base, moved), "store_path changed"))
    gone = {"store_path": "/d", "zmetadata_fingerprints": {}}
    check("a vanished group is reported", has(compare_store(base, gone), "disappeared"))


SS_FIXTURE = """LISTEN 0 200 0.0.0.0:5433 0.0.0.0:* users:(("postgres",pid=100,fd=7))
LISTEN 0 200 [::]:5433 [::]:* users:(("postgres",pid=101,fd=8))
LISTEN 0 2048 127.0.0.1:8050 0.0.0.0:* users:(("gunicorn",pid=4366,fd=6),("gunicorn",pid=4334,fd=6),("gunicorn",pid=3960,fd=6))
LISTEN 0 128 127.0.0.1:8040 0.0.0.0:* users:(("gunicorn",pid=7000,fd=5))"""

# Every row here defeated an earlier port test, in the parser or in its shell twin.
SS_PORT_TRAPS = """LISTEN 0 2048 127.0.0.1:18050 0.0.0.0:* users:(("other",pid=111,fd=3))
LISTEN 0 2048 [fe80::8050]:9000 [::]:* users:(("v6svc",pid=222,fd=3))
LISTEN 0 2048 127.0.0.1:8050 0.0.0.0:* users:(("gunicorn",pid=3960,fd=5))
LISTEN 0 2048 [::]:8050 [::]:* users:(("gunicorn",pid=4366,fd=5))
LISTEN 0 2048 [fe80::1%eth0]:50 [::]:* users:(("zoned",pid=777,fd=3))
LISTEN 0 2048 0.0.0.0:* 0.0.0.0:* users:(("noport",pid=888,fd=3))
ESTAB 0 0 127.0.0.1:54321 127.0.0.1:8050 users:(("curl",pid=9999,fd=3))"""

SS_ESTABLISHED_ONLY = """ESTAB 0 0 127.0.0.1:54321 127.0.0.1:8050 users:(("curl",pid=9999,fd=3))"""

SS_WITH_HEADER = """State Recv-Q Send-Q Local Address:Port Peer Address:Port Process
LISTEN 0 2048 127.0.0.1:8050 0.0.0.0:* users:(("gunicorn",pid=3960,fd=5))"""


def test_ss_parsing_accumulates_rows() -> None:
    """A server appears once per address family; stopping at the first row loses one."""
    check("both address families are collected",
          parse_ss_listeners(SS_FIXTURE, 5433) == [100, 101],
          str(parse_ss_listeners(SS_FIXTURE, 5433)))
    check("every PID sharing one socket is collected",
          parse_ss_listeners(SS_FIXTURE, 8050) == [3960, 4334, 4366])
    check("an unused port yields nothing", parse_ss_listeners(SS_FIXTURE, 9999) == [])
    check("a port that is a suffix of another is not matched",
          parse_ss_listeners(SS_FIXTURE, 50) == [])


def test_ss_port_matching_is_exact() -> None:
    """Substring matching on `ss` output finds ports that are not there.

    Each row below broke an earlier form of this parser or of its shell equivalent.
    """
    check("a longer port containing the wanted one is not matched",
          parse_ss_listeners(SS_PORT_TRAPS, 8050) == [3960, 4366],
          str(parse_ss_listeners(SS_PORT_TRAPS, 8050)))
    check("18050's own listener is still found",
          parse_ss_listeners(SS_PORT_TRAPS, 18050) == [111])

    # `[fe80::8050]:9000` is a different service on a different port whose *address*
    # happens to contain the digits. The shell's `$0 ~ ":8050"` matched it.
    check("the port's digits appearing in an IPv6 address are not a match",
          222 not in parse_ss_listeners(SS_PORT_TRAPS, 8050))
    check("that row is found under its real port",
          parse_ss_listeners(SS_PORT_TRAPS, 9000) == [222])

    # `[fe80::1%eth0]:50` ends in `:50` after a word boundary, which a `\b`-anchored
    # grep for port 50 matched even when nothing listened on 50.
    check("an IPv6 address ending in the port's digits is not a match",
          parse_ss_listeners(SS_PORT_TRAPS, 50) == [777],
          str(parse_ss_listeners(SS_PORT_TRAPS, 50)))

    # A client *connected to* 8050 has it in the peer column. Scanning the first
    # five fields reported the client's PID as holding the port.
    check("a peer address is not a listening socket",
          9999 not in parse_ss_listeners(SS_PORT_TRAPS, 8050))
    check("only LISTEN rows are considered",
          parse_ss_listeners(SS_ESTABLISHED_ONLY, 8050) == [],
          str(parse_ss_listeners(SS_ESTABLISHED_ONLY, 8050)))

    check("a header line is not parsed as a row",
          parse_ss_listeners(SS_WITH_HEADER, 8050) == [3960])
    check("an address column with no port is skipped",
          parse_ss_listeners(SS_PORT_TRAPS, 0) == [])

    check("both address families of one service are still collected",
          parse_ss_listeners(SS_PORT_TRAPS, 8050) == sorted([3960, 4366]))
    for field, want in (("127.0.0.1:8050", 8050), ("[::]:8050", 8050),
                        ("[fe80::1%eth0]:50", 50), ("0.0.0.0:*", None),
                        ("Address:Port", None), ("nocolon", None)):
        check(f"local_port_of({field!r}) == {want}", local_port_of(field) == want,
              str(local_port_of(field)))


def test_master_selection() -> None:
    """gunicorn's workers hold the inherited socket, so membership is not mastery."""
    tree = {3960: 3916, 4334: 3960, 4366: 3960}      # master + two workers
    check("the master is the process whose parent is outside the set",
          master_of([3960, 4334, 4366], ppid=tree.get) == 3960)
    check("a worker is not selected",
          master_of([4334, 4366], ppid=tree.get) != 4366)

    two_servers = {100: 1, 200: 1}
    check("two unrelated roots are ambiguous, not a guess",
          master_of([100, 200], ppid=two_servers.get) is None)
    orphaned = {}
    check("a set with no resolvable parent is ambiguous",
          master_of([1, 2, 3], ppid=orphaned.get) is None)
    check("a single listener is its own master",
          master_of([7000], ppid={7000: 1}.get) == 7000)

    # The Rev18 defect: a None parent is trivially "not in the set", so an
    # unreadable /proc entry looked like a root instead of like knowing nothing.
    check("a single PID with an unreadable parent is NOT the master",
          master_of([7000], ppid=lambda _: None) is None,
          str(master_of([7000], ppid=lambda _: None)))
    check("one unreadable parent poisons the whole set",
          master_of([3960, 4334, 4366], ppid={3960: 3916, 4334: 3960}.get) is None)
    check("an empty listener set has no master", master_of([], ppid=lambda _: 1) is None)


def test_identity_comparison() -> None:
    """`--against` must refuse to compare two different backends."""
    b = {"label": "candidate", "port": 8051, "manifest_patterns": ["api/**/*.py"],
         "cwd": "/x", "master_pid": 1, "proc_starttime": 555, "boot_id": "abc",
         "launch_argv": ["gunicorn"], "source_sha256": {"a.py": "1"}}
    check("an unchanged record compares clean", compare_identity(b, b) == [])
    for field, needle in (("label", "arm changed"),
                          ("port", "port changed"),
                          ("manifest_patterns", "manifest changed"),
                          ("cwd", "working directory changed"),
                          ("master_pid", "process changed"),
                          ("proc_starttime", "process start time changed"),
                          ("boot_id", "boot changed"),
                          ("launch_argv", "process argv changed"),
                          ("source_sha256", "source files changed")):
        after = dict(b)
        after[field] = ("different" if field not in
                        ("master_pid", "proc_starttime", "port") else 2)
        check(f"a changed {field} is caught", has(compare_identity(b, after), needle))


def test_store_agreement() -> None:
    """Two arms reading different data measure the stores, not the change."""
    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d))
        a = good_meta("candidate", cwd)
        b = good_meta("reference", make_cwd(Path(d), "reference"))
        b["store_path"] = a["store_path"]
        b["zmetadata_fingerprints"] = dict(a["zmetadata_fingerprints"])
        check("matching stores agree", validate_store_agreement(a, b) == [],
              str(validate_store_agreement(a, b)))

        b2 = dict(b, store_path="/somewhere/else")
        check("different store paths are rejected",
              has(validate_store_agreement(a, b2), "store mismatch"))

        key = "1_degree/annual/TS/.zmetadata"
        b3 = dict(b, zmetadata_fingerprints={key: {"mtime_ns": 1, "size": 1,
                                                   "sha256": "d" * 64}})
        check("differing consolidated metadata is rejected",
              has(validate_store_agreement(a, b3), "different consolidated metadata"))

        b4 = dict(b, zmetadata_fingerprints={
            key: {**a["zmetadata_fingerprints"][key], "mtime_ns": 999}})
        check("same metadata, different mtime is reported as a touch between "
              "collections",
              has(validate_store_agreement(a, b4), "touched between provenance"))

        b5 = dict(b, zmetadata_fingerprints={**b["zmetadata_fingerprints"],
                                             "extra/.zmetadata": {"sha256": "e" * 64,
                                                                  "mtime_ns": 1}})
        check("a group set mismatch is rejected",
              has(validate_store_agreement(a, b5), "seen by the reference"))

        check("absence is left to validate_meta",
              validate_store_agreement(None, b) == [])


def test_missing_metadata() -> None:
    p = validate_meta(None, "candidate")
    check("missing metadata is rejected", has(p, "no backend metadata"))


def test_stub_metadata_is_rejected() -> None:
    """The exact stub from the Rev9 review, which the old validator let through."""
    stub = {"source_sha256": {"fake.py": "0" * 64}, "env": {"PYTHONHASHSEED": "0"}}
    p = validate_meta(stub, "candidate")
    check("a two-field stub is rejected", len(p) >= 10, f"only {len(p)} problems")
    for field in ("kind", "label", "manifest_patterns", "cwd", "master_pid",
                  "launch_argv", "executable", "dependencies", "collected_at"):
        check(f"stub flagged for missing {field}", has(p, repr(field)))


def test_identity_fields_are_required() -> None:
    """The gap found in Rev14 review: a record without these passed validation.

    `expect_argv_contains` missing meant `post_run_runtime_check()` had nothing to
    assert and silently skipped the argv identity check — a validator that passes a
    record which disables a downstream check is worse than no validator.
    """
    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d))
        for field in ("expect_argv_contains", "port", "proc_starttime", "boot_id",
                      "env_whitelist", "collector_python", "listener_pids",
                      "port_verified"):
            m = good_meta("candidate", cwd); del m[field]
            check(f"{field} is required",
                  has(validate_meta(m, "candidate"), repr(field)))

        m = good_meta("candidate", cwd); m["expect_argv_contains"] = []
        check("an empty expect_argv_contains is rejected",
              has(validate_meta(m, "candidate"), "expect_argv_contains"))
        m = good_meta("candidate", cwd); m["expect_argv_contains"] = [""]
        check("a blank expected-argv entry is rejected",
              has(validate_meta(m, "candidate"), "non-empty strings"))
        m = good_meta("candidate", cwd); m["port"] = 0
        check("port 0 is rejected", has(validate_meta(m, "candidate"), "outside"))
        m = good_meta("candidate", cwd); m["port"] = 70000
        check("an out-of-range port is rejected",
              has(validate_meta(m, "candidate"), "outside"))
        m = good_meta("candidate", cwd); m["proc_starttime"] = -1
        check("a negative start time is rejected",
              has(validate_meta(m, "candidate"), "proc_starttime"))
        m = good_meta("candidate", cwd); m["port_verified"] = False
        check("an unverified port fails the gate",
              has(validate_meta(m, "candidate"), "port_verified is false"))
        m = good_meta("candidate", cwd); m["port_verified"] = True
        check("a verified port passes", validate_meta(m, "candidate") == [])


def test_listener_pids_contents() -> None:
    """Rev18 checked only that the list was non-empty."""
    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d))
        m = good_meta("candidate", cwd); m["listener_pids"] = ["not-a-pid"]
        check("non-integer listener PIDs are rejected",
              has(validate_meta(m, "candidate"), "positive integers"))
        m = good_meta("candidate", cwd); m["listener_pids"] = [0, 12345]
        check("a zero PID is rejected",
              has(validate_meta(m, "candidate"), "positive integers"))
        m = good_meta("candidate", cwd); m["listener_pids"] = [True, 12345]
        check("a boolean masquerading as a PID is rejected",
              has(validate_meta(m, "candidate"), "positive integers"))
        m = good_meta("candidate", cwd); m["listener_pids"] = [12345, 12345]
        check("duplicate listener PIDs are rejected",
              has(validate_meta(m, "candidate"), "duplicates"))
        m = good_meta("candidate", cwd); m["listener_pids"] = [999, 998]
        check("a master outside its own listener set is rejected",
              has(validate_meta(m, "candidate"), "not among"))


def test_post_run_source_drift() -> None:
    """Sources are re-hashed before sampling, and again after it.

    Revision 21 checked them only before, which left a file edited mid-run
    describing code that had stopped serving requests partway through.
    """
    import bench.paired_bench as pb

    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d))
        meta = good_meta("candidate", cwd)
        meta["port"] = None            # skip the listener branch; not under test
        saved = (pb.cmdline, pb.boot_id, pb.proc_starttime, pb.zmetadata_fingerprints)
        try:
            pb.cmdline = lambda pid: (["gunicorn"], "gunicorn")
            pb.boot_id = lambda: None
            pb.proc_starttime = lambda pid: None
            pb.zmetadata_fingerprints = lambda store: None
            meta["launch_argv"] = ["gunicorn"]
            meta["expect_argv_contains"] = []
            meta["boot_id"] = None
            meta["proc_starttime"] = None

            out = pb.post_run_runtime_check(meta, "candidate")
            check("untouched sources raise no drift",
                  not any("source changed" in o for o in out), str(out))

            (cwd / "api" / "app.py").write_text("edited while the benchmark ran")
            out = pb.post_run_runtime_check(meta, "candidate")
            check("a source edited during sampling is caught",
                  has(out, "source changed during sampling"), str(out))
        finally:
            (pb.cmdline, pb.boot_id, pb.proc_starttime,
             pb.zmetadata_fingerprints) = saved


def test_post_run_listener_change() -> None:
    """A port changing hands is invisible to PID, start-time and argv checks alike."""
    import bench.paired_bench as pb

    meta = {"master_pid": 3960, "port": 8050, "store_path": None,
            "boot_id": None, "proc_starttime": None,
            "launch_argv": ["gunicorn"], "expect_argv_contains": ["woa23_app:app"]}
    tree = {3960: 3916, 4334: 3960, 4366: 3960, 7000: 1}
    saved = (pb.pids_on_port, pb.master_of, pb.cmdline, pb.boot_id,
             pb.proc_starttime, pb.zmetadata_fingerprints)
    try:
        pb.cmdline = lambda pid: (["gunicorn", "woa23_app:app"], "gunicorn woa23_app:app")
        pb.boot_id = lambda: None
        pb.proc_starttime = lambda pid: None
        pb.zmetadata_fingerprints = lambda store: None
        pb.master_of = lambda pids: master_of(pids, ppid=tree.get)

        pb.pids_on_port = lambda port: [3960, 4334, 4366]
        out = pb.post_run_runtime_check(meta, "reference")
        check("an unchanged listener set raises no port problem",
              not any("port" in o and "master" in o for o in out), str(out))

        pb.pids_on_port = lambda port: []
        check("a vanished listener is reported",
              has(pb.post_run_runtime_check(meta, "reference"), "nothing is listening"))

        pb.pids_on_port = lambda port: [7000]
        check("a port taken over by another process is reported",
              has(pb.post_run_runtime_check(meta, "reference"), "is now held by"))

        # Original master still present, but now sharing with an unrelated root:
        # master_of goes ambiguous, which must fail rather than pass.
        pb.pids_on_port = lambda port: [3960, 7000]
        check("an ambiguous listener set fails rather than passing",
              has(pb.post_run_runtime_check(meta, "reference"), "no longer mastered"))
    finally:
        (pb.pids_on_port, pb.master_of, pb.cmdline, pb.boot_id,
         pb.proc_starttime, pb.zmetadata_fingerprints) = saved


def test_against_validates_both_records() -> None:
    """A fresh record must be valid too, or "unchanged" reassures about nothing.

    `--against` used to validate only the baseline, so a current record with an
    unpinned hash seed or a truncated source manifest could still be reported as
    showing no drift.
    """
    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d))
        good = good_meta("candidate", cwd)
        check("a valid current record has nothing to report",
              validate_meta(good, "candidate") == [])

        for field, value, needle in (
                ("env", {"PYTHONHASHSEED": "1"}, "PYTHONHASHSEED"),
                ("source_sha256", {"api/app.py": "a" * 64}, "omits"),
                ("zmetadata_fingerprints", {"error": "gone"}, "store scan reports"),
                ("port_verified", False, "port_verified is false")):
            bad = dict(good)
            bad[field] = value
            problems = validate_meta(bad, "candidate")
            check(f"an invalid current record is caught via {field}",
                  has(problems, needle), str(problems))


def test_verify_prior_rung() -> None:
    """Escalation inherits what the earlier rung established, so it must still hold."""
    with tempfile.TemporaryDirectory() as d:
        cand = good_meta("candidate", make_cwd(Path(d)))
        ref = good_meta("reference", make_cwd(Path(d), "reference"))
        _prior_rung_body(cand, ref)


def _prior_rung_body(cand, ref) -> None:
    from bench.queries import select
    CASES = {q.id for q in select(None, include_heavy=True)}
    ok = {"kind": "paired_latency", "gate_variant": "5.2B",
          "warm_samples_per_arm": 21, "metadata_complete": True,
          "post_run_drift": [], "gate": "INCONCLUSIVE",
          "candidate_url": "http://127.0.0.1:8051",
          "reference_url": "https://127.0.0.1:8050",
          "results": [{"id": i, "regression_verdict": "INCONCLUSIVE"} for i in CASES],
          "candidate_meta": cand, "reference_meta": ref}

    check("a sound prior rung is accepted",
          verify_prior_rung(ok, cand, ref, 21, "5.2B") == [],
          str(verify_prior_rung(ok, cand, ref, 21, "5.2B")))
    check("a missing prior rung is rejected",
          has(verify_prior_rung(None, cand, ref, 21, "5.2B"), "no prior rung"))

    for field, value, needle in (
            ("kind", "something", "kind"),
            ("gate_variant", "5.2A", "not comparable"),
            ("warm_samples_per_arm", 60, "expected 21"),
            ("metadata_complete", False, "must be exactly True"),
            ("post_run_drift", ["moved"], "recorded runtime drift"),
            ("gate", "INVALID_METADATA", "gate was"),
            ("results", [], "no case results")):
        check(f"a prior rung with {field}={value!r} is rejected",
              has(verify_prior_rung(dict(ok, **{field: value}), cand, ref, 21, "5.2B"),
                  needle))

    changed_code = dict(cand, source_sha256={"api/app.py": "f" * 64})
    check("code changing between rungs stops the escalation",
          has(verify_prior_rung(ok, changed_code, ref, 21, "5.2B"),
              "the code under test changed"))
    changed_data = dict(cand, zmetadata_fingerprints={"g": {"sha256": "9" * 64}})
    check("data changing between rungs stops the escalation",
          has(verify_prior_rung(ok, changed_data, ref, 21, "5.2B"),
              "the data changed"))
    moved_store = dict(cand, store_path="/elsewhere")
    check("a moved store stops the escalation",
          has(verify_prior_rung(ok, moved_store, ref, 21, "5.2B"), "store path differs"))
    check("absent current metadata is reported, not ignored",
          has(verify_prior_rung(ok, None, ref, 21, "5.2B"), "cannot compare provenance"))

    # --- Rev32 review: a truncated file must not pass by omission ---
    truncated = {"kind": "paired_latency", "results": [{"id": "point_profile"}]}
    p = verify_prior_rung(truncated, cand, ref, 21, "5.2B")
    check("a truncated prior result is rejected for the fields it lacks",
          len(p) >= 5 and all("has no" in x for x in p), str(p[:2]))
    for field in ("gate", "gate_variant", "warm_samples_per_arm", "candidate_url",
                  "reference_url", "candidate_meta", "reference_meta"):
        check(f"a prior result missing {field!r} is rejected",
              has(verify_prior_rung({k: v for k, v in ok.items() if k != field},
                                    cand, ref, 21, "5.2B"), repr(field)))

    check("a prior rung whose candidate URL differs is rejected",
          has(verify_prior_rung(ok, cand, ref, 21, "5.2B",
                                expect_candidate_url="http://127.0.0.1:9999"),
              "a different backend"))
    check("a prior rung whose reference URL differs is rejected",
          has(verify_prior_rung(ok, cand, ref, 21, "5.2B",
                                expect_reference_url="https://elsewhere"),
              "a different backend"))

    from bench.queries import select
    cases = {q.id for q in select(None, include_heavy=True)}
    short = dict(ok, results=ok["results"][:3])
    check("a prior rung covering fewer cases is rejected",
          has(verify_prior_rung(short, cand, ref, 21, "5.2B", expect_cases=cases),
              "is missing cases"))
    dupes = dict(ok, results=ok["results"] + [ok["results"][0]])
    check("duplicate case ids are rejected",
          has(verify_prior_rung(dupes, cand, ref, 21, "5.2B", expect_cases=cases),
              "duplicate case ids"))
    check("a complete case set is accepted",
          verify_prior_rung(ok, cand, ref, 21, "5.2B", expect_cases=cases,
                            expect_candidate_url="http://127.0.0.1:8051",
                            expect_reference_url="https://127.0.0.1:8050") == [])

    check("an established FAIL cannot be escalated past",
          has(verify_prior_rung(dict(ok, gate="FAIL"), cand, ref, 21, "5.2B"),
              "cannot overturn an established failure"))
    regressed = dict(ok, results=[dict(ok["results"][0], regression_verdict="REGRESSION")]
                     + ok["results"][1:])
    check("a prior rung with an established regression is rejected",
          has(verify_prior_rung(regressed, cand, ref, 21, "5.2B"),
              "established a regression"))

    # --- Rev33 review: row-level schema, which only rung 60 depends on ---
    no_verdict = dict(ok, results=[{"id": i} for i in cases])
    check("rows without a regression_verdict are rejected",
          has(verify_prior_rung(no_verdict, cand, ref, 21, "5.2B",
                                expect_cases=cases), "regression_verdict"),
          "without this, escalation finds no INCONCLUSIVE case and reports "
          "'no escalation needed' from a malformed file")
    bad_verdict = dict(ok, results=[dict(r, regression_verdict="MAYBE")
                                    for r in ok["results"]])
    check("an unknown verdict value is rejected",
          has(verify_prior_rung(bad_verdict, cand, ref, 21, "5.2B"), "not one of"))
    no_id = dict(ok, results=[{"regression_verdict": "INCONCLUSIVE"}])
    check("a row without a usable id is rejected",
          has(verify_prior_rung(no_id, cand, ref, 21, "5.2B"), "no usable 'id'"))
    not_obj = dict(ok, results=["point_profile"])
    check("a non-object result row is rejected",
          has(verify_prior_rung(not_obj, cand, ref, 21, "5.2B"), "expected an object"))
    check("results that are not a list are rejected",
          has(verify_prior_rung(dict(ok, results={}), cand, ref, 21, "5.2B"),
              "results is dict"))
    check("post_run_drift that is not a list is rejected",
          has(verify_prior_rung(dict(ok, post_run_drift="none"), cand, ref, 21, "5.2B"),
              "post_run_drift is str"))
    for truthy in (1, "yes", [1]):
        check(f"metadata_complete={truthy!r} is rejected — it must be exactly True",
              has(verify_prior_rung(dict(ok, metadata_complete=truthy), cand, ref,
                                    21, "5.2B"), "must be exactly True"))

    broken_meta = dict(ok, candidate_meta={"label": "candidate"})
    check("an invalid embedded record is rejected on its own terms",
          has(verify_prior_rung(broken_meta, cand, ref, 21, "5.2B"), "prior candidate:"))


def test_verify_prior_contract() -> None:
    """Carrying a contract result forward means not paying production again for it."""
    with tempfile.TemporaryDirectory() as d:
        cand = good_meta("candidate", make_cwd(Path(d)))
        ref = good_meta("reference", make_cwd(Path(d), "reference"))
        ref["env"] = {"PYTHONHASHSEED": "<unset — randomised>",
                      "WOA23_ZARR_STORE": "/d"}
        from bench.contract_cases import all_cases
        real_ids = {c.id for c in all_cases()}
        defect = ["unparseable body (8597 vs 8597 bytes)"]
        # The fixture is the real case list, because the verifier derives it when the
        # caller does not supply one — a three-row stand-in would now fail for the
        # right reason and hide what each assertion is actually testing.
        defect_rows = [{"id": i, "verdict": "MATCH"}
                       for i in sorted(real_ids - {"C20a", "C20b"})] + \
                      [{"id": x, "verdict": "DIFFER", "notes": defect}
                       for x in ("C20a", "C20b")]
        ok = {"kind": "contract_diff", "gate": "FAIL", "variant": "5.2B",
              "candidate_meta": cand, "reference_meta": ref,
              "results": defect_rows}
        V = lambda p=ok, c=cand, r=ref, **kw: verify_prior_contract(
            p, c, "5.2B", ref_meta=r, **kw)
        check("a result failing only on the known harness defect is reusable",
              V() == [], str(V()))
        check("a missing result is rejected",
              has(verify_prior_contract(None, cand, "5.2B"), "no prior contract"))

        # --- Rev36 review: the reference arm must be verified too ---
        check("an absent reference record blocks reuse",
              has(V(r=None), "cannot compare reference provenance"))
        check("a missing reference_meta field blocks reuse",
              has(V(p={k: v for k, v in ok.items() if k != "reference_meta"}),
                  "'reference_meta'"))
        for field, needle in (("source_sha256", "the code under test changed"),
                              ("zmetadata_fingerprints", "the data changed"),
                              ("dependencies", "the installed dependencies changed")):
            now = dict(ref); now[field] = {"changed": "yes"}
            check(f"a changed reference {field} blocks reuse",
                  has(V(r=now), f"reference: {needle}"))

        # A permitted case id is not a permit for any failure on that case.
        other = dict(ok, results=defect_rows[:-1] + [
            dict(defect_rows[-1], notes=["row 3 key temperature: 1.0 vs 2.0"])])
        check("a permitted case failing for a different reason is rejected",
              has(V(p=other), "does not make an unrelated failure permitted"))
        check("fabricated case ids are rejected even at the right count",
              has(V(p=dict(ok, results=[{"id": f"FAKE-{i}", "verdict": "MATCH"}
                                        for i in range(len(real_ids))]),
                    expect_case_ids=real_ids), "not in the case list"),
              "64 invented ids satisfied a count check and covered no real case")
        check("a missing case is named",
              has(V(p=dict(ok, results=[{"id": i, "verdict": "MATCH"}
                                        for i in sorted(real_ids)[:-1]]),
                    expect_case_ids=real_ids), "is missing 1 case"))
        # All-MATCH rows must come with gate PASS; the fixture's gate is FAIL, and
        # the consistency rule added in rev 39 correctly rejects the mismatch. The
        # accepting cases are covered below under the gate-state checks.
        check("the real case list with a matching gate is accepted",
              V(p=dict(ok, gate="PASS",
                       results=[{"id": i, "verdict": "MATCH"} for i in sorted(real_ids)]),
                expect_case_ids=real_ids) == [])
        dupes = dict(ok, results=ok["results"] + [ok["results"][0]])
        check("duplicate contract case ids are rejected",
              has(V(p=dupes), "duplicate case ids"))
        check("an empty case list is rejected",
              has(V(p=dict(ok, results=[])), "no case results"))

        real = dict(ok, results=ok["results"] + [{"id": "C4", "verdict": "DIFFER"}])
        check("a genuine disagreement cannot be carried forward",
              has(verify_prior_contract(real, cand, "5.2B"), "not the known harness"))
        check("the offending case is named",
              has(verify_prior_contract(real, cand, "5.2B"), "'C4'"))

        # --- Rev38 review: the gate state itself was never checked ---
        allm = [{"id": i, "verdict": "MATCH"} for i in sorted(real_ids)]
        for bad_gate in ("INVALID_METADATA", "INVALID_RUNTIME_DRIFT", "INCONCLUSIVE",
                         "", None):
            check(f"gate {bad_gate!r} cannot be carried forward",
                  has(V(p=dict(ok, gate=bad_gate, results=allm),
                        expect_case_ids=real_ids), "describe a run that actually"))
        check("PASS carrying a failure is rejected",
              has(V(p=dict(ok, gate="PASS", results=defect_rows),
                    expect_case_ids=real_ids), "contradicts the recorded rows"))
        check("FAIL carrying no failure is rejected",
              has(V(p=dict(ok, gate="FAIL", results=allm), expect_case_ids=real_ids),
                  "every case matched"))
        check("an unknown verdict is rejected",
              has(V(p=dict(ok, gate="PASS",
                           results=[{"id": i, "verdict": "BANANA"} for i in sorted(real_ids)]),
                    expect_case_ids=real_ids), "not one of"))
        check("PASS with every case matching is accepted",
              V(p=dict(ok, gate="PASS", results=allm), expect_case_ids=real_ids) == [])
        check("FAIL with only the known defect is accepted",
              V(p=dict(ok, gate="FAIL", results=defect_rows),
                expect_case_ids=real_ids) == [])

        # ERROR is a legal verdict value but never a carriable one: a case that
        # errored was not compared, whatever its id.
        err_other = [{"id": i, "verdict": "MATCH"} for i in sorted(real_ids - {"C4"})] + \
                    [{"id": "C4", "verdict": "ERROR", "notes": ["boom"]}]
        check("an ERROR on a non-permitted case is rejected",
              has(V(p=dict(ok, gate="FAIL", results=err_other), expect_case_ids=real_ids),
                  "not the known harness defect"))
        err_c20 = [{"id": i, "verdict": "MATCH"} for i in sorted(real_ids - {"C20a"})] + \
                  [{"id": "C20a", "verdict": "ERROR", "notes": ["connection reset"]}]
        check("an ERROR on a permitted case without the known note is rejected",
              has(V(p=dict(ok, gate="FAIL", results=err_c20), expect_case_ids=real_ids),
                  "not with the known comparator defect"))
        check("the case list is derived when the caller omits it",
              has(verify_prior_contract(dict(ok, results=[{"id": "X", "verdict": "MATCH"}]),
                                        cand, "5.2B", ref_meta=ref), "is missing"),
              "an expect_case_ids a caller may omit is one a caller will omit")

        check("a different variant is rejected", has(V(p=dict(ok, variant="5.2A")), "variant"))
        check("a wrong kind is rejected", has(V(p=dict(ok, kind="paired_latency")), "kind"))
        check("an empty case list is rejected",
              has(V(p=dict(ok, results=[])), "no case results"))
        for field in ("kind", "gate", "variant", "results", "candidate_meta"):
            check(f"a contract result missing {field!r} is rejected",
                  has(V(p={k: v for k, v in ok.items() if k != field}), repr(field)))

        for field, needle in (("source_sha256", "the code under test changed"),
                              ("store_path", "the store path changed"),
                              ("zmetadata_fingerprints", "the data changed"),
                              ("dependencies", "the installed dependencies changed")):
            now = dict(cand); now[field] = {"changed": "yes"} if field != "store_path" \
                else "/elsewhere"
            check(f"a changed candidate {field} blocks reuse",
                  has(V(c=now), f"candidate: {needle}"))


def test_verify_environment_match() -> None:
    """Variant 5.2A exists to remove exactly this variable, so it must be checked.

    The 2026-08-07 campaign agreed on twelve pinned packages and disagreed on 23
    transitive ones. Comparing a hand-picked subset is how that stayed invisible, so
    this compares the digest of the whole distribution list.
    """
    arm = {"env_python_version": "3.11.4", "env_python": "/v/bin/python",
           "dependencies": {"distributions_sha256": "a" * 64,
                            "lockfile_sha256": "b" * 64,
                            "distributions": ["fsspec==2026.7.0", "anyio==4.14.2"]}}
    check("identical arms match", verify_environment_match(arm, arm) == [])
    check("absent metadata is reported",
          has(verify_environment_match(arm, None), "metadata missing"))
    check("a different interpreter version is caught",
          has(verify_environment_match(arm, dict(arm, env_python_version="3.12.0")),
              "interpreter version differs"))
    check("a different environment path is caught",
          has(verify_environment_match(arm, dict(arm, env_python="/other/python")),
              "package environment path differs"))

    other = dict(arm, dependencies={"distributions_sha256": "c" * 64,
                                    "lockfile_sha256": "b" * 64,
                                    "distributions": ["fsspec==2025.10.0",
                                                      "anyio==4.14.2"]})
    out = verify_environment_match(arm, other)
    check("a differing distribution set is caught",
          has(out, "installed distribution set differs"))
    check("the differing package is named, not just the digest",
          has(out, "fsspec: candidate 2026.7.0 vs reference 2025.10.0"),
          '"the sets differ" is not actionable')
    check("a package that agrees is not listed", not has(out, "anyio"))

    check("a different lockfile is caught",
          has(verify_environment_match(
              arm, dict(arm, dependencies={**arm["dependencies"],
                                           "lockfile_sha256": "z" * 64})),
              "lockfile differs"))
    check("a missing digest is reported rather than skipped",
          has(verify_environment_match(arm, dict(arm, dependencies={})),
              "digest missing on reference"))


def test_verify_environment_record() -> None:
    """Agreeing with each other is not the same as being the right environment.

    `verify_environment_match` is satisfied by two arms sharing any venv at all —
    including a stale `.venv` left by an earlier run, which is the case this exists
    to catch. The record written before the processes start is the anchor.
    """
    record = {"kind": "controlled_environment", "python_version": "3.11.4",
              "env_python": "/w/.venv/bin/python", "lockfile_sha256": "b" * 64,
              "distributions_sha256": "a" * 64}
    arm = {"env_python_version": "3.11.4", "env_python": "/w/.venv/bin/python",
           "dependencies": {"distributions_sha256": "a" * 64,
                            "lockfile_sha256": "b" * 64}}
    check("an arm running the recorded environment passes",
          verify_environment_record(record, arm, "candidate") == [])

    # The case the digest-only check missed: both arms on a stale venv agree with
    # each other, so verify_environment_match passes and proves nothing.
    stale = {"env_python_version": "3.11.4", "env_python": "/old/.venv/bin/python",
             "dependencies": {"distributions_sha256": "a" * 64,
                              "lockfile_sha256": "b" * 64}}
    check("two arms on a stale venv still satisfy the arms-agree check",
          verify_environment_match(stale, stale) == [],
          "which is why the record must also be compared")
    check("but the record catches the wrong environment path",
          has(verify_environment_record(record, stale, "candidate"),
              "package environment path is not the environment this run built"))

    check("a mismatched interpreter version is caught",
          has(verify_environment_record(
              record, dict(arm, env_python_version="3.14.0"), "reference"),
              "interpreter version is not the environment this run built"))
    check("a mismatched lockfile digest is caught",
          has(verify_environment_record(
              record, dict(arm, dependencies={**arm["dependencies"],
                                              "lockfile_sha256": "z" * 64}),
              "candidate"), "lockfile digest is not the environment this run built"))
    check("a mismatched distribution digest is caught",
          has(verify_environment_record(
              record, dict(arm, dependencies={**arm["dependencies"],
                                              "distributions_sha256": "z" * 64}),
              "candidate"),
              "installed distribution set digest is not the environment"))
    check("the failing arm is named",
          has(verify_environment_record(record, stale, "reference"), "reference:"))

    # Fail closed: nothing here may be read as a pass.
    check("a missing record is a problem, not a pass",
          has(verify_environment_record(None, arm, "candidate"),
              "no environment record"))
    check("missing metadata is a problem, not a pass",
          has(verify_environment_record(record, None, "candidate"),
              "metadata missing"))
    for key in ("env_python", "python_version", "lockfile_sha256",
                "distributions_sha256"):
        holed = {k: v for k, v in record.items() if k != key}
        check(f"a record missing {key} is reported",
              has(verify_environment_record(holed, arm, "candidate"),
                  f"no usable {key!r}"))
    check("metadata with a null digest is reported, not skipped",
          has(verify_environment_record(
              record, dict(arm, dependencies={**arm["dependencies"],
                                              "distributions_sha256": None}),
              "candidate"), "no usable 'dependencies.distributions_sha256'"))
    check("a non-string value is not compared as equal",
          verify_environment_record(record, dict(arm, env_python=None),
                                    "candidate") != [])
    check("every field is checked, so a wholly wrong arm reports all four",
          len(verify_environment_record(
              {**record, "python_version": "9", "env_python": "/x",
               "lockfile_sha256": "c" * 64, "distributions_sha256": "d" * 64},
              arm, "candidate")) == 4)


def test_gate_precedence() -> None:
    """Pin the findings-to-verdict mapping itself, not just the findings.

    Every check feeding this has its own test; the mapping had none, so an edit
    routing drift to a passing status would have gone unnoticed.
    """
    from bench.paired_bench import decide_gate

    none = dict(meta_problems=[], drift=[], invalid=[], hard_fail=[], unproven=[],
                undecided=[])
    check("no findings passes", decide_gate(**none) == "PASS")

    for field, expected in (("meta_problems", "INVALID_METADATA"),
                            ("drift", "INVALID_RUNTIME_DRIFT"),
                            ("invalid", "INVALID"),
                            ("hard_fail", "FAIL"),
                            ("unproven", "FAIL_NO_ESTABLISHED_IMPROVEMENT"),
                            ("undecided", "INCONCLUSIVE")):
        one = dict(none); one[field] = ["x"]
        check(f"{field} alone yields {expected}", decide_gate(**one) == expected,
              decide_gate(**one))

    # Precedence: a validity problem must outrank any performance result, including
    # a passing one, and drift must outrank a clean set of samples.
    everything = {k: ["x"] for k in none}
    check("metadata invalidity outranks everything",
          decide_gate(**everything) == "INVALID_METADATA")
    no_meta = dict(everything, meta_problems=[])
    check("runtime drift outranks every performance verdict",
          decide_gate(**no_meta) == "INVALID_RUNTIME_DRIFT")
    check("drift beats an otherwise clean run",
          decide_gate(**dict(none, drift=["moved"])) == "INVALID_RUNTIME_DRIFT")
    check("a regression outranks an unproven improvement",
          decide_gate(**dict(none, hard_fail=["x"], unproven=["y"])) == "FAIL")


def test_schema_matches_sidecar_output() -> None:
    """The two lists must agree, and this is what proves it rather than asserting it.

    A field the sidecar writes but the validator never checks is one nobody notices
    going missing — that is exactly how `expect_argv_contains` slipped through.
    """
    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d))
        m = build_meta(manifest="candidate", port=8051, pid=1, listeners=[1],
                       port_verified=True, cwd=cwd, exe=None, argv=["x"],
                       argv_str="x", env={}, lockfile=None,
                       store={"store_path": str(cwd), "store_source": "env"},
                       expect_argv=["api.app:app"])
        required = {f for f, _ in REQUIRED_META_FIELDS}
        check("the sidecar emits nothing the validator ignores",
              set(m) - required == set(), str(sorted(set(m) - required)))
        check("the validator requires nothing the sidecar omits",
              required - set(m) == set(), str(sorted(required - set(m))))


def test_field_types() -> None:
    m = good_meta(); m["master_pid"] = "12345"
    check("a string PID is rejected", has(validate_meta(m, "candidate"), "master_pid"))
    m = good_meta(); m["port"] = "8051"
    check("a string port is rejected", has(validate_meta(m, "candidate"), "port"))
    m = good_meta(); m["launch_argv"] = [1, 2]
    check("non-string argv entries are rejected",
          has(validate_meta(m, "candidate"), "launch_argv"))
    m = good_meta(); m["worker_pids"] = []
    check("an empty required list is rejected",
          has(validate_meta(m, "candidate"), "worker_pids"))


def test_wrong_label_and_manifest() -> None:
    m = good_meta("reference")
    check("a record labelled for the other arm is rejected",
          has(validate_meta(m, "candidate"), "labelled"))
    m = good_meta(); m["manifest_patterns"] = ["api/*.py"]
    check("a stale manifest is rejected",
          has(validate_meta(m, "candidate"), "manifest_patterns"))
    m = good_meta(); m["kind"] = "something_else"
    check("a wrong kind is rejected", has(validate_meta(m, "candidate"), "kind"))


def test_hashes() -> None:
    m = good_meta(); m["source_sha256"] = {}
    check("an empty hash set is rejected",
          has(validate_meta(m, "candidate"), "source_sha256"))
    m = good_meta(); m["source_sha256"] = {"api/app.py": "<unreadable: OSError>"}
    check("an unreadable source is rejected",
          has(validate_meta(m, "candidate"), "not hashed"))
    m = good_meta(); m["source_sha256"] = {"api/app.py": "abc123"}
    check("a short digest is rejected", has(validate_meta(m, "candidate"), "64 hex"))
    m = good_meta(); m["source_sha256"] = {"api/app.py": "A" * 64}
    check("an uppercase digest is rejected", has(validate_meta(m, "candidate"), "64 hex"))
    m = good_meta(); m["source_sha256"] = {"api/app.py": 12345}
    check("a non-string digest is rejected",
          has(validate_meta(m, "candidate"), "not a string"))


def test_hash_seed_requirement_is_per_arm() -> None:
    """5.2B's reference is production, which we may not restart to pin its seed.

    Requiring a pinned seed of every arm in every variant would have made the only
    variant D2a permits unrunnable — which is what the first campaign found when
    production reported `<unset — randomised>`. The waiver is per-arm and stated by
    the caller; it is never applied to a backend we start ourselves.
    """
    with tempfile.TemporaryDirectory() as d:
        cwd = make_cwd(Path(d), "reference")
        m = good_meta("reference", cwd)
        m["env"] = {"PYTHONHASHSEED": "<unset — randomised>",
                    "WOA23_ZARR_STORE": "/d"}
        check("an unpinned seed fails when the caller requires one",
              has(validate_meta(m, "reference"), "PYTHONHASHSEED"))
        check("an unpinned seed passes when the caller waives it",
              not has(validate_meta(m, "reference", require_pinned_seed=False),
                      "PYTHONHASHSEED"))
        check("waiving the seed does not waive anything else",
              validate_meta({"env": {}}, "reference", require_pinned_seed=False) != [])


def test_hash_seed() -> None:
    for bad in ("1", "<unset — randomised>", "", None):
        m = good_meta(); m["env"] = {"PYTHONHASHSEED": bad} if bad is not None else {}
        check(f"PYTHONHASHSEED={bad!r} is rejected",
              has(validate_meta(m, "candidate"), "PYTHONHASHSEED"))


def test_dependencies() -> None:
    m = good_meta(); m["dependencies"] = {"lockfile": "/x/uv.lock"}
    check("dependencies without a distribution digest are rejected",
          has(validate_meta(m, "candidate"), "no distribution digest"))
    m = good_meta(); m["dependencies"] = {"distributions_error": "no such file"}
    check("a failed distribution listing is rejected",
          has(validate_meta(m, "candidate"), "could not be listed"))
    m = good_meta(); m["env_python_source"] = "unresolved"
    check("an unresolved package environment is rejected",
          has(validate_meta(m, "candidate"), "could not be resolved"))


def test_load_meta_malformed() -> None:
    """A broken file must become INVALID_METADATA, not a traceback after sampling."""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        bad = root / "bad.json"
        bad.write_text("{not json at all")
        meta, probs = load_meta(bad, "candidate")
        check("malformed JSON is reported, not raised",
              meta is None and has(probs, "not valid JSON"))

        arr = root / "arr.json"
        arr.write_text(json.dumps([1, 2, 3]))
        meta, probs = load_meta(arr, "candidate")
        check("a non-object document is rejected",
              meta is None and has(probs, "expected an object"))

        meta, probs = load_meta(root / "nope.json", "candidate")
        check("an unreadable file is reported, not raised",
              meta is None and has(probs, "cannot read"))

        ok = root / "ok.json"
        ok.write_text(json.dumps(good_meta()))
        meta, probs = load_meta(ok, "candidate")
        check("a valid file loads cleanly", meta is not None and probs == [])


def test_manifest_expansion() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "api" / "sub").mkdir(parents=True)
        (root / "api" / "app.py").write_text("x")
        (root / "api" / "sub" / "deep.py").write_text("y")
        (root / "api" / "notes.txt").write_text("z")
        found = {p.name for p in expand("candidate", root)}
        check("recursive glob reaches a nested module", "deep.py" in found, str(found))
        check("non-python files are not hashed", "notes.txt" not in found)

    with tempfile.TemporaryDirectory() as d:
        try:
            expand("candidate", Path(d))
            check("a pattern matching nothing aborts", False, "no SystemExit")
        except SystemExit as exc:
            check("a pattern matching nothing aborts", "matched no files" in str(exc))

    try:
        expand("nonsense", Path("."))
        check("an unknown manifest label aborts", False, "no SystemExit")
    except SystemExit:
        check("an unknown manifest label aborts", True)


def main() -> int:
    for fn in (test_good_meta_passes, test_source_set_must_match_manifest,
               test_ss_parsing_accumulates_rows, test_ss_port_matching_is_exact,
               test_master_selection,
               test_post_run_listener_change, test_post_run_source_drift,
               test_identity_fields_are_required, test_listener_pids_contents,
               test_against_validates_both_records,
               test_verify_prior_rung, test_verify_prior_contract,
               test_verify_environment_match, test_verify_environment_record,
               test_gate_precedence,
               test_schema_matches_sidecar_output,
               test_source_digests_are_recomputed,
               test_kernel_and_store_fields_required,
               test_resolve_store_is_label_driven, test_post_run_drift,
               test_identity_comparison,
               test_store_agreement,
               test_missing_metadata,
               test_stub_metadata_is_rejected, test_field_types,
               test_wrong_label_and_manifest, test_hashes, test_hash_seed,
               test_dependencies, test_hash_seed_requirement_is_per_arm,
               test_load_meta_malformed, test_manifest_expansion):
        print(f"\n{fn.__name__}")
        fn()
    total = len(passed) + len(failures)
    if failures:
        print(f"\nFAILED {len(failures)}/{total}: {', '.join(failures)}")
    else:
        # Reported rather than written into prose: every hand-maintained count in
        # the spec has gone stale within a revision or two.
        print(f"\nall passed ({total} assertions)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
