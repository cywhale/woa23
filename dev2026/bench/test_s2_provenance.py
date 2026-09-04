"""Offline tests for the S2 interpreter/import-path evidence.

Everything here runs on any machine: the pure checks work on literal dictionaries,
and the two tests that really launch an interpreter launch *this* one, with a
temporary directory standing in for the package clone. No production path is read,
no host is contacted, nothing is started that outlives the test.

    uv run python -m bench.test_s2_provenance
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.s2_provenance import (       # noqa: E402
    DEFAULT_MODULES, FIXED_STRINGS, _under, binary_identity, check_import_paths,
    check_maps, expand_roots, interpreter_facts, launch_env, parse_maps,
    probe_source, seed_digest,
)
from bench.suite_summary import summary          # noqa: E402

PASS = 0
FAIL = 0


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


def check_true(name, actual):
    check(name, True, bool(actual))


PROD = "/home/odbadmin/python/woa23"
PROD_SP = "/home/odbadmin/.pyenv/versions/3.11.4/envs/py311/lib/python3.11/site-packages"
CLONE = "/home/odbadmin/woa23-s2-package-clone/dist"
STAGE = "/home/odbadmin/woa23-s2-c1-work"
STDLIB = "/home/odbadmin/.pyenv/versions/3.11.4/lib"
FORBID = [PROD, PROD_SP]
ALLOW = [CLONE, STAGE, STDLIB]


def entry(raw, real=None):
    real = real or raw
    return {"raw": raw, "abs": real, "real": real}


def facts(sys_path, modules=None, flags=None, dwb=True):
    return {
        "sys_path": sys_path,
        "modules": modules if modules is not None else {},
        "flags": {"no_site": 1, "ignore_environment": 0,
                  "dont_write_bytecode": 1, "hash_randomization": 1,
                  **(flags or {})},
        "dont_write_bytecode": dwb,
        "base_prefix": "/home/odbadmin/.pyenv/versions/3.11.4",
    }


# ------------------------------------------------------------ containment ---
print("path containment is on component boundaries, not string prefixes")
check_true("a path inside a root is inside it", _under("/a/b/c", "/a/b"))
check_true("a root is inside itself", _under("/a/b", "/a/b"))
check("a sibling sharing the prefix is NOT inside", False, _under("/a/bc", "/a/b"))
check("a name extending the last component is NOT inside", False,
      _under("/home/x/woa23-staging/y", "/home/x/woa23"))
check("a trailing slash on the root does not change the answer", True,
      _under("/a/b/c", "/a/b/"))
check("an empty root contains nothing", False, _under("/a", ""))
check("a parent is not inside its child", False, _under("/a", "/a/b"))


# ---------------------------------------------------------- sys.path check ---
print()
print("sys.path is checked against both the allow-list and production")
ok = facts([entry("", STAGE + "/candidate"), entry(CLONE),
            entry(STDLIB + "/python3.11")])
check("a clean path has no problems", [], check_import_paths(ok, allowed=ALLOW, forbidden=FORBID))

bad = facts([entry("", STAGE + "/candidate"), entry(CLONE), entry(PROD_SP)])
probs = check_import_paths(bad, allowed=ALLOW, forbidden=FORBID)
check_true("production site-packages on sys.path is a problem", probs)
check_true("and the message names production",
           any("inside production" in p for p in probs))

bad2 = facts([entry(CLONE), entry("/somewhere/else")])
probs2 = check_import_paths(bad2, allowed=ALLOW, forbidden=FORBID)
check_true("a path under no allowed root is a problem too", probs2)
check_true("and says so rather than calling it production",
           any("under none of the allowed roots" in p for p in probs2))

# The empty entry is the one that hides a cwd import. It must be judged by where
# it expands to, not by the empty string.
sneaky = facts([{"raw": "", "abs": PROD, "real": PROD}])
check_true("an empty entry expanding into production is caught",
           any("inside production" in p
               for p in check_import_paths(sneaky, allowed=ALLOW, forbidden=FORBID)))

# A symlink in the clone pointing at production would pass the `abs` test and
# fail the `real` one. Both forms are checked for exactly this.
symlinked = facts([{"raw": CLONE + "/polars", "abs": CLONE + "/polars",
                    "real": PROD_SP + "/polars"}])
check_true("a clone path whose realpath is production is caught",
           any("inside production" in p
               for p in check_import_paths(symlinked, allowed=ALLOW, forbidden=FORBID)))

check("a probe that did not run is a single clear problem", 1,
      len(check_import_paths({"probe_error": "boom"}, allowed=ALLOW, forbidden=FORBID)))
check_true("an absent sys.path is a problem, not an empty pass",
           check_import_paths(facts([]), allowed=ALLOW, forbidden=FORBID))


# ------------------------------------------------------------ module files ---
print()
print("every named module's __file__ is checked the same way")
mods_ok = {"polars": {"file": CLONE + "/polars/__init__.py",
                      "real": CLONE + "/polars/__init__.py", "version": "1.27.1"}}
check("a module inside the clone is fine", [],
      check_import_paths(facts([entry(CLONE)], mods_ok), allowed=ALLOW, forbidden=FORBID))

mods_bad = {"polars": {"file": PROD_SP + "/polars/__init__.py",
                       "real": PROD_SP + "/polars/__init__.py"}}
check_true("a module loaded from production site-packages is caught",
           any("polars" in p and "inside production" in p
               for p in check_import_paths(facts([entry(CLONE)], mods_bad),
                                           allowed=ALLOW, forbidden=FORBID)))

mods_src = {"src": {"file": PROD + "/src/config.py", "real": PROD + "/src/config.py"}}
check_true("a module loaded from production's src is caught",
           any("inside production" in p
               for p in check_import_paths(facts([entry(CLONE)], mods_src),
                                           allowed=ALLOW, forbidden=FORBID)))

mods_err = {"zarr": {"error": "ModuleNotFoundError: No module named 'zarr'"}}
check_true("a module that would not import is a problem, not a pass",
           any("did not import" in p
               for p in check_import_paths(facts([entry(CLONE)], mods_err),
                                           allowed=ALLOW, forbidden=FORBID)))


# ------------------------------------------------------------------- flags ---
print()
print("the flags that would void the record are refused")
check_true("-E voids the run (section 4.2.0a)",
           any("ignore_environment" in p
               for p in check_import_paths(facts([entry(CLONE)], flags={"ignore_environment": 1}),
                                           allowed=ALLOW, forbidden=FORBID)))
check_true("a missing -S is a problem for the C1 procedure",
           any("no_site" in p
               for p in check_import_paths(facts([entry(CLONE)], flags={"no_site": 0}),
                                           allowed=ALLOW, forbidden=FORBID)))
check_true("bytecode writing is refused: the clone is immutable",
           any("BYTECODE" in p.upper()
               for p in check_import_paths(facts([entry(CLONE)], dwb=False),
                                           allowed=ALLOW, forbidden=FORBID)))


# -------------------------------------------------------------------- maps ---
print()
print("/proc/<pid>/maps is parsed and checked against production only")
MAPS = f"""\
55a1c0000000-55a1c0021000 r--p 00000000 fd:00 1234    /usr/bin/python3.11
7f2a00000000-7f2a00100000 rw-p 00000000 00:00 0
7f2a10000000-7f2a10800000 r-xp 00000000 fd:00 5678    {CLONE}/polars/polars.abi3.so
7f2a20000000-7f2a20800000 r-xp 00000000 fd:00 9012    {PROD_SP}/numpy/core/_multiarray_umath.cpython-311-x86_64-linux-gnu.so
7ffd00000000-7ffd00021000 rw-p 00000000 00:00 0       [stack]
7f2a30000000-7f2a30001000 r--s 00000000 fd:00 3456    /usr/lib/locale/locale-archive
"""
paths = parse_maps(MAPS)
check("only real file paths are taken", 4, len(paths))
check("[stack] is not a file", False, any("[" in p for p in paths))
check("anonymous mappings contribute nothing", False, any(p == "" for p in paths))
hits = check_maps(paths, forbidden=FORBID)
check("exactly the production mapping is flagged", 1, len(hits))
check_true("and it names the file", "_multiarray_umath" in hits[0])
check("the clone's own .so is not flagged", False,
      any("polars.abi3.so" in h for h in hits))
check("a clean maps dump yields nothing", [],
      check_maps([f"{CLONE}/x.so", "/usr/lib/libc.so.6"], forbidden=FORBID))
check("an empty dump parses to nothing", [], parse_maps(""))
check("a path with spaces survives the split", ["/opt/a b/c.so"],
      parse_maps("7f00-7f01 r-xp 0 fd:00 1 /opt/a b/c.so"))


# ------------------------------------------------------------- launch env ---
print()
print("the launch environment is built once and shared by the probe and the arms")
base = {"PYTHONHOME": "/prod/home", "VIRTUAL_ENV": "/prod/venv",
        "PYTHONHASHSEED": "0", "PATH": "/usr/bin"}
e1 = launch_env(CLONE, hashseed="0", base=base)
check("PYTHONPATH is the clone", CLONE, e1["PYTHONPATH"])
check("PYTHONHOME is removed", None, e1.get("PYTHONHOME"))
check("VIRTUAL_ENV is removed", None, e1.get("VIRTUAL_ENV"))
check("user site is disabled", "1", e1["PYTHONNOUSERSITE"])
check("bytecode writing is disabled", "1", e1["PYTHONDONTWRITEBYTECODE"])
check("a pinned seed is set", "0", e1["PYTHONHASHSEED"])
e2 = launch_env(CLONE, hashseed=None, base=base)
check("an unpinned run has no PYTHONHASHSEED at all", False, "PYTHONHASHSEED" in e2)
check("PATH survives", "/usr/bin", e2["PATH"])
check("-E is never added", False, "-E" in probe_source())


# ----------------------------------------------------- the probe really runs ---
print()
print("the probe runs, against this interpreter and a stand-in clone")
with tempfile.TemporaryDirectory() as td:
    clone = Path(td) / "clone"
    (clone / "fakepkg").mkdir(parents=True)
    (clone / "fakepkg" / "__init__.py").write_text("__version__ = '9.9'\n")
    armdir = Path(td) / "arm"
    armdir.mkdir()

    f = interpreter_facts(sys.executable, str(clone), str(armdir),
                          hashseed="0", modules=("json", "fakepkg"))
    check("the probe produced a report", False, "probe_error" in f)
    check("-S took effect", 1, (f.get("flags") or {}).get("no_site"))
    check("-E did not", 0, (f.get("flags") or {}).get("ignore_environment"))
    check("bytecode writing is off", True, f.get("dont_write_bytecode"))
    check("cwd is the arm directory", os.path.realpath(str(armdir)),
          os.path.realpath(f["cwd"]))
    check("the stand-in clone is importable from",
          os.path.realpath(str(clone / "fakepkg" / "__init__.py")),
          os.path.realpath(f["modules"]["fakepkg"]["real"]))

    check("expand_roots keeps the given form", True,
          str(clone) in expand_roots([str(clone)]))
    check("and adds the resolved one", True,
          os.path.realpath(str(clone)) in expand_roots([str(clone)]))
    check("a root that is already resolved is not duplicated", 1,
          len(expand_roots(["/"])))
    check("a stdlib module still resolves", True,
          f["modules"]["json"]["real"].endswith("json/__init__.py"))
    check("the empty sys.path entry is expanded to the cwd", True,
          any(e["raw"] == "" and os.path.realpath(e["real"]) == os.path.realpath(str(armdir))
              for e in f["sys_path"]))
    check("the site limitation travels with the record", True,
          "site.py did not run" in f["site_limitation"])
    # The claim this record must never be read as making.
    wl = f["worker_provenance_limitation"]
    check("the record says it is a sibling, not a worker", True, "SIBLING" in wl)
    check("and that what it establishes is the launch environment", True,
          "launch environment and import" in wl)
    check("and that no worker-level mechanism exists here", True,
          "No worker-level Python provenance mechanism exists" in wl)
    check("and names what such a mechanism would take", True, "post_fork" in wl)
    check("and that maps refutes rather than establishes", True,
          "REFUTE" in wl and "not proof of" in wl)
    check("the launch is recorded", "0", f["launch"]["pythonhashseed"])
    check("no .pyc was written into the clone", 0,
          len(list(clone.rglob("__pycache__"))))

    # The isolation check, end to end, against the real report.
    probs = check_import_paths(f, allowed=expand_roots([str(clone), str(armdir)]),
                               forbidden=expand_roots(["/nonexistent-production"]))
    stdlib_only = [p for p in probs if "allowed roots" in p]
    check("the only complaints are stdlib roots the caller did not allow",
          len(probs), len(stdlib_only))
    probs2 = check_import_paths(
        f, allowed=expand_roots([str(clone), str(armdir),
                                 str(Path(sys.base_prefix) / "lib")]),
        forbidden=expand_roots(["/nonexistent-production"]))
    check("allowing the interpreter's own stdlib clears it", [], probs2)

    # Seed diversity: pinned starts agree, unpinned starts are observed, never forced.
    g = interpreter_facts(sys.executable, str(clone), str(armdir),
                          hashseed="0", modules=("json",))
    check("two pinned starts have the same seed digest", seed_digest(f), seed_digest(g))
    check("the digest is a digest", 64, len(seed_digest(f) or ""))
    check("the probe hashes the whole fixed set", len(FIXED_STRINGS),
          len(f["hash_probe"]["hashes"]))

    unpinned = [seed_digest(interpreter_facts(sys.executable, str(clone), str(armdir),
                                              hashseed=None, modules=("json",)))
                for _ in range(3)]
    check("three unpinned starts each produced a digest", 3, len([d for d in unpinned if d]))
    # Not asserted as "must differ": that is precisely the observation C2 makes and
    # reports as INSUFFICIENT when it does not happen. Asserting it here would make
    # this suite fail on a machine where hash randomisation is off, which is a fact
    # about the machine and not a defect.
    print(f"       (observed {len(set(unpinned))} distinct seeds in 3 unpinned starts)")

    check("a report with no hash probe has no digest", None, seed_digest({}))

    # binary_identity distinguishes the symlink from the ELF behind it.
    link = Path(td) / "pylink"
    link.symlink_to(sys.executable)
    bi = binary_identity(str(link))
    check("the symlink is identified as one", True, bi["is_symlink"])
    check("realpath follows it", os.path.realpath(sys.executable), bi["real"])
    check("the size is the target's, not the link's", True,
          bi["size"] > bi["lstat_size"])
    check("the digest is of the target", binary_identity(sys.executable)["sha256"],
          bi["sha256"])


# ------------------------------------------------------------------- CLI ---
print()
print("the CLI fails closed and writes its record")
with tempfile.TemporaryDirectory() as td:
    clone = Path(td) / "clone"
    clone.mkdir()
    out = Path(td) / "rec.json"
    r = subprocess.run(
        [sys.executable, "-m", "bench.s2_provenance",
         "--python-binary", sys.executable, "--package-clone", str(clone),
         "--cwd", td, "--label", "candidate", "--hashseed", "0",
         "--module", "json", "--allow", str(clone), "--allow", td,
         "--forbid", "/nonexistent-production", "--out", str(out)],
        cwd=str(Path(__file__).resolve().parent.parent),
        capture_output=True, text=True)
    check("it exits 0 when isolation holds", 0, r.returncode)
    check("the record was written", True, out.exists())
    rec = json.loads(out.read_text())
    check("the record carries the label", "candidate", rec["label"])
    check("the record carries no problems", [], rec["problems"])
    check("the record carries the binary's digest", 64, len(rec["binary"]["sha256"]))
    check("the record carries the seed digest", 64, len(rec["seed_digest"]))
    check("the stdlib was allowed without being named", True,
          any(p.endswith("/lib") for p in rec["allowed_roots"]))
    check("the -S limitation is in the record", True,
          "site.py did not run" in rec["site_limitation"])
    check("the worker-provenance limitation is in the record too", True,
          "SIBLING" in rec["worker_provenance_limitation"])
    check("the -S limitation is printed", True, "LIMITATION (site)" in r.stdout)
    check("and so is the worker-provenance one", True,
          "LIMITATION (worker)" in r.stdout)

    r2 = subprocess.run(
        [sys.executable, "-m", "bench.s2_provenance",
         "--python-binary", sys.executable, "--package-clone", str(clone),
         "--cwd", td, "--label", "candidate", "--hashseed", "0",
         "--module", "json", "--allow", str(clone),
         "--forbid", str(Path(sys.base_prefix))],
        cwd=str(Path(__file__).resolve().parent.parent),
        capture_output=True, text=True)
    check("forbidding the interpreter's own prefix fails the run", 1, r2.returncode)
    check("and says isolation is not established", True,
          "import isolation is not established" in r2.stderr)

    r3 = subprocess.run(
        [sys.executable, "-m", "bench.s2_provenance",
         "--python-binary", str(Path(td) / "no-such-python"),
         "--package-clone", str(clone), "--cwd", td, "--label", "x"],
        cwd=str(Path(__file__).resolve().parent.parent),
        capture_output=True, text=True)
    check("a missing interpreter is a failure, not an empty pass", 1, r3.returncode)

    # An unreadable PID must fail closed rather than be skipped silently.
    # maps is checked against the EXPANDED forbidden roots, like sys.path is. On a
    # host where production's site-packages is reached as .../versions/py311/... and
    # IS .../versions/3.11.4/envs/py311/..., a root recorded in one form would never
    # match a mapped file resolved in the other, and the refutation test would
    # quietly refute nothing.
    real_sp = "/home/odbadmin/.pyenv/versions/3.11.4/envs/py311/lib/python3.11/site-packages"
    mapped = [f"{real_sp}/numpy/core/_multiarray_umath.cpython-311-x86_64-linux-gnu.so"]
    check("a symlinked forbidden root misses the resolved path unexpanded", [],
          check_maps(mapped, forbidden=["/home/odbadmin/.pyenv/versions/py311"]))
    check("and catches it once the root is expanded", 1,
          len(check_maps(mapped, forbidden=expand_roots(
              ["/home/odbadmin/.pyenv/versions/3.11.4/envs/py311"]))))
    # The wiring, since the pure check cannot see which list it was handed.
    src = (Path(__file__).resolve().parent / "s2_provenance.py").read_text()
    check("the CLI hands check_maps the expanded roots", True,
          "check_maps(paths, forbidden=forbidden)" in src)
    check("and not the raw argument", False,
          "check_maps(paths, forbidden=args.forbid)" in src)

    # The live-PID path needs /proc, which exists on the host this runs against and
    # not on the machine these tests are written on. Skipped rather than faked: a
    # stub /proc would test the stub.
    if Path("/proc/self/maps").exists():
        r_self = subprocess.run(
            [sys.executable, "-m", "bench.s2_provenance",
             "--python-binary", sys.executable, "--package-clone", str(clone),
             "--cwd", td, "--label", "x", "--hashseed", "0", "--module", "json",
             "--allow", str(clone), "--allow", td,
             "--forbid", "/nonexistent-production", "--pid", str(os.getpid()),
             "--out", str(Path(td) / "maps.json")],
            cwd=str(Path(__file__).resolve().parent.parent),
            capture_output=True, text=True)
        check("a readable PID passes when nothing production is mapped", 0,
              r_self.returncode)
        rec_m = json.loads((Path(td) / "maps.json").read_text())["maps"][str(os.getpid())]
        check("the mapped-file count is recorded", True, rec_m["n_mapped_files"] > 0)
        check("no production hit", [], rec_m["production_hits"])
        check("the record states what an empty result establishes", True,
              "no mapped file of this process came from production" in rec_m["establishes"])
        check("and states what it does not", True,
              "not imports" in rec_m["does_not_establish"])
        check("the printed line says it is a refutation test", True,
              "refutation test only" in r_self.stdout)
    else:
        print("       (no /proc on this machine: the live-PID maps read is not "
              "exercised here; the parsing and the checks above are)")

    r4 = subprocess.run(
        [sys.executable, "-m", "bench.s2_provenance",
         "--python-binary", sys.executable, "--package-clone", str(clone),
         "--cwd", td, "--label", "x", "--hashseed", "0", "--module", "json",
         "--allow", str(clone), "--allow", td,
         "--forbid", "/nonexistent-production", "--pid", "999999"],
        cwd=str(Path(__file__).resolve().parent.parent),
        capture_output=True, text=True)
    check("a PID whose maps cannot be read fails the run", 1, r4.returncode)
    check("and says the loaded files could not be checked", True,
          "cannot be checked" in r4.stderr)


print()
check("the default module list covers the read path", True,
      {"polars", "xarray", "zarr", "numpy"}.issubset(set(DEFAULT_MODULES)))

print()
raise SystemExit(summary(PASS, FAIL))