"""Offline tests for the clone's ancestor chain and manifest verification.

Real directories in a temporary tree, real modes, real re-hashing. Nothing here
reads production or the actual clone.

    uv run python -m bench.test_clone_integrity
"""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.clone_integrity import (  # noqa: E402
    ancestor_chain, classify, describe, read_manifest, verify_manifest,
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


def build_clone(root: Path, files: dict) -> Path:
    """A stand-in clone plus its manifest, in the real on-disk format."""
    dist = root / "dist"
    lines = []
    for rel, content in sorted(files.items()):
        p = dist / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)
        st = p.stat()
        lines.append(f"{rel}\t{hashlib.sha256(content).hexdigest()}\t"
                     f"{st.st_size}\t{st.st_mtime_ns}")
    (root / "clone.manifest").write_text("\n".join(lines) + "\n")
    return dist


FILES = {"pkg/__init__.py": b"__version__='1'\n",
         "pkg/core.py": b"x = 1\n",
         "other.so": b"\x7fELF" + b"\x00" * 40}


# ------------------------------------------------------ manifest verification ---
print("the manifest is verified against the whole tree, not sampled")
with tempfile.TemporaryDirectory() as td:
    root = Path(td) / "clone-root"
    root.mkdir()
    dist = build_clone(root, FILES)
    man = str(root / "clone.manifest")

    v = verify_manifest(str(dist), man)
    check("a matching tree verifies", True, v["ok"])
    check("every entry was compared", 3, v["n_manifest_entries"])
    check("and every file was found", 3, v["n_files_on_disk"])
    check("the manifest's own digest is recorded", 64, len(v["manifest_sha256"]))
    check("bytes hashed is reported", True, v["bytes_hashed"] > 0)

    # Content changed, size and mtime preserved: the case a stat-only check misses.
    victim = dist / "pkg" / "core.py"
    st = victim.stat()
    victim.write_bytes(b"x = 2\n")
    os.utime(victim, ns=(st.st_atime_ns, st.st_mtime_ns))
    v = verify_manifest(str(dist), man)
    check("a same-size same-mtime content change is caught", 1, v["n_digest_mismatch"])
    check("and named", ["pkg/core.py"], v["digest_mismatch"])
    check("while size and mtime look fine", (0, 0),
          (v["n_size_mismatch"], v["n_mtime_mismatch"]))
    check("so the verification fails", False, v["ok"])
    victim.write_bytes(b"x = 1\n")
    os.utime(victim, ns=(st.st_atime_ns, st.st_mtime_ns))
    check("restoring it verifies again", True, verify_manifest(str(dist), man)["ok"])

    # An added file is the substitution signature: the tree is a superset.
    (dist / "sneaky.py").write_bytes(b"import os\n")
    v = verify_manifest(str(dist), man)
    check("a file added to the clone is caught", 1, v["n_extra"])
    check("and named", ["sneaky.py"], v["extra"])
    (dist / "sneaky.py").unlink()

    (dist / "other.so").unlink()
    v = verify_manifest(str(dist), man)
    check("a removed file is caught", 1, v["n_missing"])
    check("and named", ["other.so"], v["missing"])

with tempfile.TemporaryDirectory() as td:
    root = Path(td) / "r"
    root.mkdir()
    dist = build_clone(root, FILES)
    # A wholly substituted tree: right path, right file names, different contents.
    other = Path(td) / "evil"
    other.mkdir()
    for rel in FILES:
        p = other / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"malicious\n")
    v = verify_manifest(str(other), str(root / "clone.manifest"))
    check("a substituted tree at the same path is caught", False, v["ok"])
    check("every file mismatches", 3, v["n_digest_mismatch"])


print()
print("a manifest that cannot be trusted is an error, never an empty pass")
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    (root / "empty.manifest").write_text("")
    want, err = read_manifest(str(root / "empty.manifest"))
    check("an empty manifest is rejected", True, err is not None)
    check("and says why it would match everything", True, "every tree" in err)

    (root / "bad.manifest").write_text("only\ttwo\n")
    _, err = read_manifest(str(root / "bad.manifest"))
    check("a malformed line is rejected", True, "expected 4 tab-separated" in err)

    (root / "nonint.manifest").write_text("a\tb\tnotanumber\t1\n")
    _, err = read_manifest(str(root / "nonint.manifest"))
    check("non-integer size/mtime is rejected", True, "not integers" in err)

    _, err = read_manifest(str(root / "no-such-file"))
    check("an unreadable manifest is rejected", True, "cannot read" in err)

    v = verify_manifest(str(root), str(root / "empty.manifest"))
    check("verification refuses to run on it", False, v["ok"])
    check("and reports the error rather than a match", True, "error" in v)


print()
print("the digest list next door is named as the wrong file, not just miscounted")
# The clone root holds `clone.manifest` AND a `SHA256SUMS` listing the digests of
# the manifest files. On 2026-08-10 a C2 run was launched against the second: the
# error said "expected 4 tab-separated fields, got 1", which is true and does not
# tell anyone which file to use. These assert that it now does.
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    SUMS = ("3c14cfe70369d081b9b10467d258892654299cba360a3852b886274550630407  "
            "source.manifest\n"
            "f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4  "
            "clone.manifest\n")
    (root / "SHA256SUMS").write_text(SUMS)
    _, err = read_manifest(str(root / "SHA256SUMS"))
    check("VM24's actual SHA256SUMS is rejected", True, err is not None)
    check("and is identified as a digest list", True, "digest list" in err)
    check("and the right file is named", True, "clone.manifest" in err)
    check("with its four columns spelled out", True,
          all(c in err for c in ("relpath", "sha256", "size", "mtime_ns")))
    check("and it says why the wrong file cannot do the job", True,
          "verify a tree" in err)

    # `sha256sum --binary` writes an asterisk instead of the second space.
    (root / "binary.sums").write_text(
        "3c14cfe70369d081b9b10467d258892654299cba360a3852b886274550630407 *x.tar\n")
    _, err = read_manifest(str(root / "binary.sums"))
    check("the binary-mode spelling is caught too", True, "digest list" in err)

    # A four-column manifest whose first field happens to be a digest-shaped name
    # must not be mistaken for one: the tabs decide, and it parses.
    ok_line = ("3c14cfe70369d081b9b10467d258892654299cba360a3852b886274550630407\t"
               "f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4\t"
               "12\t1722906835056193900\n")
    (root / "odd.manifest").write_text(ok_line)
    want, err = read_manifest(str(root / "odd.manifest"))
    check("a real manifest is not misdiagnosed", None, err)
    check("and it parses", 1, len(want))

    # A plain wrong file gets the generic message, and that message must still say
    # where the real manifest is — the whole failure was not knowing.
    (root / "notes.txt").write_text("just some prose\n")
    _, err = read_manifest(str(root / "notes.txt"))
    check("an unrelated file is rejected", True, err is not None)
    check("without claiming to be a digest list", False, "digest list" in err)
    check("but still naming clone.manifest", True, "clone.manifest" in err)

    # The real shape, taken from VM24's clone.manifest line 1.
    (root / "clone.manifest").write_text(
        "_cffi_backend.cpython-311-x86_64-linux-gnu.so\t"
        "39056b969418bf05203dcf9e35fe60f116e3fbb68216bec0a47bd5d72d40808a\t"
        "1064368\t1722906835056193900\n")
    want, err = read_manifest(str(root / "clone.manifest"))
    check("VM24's actual manifest shape is accepted", None, err)
    check("and yields the file it names", True,
          "_cffi_backend.cpython-311-x86_64-linux-gnu.so" in want)


# ---------------------------------------------------------- ancestor chain ---
print()
print("the ancestor chain is walked, resolved, and classified by remediability")
with tempfile.TemporaryDirectory() as td:
    base = Path(td) / "a" / "b" / "clone-root"
    base.mkdir(parents=True)
    dist = base / "dist"
    dist.mkdir()

    chain = ancestor_chain(str(dist))
    check("the chain starts at the clone root", os.path.realpath(str(dist)),
          chain[0]["path"])
    check("the second link is its parent", os.path.realpath(str(base)),
          chain[1]["path"])
    check("and it reaches the filesystem root", "/", chain[-1]["path"])
    check("every link is a directory", True, all(l.get("is_dir") for l in chain))

    # This is the real configuration that prompted the check: dist 555, parent 775.
    os.chmod(dist, 0o555)
    os.chmod(base, 0o775)
    verdict = classify(ancestor_chain(str(dist)))
    check("a writable parent is a problem, not a note", True,
          any("writable by this user" in p and "parent" in p
              for p in verdict["problems"]))
    check("and the message explains the unlink rule", True,
          any("write permission on the" in p for p in verdict["problems"]))
    check("and names the fix as a host write action", True,
          any("needs its own authorisation" in p for p in verdict["problems"]))

    os.chmod(base, 0o555)
    verdict = classify(ancestor_chain(str(dist)))
    check("an unwritable parent raises no problem", [], verdict["problems"])
    # Higher ancestors are still writable here (the temp dir), and that is recorded
    # rather than refused: a home directory must be writable.
    check("higher writable ancestors are recorded as exposure", True,
          len(verdict["residual_exposure"]) > 0)
    check("and say they are not refused", True,
          any("not refused" in e for e in verdict["residual_exposure"]))
    check("and say what it still permits", True,
          any("re-pointed" in e for e in verdict["residual_exposure"]))

    os.chmod(dist, 0o755)
    check("a writable clone root is a problem", True,
          any("clone root" in p for p in classify(ancestor_chain(str(dist)))["problems"]))
    os.chmod(dist, 0o555)
    os.chmod(base, 0o775)

with tempfile.TemporaryDirectory() as td:
    # A symlinked ancestor: the lexical parent is fine and the real one is not.
    real = Path(td) / "real"
    (real / "clone").mkdir(parents=True)
    os.chmod(real / "clone", 0o555)
    os.chmod(real, 0o775)
    link = Path(td) / "link"
    link.symlink_to(real)
    chain = ancestor_chain(str(link / "clone"))
    check("the chain follows the symlink to the real parent",
          os.path.realpath(str(real)), chain[1]["path"])
    check("so a symlinked writable parent is still caught", True,
          any("parent" in p for p in classify(chain)["problems"]))
    os.chmod(real / "clone", 0o755)

print()
print("modes that are wrong at any depth are refused regardless of remediability")
with tempfile.TemporaryDirectory() as td:
    deep = Path(td) / "w" / "x" / "y" / "clone"
    deep.mkdir(parents=True)
    os.chmod(deep, 0o555)
    os.chmod(deep.parent, 0o555)
    os.chmod(deep.parent.parent, 0o777)          # world-writable, not sticky
    verdict = classify(ancestor_chain(str(deep)))
    check("a world-writable ancestor is a problem even three levels up", True,
          any("world-writable" in p for p in verdict["problems"]))
    os.chmod(deep.parent.parent, 0o1777)         # sticky, like /tmp
    check("a sticky world-writable ancestor is not", False,
          any("world-writable" in p for p in classify(ancestor_chain(str(deep)))["problems"]))
    os.chmod(deep.parent.parent, 0o755)
    os.chmod(deep.parent, 0o755)
    os.chmod(deep, 0o755)

print()
print("VM24's actual chain, as read on 2026-08-09, classified")
# Literal, from `os.lstat` up the realpath chain of the real clone. Kept as a fixture
# so the classification is tested against the configuration it has to judge, not only
# against ones this file constructs — and so a change on the host shows up as a
# fixture that no longer matches rather than as a silent pass.
VM24_CHAIN = [
    {"path": "/home/odbadmin/woa23-s2-package-clone/dist", "mode": "dr-xr-xr-x",
     "mode_octal": "0o555", "uid": 1000, "gid": 1000, "is_dir": True,
     "is_symlink": False, "group_writable": False, "world_writable": False,
     "sticky": False, "writable_by_us": False,
     "realpath": "/home/odbadmin/woa23-s2-package-clone/dist"},
    {"path": "/home/odbadmin/woa23-s2-package-clone", "mode": "drwxrwxr-x",
     "mode_octal": "0o775", "uid": 1000, "gid": 1000, "is_dir": True,
     "is_symlink": False, "group_writable": True, "world_writable": False,
     "sticky": False, "writable_by_us": True,
     "realpath": "/home/odbadmin/woa23-s2-package-clone"},
    {"path": "/home/odbadmin", "mode": "drwxr-xr-x", "mode_octal": "0o755",
     "uid": 1000, "gid": 1000, "is_dir": True, "is_symlink": False,
     "group_writable": False, "world_writable": False, "sticky": False,
     "writable_by_us": True, "realpath": "/home/odbadmin"},
    {"path": "/home", "mode": "drwxr-xr-x", "mode_octal": "0o755", "uid": 0,
     "gid": 0, "is_dir": True, "is_symlink": False, "group_writable": False,
     "world_writable": False, "sticky": False, "writable_by_us": False,
     "realpath": "/home"},
    {"path": "/", "mode": "drwxr-xr-x", "mode_octal": "0o755", "uid": 0, "gid": 0,
     "is_dir": True, "is_symlink": False, "group_writable": False,
     "world_writable": False, "sticky": False, "writable_by_us": False,
     "realpath": "/"},
]
v = classify(VM24_CHAIN, uid=1000)
check("as it stands today, the chain is refused", 1, len(v["problems"]))
check("and the refusal is the writable parent", True,
      "woa23-s2-package-clone is writable" in v["problems"][0])
check("the clone root itself is not the problem", False,
      any("clone root" in p for p in v["problems"]))
# /home/odbadmin must stay writable — the account needs it. So even after the parent
# is fixed, the path can be re-pointed by this account and that is why the manifest
# is re-verified rather than the modes being trusted.
check("the home directory is recorded as exposure, not refused", 1,
      len(v["residual_exposure"]))
check("and it is the home directory", True, "/home/odbadmin" in v["residual_exposure"][0])

after_chmod = [dict(link) for link in VM24_CHAIN]
after_chmod[1].update({"mode": "dr-xr-xr-x", "mode_octal": "0o555",
                       "group_writable": False, "writable_by_us": False})
v2 = classify(after_chmod, uid=1000)
check("chmod a-w on the parent clears the refusal", [], v2["problems"])
check("but the home-directory exposure remains", 1, len(v2["residual_exposure"]))
check("so the fix is necessary and not sufficient", True,
      not v2["problems"] and bool(v2["residual_exposure"]))

check("a path that cannot be read fails closed", True,
      any("cannot be read" in p
          for p in classify([describe("/no/such/path/anywhere")])["problems"]))
check("an empty chain is a problem, not a pass", True,
      bool(classify([])["problems"]))


# ------------------------------------------------------------------- CLI ---
print()
print("the CLI fails closed and records both halves")
with tempfile.TemporaryDirectory() as td:
    root = Path(td) / "clone-root"
    root.mkdir()
    dist = build_clone(root, FILES)
    man = str(root / "clone.manifest")
    repo = str(Path(__file__).resolve().parent.parent)
    os.chmod(dist, 0o555)
    os.chmod(root, 0o555)

    def cli(*extra, out=None):
        cmd = [sys.executable, "-m", "bench.clone_integrity",
               "--clone", str(dist), "--manifest", man, *extra]
        if out:
            cmd += ["--out", str(out)]
        return subprocess.run(cmd, cwd=repo, capture_output=True, text=True)

    out = Path(td) / "rec.json"
    r = cli("--stage", "preflight", out=out)
    check("a clean clone with an unwritable parent exits 0", 0, r.returncode)
    rec = json.loads(out.read_text())
    check("the stage is recorded", "preflight", rec["stage"])
    check("the ancestors are recorded", True, len(rec["ancestors"]) >= 2)
    check("the manifest verification is recorded", True,
          rec["manifest_verification"]["ok"])
    check("no problems", [], rec["problems"])
    check("the detection limit travels with the record", True,
          "DETECTION, not immutability" in rec["detection_limit"])
    check("and is printed", True, "LIMIT" in r.stdout)
    check("the limit says the window is not zero", True,
          "not zero" in rec["detection_limit"])

    r = cli("--chain-only")
    check("--chain-only skips the re-hash", 0, r.returncode)
    check("and does not claim a verification it did not do", False,
          "MATCH" in r.stdout)

    os.chmod(root, 0o775)
    r = cli("--stage", "before-reference")
    check("a writable parent fails the check", 1, r.returncode)
    check("and says integrity is not established", True,
          "clone integrity is not established" in r.stderr)

    os.chmod(root, 0o555)
    os.chmod(dist, 0o755)
    (dist / "added.py").write_bytes(b"# added\n")
    os.chmod(dist, 0o555)
    r = cli("--stage", "before-candidate")
    check("a tree that drifted from its manifest fails", 1, r.returncode)
    check("and says so in those terms", True,
          "does not match its manifest" in r.stderr)

    os.chmod(dist, 0o755)
    (dist / "added.py").unlink()
    os.chmod(root, 0o755)


print()
raise SystemExit(summary(PASS, FAIL))