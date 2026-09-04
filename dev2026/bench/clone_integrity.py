"""Is the package clone actually immutable, or only unwritable at its own inode?

`dist/` is mode 555 and every manifest artefact is 444, and that was reported as a
read-only clone. It is not the same thing. **Unlinking a file needs write permission
on its directory, not on the file**, so a writable parent lets the whole tree be
renamed and a different one put in its place — under the same path, passing the same
`[ -w "$PKG_CLONE" ]` check, with 444 modes throughout. The contents of `dist/` are
genuinely protected; the *binding of the path to those contents* is not.

Two things follow, and this module does both.

**The ancestor chain is inspected, not assumed.** Every directory from the clone root
up to `/` is recorded with its mode, owner and whether this user can write to it, and
the immediate parent being writable is a **refusal**, not a note. Higher ancestors are
recorded as residual exposure rather than refused, because `$HOME` must be writable
for the account to function and a rule that refuses on it can never be satisfied.

**The manifest is re-verified against the tree, immediately before it is used.** A
mode check answers "could this be replaced?"; only re-hashing answers "is this still
the tree that was verified?". That converts prevention into detection with a bounded
window, and the window is stated rather than glossed: between the last verification
and the moment a worker actually opens a file, the path can still be re-pointed by
anyone who can write to the parent. Detection is weaker than immutability. It is what
is available without changing permissions on the host, and the difference is the
point of §7a.3c.

    uv run python -m bench.clone_integrity --clone /path/dist \\
        --manifest /path/clone.manifest --out results/x_clone_integrity.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import time
from pathlib import Path


def describe(path: str) -> dict:
    """One link in the ancestor chain, as it actually is on disk."""
    out: dict = {"path": path}
    try:
        st = os.lstat(path)
    except OSError as exc:
        out["error"] = repr(exc)
        return out
    out.update({
        "is_symlink": stat.S_ISLNK(st.st_mode),
        "is_dir": stat.S_ISDIR(st.st_mode),
        "mode": stat.filemode(st.st_mode),
        "mode_octal": oct(stat.S_IMODE(st.st_mode)),
        "uid": st.st_uid,
        "gid": st.st_gid,
        "group_writable": bool(st.st_mode & stat.S_IWGRP),
        "world_writable": bool(st.st_mode & stat.S_IWOTH),
        "sticky": bool(st.st_mode & stat.S_ISVTX),
        "writable_by_us": os.access(path, os.W_OK),
        "realpath": os.path.realpath(path),
    })
    return out


def ancestor_chain(clone: str) -> list[dict]:
    """The clone root and every directory above it, resolved, nearest first.

    Walks the **realpath**: a symlinked ancestor whose target is writable is exactly
    the case a lexical walk would miss, and it is the same class of mistake that let
    a symlinked `--workdir` past the production boundary once already.
    """
    chain, seen = [], set()
    p = os.path.realpath(clone)
    while True:
        if p in seen:
            break
        seen.add(p)
        chain.append(describe(p))
        parent = os.path.dirname(p)
        if parent == p:
            break
        p = parent
    return chain


def classify(chain: list[dict], uid: int | None = None) -> dict:
    """Which links are refusals, which are exposure to be recorded and lived with.

    The distinction is not severity, it is remediability. A writable *immediate*
    parent is fixable with one `chmod` and until it is fixed the immutability claim
    is simply false. A writable `$HOME` three levels up is not fixable — the account
    needs it — so refusing on it would make the check unsatisfiable, and an
    unsatisfiable check gets disabled rather than passed.
    """
    uid = os.getuid() if uid is None else uid
    problems: list[str] = []
    exposures: list[str] = []

    if not chain:
        return {"problems": ["the ancestor chain could not be read at all"],
                "residual_exposure": [], "chain": chain}

    for i, link in enumerate(chain):
        if "error" in link:
            problems.append(f"{link['path']}: cannot be read ({link['error']}), so it "
                            f"cannot be shown not to be writable")
            continue
        where = "the clone root" if i == 0 else \
                "the clone's parent directory" if i == 1 else \
                f"an ancestor {i} levels up"

        if link["world_writable"] and not link["sticky"]:
            problems.append(f"{link['path']} is world-writable ({link['mode']}) and "
                            f"not sticky — {where}")
        if link["uid"] != uid and link["group_writable"]:
            problems.append(f"{link['path']} is group-writable ({link['mode']}) and "
                            f"owned by uid {link['uid']}, not {uid} — {where}")

        if i == 0 and link["writable_by_us"]:
            problems.append(
                f"the clone root {link['path']} is writable by this user "
                f"({link['mode']}); it must not be")
        elif i == 1 and link["writable_by_us"]:
            problems.append(
                f"the clone's parent {link['path']} is writable by this user "
                f"({link['mode']}). Unlinking needs write permission on the "
                f"DIRECTORY, not the file, so the whole clone can be renamed and "
                f"replaced under the same path while every mode inside it stays "
                f"read-only. Make it unwritable (chmod a-w) before relying on the "
                f"clone being immutable — that is a write action on the host and "
                f"needs its own authorisation.")
        elif i > 1 and link["writable_by_us"]:
            exposures.append(
                f"{link['path']} ({link['mode']}) is writable by this user — {where}. "
                f"Recorded, not refused: a home directory must be writable, so a rule "
                f"that refused here could never be satisfied. It means the clone's "
                f"path can still be re-pointed by this account.")
    return {"problems": problems, "residual_exposure": exposures, "chain": chain}


_SHA256SUM_LINE = re.compile(r"^[0-9a-fA-F]{64} [ *]\S")


def _wrong_file_diagnosis(path: str, line: str) -> str | None:
    """Name the mistake if this looks like a file someone would pass by mistake.

    One file has actually been passed here in place of the manifest, and it is the
    obvious one to reach for: the clone root holds both `clone.manifest` and a
    `SHA256SUMS` that is a sha256sum-style digest list of the manifest FILES. The
    names are similar, `SHA256SUMS` is the conventional name for exactly this kind
    of check elsewhere, and `run_c2_cycles.sh`'s usage example pointed at a
    `manifest/SHA256SUMS` path that does not exist.

    "expected 4 tab-separated fields, got 1" is true and tells the reader nothing
    about which file to use instead. This does.
    """
    if _SHA256SUM_LINE.match(line):
        return (
            f"{path} is a sha256sum-style digest list (`<sha256>  <name>`), not a "
            f"package manifest. It records digests of a few named FILES; it does "
            f"not list the clone's contents, so nothing in it can verify a tree. "
            f"The manifest is the four-column file written when the clone was "
            f"built — normally <clone-root>/clone.manifest — whose columns are "
            f"relpath, sha256, size, mtime_ns, tab-separated. Pass that.")
    return None


def read_manifest(path: str) -> tuple[dict, str | None]:
    """`relpath\\tsha256\\tsize\\tmtime_ns` per line."""
    want: dict = {}
    try:
        with open(path, "rb") as fh:
            for n, line in enumerate(fh, 1):
                text = line.decode().rstrip("\n")
                parts = text.split("\t")
                if len(parts) != 4:
                    diagnosis = _wrong_file_diagnosis(path, text)
                    if diagnosis:
                        return {}, diagnosis
                    return {}, (f"{path}:{n}: expected 4 tab-separated fields "
                                f"(relpath, sha256, size, mtime_ns), got "
                                f"{len(parts)}. This is not the manifest written "
                                f"when the clone was built; that file is normally "
                                f"<clone-root>/clone.manifest.")
                rel, digest, size, mtime = parts
                try:
                    want[rel] = (digest, int(size), int(mtime))
                except ValueError:
                    return {}, f"{path}:{n}: size/mtime are not integers"
    except OSError as exc:
        return {}, f"cannot read {path}: {exc!r}"
    if not want:
        return {}, f"{path} lists no files; an empty manifest matches every tree"
    return want, None


def verify_manifest(clone: str, manifest: str, *, sample: int = 5) -> dict:
    """Re-hash every file in the clone and compare it with the manifest.

    Every file, not a sample. A sampled check answers a question nobody asked: the
    thing being guarded against is a substituted tree, and a substituted tree matches
    a sample as easily as it matches nothing.
    """
    t0 = time.monotonic()
    want, err = read_manifest(manifest)
    if err:
        return {"ok": False, "error": err}

    seen: set[str] = set()
    bad_digest: list[str] = []
    bad_size: list[str] = []
    bad_mtime: list[str] = []
    unreadable: list[str] = []
    total_bytes = 0

    for dirpath, dirnames, filenames in os.walk(clone):
        dirnames.sort()
        for fn in sorted(filenames):
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, clone)
            seen.add(rel)
            entry = want.get(rel)
            if entry is None:
                continue
            w_digest, w_size, w_mtime = entry
            try:
                st = os.lstat(full)
                h = hashlib.sha256()
                with open(full, "rb") as f:
                    for chunk in iter(lambda: f.read(1 << 20), b""):
                        h.update(chunk)
            except OSError as exc:
                unreadable.append(f"{rel}: {exc!r}")
                continue
            total_bytes += st.st_size
            if h.hexdigest() != w_digest:
                bad_digest.append(rel)
            if st.st_size != w_size:
                bad_size.append(rel)
            if st.st_mtime_ns != w_mtime:
                bad_mtime.append(rel)

    missing = sorted(set(want) - seen)
    extra = sorted(seen - set(want))
    ok = not (missing or extra or bad_digest or bad_size or bad_mtime or unreadable)
    return {
        "ok": ok,
        "manifest": manifest,
        "manifest_sha256": hashlib.sha256(Path(manifest).read_bytes()).hexdigest(),
        "clone": clone,
        "n_manifest_entries": len(want),
        "n_files_on_disk": len(seen),
        "n_missing": len(missing), "missing": missing[:sample],
        "n_extra": len(extra), "extra": extra[:sample],
        "n_digest_mismatch": len(bad_digest), "digest_mismatch": bad_digest[:sample],
        "n_size_mismatch": len(bad_size), "size_mismatch": bad_size[:sample],
        "n_mtime_mismatch": len(bad_mtime), "mtime_mismatch": bad_mtime[:sample],
        "n_unreadable": len(unreadable), "unreadable": unreadable[:sample],
        "bytes_hashed": total_bytes,
        "elapsed_s": round(time.monotonic() - t0, 2),
    }


DETECTION_LIMIT = (
    "This is DETECTION, not immutability. It establishes that the tree matched the "
    "manifest at the moment it was checked. If any ancestor is writable by this user, "
    "the path can be re-pointed after the check and before a worker opens a file; the "
    "window is bounded by re-verifying immediately before each arm starts, and it is "
    "not zero. Only making the ancestors unwritable removes it."
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--clone", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--stage", default="preflight",
                    help="what this check is guarding, e.g. preflight, before-reference")
    ap.add_argument("--chain-only", action="store_true",
                    help="inspect the ancestors and skip the re-hash")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    chain = ancestor_chain(args.clone)
    verdict = classify(chain)
    record = {"kind": "clone_integrity", "stage": args.stage,
              "checked_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
              "uid": os.getuid(),
              "ancestors": verdict["chain"],
              "problems": list(verdict["problems"]),
              "residual_exposure": verdict["residual_exposure"],
              "detection_limit": DETECTION_LIMIT}

    if not args.chain_only:
        v = verify_manifest(args.clone, args.manifest)
        record["manifest_verification"] = v
        if not v.get("ok"):
            if "error" in v:
                record["problems"].append(f"manifest: {v['error']}")
            else:
                record["problems"].append(
                    f"the clone does not match its manifest: {v['n_missing']} missing, "
                    f"{v['n_extra']} extra, {v['n_digest_mismatch']} content, "
                    f"{v['n_size_mismatch']} size, {v['n_mtime_mismatch']} mtime, "
                    f"{v['n_unreadable']} unreadable")

    print(f"clone integrity [{args.stage}]  {args.clone}")
    for link in record["ancestors"]:
        print(f"  {link.get('mode', '?????'):11s} uid={link.get('uid', '?'):<6} "
              f"writable={str(link.get('writable_by_us')):5s}  {link['path']}")
    v = record.get("manifest_verification")
    if v and "error" not in v:
        print(f"  manifest {v['n_manifest_entries']} entries vs {v['n_files_on_disk']} "
              f"files; {v['bytes_hashed']:,} bytes in {v['elapsed_s']}s -> "
              f"{'MATCH' if v['ok'] else 'MISMATCH'}")
    for e in record["residual_exposure"]:
        print(f"  EXPOSURE {e}")
    print(f"  LIMIT {DETECTION_LIMIT}")

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(record, indent=2))

    if record["problems"]:
        print("clone integrity is not established:", file=sys.stderr)
        for p in record["problems"]:
            print(f"  - {p}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
