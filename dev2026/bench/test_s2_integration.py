"""S2 end to end, offline: the real gate, over real HTTP, on the real artefacts.

Two authorised C1 runs died before the contract gate, both on wiring that every
offline test walked past — argparse in one case, a dead assignment inside a shell
heredoc in the other. Unit tests on the pieces did not help, because the pieces were
right. What was missing was a test that goes from the provenance records a run
actually writes, through the gate the runner actually invokes, to a MATCH/DIFFER
verdict, without a host.

So this test:

- uses the **real** `c1_meta_candidate.json`, `c1_meta_reference.json` and
  `c1_environment.json` written by the 2026-08-09 run on VM24, not hand-built ones;
- **materialises what the fixtures reference** rather than filtering complaints away.
  The records name staging directories on VM24, so the arms' source trees are
  rebuilt here and the digests re-recorded from the files actually written. Nothing
  is skipped: `validate_meta` re-hashes every file and compares, for real. The only
  mapping is *where* the trees live, which is a property of the machine and not of
  the records;
- runs `compare_arms(..., s2=True)` and requires **zero** problems;
- serves the 64 contract cases from two loopback HTTP servers and runs
  `bench.contract_diff` as the runner invokes it — **with the argument list read out
  of `run_controlled.sh`**, so a change to the runner's flags changes this test;
- checks the gate reaches PASS with matching arms and FAIL naming the case when one
  response differs.

    uv run python -m bench.test_s2_integration
"""

import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.contract_cases import all_cases          # noqa: E402
from bench.provenance import compare_arms            # noqa: E402

PASS = 0
FAIL = 0
HERE = Path(__file__).resolve().parent.parent
FIX = HERE / "bench" / "fixtures"


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


# --------------------------------------------------------------- the fixtures ---
def stage_arms(root: Path) -> tuple[dict, dict, dict]:
    """Rebuild both arms' source trees here and re-record their digests.

    The manifests are `api/**/*.py` for the candidate and `woa23_app.py` plus
    `src/**/*.py` for the reference. The candidate's files are this repository's own
    and are copied verbatim. The reference's live only on VM24, so stand-ins are
    written under the same names — and then the recorded digests are recomputed from
    what was written, so `validate_meta` does a genuine re-hash-and-compare rather
    than being told to skip.

    What is mapped: the directory the trees live in. What is not: any validation
    result.
    """
    cand = json.loads((FIX / "c1_meta_candidate.json").read_text())
    ref = json.loads((FIX / "c1_meta_reference.json").read_text())
    env = json.loads((FIX / "c1_environment.json").read_text())

    cdir = root / "candidate"
    (cdir / "api").mkdir(parents=True)
    for src in sorted((HERE / "api").glob("*.py")):
        shutil.copy2(src, cdir / "api" / src.name)

    rdir = root / "reference"
    (rdir / "src").mkdir(parents=True)
    (rdir / "woa23_app.py").write_text('zarr_store_path = "data/"\n')
    for name in ("__init__.py", "config.py", "dask_client_manager.py",
                 "woa23_utils.py"):
        (rdir / "src" / name).write_text(f"# stand-in for src/{name}\n")

    for meta, base in ((cand, cdir), (ref, rdir)):
        meta["cwd"] = str(base)
        digests = {}
        for path in sorted(base.rglob("*.py")):
            digests[str(path.relative_to(base))] = hashlib.sha256(
                path.read_bytes()).hexdigest()
        meta["source_sha256"] = digests
    return cand, ref, env


# ------------------------------------------------------------- the fake arms ---
def body_for(case, arm: str, differ_on: str | None) -> bytes:
    """A deterministic body per case. Both arms agree unless told otherwise."""
    if case.expect_status != 200:
        return json.dumps({"detail": f"error for {case.id}"}).encode()
    rows = [{"lon": 135.5, "lat": 15.5, "depth": float(d), "time_period": 0,
             "temperature_an": 1.0 + d}
            for d in (0, 10)]
    if differ_on == case.id and arm == "candidate":
        rows[1]["temperature_an"] = 99.0
    if case.path.endswith("/csv"):
        head = ",".join(rows[0])
        lines = [head] + [",".join(str(r[k]) for k in rows[0]) for r in rows]
        return ("\n".join(lines) + "\n").encode()
    if case.path.endswith("openapi.json"):
        return json.dumps({"openapi": "3.1.0", "info": {"title": "woa23"}}).encode()
    return json.dumps(rows).encode()


def make_server(arm: str, differ_on: str | None):
    by_key = {}
    for case in all_cases():
        key = (case.path, tuple(sorted((k, str(v)) for k, v in case.params.items())))
        by_key[key] = (case.expect_status, body_for(case, arm, differ_on),
                       "text/csv" if case.path.endswith("/csv") else "application/json")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            from urllib.parse import parse_qsl, urlparse
            u = urlparse(self.path)
            key = (u.path, tuple(sorted(parse_qsl(u.query))))
            status, body, ctype = by_key.get(
                key, (404, json.dumps({"detail": "no such case"}).encode(),
                      "application/json"))
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


# ------------------------------------------- the gate, as the runner calls it ---
def runner_contract_argv() -> list[str]:
    """The contract_diff invocation, read out of run_controlled.sh.

    Not a copy. If the runner's flags change, this test runs the changed flags —
    which is the whole point, because both failed C1 attempts were wiring the tests
    never executed.
    """
    src = (HERE / "scripts" / "run_controlled.sh").read_text()
    m = re.search(r"^uv run python -m bench\.contract_diff \\\n((?:.*\\\n)*.*)$",
                  src, re.M)
    if not m:
        raise SystemExit("cannot find the contract_diff invocation in the runner")
    block = "uv run python -m bench.contract_diff " + m.group(1)
    block = block.split("\n  || ")[0].replace("\\\n", " ")
    # The invocation ends in a line continuation before the `|| { … }` clause, and
    # splitting on the clause leaves that backslash behind as an argument.
    return [a for a in block.split() if a != "\\"]


print("the fixtures are the artefacts a real run wrote")
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    cand, ref, env = stage_arms(root)
    check("the candidate record names production's interpreter",
          "/home/odbadmin/.pyenv/versions/py311/bin/python3.11", cand["env_python"])
    check("there is no lockfile digest anywhere in it", None,
          cand["dependencies"].get("lockfile_sha256"))
    check("the clone manifest digest is the verified one",
          "f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4",
          cand["dependencies"]["clone_manifest_sha256"])
    check("the package-tree digest is section 4.1.2b's",
          "b8754d32c8aaec6d2049de5d67d3f81aeff4f19effd1525d76b62955447c9b4b",
          cand["dependencies"]["package_tree_digest"])
    check("and so is the runtime-distribution digest",
          "a26ca6c3cfe20ea643c30075d910bb03dbbc01eb3f9d2b4fd224b5d76701447b",
          cand["dependencies"]["runtime_distribution_digest"])
    check("the name-keyed digest is recorded under its own name, and is the old one",
          "60236d7210c8c3647a32e7da55714e246ecc878d1d6296d10e9caf966d2b0b2a",
          cand["dependencies"]["name_version_set_sha256"])
    check("it is not called the runtime distribution digest", True,
          cand["dependencies"]["name_version_set_sha256"]
          != cand["dependencies"]["runtime_distribution_digest"])

    print()
    print("compare_arms(s2=True) passes with NOTHING filtered out")
    problems = compare_arms(cand, ref, env, s2=True)
    check("zero problems", [], problems)
    check("and the source re-hash really ran", True,
          len(cand["source_sha256"]) >= 4 and len(ref["source_sha256"]) >= 5)

    # Proof the re-hash is live: change a staged file and the same call must object.
    victim = root / "candidate" / "api" / "query.py"
    original = victim.read_bytes()
    victim.write_bytes(original + b"\n# edited after provenance was collected\n")
    check("editing a source file after collection is caught", True,
          any("does not match its recorded digest" in p
              for p in compare_arms(cand, ref, env, s2=True)))
    victim.write_bytes(original)
    check("restoring it clears the problem", [],
          compare_arms(cand, ref, env, s2=True))

    print()
    print("S2 does not require a lockfile; D2b does")
    check("the S2 gate is clean without one", [],
          compare_arms(cand, ref, env, s2=True))
    d2b = compare_arms(cand, ref, env, s2=False)
    check("the D2b gate objects", True, any("lockfile" in p for p in d2b))

    print()
    print("the 5.2A gate runs over 64 cases, as the runner invokes it")
    (root / "results").mkdir()
    cand_path = root / "results" / "c1_meta_candidate.json"
    ref_path = root / "results" / "c1_meta_reference.json"
    cand_path.write_text(json.dumps(cand))
    ref_path.write_text(json.dumps(ref))

    argv_template = runner_contract_argv()
    check("the runner's invocation was found in the script", True,
          "bench.contract_diff" in " ".join(argv_template))
    check("it passes --variant", True, "--variant" in argv_template)
    check("no stray line-continuation survived the extraction", False,
          "\\" in argv_template)
    check("and passes both provenance records", True,
          "--candidate-meta" in argv_template and "--reference-meta" in argv_template)

    def run_gate(differ_on, out_name):
        csrv, curl = make_server("candidate", differ_on)
        rsrv, rurl = make_server("reference", differ_on)
        try:
            subst = {
                '"http://127.0.0.1:${CAND_PORT}"': curl,
                '"http://127.0.0.1:${REF_PORT}"': rurl,
                '"$VARIANT"': "5.2A",
                '"$SEED_POLICY"': "both-pinned",
                '"results/${LABEL}_meta_candidate.json"': str(cand_path),
                '"results/${LABEL}_meta_reference.json"': str(ref_path),
                '"results/${LABEL}_contract.json"': str(root / "results" / out_name),
            }
            argv = [subst.get(a, a) for a in argv_template]
            # `uv run python -m X` -> `<this python> -m X`; only the three leading
            # words are rewritten, never a later argument that happens to match.
            assert argv[:3] == ["uv", "run", "python"], argv[:3]
            argv = [sys.executable] + argv[3:]
            assert "5.2A" in argv, argv
            r = subprocess.run(argv, cwd=str(HERE), capture_output=True, text=True)
            out = root / "results" / out_name
            if not out.exists():
                raise SystemExit(f"gate wrote no artefact (rc={r.returncode})\n"
                                 f"stdout:\n{r.stdout[-1500:]}\n"
                                 f"stderr:\n{r.stderr[-1500:]}")
            return r, json.loads(out.read_text())
        finally:
            csrv.shutdown(); rsrv.shutdown()

    r, payload = run_gate(None, "c1_contract.json")
    check("the gate exits 0 when the arms agree", 0, r.returncode)
    check("it reports PASS", "PASS", payload["gate"])
    check("over all 64 cases", 64, len(payload["results"]))
    check("every case MATCHed", 64,
          sum(1 for x in payload["results"] if x["verdict"] == "MATCH"))
    check("no case DIFFERed", 0,
          sum(1 for x in payload["results"] if x["verdict"] == "DIFFER"))
    check("request order was counterbalanced", (32, 32),
          (payload["request_order_counts"]["RC"], payload["request_order_counts"]["CR"]))
    check("the variant is recorded", "5.2A", payload["variant"])
    check("both provenance records are embedded", True,
          bool(payload["candidate_meta"]) and bool(payload["reference_meta"]))
    check("order fingerprints were recorded per case", True,
          all("candidate_order" in x and "reference_order" in x
              for x in payload["results"]))

    victim_id = next(c.id for c in all_cases() if c.expect_status == 200
                     and not c.path.endswith("openapi.json"))
    r, payload = run_gate(victim_id, "c1_contract_differ.json")
    check("the gate exits non-zero when one case differs", 1, r.returncode)
    check("it reports FAIL", "FAIL", payload["gate"])
    check("exactly one case DIFFERs", 1,
          sum(1 for x in payload["results"] if x["verdict"] == "DIFFER"))
    check("and it is the one that was altered", victim_id,
          next(x["id"] for x in payload["results"] if x["verdict"] == "DIFFER"))
    check("the other 63 still MATCH", 63,
          sum(1 for x in payload["results"] if x["verdict"] == "MATCH"))
    check("the difference is localised for a human", True,
          bool(next(x["notes"] for x in payload["results"]
                    if x["verdict"] == "DIFFER")))

    print()
    print("C2: the same gate, over unpinned arms, with the 5.2B policy")
    # The C1 path above is not enough on its own. Every fixture in it is pinned, so
    # the seed rule is never exercised in the arrangement C2 actually uses — both
    # arms ours, both unset — which is precisely how a pinned-only rule reached a
    # real C2 cycle and stopped it before the contract.
    c2c = json.loads((FIX / "c2_cycle1_meta_candidate.json").read_text())
    c2r = json.loads((FIX / "c2_cycle1_meta_reference.json").read_text())
    c2e = json.loads((FIX / "c2_cycle1_environment.json").read_text())
    for meta, base in ((c2c, root / "candidate"), (c2r, root / "reference")):
        meta["cwd"] = str(base)
        meta["source_sha256"] = {
            str(p2.relative_to(base)): hashlib.sha256(p2.read_bytes()).hexdigest()
            for p2 in sorted(base.rglob("*.py"))}
    check("compare_arms accepts the unpinned arms under both-unpinned", [],
          compare_arms(c2c, c2r, c2e, s2=True, seed_policy="both-unpinned"))

    c2c_path = root / "results" / "c2_meta_candidate.json"
    c2r_path = root / "results" / "c2_meta_reference.json"
    c2c_path.write_text(json.dumps(c2c)); c2r_path.write_text(json.dumps(c2r))

    def run_c2_gate(policy, out_name, differ_on=None):
        csrv, curl = make_server("candidate", differ_on)
        rsrv, rurl = make_server("reference", differ_on)
        try:
            subst = {'"http://127.0.0.1:${CAND_PORT}"': curl,
                     '"http://127.0.0.1:${REF_PORT}"': rurl,
                     '"$VARIANT"': "5.2B",
                     '"$SEED_POLICY"': policy,
                     '"results/${LABEL}_meta_candidate.json"': str(c2c_path),
                     '"results/${LABEL}_meta_reference.json"': str(c2r_path),
                     '"results/${LABEL}_contract.json"': str(root / "results" / out_name)}
            argv = [subst.get(a, a) for a in argv_template]
            argv = [sys.executable] + argv[3:]
            r = subprocess.run(argv, cwd=str(HERE), capture_output=True, text=True)
            out = root / "results" / out_name
            return r, (json.loads(out.read_text()) if out.exists() else None)
        finally:
            csrv.shutdown(); rsrv.shutdown()

    check("the runner passes a seed policy to the gate", True,
          '"$SEED_POLICY"' in argv_template)

    r, payload = run_c2_gate("both-unpinned", "c2_contract.json")
    check("the 5.2B gate runs with unpinned arms", 0, r.returncode)
    check("and passes", "PASS", payload["gate"])
    check("over 64 cases", 64, len(payload["results"]))
    check("all semantic MATCH", 64,
          sum(1 for x in payload["results"] if x["verdict"] == "MATCH"))
    check("the variant is 5.2B", "5.2B", payload["variant"])
    check("and the policy is recorded in the artefact", "both-unpinned",
          payload["seed_policy"])
    # Every 200 that is a row payload carries a row-order digest; the OpenAPI
    # document is a 200 with no rows and correctly carries none. Asserting "every
    # 200" would have been asserting a fabricated ordering into existence.
    rowed = [x for x in payload["results"]
             if x["reference_status"] == 200 and x["candidate_order"]["n_rows"] is not None]
    orderless = [x for x in payload["results"]
                 if x["reference_status"] == 200 and x["candidate_order"]["n_rows"] is None]
    check("every row-bearing 200 has a row-order digest", True,
          bool(rowed) and all(x["candidate_order"]["row_order_sha256"] is not None
                              for x in rowed))
    check("and the non-row 200s have none, rather than a fabricated one", True,
          all(x["candidate_order"]["row_order_sha256"] is None for x in orderless))
    check("both kinds are present, so neither branch is untested", True,
          bool(rowed) and bool(orderless))

    # The failure that actually happened, reproduced through the real gate.
    r, payload = run_c2_gate("both-pinned", "c2_contract_wrongpolicy.json")
    check("under both-pinned the same run is refused", True, r.returncode != 0)
    check("naming PYTHONHASHSEED", True, "PYTHONHASHSEED" in (r.stdout + r.stderr))
    check("and no contract artefact is produced", None, payload)

    r, payload = run_c2_gate("both-unpinned", "c2_contract_differ.json",
                             differ_on=victim_id)
    check("a real difference still fails under 5.2B", 1, r.returncode)
    check("and is reported", "FAIL", payload["gate"])

    # 5.2B's own arrangement, through the real gate, so the fix to C2 cannot have
    # regressed it. The shape is assembled from real records: C1's pinned candidate
    # against C2's unpinned reference, which is what a live-production reference
    # looks like.
    c1c_5b = json.loads(cand_path.read_text())          # pinned candidate
    live_path = root / "results" / "live_meta_reference.json"
    live_path.write_text(json.dumps(c2r))               # unpinned reference
    csrv, curl = make_server("candidate", None)
    rsrv, rurl = make_server("reference", None)
    try:
        subst = {'"http://127.0.0.1:${CAND_PORT}"': curl,
                 '"http://127.0.0.1:${REF_PORT}"': rurl,
                 '"$VARIANT"': "5.2B",
                 '"$SEED_POLICY"': "reference-unpinned",
                 '"results/${LABEL}_meta_candidate.json"': str(cand_path),
                 '"results/${LABEL}_meta_reference.json"': str(live_path),
                 '"results/${LABEL}_contract.json"': str(root / "results" / "live.json")}
        argv = [subst.get(a, a) for a in argv_template]
        argv = [sys.executable] + argv[3:]
        r5b = subprocess.run(argv, cwd=str(HERE), capture_output=True, text=True)
    finally:
        csrv.shutdown(); rsrv.shutdown()
    live = json.loads((root / "results" / "live.json").read_text())
    check("5.2B against an unpinned reference still passes — no regression", 0,
          r5b.returncode)
    # And a PINNED reference under the same policy, because the name says otherwise.
    pinref_path = root / "results" / "pinned_ref.json"
    pinref_path.write_text(json.dumps(json.loads(ref_path.read_text())))
    csrv2, curl2 = make_server("candidate", None)
    rsrv2, rurl2 = make_server("reference", None)
    try:
        subst = {'"http://127.0.0.1:${CAND_PORT}"': curl2,
                 '"http://127.0.0.1:${REF_PORT}"': rurl2,
                 '"$VARIANT"': "5.2B",
                 '"$SEED_POLICY"': "reference-unpinned",
                 '"results/${LABEL}_meta_candidate.json"': str(cand_path),
                 '"results/${LABEL}_meta_reference.json"': str(pinref_path),
                 '"results/${LABEL}_contract.json"': str(root / "results" / "pinref.json")}
        argv = [sys.executable] + [subst.get(a, a) for a in argv_template][3:]
        rpin = subprocess.run(argv, cwd=str(HERE), capture_output=True, text=True)
    finally:
        csrv2.shutdown(); rsrv2.shutdown()
    check("reference-unpinned also accepts a PINNED reference", 0, rpin.returncode)
    check("so the name is not a requirement on the reference", True,
          "MAY be either" in rpin.stdout)
    check("with its own policy recorded", "reference-unpinned", live["seed_policy"])
    check("over 64 cases", 64, len(live["results"]))
    # Not "PYTHONHASHSEED does not appear" — the gate now prints the policy's
    # meaning, which names it. The question is whether it was reported as a problem.
    check("and no seed problem was raised", True,
          "must be" not in r5b.stderr and "requires it to be unset" not in r5b.stderr)
    check("the run reached the cases rather than stopping at provenance", 64,
          len(live["results"]))

    print()
    print("the fourth corner — unpinned candidate, pinned reference — through the gate")
    # No campaign produces this on purpose. It is what a half-applied change looks
    # like: C2's launch on one arm and C1's on the other. Every policy must refuse
    # it, and the gate must refuse it before sampling anything.
    mixed_c = root / "results" / "mixed_meta_candidate.json"
    mixed_r = root / "results" / "mixed_meta_reference.json"
    mixed_c.write_text(json.dumps(c2c))      # unpinned candidate
    mixed_r.write_text(json.dumps(c1r_fixed := json.loads(ref_path.read_text())))
    for policy in ("both-pinned", "reference-unpinned", "both-unpinned"):
        csrv3, curl3 = make_server("candidate", None)
        rsrv3, rurl3 = make_server("reference", None)
        try:
            out_p = root / "results" / f"mixed_{policy}.json"
            subst = {'"http://127.0.0.1:${CAND_PORT}"': curl3,
                     '"http://127.0.0.1:${REF_PORT}"': rurl3,
                     '"$VARIANT"': "5.2B" if policy != "both-pinned" else "5.2A",
                     '"$SEED_POLICY"': policy,
                     '"results/${LABEL}_meta_candidate.json"': str(mixed_c),
                     '"results/${LABEL}_meta_reference.json"': str(mixed_r),
                     '"results/${LABEL}_contract.json"': str(out_p)}
            argv = [sys.executable] + [subst.get(a, a) for a in argv_template][3:]
            rm = subprocess.run(argv, cwd=str(HERE), capture_output=True, text=True)
        finally:
            csrv3.shutdown(); rsrv3.shutdown()
        check(f"the gate refuses the mixed shape under {policy}", True,
              rm.returncode != 0)
        check(f"  naming PYTHONHASHSEED under {policy}", True,
              "PYTHONHASHSEED" in (rm.stdout + rm.stderr))
        check(f"  and writing no artefact under {policy}", False, out_p.exists())

    print()
    print("the two layers guard different things, in the runner's order")
    # A disagreeing name==version set between the arms is NOT contract_diff's job.
    # compare_arms catches it, and the runner runs compare_arms first — so the gate
    # is never reached. Asserting the gate catches it would have been asserting the
    # wrong layer, which is how the last two failures were built.
    drifted = json.loads(json.dumps(cand))
    drifted["dependencies"]["name_version_set_sha256"] = "z" * 64
    check("compare_arms catches an inter-arm environment drift", True,
          any("name==version set" in p
              for p in compare_arms(drifted, ref, env, s2=True)))

    runner_src = (HERE / "scripts" / "run_controlled.sh").read_text()
    check("and the runner calls compare_arms before the contract gate", True,
          runner_src.index("compare_arms(cand, ref, env")
          < runner_src.index("uv run python -m bench.contract_diff"))
    check("with the gate guarded by the comparison's exit status", True,
          "raise SystemExit(1)" in runner_src[
              runner_src.index("compare_arms(cand, ref, env"):
              runner_src.index("uv run python -m bench.contract_diff")])

    # What the gate itself refuses: arms pointed at different stores. This is the
    # check that belongs here, because only the gate sees both bodies.
    diverted = json.loads(json.dumps(cand))
    diverted["store_path"] = "/home/odbadmin/somewhere/else/data"
    div_path = root / "results" / "c1_meta_candidate_diverted.json"
    div_path.write_text(json.dumps(diverted))
    csrv, curl = make_server("candidate", None)
    rsrv, rurl = make_server("reference", None)
    try:
        subst = {'"http://127.0.0.1:${CAND_PORT}"': curl,
                 '"http://127.0.0.1:${REF_PORT}"': rurl,
                 '"$VARIANT"': "5.2A",
                 '"$SEED_POLICY"': "both-pinned",
                 '"results/${LABEL}_meta_candidate.json"': str(div_path),
                 '"results/${LABEL}_meta_reference.json"': str(ref_path),
                 '"results/${LABEL}_contract.json"': str(root / "results" / "x.json")}
        argv = [subst.get(a, a) for a in argv_template]
        argv = [sys.executable] + argv[3:]
        r = subprocess.run(argv, cwd=str(HERE), capture_output=True, text=True)
    finally:
        csrv.shutdown(); rsrv.shutdown()
    out = r.stdout + r.stderr
    check("the gate refuses arms pointed at different stores", True, r.returncode != 0)
    check("and says so before sampling anything", True, "store" in out.lower())
    check("no contract artefact was written from the refused run", False,
          (root / "results" / "x.json").exists())

print()
if FAIL:
    print(f"FAILED {FAIL}/{PASS + FAIL}")
    raise SystemExit(1)
print(f"all passed ({PASS} assertions)")
