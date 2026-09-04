"""F1-F5 end to end, offline: a real store, the real app, real HTTP, no host.

The S2 campaign has twice had a run die on wiring that every unit test walked past —
argparse in one case, a dead assignment inside a shell heredoc in the other. The
pieces were right; nothing went from end to end. So this does, for D1:

- builds a **synthetic Zarr store shaped like WOA23's**, with an annual Nutrients
  group reaching 5500 m and a seasonal one stopping at 800 m — the asymmetry in
  Table 4 that the whole depth pair rests on;
- runs the **real `bench.store_survey`** against it and requires P1-P6 to pass;
- serves the **real `api.app`** — the actual candidate, unmodified — from two
  loopback servers, one per arm;
- drives them with the **real `bench.d1_probe`**, and feeds the result to the **real
  `bench.d1_evidence`**;
- reads the runner's own `--d1` invocation out of `run_controlled.sh`, so a change
  to the runner's flags changes this test.

**What the responses are is not asserted.** This is a characterization pipeline, and
a test that pinned the status codes would pin exactly what the run exists to find
out. What is asserted is that the pipeline records them, that identical arms compare
equal, that a divergence is caught, and that an unmet precondition issues nothing.

    uv run python -m bench.test_d1_integration
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import numpy as np
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.d1_cases import (                                  # noqa: E402
    RECOVERY, STORE_PROBE_REQUESTS_PER_ARM, characterization_requests_per_arm,
    countable_requests_per_arm, depth_cases, request_total_range, scheduled,
)
from bench.d1_evidence import build                           # noqa: E402
from bench.d1_probe import run as probe_run                   # noqa: E402
from bench.store_survey import survey                         # noqa: E402
from bench.suite_summary import summary          # noqa: E402

PASS = 0
FAIL = 0
REPO = Path(__file__).resolve().parent.parent
RUNNER = REPO / "scripts" / "run_controlled.sh"


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


# ---------------------------------------------------------------- the fixture ---
def build_group(path, *, depths, params, periods):
    lon, lat = [134.5, 135.5, 136.5], [14.5, 15.5, 16.5]
    shape = (len(lon), len(lat), len(depths), len(params), len(periods))
    data = np.arange(np.prod(shape), dtype="float32").reshape(shape)
    xr.Dataset(
        {"an": (("lon", "lat", "depth", "parameters", "time_periods"), data),
         "mn": (("lon", "lat", "depth", "parameters", "time_periods"), data + 1)},
        coords={"lon": lon, "lat": lat, "depth": list(depths),
                "parameters": list(params), "time_periods": list(periods)},
    ).to_zarr(str(path), mode="w", consolidated=True)


def axis(n, lo, hi):
    return [lo + (hi - lo) * i / (n - 1) for i in range(n)]


def make_store(root):
    """WOA23's shape where it matters, per variable AND per climatology.

    The depth axes carry the real Table 4 counts — annual nitrate 102 levels over
    0-5500 m, seasonal nitrate 43 over 0-800 m — because the survey now compares each
    group against its own row. A token three-level axis would be a
    STORE_SCHEMA_MISMATCH here, which is correct and would stop the run.
    """
    store = Path(root)
    build_group(store / "1_degree" / "annual" / "TS",
                depths=axis(102, 0.0, 5500.0),
                params=["temperature", "salinity"], periods=["0"])
    build_group(store / "1_degree" / "annual" / "Nutrients",
                depths=axis(102, 0.0, 5500.0),       # Table 4: annual nitrate
                params=["nitrate", "phosphate", "silicate"], periods=["0"])
    build_group(store / "1_degree" / "seasonal" / "Nutrients",
                depths=axis(43, 0.0, 800.0),         # Table 4: seasonal nitrate
                params=["nitrate", "phosphate", "silicate"],
                periods=["13", "14", "15", "16"])
    return str(store)


# ------------------------------------------------------------------ the arms ---
def serve_app(store: str, cwd: str):
    """The real `api.app` on a loopback port, in its own process.

    A subprocess and not an in-process ASGI client: `api.config` validates the store
    at import and `api.app`'s lifespan opens the anchor group, and both of those are
    part of what is being exercised. Importing the module into this process once
    would run them once and never again.
    """
    port_holder = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
    port = port_holder.server_address[1]
    port_holder.server_close()
    env = dict(os.environ, WOA23_ZARR_STORE=store, PYTHONPATH=str(REPO),
               PYTHONHASHSEED="0")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "api.app:app", "--host", "127.0.0.1",
         "--port", str(port), "--log-level", "error"],
        cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    url = f"http://127.0.0.1:{port}"
    import urllib.error
    import urllib.request
    for _ in range(120):
        if proc.poll() is not None:
            out = proc.stdout.read().decode("utf-8", "replace")[-800:]
            raise RuntimeError(f"the app exited before serving:\n{out}")
        try:
            urllib.request.urlopen(f"{url}/api/swagger/woa23/openapi.json", timeout=1)
            return proc, url
        except Exception:                                     # noqa: BLE001
            import time
            time.sleep(0.25)
    proc.kill()
    raise RuntimeError("the app never became ready")


def stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


print("the survey passes on a store shaped like WOA23's")
tmp = Path(tempfile.mkdtemp())
try:
    store = make_store(tmp / "store")
    arm_dir = tmp / "arm"
    arm_dir.mkdir()
    os.symlink(store, arm_dir / "data")

    s = survey("data/", str(arm_dir))
    check("preconditions met", True, s["preconditions_met"])
    check("depth is the isolated variable", True, s["depth_isolated"])
    check("the seasonal axis stops short of the requested depths", 800.0,
          s["seasonal_depth"]["max"])
    check("with seasonal nitrate's own 43 levels", 43, s["seasonal_depth"]["n_levels"])
    check("and nothing selectable in 3000-4000 m", [],
          s["selectable_levels_in_requested_range"])
    check("the annual control has ITS own 102 levels to 5500 m", (102, 5500.0),
          (s["annual_depth"]["n_levels"], s["annual_depth"]["max"]))
    check("each compared against its own Table 4 row", (True, True),
          (s["seasonal_depth_vs_p12"]["matches"], s["annual_depth_vs_p12"]["matches"]))
    check("and no schema mismatch", False, s["store_schema_mismatch"])
    check("the required time_period is 13, not a discovered one", "13",
          s["required_seasonal_period"])
    (tmp / "survey.json").write_text(json.dumps(s))

    print()
    print("the real app serves both arms, and the pipeline records what came back")
    procs = []
    try:
        cproc, curl = serve_app(store, str(arm_dir))
        procs.append(cproc)
        rproc, rurl = serve_app(store, str(arm_dir))
        procs.append(rproc)

        records = probe_run(candidate_url=curl, reference_url=rurl, survey=s,
                            candidate_pids=[cproc.pid], reference_pids=[rproc.pid])
        check("eight requests to the candidate", 8, len(records["candidate"]))
        check("eight to the reference", 8, len(records["reference"]))
        check("four of them are recovery probes", 4,
              sum(1 for r in records["candidate"] if r["case_id"] == RECOVERY.id))
        check("the per-arm order is case, recovery, case, recovery",
              [c.id for c in scheduled()],
              [r["case_id"] for r in records["candidate"]])

        # The sequence actually issued, counted from the records rather than from a
        # constant — this is what the budget has to match.
        issued = len(records["candidate"])
        check("the probe issued the characterization requests it declares",
              characterization_requests_per_arm(), issued)
        check("and the per-arm countable total adds the two store probes",
              issued + STORE_PROBE_REQUESTS_PER_ARM, countable_requests_per_arm())
        check("which is ten, not eight", 10, countable_requests_per_arm())
        check("twenty across both arms",
              20, len(records["candidate"]) + len(records["reference"])
              + STORE_PROBE_REQUESTS_PER_ARM * 2)
        check("and the reported total is the range 22 to 80", (22, 80),
              request_total_range())
        check("the order alternates per case, so both RC and CR appear",
              {"RC", "CR"}, {r["request_order"] for r in records["candidate"]})

        # What the statuses ARE is exactly what this pipeline exists to find out, so
        # they are printed and not asserted.
        print("        observed (recorded, not asserted):")
        for r in records["candidate"]:
            if r["case_id"] == RECOVERY.id:
                continue
            print(f"          {r['case_id']:18s} HTTP {r['http_status']} "
                  f"{r['content_type']} {r['body_bytes']}B rows={r['row_count']}")

        check("every case was answered by something", True,
              all(r["http_status"] != 0 for r in records["candidate"]))
        check("no transport error", [None] * 8,
              [r["transport_error"] for r in records["candidate"]])
        check("the recovery probes all returned 200", [200] * 4,
              [r["http_status"] for r in records["candidate"]
               if r["case_id"] == RECOVERY.id])
        check("so every case is marked request-level", True,
              all(r["request_level"] is True for r in records["candidate"]))
        check("and the process was alive throughout", True,
              all(r["process_alive"] is True for r in records["candidate"]))
        check("bodies are hashed, not stored raw", True,
              all(len(r["body_sha256"]) == 64 for r in records["candidate"]))
        check("and the first bytes are kept verbatim", True,
              all(r["body_head_b64"] is not None for r in records["candidate"]))

        # The JSON and CSV endpoints are recorded separately, and this fixture shows
        # exactly why: they need not agree.
        by_id = {r["case_id"]: r for r in records["candidate"]}
        print(f"        JSON  {by_id['D1-DEPTH-OOR-tp13']['http_status']}  "
              f"CSV  {by_id['D1-DEPTH-OOR-tp13-csv']['http_status']}  "
              f"(recorded separately; no assertion that they agree)")

        cand_path, ref_path = tmp / "c.jsonl", tmp / "r.jsonl"
        for path, arm in ((cand_path, "candidate"), (ref_path, "reference")):
            path.write_text("".join(json.dumps(x) + "\n" for x in records[arm]))

        result = build(cand_path, ref_path, tmp / "survey.json")
        check("identical arms characterize cleanly", "CHARACTERIZATION_RECORDED",
              result["outcome"])
        check("exit code 0", 0, result["exit_code"])
        check("every observation is carried into the artefact", 16,
              len(result["observations"]))
        check("the survey's findings travel with it", True,
              result["survey"]["depth_isolated"])
        check("including that it read coordinate chunks", True,
              result["survey"]["reads_coordinate_chunks"])

        # A divergence must be caught by the same pipeline, not only in unit tests.
        tampered = json.loads(cand_path.read_text().splitlines()[0])
        tampered["body_sha256"] = "0" * 64
        lines = cand_path.read_text().splitlines()
        lines[0] = json.dumps(tampered)
        (tmp / "c_bad.jsonl").write_text("\n".join(lines) + "\n")
        bad = build(tmp / "c_bad.jsonl", ref_path, tmp / "survey.json")
        check("a single differing body is a DIVERGENCE", "DIVERGENCE", bad["outcome"])
        check("and the case is named", True,
              any("D1-DEPTH-SUP" in p for p in bad["comparison"]["problems"]))
        check("every record names its variable, climatology, season and period",
              True,
              all(r.get("variable") and r.get("climatology") and r.get("season")
                  for r in records["candidate"]))
        check("the winter case's id carries its period", True,
              any(r["case_id"] == "D1-DEPTH-OOR-tp13"
                  for r in records["candidate"]))
        check("and no bare D1-DEPTH-OOR is issued", False,
              any(r["case_id"] == "D1-DEPTH-OOR" for r in records["candidate"]))
        check("the evidence carries the scope statement", True,
              "WINTER nitrate" in result["scope"])
    finally:
        for p in procs:
            stop(p)

    print()
    print("an unmet precondition issues nothing at all")
    unmet = dict(s, preconditions_met=False, depth_isolated=False,
                 verdict="PRECONDITION_UNMET",
                 verdict_note="depth behaviour cannot be isolated")
    records = probe_run(candidate_url="http://127.0.0.1:1", reference_url="http://127.0.0.1:1",
                        survey=unmet, candidate_pids=[], reference_pids=[])
    check("no candidate request", 0, len(records["candidate"]))
    check("no reference request", 0, len(records["reference"]))
    (tmp / "unmet.json").write_text(json.dumps(unmet))
    (tmp / "empty.jsonl").write_text("")
    res = build(tmp / "empty.jsonl", tmp / "empty.jsonl", tmp / "unmet.json")
    check("and the outcome says why", "PRECONDITION_UNMET", res["outcome"])
    check("with the survey's own words", True,
          "cannot be isolated" in res["because"])

    print()
    print("a store missing the seasonal group stops before any request")
    shallow = tmp / "shallow"
    build_group(shallow / "1_degree" / "annual" / "Nutrients",
                depths=axis(102, 0.0, 5500.0), params=["nitrate"], periods=["0"])
    arm2 = tmp / "arm2"
    arm2.mkdir()
    os.symlink(str(shallow), arm2 / "data")
    s2 = survey("data/", str(arm2))
    check("the survey refuses", "PRECONDITION_UNMET", s2["verdict"])
    r2 = probe_run(candidate_url="http://127.0.0.1:1", reference_url="http://127.0.0.1:1",
                   survey=s2, candidate_pids=[], reference_pids=[])
    check("and nothing is issued", (0, 0), (len(r2["candidate"]), len(r2["reference"])))
finally:
    shutil.rmtree(tmp, ignore_errors=True)

print()
print("the runner's --d1 mode is wired the way this test drives it")
runner = RUNNER.read_text()
check("--d1 is a flag", True, "--d1)            D1_MODE=yes" in runner)
check("it needs its own grant", True, "WOA23_D1_GRANTED" in runner)
check("the survey runs before either arm is started", True,
      runner.index("bench.store_survey") < runner.index("start_tracked reference"))
check("and before the store probe", True,
      runner.index("bench.store_survey") < runner.index("both arms are STORE-ready"))
check("a survey exit 3 stops the run", True, "PRECONDITION_UNMET" in runner)
check("the probe is invoked with both arms", True,
      all(x in runner for x in ("--candidate-url", "--reference-url",
                                "bench.d1_probe")))
check("the evidence step follows it", True, "bench.d1_evidence" in runner)
check("the D1 branch never reaches the contract gate", True,
      runner.index("bench.d1_evidence") < runner.index('echo "== contract gate'))
check("D1 pins one worker per arm, on the same launch line C1 uses", True,
      'elif [ "$S2_MODE" = c1 ] || [ "$S2_MODE" = d1 ]' in runner)
check("and so does the performance mode, which shares it", True,
      '|| [ "$S2_MODE" = s2perf ]; then' in runner)
check("the runner names the characterization subtotal as such", True,
      "BUDGET_D1_CHARACTERIZATION=8" in runner)
check("and never states eight as the per-arm countable total", False,
      "BUDGET_D1=" in runner)
check("the banner prints the countable per-arm figure", True,
      "COUNTABLE per arm" in runner)
check("and says the total must be reported as a range", True,
      "a RANGE, and must be reported as one" in runner)

# The flags this test drives d1_probe with must be the flags the runner passes.
for flag in ("--candidate-url", "--reference-url", "--survey", "--out-candidate",
             "--out-reference", "--candidate-pid", "--reference-pid"):
    if flag in runner:
        PASS += 1
    else:
        FAIL += 1
        print(f"  FAIL the runner does not pass {flag}")
print(f"  ok   every d1_probe flag this test uses is passed by the runner")

print()
raise SystemExit(summary(PASS, FAIL))