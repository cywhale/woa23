"""The `--s2-perf` main path, walked end to end against stand-in backends.

Contract gate -> symmetric warm-up -> latency gate -> noise pilot -> finalization,
each stage invoked exactly as `scripts/run_controlled.sh` invokes it, over real HTTP
on loopback. Timing is **synthetic**: each stand-in arm sleeps a scripted amount per
request, so a run can be made to land on `PASS`, on a per-case `REGRESSION`, on
`INCONCLUSIVE`, or on `FAIL_NO_ESTABLISHED_IMPROVEMENT` on purpose.

**What this test does NOT do, stated so no one reads more into a green run:**

- It makes **no claim about confidence coverage.** The interval `paired_stats`
  produces is a percentile range of 5,000 bootstrap ratios of medians. Nothing here
  measures how often such an interval covers a true ratio, and the scripted delays
  are neither i.i.d. nor free of the sleep clock's own bias. That a scripted 2.0
  ratio comes back as `REGRESSION` shows the plumbing carries the verdict, not that
  the interval is calibrated.
- The backends are **stand-ins, not the API.** They return fixed bodies. No Zarr
  store is read, `api/` is not imported, and no figure here describes WOA23.
- **No process-level cleanup happens here.** These are in-process HTTP servers, not
  gunicorn arbiters. Shutdown, tree verification and the port ledger are covered by
  `scripts/test_procs.sh`, `scripts/test_stop_multiworker.sh` and the C2 evidence;
  what this file checks about cleanup is the runner's *control flow* around it.
- **The runner script itself is not executed.** It requires Linux `/proc`, a
  production interpreter and a real store. Stage order and the stop-on-failure
  wiring are therefore read from its source and, where a shell construct decides
  something, that construct is executed here with the real exit status feeding it.
- **Provenance validation is stubbed out, and only provenance.** Both gates refuse
  to issue a request without backend metadata collected from `/proc` on the
  backend's own host — correctly, and that refusal is itself tested in
  `bench/test_provenance.py` and `bench/test_s2_provenance.py`. Stubbing the five
  provenance entry points is what lets the *rest* of each gate run here; sampling,
  the statistics, `decide_gate`'s precedence and every artefact are the real code.
  A green run here says nothing about whether provenance would pass on VM24.

    uv run python -m bench.test_s2perf_integration
"""

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.contract_cases import all_cases                          # noqa: E402
from bench.paired_bench import IMPROVEMENT_REQUIRED                 # noqa: E402
from bench.paired_stats import WARMUP_REQUESTS                      # noqa: E402
from bench.perf_counts import counts                                # noqa: E402
from bench.queries import select                                    # noqa: E402
from bench.symmetric_warmup import request_count_per_arm            # noqa: E402
from bench.suite_summary import summary          # noqa: E402

PASS_N = 0
FAIL_N = 0
REPO = Path(__file__).resolve().parent.parent
RUNNER = (REPO / "scripts" / "run_controlled.sh").read_text()
# The stage chain the runner sources. Claims about what the harness *invokes* are
# claims about this file; claims about guards, banners and the cleanup trap are
# claims about the runner. They are kept apart so an assertion says which it means.
CHAIN = (REPO / "scripts" / "lib_s2perf.sh").read_text()

GATE_CASES = select(None, include_heavy=True)
PILOT_WARM = 25                       # what the runner passes


def check(name, expected, actual):
    global PASS_N, FAIL_N
    if expected == actual:
        PASS_N += 1
        print(f"  ok   {name}")
    else:
        FAIL_N += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


def section(title):
    print()
    print(title)


# --------------------------------------------------------------------------------
# The stand-in backend.
#
# Contract cases are answered from the case list itself: `contract_diff` requires the
# observed status to equal the case's `expect_status`, so a backend that always
# answered 200 would fail the gate for reasons that have nothing to do with what is
# being tested. Bodies are derived from the request, so both arms are byte-identical
# unless a test asks for a difference.
# --------------------------------------------------------------------------------

CASE_STATUS = {}
for _c in all_cases():
    CASE_STATUS[(_c.path, tuple(sorted((k, str(v)) for k, v in _c.params.items())))] = \
        _c.expect_status

QUERY_ID = {tuple(sorted((k, str(v)) for k, v in q.params().items())): q.id
            for q in GATE_CASES}


class Arm:
    """One stand-in backend. `delay` is seconds to sleep, by gate-case id."""

    def __init__(self, name, delay=None, status=200, differ_on=None, body_suffix=b""):
        self.name = name
        self.delay = delay or {}
        self.status = status
        self.differ_on = differ_on          # a contract case id to answer differently
        self.body_suffix = body_suffix
        self.paths = []
        self.by_query = {}
        self.srv = None
        self.url = None

    def start(self):
        arm = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                parsed = urllib.parse.urlparse(self.path)
                raw = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
                params = tuple(sorted((k, v) for k, v in raw if k != "_cb"))
                arm.paths.append(parsed.path)

                qid = QUERY_ID.get(params)
                if qid is not None:
                    arm.by_query[qid] = arm.by_query.get(qid, 0) + 1
                    d = arm.delay.get(qid)
                    if d:
                        # The scripted delay IS the measurement. Everything the gate
                        # decides in this file comes from these sleeps.
                        time.sleep(d(arm.by_query[qid]) if callable(d) else d)

                status = CASE_STATUS.get((parsed.path, params), arm.status)
                body = json.dumps(
                    {"path": parsed.path, "params": [list(p) for p in params]},
                    sort_keys=True).encode()
                if arm.differ_on is not None and CASE_STATUS.get(
                        (parsed.path, params)) is not None and \
                        arm.differ_on == (parsed.path, params):
                    body += b"  DIFFERENT"
                body += arm.body_suffix
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"
        return self

    def stop(self):
        # `shutdown()` alone stops the accept loop but leaves the socket bound, so a
        # later connection to this port would sit in the backlog instead of being
        # refused — a stand-in for a dead backend that quietly hangs instead.
        if self.srv:
            self.srv.shutdown()
            self.srv.server_close()

    @property
    def requests(self):
        return len(self.paths)


# The provenance stub. Both gates abort before their first request unless backend
# metadata collected from the backend's own `/proc` is supplied — the right
# behaviour, tested where it belongs, and impossible to satisfy from a stand-in
# server on this host. These five names are replaced and nothing else is: the
# sampling loop, the bootstrap, `decide_gate` and the artefact writer all run as
# shipped.
STUB = """
import sys
import {mod} as m
for _name in ("validate_meta", "validate_store_agreement",
              "verify_group_path_agreement", "post_run_runtime_check"):
    if hasattr(m, _name):
        setattr(m, _name, lambda *a, **k: [])
if hasattr(m, "load_meta"):
    m.load_meta = lambda path, label: ({{"stub_provenance": True}}, [])
sys.argv = ["{mod}"] + sys.argv[1:]
raise SystemExit(m.main())
"""


def run_module(module, *args):
    """For the stages that need no provenance: the warm-up and the noise pilot.

    NOT for the two gates. An earlier version of the timeout case below called this
    for `paired_bench`, which then aborted with INVALID_METADATA before issuing a
    single request — and the assertions about what a timeout costs were being made
    against a run that never timed out.
    """
    return subprocess.run([sys.executable, "-m", module, *args],
                          capture_output=True, text=True, cwd=str(REPO))


def run_stubbed(module, *args):
    return subprocess.run([sys.executable, "-c", STUB.format(mod=module), *args],
                          capture_output=True, text=True, cwd=str(REPO))


def contract(cand, ref, out, variant="5.2A"):
    """The runner's invocation, with the provenance files it also passes stubbed."""
    return run_stubbed("bench.contract_diff", "--candidate", cand.url,
                       "--reference", ref.url, "--variant", variant,
                       "--pause", "0", "--out", str(out))


def warmup(cand, ref, out):
    return run_module("bench.symmetric_warmup", "--candidate", cand.url,
                      "--reference", ref.url, "--include-heavy", "--out", str(out))


def latency(cand, ref, out, warm=21, journals=None):
    # `--pause 0` where the runner uses its default: the pause spaces requests and
    # changes nothing the gate computes, and 8 x 22 x 2 x 0.2 s of it would put this
    # test out of reach of a pre-commit run.
    extra = []
    if journals is not None:
        extra = ["--request-log", str(journals / "latency.jsonl")]
    return run_stubbed("bench.paired_bench", "--candidate", cand.url,
                       "--reference", ref.url, "--gate-variant", "5.2A",
                       "--warm", str(warm), "--include-heavy", "--margin", "0.05",
                       "--pause", "0", *extra, "--out", str(out))


def pilot(arm, out, warm=PILOT_WARM):
    return run_module("bench.noise_pilot", "--base-url", arm.url, "--warm", str(warm),
                      "--pause", "0", "--out", str(out))


def gate_of(path):
    return json.loads(Path(path).read_text())["gate"]


# ================================================================================
# 1. One case set, and the warm-up warms what the gate measures
# ================================================================================
section("1. the warm-up and the latency gate use ONE case set")

WORK = Path(tempfile.mkdtemp(prefix="s2perf-"))
fast = {q.id: 0.012 for q in GATE_CASES}
slow = {q.id: 0.030 for q in GATE_CASES}

cand = Arm("candidate", delay=fast).start()
ref = Arm("reference", delay=slow).start()
try:
    # The main path, in the runner's order. The contract gate first: a latency
    # figure for an arm not shown to answer correctly is not worth having.
    ctr = WORK / "s2p_contract.json"
    rc_c = contract(cand, ref, ctr)
    check("the contract gate exits 0", 0, rc_c.returncode)
    check("and passes on 64 cases", "PASS", json.loads(ctr.read_text())["gate"])
    check("one request per case per arm", (64, 64), (cand.requests, ref.requests))

    w = WORK / "s2p_symmetric_warmup.json"
    rc_w = warmup(cand, ref, w)
    check("the warm-up exits 0", 0, rc_w.returncode)
    warm_doc = json.loads(w.read_text())
    warmed = {s["case"] for s in warm_doc["statuses"]}

    p = WORK / "s2p_paired.json"
    rc_l = latency(cand, ref, p)
    check("the latency gate exits 0", 0, rc_l.returncode)
    paired = json.loads(p.read_text())
    measured = {r["id"] for r in paired["results"]}

    check("eight cases", 8, len(measured))
    check("the warm-up warmed exactly what the gate measured", set(), warmed ^ measured)
    check("and that is `select(None, include_heavy=True)`",
          {q.id for q in GATE_CASES}, measured)
    # A warm-up over a different set would leave the candidate warm on cases the gate
    # never measures and cold on cases it does — the asymmetry it exists to remove.
    check("the harness passes --include-heavy to the warm-up", True,
          "bench.symmetric_warmup" in CHAIN
          and "--candidate \"$cand\" --reference \"$ref\" --include-heavy" in CHAIN)
    check("and to the latency gate", True,
          "--gate-variant 5.2C --warm 21 --include-heavy" in CHAIN)
    # 5.2C, not 5.2A, since spec 008: the candidate deliberately reorders rows, so a
    # byte-equality gate would fail every multi-row case for the change working as
    # decided. The latency gate records which contract variant was in force, and the
    # record has to name the one that actually ran.
    check("the contract variant recorded is the one the runner runs", True,
          "5.2A" not in CHAIN.split("--gate-variant")[1][:20])

    # ============================================================================
    # 2. Per case: its own ratio, its own bootstrap, its own verdict. No total.
    # ============================================================================
    section("2. each case is decided alone, and no total ratio exists")
    check("eight results", 8, len(paired["results"]))
    missing = [f"{r['id']}.{k}" for r in paired["results"]
               for k in ("regression_verdict", "improvement_verdict", "margin")
               if k not in r]
    missing += [f"{r['id']}.stats.{k}" for r in paired["results"]
                for k in ("median_ratio", "ci95_low", "ci95_high",
                          "bootstrap_rounds", "seed")
                if k not in r.get("stats", {})]
    check("every case carries its own ratio, interval and two verdicts", [], missing)

    ratios = {r["id"]: r["stats"]["median_ratio"] for r in paired["results"]}
    check("no two cases share one ratio object", True,
          len({round(v, 9) for v in ratios.values()}) > 1)
    check("each ratio came from that case's own samples", True,
          all(0.2 < v < 0.9 for v in ratios.values()))   # 10 ms against 30 ms
    check("each case bootstrapped 5,000 rounds of its own", {5000},
          {r["stats"]["bootstrap_rounds"] for r in paired["results"]})
    check("n is per case and per arm, never summed", {21},
          {r["stats"][k] for r in paired["results"] for k in ("n_a", "n_b")})

    pooled = {k for k in paired
              if k in ("ratio", "median_ratio", "ci95_low", "ci95_high",
                       "overall_ratio", "combined_ratio", "pooled_ratio")}
    check("the artefact has NO top-level ratio or interval", set(), pooled)
    check("nothing outside a case carries a stats block", set(),
          {k for k, v in paired.items()
           if isinstance(v, dict) and "median_ratio" in v})
    check("the run's single top-level verdict is a gate, not a number", "PASS",
          paired["gate"])
    # Pooling would let a fast case pay for a slow one; the gate instead takes the
    # worst case's verdict through `decide_gate`'s precedence.
    check("samples are stored per case and per arm", {"candidate", "reference"},
          set(paired["results"][0]["samples_ms"]))
    # 22, not 21: `samples_ms` is RAW, the discarded leading sample included, "so
    # every number above can be recomputed". The statistics use 21 of them.
    check("22 raw samples per case per arm", {22},
          {len(s) for r in paired["results"] for s in r["samples_ms"].values()})
    check("of which 21 enter the statistics", {21},
          {r["stats"]["n_a"] for r in paired["results"]})
    check("and the artefact says which one was dropped", (21, 1),
          (paired["warm_samples_per_arm"], paired["warmup_discarded"]))
    check("each case's requests were interleaved and counterbalanced", {"AB", "BA"},
          set(paired["results"][0]["order_log"]))

    # ============================================================================
    # 4a. The pilot, after the gate, on both arms
    # ============================================================================
    section("4a. the noise pilot samples every case, on both arms")
    before = (cand.requests, ref.requests)
    for arm, nm in ((ref, "reference"), (cand, "candidate")):   # the runner's order
        rc_p = pilot(arm, WORK / f"s2p_noise_pilot_{nm}.json")
        check(f"the {nm} pilot exits 0", 0, rc_p.returncode)
    pilot_doc = json.loads((WORK / "s2p_noise_pilot_candidate.json").read_text())
    check("it visited all eight cases", 8, len(pilot_doc["results"]))
    check("with 25 + 1 raw samples each", {PILOT_WARM + WARMUP_REQUESTS},
          {len(r["samples_ms"]) for r in pilot_doc["results"]})
    check("so one invocation is 208 requests, not 26",
          8 * (PILOT_WARM + WARMUP_REQUESTS), cand.requests - before[0])
    check("and the same on the other arm",
          8 * (PILOT_WARM + WARMUP_REQUESTS), ref.requests - before[1])

    cand_total_observed, ref_total_observed = cand.requests, ref.requests
finally:
    cand.stop(); ref.stop()

# ================================================================================
# 4b. F2 counts what was issued — measured against what the servers saw
# ================================================================================
section("4b. the recorded counts equal what the backends actually received")
rec = counts(WORK, "s2p")
check("no problems", [], rec["problems"])
check("the warm-up is 16 per arm", 16, rec["per_arm"]["candidate"]["symmetric_warmup"])
check("which is what the module budgets", request_count_per_arm(8),
      rec["per_arm"]["candidate"]["symmetric_warmup"])
check("the latency gate is 176 per arm", 176, rec["per_arm"]["candidate"]["latency"])
check("the pilot is 208 per arm", 208, rec["per_arm"]["candidate"]["noise_pilot"])

for arm, observed in (("candidate", cand_total_observed),
                      ("reference", ref_total_observed)):
    measured = sum(rec["per_arm"][arm].values())
    # `perf_counts` covers the three MEASURED stages. The contract gate's 64 is
    # derived from the case list and recorded once, by `record_contract_count` in
    # the shell, so it is added here exactly once — the arithmetic that was wrong
    # in the harness, where it was added twice and reported 528 per arm.
    check(f"{arm}: the three measured stages plus the derived contract count "
          f"equal what the server logged", observed, measured + 64)
    check(f"{arm}: and perf_counts does not itself carry a contract count", False,
          "contract" in rec["per_arm"][arm])
check("the derived contract count is confirmed usable as attempts", True,
      rec["exact_per_stage"]["contract"])
check("and the record names who owns it", True,
      "record_contract_count" in rec["contract_note"])
# The harness must contain exactly one place that records this stage.
_owners = [ln for ln in (RUNNER + CHAIN).splitlines()
           if "request_add" in ln and " contract " in ln]
check("exactly one code location records the contract stage", 1, len(_owners))

section("4c. a refused connection is an attempt, and is counted as one")
dead = Arm("dead").start()
dead_url = dead.url
dead.stop()
time.sleep(0.05)
live = Arm("live").start()
try:
    class _Dead:
        url = dead_url
    out = WORK / "refused_symmetric_warmup.json"
    rc = subprocess.run(
        [sys.executable, "-m", "bench.symmetric_warmup", "--candidate", dead_url,
         "--reference", live.url, "--include-heavy", "--out", str(out)],
        capture_output=True, text=True, cwd=str(REPO))
    doc = json.loads(out.read_text())
    check("the warm-up still issued its full count against a dead arm", 16,
          doc["requests_per_arm"]["candidate"])
    check("and recorded the failures as transport errors, not statuses", True,
          0 in {s["http_status"] for s in doc["statuses"] if s["arm"] == "candidate"})
    check("perf_counts reports the attempts, not the successes", 16,
          counts(WORK, "refused")["per_arm"]["candidate"]["symmetric_warmup"])

    # The limitation, measured rather than asserted: the latency gate and the pilot
    # do not catch transport errors, so a refused arm ends them without an artefact —
    # and the requests they DID issue are then invisible to perf_counts.
    p2 = WORK / "refused_paired.json"
    j2 = WORK / "refused_journals"
    rc2 = latency(_Dead, live, p2, warm=2, journals=j2)
    check("the latency gate does not survive a refused arm", True, rc2.returncode != 0)
    # It used to write nothing at all, which made the requests it had already issued
    # uncountable. Now it keeps a partial artefact and a journal of every attempt.
    check("but it writes a partial artefact", True, p2.exists())
    doc2 = json.loads(p2.read_text())
    check("marked incomplete", False, doc2["complete"])
    check("with no gate verdict invented from it", "INVALID_TRANSPORT_FAILURE",
          doc2["gate"])
    check("and refusing an exact request total", False, doc2["request_total_exact"])
    rec2 = counts(WORK, "refused", j2)
    check("the count comes from the journal, so the attempts are not lost", True,
          rec2["per_arm"]["candidate"]["latency"] > 0)
    check("and the whole record is marked inexact", False, rec2["counts_exact"])
    check("which perf_counts reports as a problem rather than as zero traffic", True,
          any("did not finish" in p for p in rec2["problems"]))
    check("the reporting rule travels with it", True,
          "do NOT present a single exact total" in rec2["reporting_rule"])
finally:
    live.stop()

section("4d. a timeout is an attempt too")
slow_arm = Arm("slow", delay={q.id: 2.0 for q in GATE_CASES}).start()
quick = Arm("quick", delay={q.id: 0.001 for q in GATE_CASES}).start()
try:
    tp = WORK / "timeout_paired.json"
    tj = WORK / "timeout_journals"
    rc3 = run_stubbed("bench.paired_bench", "--candidate", slow_arm.url,
                      "--reference", quick.url, "--gate-variant", "5.2A",
                      "--warm", "1", "--include-heavy", "--margin", "0.05",
                      "--pause", "0", "--timeout", "0.25",
                      "--request-log", str(tj / "latency.jsonl"), "--out", str(tp))
    check("the gate stops on a timeout", True, rc3.returncode != 0)
    check("the request reached the server before it timed out", True,
          slow_arm.requests >= 1)
    check("and a timeout is journalled like any other attempt", True,
          (tj / "latency.jsonl").exists())
    check("the artefact records the timeout, not nothing", "INVALID_TRANSPORT_FAILURE",
          json.loads(tp.read_text())["gate"])
    check("naming it a transport failure", "STAGE_ABORTED_TRANSPORT_FAILURE",
          json.loads(tp.read_text())["aborted"]["classification"])
finally:
    slow_arm.stop(); quick.stop()

# The gap above is why the runner's ceiling stays a ceiling: it is stated per stage
# in the banner before anything is sent, so a stage that dies mid-flight is still
# bounded by an authorised number even when its artefact never appears.
check("the runner states the ceiling before the first request", True,
      RUNNER.index("request budget (ceilings, not estimates)")
      < RUNNER.index("s2perf_contract "))

# ================================================================================
# 3. Synthetic timing drives every verdict the gate can reach
# ================================================================================
section("3. scripted delays reach PASS, REGRESSION, INCONCLUSIVE and "
        "FAIL_NO_ESTABLISHED_IMPROVEMENT")

BASE = 0.030


def scenario(name, cand_delay, ref_delay, warm=21):
    c = Arm("candidate", delay=cand_delay).start()
    r = Arm("reference", delay=ref_delay).start()
    try:
        out = WORK / f"{name}_paired.json"
        latency(c, r, out, warm=warm)
        return json.loads(out.read_text())
    finally:
        c.stop(); r.stop()


# PASS: the candidate is decisively faster everywhere.
doc = scenario("pass", {q.id: BASE / 3 for q in GATE_CASES},
               {q.id: BASE for q in GATE_CASES})
check("gate PASS", "PASS", doc["gate"])
check("every case NO_REGRESSION", {"NO_REGRESSION"},
      {r["regression_verdict"] for r in doc["results"]})
check("every case IMPROVED", {"IMPROVED"},
      {r["improvement_verdict"] for r in doc["results"]})

# REGRESSION: the candidate is decisively slower. The per-case verdict is REGRESSION
# and the run's verdict is FAIL — two different words for two different things.
doc = scenario("regress", {q.id: BASE for q in GATE_CASES},
               {q.id: BASE / 3 for q in GATE_CASES})
check("every case REGRESSION", {"REGRESSION"},
      {r["regression_verdict"] for r in doc["results"]})
check("and the run's gate is FAIL", "FAIL", doc["gate"])
check("the interval sits wholly above the margin", True,
      all(r["stats"]["ci95_low"] > 1.05 for r in doc["results"]))

# FAIL_NO_ESTABLISHED_IMPROVEMENT: nothing regresses, but the two cases that must
# improve are merely equal. `decide_gate` puts this ahead of INCONCLUSIVE.
# 1.02x, not 1.00x. With the two arms scripted to the SAME delay the interval
# straddles 1.0 and can settle just below it on scheduling noise alone — one case
# came back IMPROVED from a dead heat. A 2% penalty is unambiguously not an
# improvement and is still well inside the 5% margin, so the case is NOT_IMPROVED
# and NO_REGRESSION at once, which is exactly the state this gate is for.
equal_where_required = {q.id: (BASE * 1.02 if q.id in IMPROVEMENT_REQUIRED
                               else BASE / 3)
                        for q in GATE_CASES}
doc = scenario("unproven", equal_where_required, {q.id: BASE for q in GATE_CASES})
check("no case regresses", set(),
      {r["id"] for r in doc["results"] if r["regression_verdict"] == "REGRESSION"})
check("the improvement-required cases are not IMPROVED", set(),
      {r["id"] for r in doc["results"]
       if r["improvement_required"] and r["improvement_verdict"] == "IMPROVED"})
check("both of them are flagged improvement_required", set(IMPROVEMENT_REQUIRED),
      {r["id"] for r in doc["results"] if r["improvement_required"]})
check("gate FAIL_NO_ESTABLISHED_IMPROVEMENT", "FAIL_NO_ESTABLISHED_IMPROVEMENT",
      doc["gate"])

# INCONCLUSIVE: the required cases improve, and the rest straddle the margin.
#
# The candidate's delay cycles through three values whose MIDDLE one is the margin
# itself. Over any 22-sample window each appears about seven times, so the median
# lands on 1.05x the reference wherever in the cycle a case happens to start — while
# the 0.80/1.35 wings make the bootstrap interval wide enough to contain 1.05 from
# both sides. An earlier ten-value sequence was not offset-proof: one case's window
# skewed high enough to be called a REGRESSION, which is a different verdict, not a
# noisier one.
STRADDLE = [0.80, 1.05, 1.35]


def straddling(n):
    return BASE * STRADDLE[(n - 1) % len(STRADDLE)]


doc = scenario("undecided",
               {q.id: (BASE / 3 if q.id in IMPROVEMENT_REQUIRED else straddling)
                for q in GATE_CASES},
               {q.id: BASE for q in GATE_CASES})
undecided = [r["id"] for r in doc["results"]
             if r["regression_verdict"] == "INCONCLUSIVE"]
check("at least one case is INCONCLUSIVE", True, len(undecided) > 0)
check("its interval straddles the margin", True,
      all(r["stats"]["ci95_low"] <= 1.05 <= r["stats"]["ci95_high"]
          for r in doc["results"] if r["regression_verdict"] == "INCONCLUSIVE"))
check("no case regresses", set(),
      {r["id"] for r in doc["results"] if r["regression_verdict"] == "REGRESSION"})
check("gate INCONCLUSIVE", "INCONCLUSIVE", doc["gate"])
check("and INCONCLUSIVE is not a pass", True, doc["gate"] != "PASS")
# An INCONCLUSIVE case reports what it would take to decide — and reports `null`
# when the answer is "more than the ladder holds", rather than inventing a rung.
und = [r for r in doc["results"] if r["regression_verdict"] == "INCONCLUSIVE"]
check("every undecided case carries an escalation field", True,
      all("samples_needed_estimate" in r and "next_ladder_rung" in r for r in und))
check("no rung outside the pre-specified ladder is ever proposed", set(),
      {r["next_ladder_rung"] for r in und if r["next_ladder_rung"] is not None}
      - set(doc["escalation_ladder"]))
check("and an estimate beyond the ladder is null, not a number off it", True,
      all(r["next_ladder_rung"] is not None
          or r["samples_needed_estimate"] is None
          or r["samples_needed_estimate"] > doc["escalation_ladder"][-1]
          for r in und))
check("and the ladder is the one spec 001 fixed", [21, 60, 150],
      doc["escalation_ladder"])

# ================================================================================
# 5. The contract gate stops the run before any timing is taken
# ================================================================================
section("5. a failing contract gate never reaches the latency gate")
c1 = Arm("candidate").start()
one = all_cases()[0]
r1 = Arm("reference",
         differ_on=(one.path,
                    tuple(sorted((k, str(v)) for k, v in one.params.items())))).start()
try:
    out = WORK / "badcontract_contract.json"
    rc = contract(c1, r1, out)
    check("the contract gate exits non-zero", True, rc.returncode != 0)
    check("and records FAIL", "FAIL", json.loads(out.read_text())["gate"])
    check(f"{one.id} is the differing case", True, one.id in rc.stdout)

    # The shell construct the runner actually uses, executed with that exit status.
    shell = subprocess.run(
        ["bash", "-c",
         'set -e\n'
         f'{sys.executable} -m bench.contract_diff --candidate {c1.url} '
         f'--reference {r1.url} --variant 5.2A --pause 0 >/dev/null 2>&1 '
         '|| { echo STOPPED; exit 1; }\n'
         'echo REACHED_LATENCY\n'],
         capture_output=True, text=True, cwd=str(REPO))
    check("the runner's `|| { ...; exit 1; }` stops there", "STOPPED",
          shell.stdout.strip())
    check("the latency gate is never reached", False,
          "REACHED_LATENCY" in shell.stdout)
    check("and the shell exits 1", 1, shell.returncode)
finally:
    c1.stop(); r1.stop()

check("in the runner, the contract gate precedes the warm-up", True,
      RUNNER.index("s2perf_contract ") < RUNNER.index("s2perf_warmup "))
check("the warm-up precedes the latency gate", True,
      RUNNER.index("s2perf_warmup ") < RUNNER.index("s2perf_latency "))
check("the noise pilot runs after the latency gate", True,
      RUNNER.index("s2perf_latency ") < RUNNER.index("s2perf_pilot "))
# Driver-level coverage of that order — the shell chain executed, not read — is
# scripts/test_s2perf_driver.sh. This file proves the modules compose.
check("and the chain is a sourced library, so the driver test runs it", True,
      "lib_s2perf.sh" in RUNNER)
check("the contract gate carries the stop-on-failure clause", True,
      "contract gate did not pass — stopping before anything further" in RUNNER)
check("the warm-up carries one too", True,
      "the symmetric warm-up did not complete; stopping before any" in RUNNER)
# Running the pilot first would sample one arm 208 times and the other not at all —
# the very asymmetry the warm-up exists to remove, introduced by the noise tool.
check("and the runner says why the pilot is last", True,
      "warming one side's page cache" in RUNNER)

# ================================================================================
# 6. Finalization: complete, non-empty, and recomputable
# ================================================================================
section("6. finalization writes artefacts that can be recomputed from the run")

# The runner does not call `bench.d1_finalize` directly — it calls the shell function
# `d1_finalize` from `lib_d1_finalize.sh`, which also writes the request counts and
# refuses a zero-byte artefact. That function is what runs here, with the per-stage
# counts fed to it exactly as the runner feeds them: from `bench.perf_counts`, not
# from the budget constants.
FINAL = Path(tempfile.mkdtemp(prefix="s2perf-final-"))
for src in WORK.glob("s2p_*.json"):
    (FINAL / src.name).write_text(src.read_text())

# The arm records the worker-count reader parses. `-w 1` is what the s2perf launch
# line sets, and `d1_finalize` cross-checks the recorded count against this argv.
for arm in ("candidate", "reference"):
    (FINAL / f"s2p_meta_{arm}.json").write_text(json.dumps({
        "launch_argv": ["gunicorn", "-w", "1", "--graceful-timeout", "10",
                        "-b", "127.0.0.1:18901", "api.app:app"],
        "launch_command": "gunicorn -w 1 ... api.app:app",
        "worker_pids": [4242],
    }))

FINAL_STATE = Path(tempfile.mkdtemp(prefix="s2perf-run-"))
FINALIZE_SH = """
set -u
cd "$1"
RUN="$2"
export REQUEST_COUNTS_DIR="$RUN/requests"
. scripts/lib_requests.sh
. scripts/lib_d1_finalize.sh
results="$3"
# The runner's own loop, verbatim in shape.
for arm in candidate reference; do
  for stage in symmetric_warmup latency noise_pilot; do
    request_add "$arm" "$stage" \
      "$(uv run python -m bench.perf_counts --label s2p --results "$results" \
         --stage "$stage" --arm "$arm")" || exit 90
  done
  request_add "$arm" contract 64 || exit 91
done
d1_finalize s2p "$results"
echo "FINALIZE_RC=$?"
"""
fin = subprocess.run(["bash", "-c", FINALIZE_SH, "sh", str(REPO), str(FINAL_STATE),
                      str(FINAL)], capture_output=True, text=True)
check("the runner's finalisation function exits 0", "FINALIZE_RC=0",
      fin.stdout.strip().splitlines()[-1] if fin.stdout.strip() else fin.stderr[-300:])

produced = sorted(p.name for p in FINAL.glob("s2p_*"))
for name in ("s2p_workers.json", "s2p_requests.json"):
    check(f"{name} exists", True, name in produced)
    check(f"{name} is not empty", True,
          (FINAL / name).exists() and (FINAL / name).stat().st_size > 0)
    check(f"{name} parses", True, isinstance(
        json.loads((FINAL / name).read_text()), dict))
# Written only when something failed, so its absence is the success signal.
check("no INVALID_POST_MEASUREMENT_HARNESS classification was written", False,
      (FINAL / "s2p_finalization.json").exists())

workers = json.loads((FINAL / "s2p_workers.json").read_text())
check("the worker record found one worker per arm", {1},
      {v["worker_count"] for v in workers["per_arm"].values()})
check("with no problems", [], workers["problems"])
check("and it disclaims being a production measurement", False,
      workers["derived_from_production_measurement"])

reqs = json.loads((FINAL / "s2p_requests.json").read_text())
recorded = {arm: reqs["per_arm"][arm] for arm in ("candidate", "reference")}
check("the record says these are attempts, not successes", True,
      reqs["counts_attempts_not_successes"])
check("the pilot stage records 208", {208},
      {recorded[a]["noise_pilot"] for a in recorded})
check("the latency stage records 176, the count the gate issued", {176},
      {recorded[a]["latency"] for a in recorded})
check("the warm-up records 16", {16},
      {recorded[a]["symmetric_warmup"] for a in recorded})
check("and each arm's total is the sum of its stages", True,
      all(recorded[a]["total"] == sum(v for k, v in recorded[a].items()
                                      if k != "total")
          for a in recorded))
check("which is the 464 this walk actually issued per arm", {464},
      {recorded[a]["total"] for a in recorded})
check("and that equals what the backends logged", cand_total_observed,
      recorded["candidate"]["total"])
check("the grand total is both arms", 928, reqs["total"])

# Recomputable: every count in the record is re-derivable from the raw samples the
# measurement stages left behind, with no number taken on trust.
again = counts(FINAL, "s2p")
paired = json.loads((FINAL / "s2p_paired.json").read_text())
by_hand = sum(len(r["samples_ms"]["candidate"]) for r in paired["results"])
check("the latency count recomputes from the raw samples", by_hand,
      again["per_arm"]["candidate"]["latency"])
pilot_doc = json.loads((FINAL / "s2p_noise_pilot_candidate.json").read_text())
check("and the pilot count from its raw samples",
      sum(len(r["samples_ms"]) for r in pilot_doc["results"]),
      again["per_arm"]["candidate"]["noise_pilot"])
check("re-reading the finalised artefacts gives the same counts",
      rec["per_arm"], again["per_arm"])
check("every case kept its samples, so every ratio can be recomputed", 8,
      len([r for r in paired["results"] if r["samples_ms"]["candidate"]]))
check("and the bootstrap seed travels with them", {20260805},
      {r["stats"]["seed"] for r in paired["results"]})

# The harness-failure path, which is what the classification exists for.
BROKEN = Path(tempfile.mkdtemp(prefix="s2perf-broken-"))
(BROKEN / "s2p_paired.json").write_text("{}")
broken = subprocess.run(["bash", "-c", FINALIZE_SH, "sh", str(REPO),
                         str(Path(tempfile.mkdtemp(prefix="s2perf-run2-"))),
                         str(BROKEN)], capture_output=True, text=True)
check("a run missing its arm records does not finalise", "FINALIZE_RC=6",
      broken.stdout.strip().splitlines()[-1])
cls = json.loads((BROKEN / "s2p_finalization.json").read_text())
check("it is classified, not merely non-zero", "INVALID_POST_MEASUREMENT_HARNESS",
      cls["classification"])
check("and says the measurements are not what failed", True,
      "NOT a D1 characterization FAIL" in cls["not"])
check("the chain routes exit 6 to that classification", True,
      "S2 performance classification: INVALID_POST_MEASUREMENT_HARNESS" in CHAIN)
check("and says the gate's own result is still recorded", True,
      "The gate's result IS recorded in" in CHAIN)

# ================================================================================
# 7. Cleanup control flow, and what this run may be quoted as
# ================================================================================
section("7. cleanup and the claims the runner is required to print")
check("the performance mode is NOT stopped after the contract gate", True,
      '[ "$S2_MODE" != none ] && [ "$S2_MODE" != s2perf ]' in RUNNER)
check("a cleanup failure is a run failure whatever the gates said", True,
      "CLEANUP DID NOT COMPLETE — this run is a failure regardless of its gates"
      in RUNNER)
# An EXIT trap's `return` never sets the exit status, so raising it takes an `exit`.
check("and the trap raises it with exit, not return", True,
      "An EXIT trap's return\n      # value never sets the exit status" in RUNNER)
check("without flattening a more specific status", True,
      "INVALID_POST_MEASUREMENT_HARNESS is 6 and must stay 6" in RUNNER)
check("the shutdown budget is asserted before any launch", True,
      "assert_shutdown_budget" in RUNNER)
check("the closing report refuses to pool the cases", True,
      "NEVER pooled across cases" in CHAIN)
check("it refuses the coverage claim", True,
      "not a proven 95% coverage interval" in CHAIN)
check("it names the approved threshold", True,
      "APPROVED ENGINEERING THRESHOLD: 0.05" in RUNNER + CHAIN)
check("the superseded pre-approval wording is gone from both", True,
      "PROPOSED ENGINEERING THRESHOLD" not in RUNNER + CHAIN
      and "pending approval" not in RUNNER + CHAIN)
check("it states what the margin is not, approved or otherwise", True,
      "not a confidence guarantee" in CHAIN and "not a production SLA" in CHAIN)
check("IMPROVED is reported as a gate for the two required cases only", True,
      "IMPROVED is a GATE for the improvement-required cases only" in CHAIN
      and "point_profile_multiparam" in CHAIN
      and "not a case that passed an improvement gate" in CHAIN)
check("and what the result is not", True,
      "not multi-worker, not cold-cache" in CHAIN)
check("and that it is rung 21 only, not the whole of spec 007", True,
      "rung 21 ONLY" in CHAIN and "NO startup measurement" in CHAIN)

# This test's own disclaimer, asserted so it cannot be dropped by an edit.
check("this file claims no confidence coverage", True,
      "no claim about confidence coverage" in __doc__)
check("and says why the scripted delays would not support one", True,
      "neither i.i.d." in __doc__)
check("and says the backends are stand-ins", True,
      "stand-ins, not the API" in __doc__)

print()
print("scope of this test: the --s2-perf request path, against stand-in backends,")
print("with synthetic timing. No confidence coverage is claimed, no WOA23 data was")
print("read, no api/ module was imported, and no production port was contacted.")
print()
raise SystemExit(summary(PASS_N, FAIL_N))