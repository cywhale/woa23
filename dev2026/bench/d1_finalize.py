"""The worker-count record, written from explicit arguments.

Split out of a shell heredoc inside `run_controlled.sh`, where it read the label
from `os.environ["LABEL"]` — a shell variable that was never exported. The run
measured everything, wrote its characterization result, and then died with
`KeyError: 'LABEL'` while writing this file, leaving `<label>_workers.json` empty
and `<label>_requests.json` unwritten.

Two lessons, both applied here:

* **the label arrives as an argument.** Nothing reads the environment for a value
  the caller already knows;
* **this is a module, so a test can run it.** The failure was in a path no offline
  test executed, which is the same class of defect as logic inside a heredoc.

**What it records is what the processes had**, not what the runner intended: the
count is parsed from each arm's `launch_argv` as `/proc/<pid>/cmdline` gave it.

    uv run python -m bench.d1_finalize --label d1a --results results \\
        --out results/d1a_workers.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.collect_backend_meta import worker_count  # noqa: E402

ARMS = ("candidate", "reference")

#: The sentence that keeps a mode's FIXED worker count from being read as C2's
#: MEASURED one, per mode. Carried in the artefact rather than only in a report.
#:
#: One record shape, one reader, mode-specific wording — because the S2 performance
#: mode reused this and wrote "D1 uses one worker per arm by design" into an S2
#: artefact. A reader who found that sentence beside a latency figure would have had
#: no way to tell whether the file described a D1 characterization or an S2
#: measurement, and the answer would have been neither.
MODES = {
    "d1": {
        "source": "fixed at 1 by the D1 mode (the -w on each launch line)",
        "assertion": ("--expect-worker-count 1 verifies this against each arm's own "
                      "/proc/<pid>/cmdline. It is an assertion and sets nothing; the "
                      "D1 mode is what sets the worker count."),
        "note": ("D1 uses one worker per arm by design. This is not a "
                 "production-worker-count validation and makes no claim about "
                 "multi-worker D1 behavior."),
        "contrast": ("C2 is the mode that measures production's worker count and "
                     "runs the arms at it. D1 does neither."),
    },
    "s2perf": {
        "source": ("fixed at 1 by the S2 performance mode (the -w on each launch "
                   "line)"),
        "assertion": ("verified against each arm's own /proc/<pid>/cmdline. It is an "
                      "assertion and sets nothing; the mode is what sets the worker "
                      "count."),
        "note": ("This run uses one worker per arm by design. It is a single-worker "
                 "steady-state request-path measurement and makes no claim about "
                 "multi-worker behaviour; production's worker count is S4."),
        "contrast": ("C2 is the mode that measures production's worker count and "
                     "runs the arms at it. This mode does neither."),
    },
}

#: Kept for the D1 callers and tests that import it by name.
WORKER_NOTE = MODES["d1"]["note"]


def build(label: str, results: Path, mode: str = "d1") -> tuple[dict, list[str]]:
    """The record, and every problem found building it. Never raises on bad input."""
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {sorted(MODES)}")
    words = MODES[mode]
    out = {
        "kind": "arm_worker_count",
        "label": label,
        "mode": mode,
        "arm_workers": 1,
        "source": words["source"],
        "assertion": words["assertion"],
        "derived_from_production_measurement": False,
        "note": words["note"],
        "contrast": words["contrast"],
        "per_arm": {},
    }
    problems: list[str] = []
    for arm in ARMS:
        path = results / f"{label}_meta_{arm}.json"
        try:
            meta = json.loads(path.read_text())
        except Exception as exc:                   # noqa: BLE001
            problems.append(f"{path}: {type(exc).__name__}: {exc}")
            out["per_arm"][arm] = {"worker_count": None,
                                   "worker_count_error": f"cannot read {path}"}
            continue
        argv = meta.get("launch_argv")
        if not isinstance(argv, list) or not argv:
            problems.append(f"{path}: no launch_argv to read a worker count from")
            out["per_arm"][arm] = {"worker_count": None,
                                   "worker_count_error": "no launch_argv"}
            continue
        n, err = worker_count([a for a in argv if isinstance(a, str)])
        if n is None:
            problems.append(f"{arm}: {err}")
        elif n != out["arm_workers"]:
            problems.append(
                f"{arm}: launched with {n} worker(s), but the record says "
                f"{out['arm_workers']}. The arms do not have the count this run "
                f"recorded")
        out["per_arm"][arm] = {
            "worker_count": n,
            "worker_count_error": err,
            "launch_argv": argv,
            "launch_command": meta.get("launch_command"),
            "worker_pids": meta.get("worker_pids"),
        }
    out["problems"] = problems
    return out, problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", required=True,
                    help="the run's label. An ARGUMENT: this used to be read from an "
                         "environment variable the caller never exported.")
    ap.add_argument("--results", type=Path, default=Path("results"))
    ap.add_argument("--mode", default="d1", choices=sorted(MODES),
                    help="which mode's wording the record carries. The shape is the "
                         "same; the sentences that say what the count is NOT are "
                         "mode-specific, and a D1 sentence in an S2 artefact "
                         "describes neither run")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    record, problems = build(args.label, args.results, args.mode)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    # Written whether or not there were problems: a record naming what went wrong is
    # more use than no record, and an empty file is what this module exists to stop.
    args.out.write_text(json.dumps(record, indent=2))
    for arm in ARMS:
        p = record["per_arm"].get(arm, {})
        print(f"   {arm}: worker_count={p.get('worker_count')} "
              f"pids={p.get('worker_pids')}")
    for p in problems:
        print(f"   PROBLEM {p}", file=sys.stderr)
    print(f"wrote {args.out}")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
