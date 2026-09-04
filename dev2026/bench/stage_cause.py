"""Why the run is incomplete, read from the stages' own artefacts.

The run-level classification is neutral — `INCOMPLETE_STAGE_FAILURE` — because the
run does not know why a stage stopped; the stage does. It used to be hardcoded
`INCOMPLETE_TRANSPORT_FAILURE`, so a backend answering HTTP 500 was reported at run
level as a transport failure. The stage artefact said one thing and the headline said
another, and the headline is what gets quoted.

So the cause is read back from the artefacts rather than restated, and it cannot
drift from what the stages recorded.

A stage that left **no** artefact cannot name its own reason. That is said, not
guessed at: the caller's exit status is what put the run here, and the report says
which stage it was.

    uv run python -m bench.stage_cause --label s2p --results results
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

#: Checked in this order, so the first stage to fail is named first.
STAGE_ARTEFACTS = (
    "{label}_symmetric_warmup.json",
    "{label}_paired.json",
    "{label}_noise_pilot_reference.json",
    "{label}_noise_pilot_candidate.json",
)

NO_RECORD = ("NO_STAGE_ARTEFACT_RECORDS_A_CAUSE (a stage exited non-zero without "
             "leaving one; the run report names which)")


def causes(results: Path, label: str) -> list[str]:
    found: list[str] = []
    for pattern in STAGE_ARTEFACTS:
        path = results / pattern.format(label=label)
        if not path.exists():
            continue
        try:
            doc = json.loads(path.read_text())
        except Exception:                          # noqa: BLE001
            found.append(f"{path.name}:UNREADABLE")
            continue
        if doc.get("complete") is False:
            aborted = doc.get("aborted") or {}
            found.append(f"{path.name}:"
                         f"{aborted.get('classification', 'UNCLASSIFIED')}")
    return found


def summary(results: Path, label: str) -> str:
    found = causes(results, label)
    return "; ".join(found) if found else NO_RECORD


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", required=True)
    ap.add_argument("--results", type=Path, default=Path("results"))
    args = ap.parse_args()
    print(summary(args.results, args.label))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
