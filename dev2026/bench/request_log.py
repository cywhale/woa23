"""A request journal written BEFORE each request, so an aborted stage still counts.

`bench/perf_counts.py` derives a stage's request count from the samples that stage
wrote down. That works exactly as long as the stage finishes. It does not when the
stage dies: `paired_bench` and `noise_pilot` do not catch transport errors, so a
refused or timed-out arm ends them with no artefact at all — and the requests they
had already issued became invisible. A run could put hundreds of requests on a host
and then report a total that did not include them.

The fix is the same rule `lib_requests.sh` already states for the shell counter:
**record the attempt before issuing it, never after, and never conditionally on the
outcome.** This module is that rule for the Python stages. Each line is appended and
flushed before the request leaves, so what survives a `SIGKILL` mid-request is a
journal with the attempt in it.

    {"stage": "latency", "arm": "candidate", "case": "readme_example", "seq": 42}

**These are attempts, not successes.** A line here means a request was ABOUT TO BE
sent to the host. It does not mean a response came back, and it never means a sample
was kept.

## What a journal count is, and what it is not

The line is written **before** the call. That ordering is what makes the journal
useful, and it is also exactly why the number it yields is not the same kind of
evidence in every situation.

**When the writing process exited normally** — a caught transport error, a status the
run did not expect, a clean finish — every `attempt()` that returned was followed by
an HTTP call that either completed or raised, and the file was closed. The record
count then equals the number of attempts made.

**When the process was killed asynchronously** (SIGKILL, OOM, a host that went away),
neither bound holds:

* the last line may describe a request **that was never sent** — the process can die
  between the flush and the call, so the count can be **one too high**;
* the last line may be **truncated** and unattributable to an arm — the count can be
  **one too low**.

So under abrupt termination the number of journal records is **not**:

* a floor, or a lower bound;
* "at least N requests";
* "requests actually issued";
* observed host traffic.

It is `journaled_attempts`: **the number of records this journal contains.** Nothing
more is claimed from it, and `host_attempt_count_exact` is false. A genuine bound
would need a derivation over the single-threaded request loop, the flush point and
the truncated-record case, tested as such; until that exists, the record count and
the exactness flag are what is reported, alongside the authorised ceiling.

**A journal is not a substitute for a complete artefact.** When a stage aborts, the
journal says how many attempts it recorded and the artefact says what was measured.
`perf_counts` carries both, with the evidence strength attached; see
`counts_exact` and `attempt_evidence` there.

    uv run python -m bench.request_log --read run/requests/latency.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ARMS = ("candidate", "reference")


class RequestLog:
    """Append-only journal of request attempts. One instance per stage per process."""

    def __init__(self, path: Path | str | None, stage: str):
        self.stage = stage
        self.path = Path(path) if path else None
        self._seq = 0
        self._fh = None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            # Line buffered, opened for append: two stages may share a directory and
            # a re-run must never truncate a journal it did not write.
            self._fh = self.path.open("a", buffering=1)

    def attempt(self, arm: str, case: str = "") -> None:
        """Record that a request is about to be issued. Call it BEFORE the request."""
        self._seq += 1
        if self._fh is None:
            return
        self._fh.write(json.dumps({"stage": self.stage, "arm": arm, "case": case,
                                   "seq": self._seq}) + "\n")
        # flush() alone leaves the line in the kernel's page cache, which survives
        # this process dying but not the host doing so. The journal exists for the
        # first case; fsync on every request would cost more than it is worth here,
        # so the line buffering above is the guarantee and this is not claimed to be
        # crash-durable beyond process death.
        self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def read_counts(path: Path) -> tuple[dict, list[str]]:
    """Per-arm journal RECORD counts. See the module docstring for what they mean.

    A truncated last line cannot be attributed to an arm, so it is reported and NOT
    added to either — which is one of the two reasons the total is not a bound in
    both directions. It is not silently dropped.
    """
    out = {a: 0 for a in ARMS}
    problems: list[str] = []
    if not path.exists():
        return out, [f"{path}: no request journal"]
    text = path.read_text()
    for n, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:                          # noqa: BLE001
            problems.append(f"{path}:{n}: truncated journal record — the writer was "
                            f"killed mid-write. It cannot be attributed to an arm "
                            f"and is NOT counted. The record exists and the count "
                            f"does not include it, which is one of the two reasons "
                            f"a journal count is not a bound after a hard kill")
            continue
        arm = rec.get("arm")
        if arm in out:
            out[arm] += 1
        else:
            problems.append(f"{path}:{n}: record for unknown arm {arm!r}, not counted")
    return out, problems


def integrity(path: Path) -> dict:
    """What can be said about this journal as evidence, without interpreting it.

    `truncated` is the mid-write case; `attributable` is what `read_counts` totals.
    A caller decides evidence strength from these plus what it knows about HOW the
    writer ended, which the journal itself cannot record — a process that is killed
    does not get to write "I was killed".
    """
    out = {"exists": path.exists(), "attributable_records": 0,
           "truncated_records": 0, "unattributed_records": 0}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:                          # noqa: BLE001
            out["truncated_records"] += 1
            continue
        if rec.get("arm") in ARMS:
            out["attributable_records"] += 1
        else:
            out["unattributed_records"] += 1
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--read", type=Path, required=True)
    args = ap.parse_args()
    counts, problems = read_counts(args.read)
    print(json.dumps({"kind": "request_journal_counts", "path": str(args.read),
                      "counts_attempts_not_successes": True,
                      "journaled_attempts_per_arm": counts,
                      "integrity": integrity(args.read),
                      "note": ("record counts. Under abrupt termination these are "
                               "neither a bound nor host traffic — see the module "
                               "docstring"),
                      "problems": problems}, indent=2))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
