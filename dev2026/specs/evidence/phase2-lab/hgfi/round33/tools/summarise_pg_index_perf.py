#!/usr/bin/env python3
"""Rebuild Round 33's per-layer warm execution summary from the selected perf.json record."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path

ARMS = ("A", "C", "D", "CD")
FIELDS = ("layer", "family", "regime", "grid", "parameter")
HEADER = (
    [*FIELDS, "cells"]
    + [f"exec_med_{arm}" for arm in ARMS]
    + [f"windowagg_{arm}" for arm in ARMS]
    + ["A_to_C", "A_to_D", "C_to_CD", "D_to_CD"]
)


def pct(new: float, old: float) -> str:
    return "" if old == 0 else f"{(new - old) / old * 100:+.1f}%"


def summarize(rows: list[dict]) -> list[dict[str, str]]:
    cells: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        if "cell" not in row or row.get("arm") not in ARMS:
            raise ValueError("each input row must have a known cell and arm")
        if not isinstance(row.get("execution_ms"), (int, float)):
            raise ValueError("execution_ms must be numeric")
        cells[str(row["cell"])][row["arm"]].append(row)

    by_layer: dict[str, list[dict]] = defaultdict(list)
    for cell_id, arms in cells.items():
        if set(arms) != set(ARMS):
            raise ValueError(f"cell {cell_id} does not contain all four arms")
        first = arms["A"][0]
        if any(any(r.get(k) != first.get(k) for k in FIELDS) for rs in arms.values() for r in rs):
            raise ValueError(f"cell {cell_id} has inconsistent layer metadata")
        medians = {arm: statistics.median(r["execution_ms"] for r in arms[arm]) for arm in ARMS}
        by_layer[str(first["layer"])].append(
            {"first": first, "medians": medians,
             "windowagg": {arm: [bool(r.get("windowagg")) for r in arms[arm]] for arm in ARMS}}
        )

    output = []
    for layer in sorted(by_layer):
        layer_cells = by_layer[layer]
        first = layer_cells[0]["first"]
        med = {arm: statistics.median(c["medians"][arm] for c in layer_cells) for arm in ARMS}
        row = {k: str(first[k]) for k in FIELDS}
        row["cells"] = str(len(layer_cells))
        row.update({f"exec_med_{arm}": f"{med[arm]:.3f}" for arm in ARMS})
        for arm in ARMS:
            values = [flag for cell in layer_cells for flag in cell["windowagg"][arm]]
            row[f"windowagg_{arm}"] = "all" if all(values) else ("none" if not any(values) else "mixed")
        row.update({
            "A_to_C": pct(med["C"], med["A"]),
            "A_to_D": pct(med["D"], med["A"]),
            "C_to_CD": pct(med["CD"], med["C"]),
            "D_to_CD": pct(med["CD"], med["D"]),
        })
        output.append(row)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Round 33 perf.json")
    parser.add_argument("output", type=Path, help="destination TSV")
    args = parser.parse_args()
    rows = json.loads(args.input.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or not rows:
        raise ValueError("input must be a non-empty JSON list")
    summary = summarize(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as stream:
        stream.write("\t".join(HEADER) + "\n")
        for row in summary:
            stream.write("\t".join(row.get(key, "") for key in HEADER) + "\n")
    print(f"PASS: {len(rows)} timing rows, {len(summary)} layers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
