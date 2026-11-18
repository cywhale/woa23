#!/usr/bin/env python3
"""Constrained N1 transformer for the observed WOA23 256-color YSLD pattern.

Usage:
  n1_ysld.py make ORIGINAL OUTPUT_DIR
  n1_ysld.py check ORIGINAL CANDIDATE

This is deliberately not a general YSLD parser. It accepts only an ASCII style with
four identical 256-color ``categorize(value, ...)`` expressions whose 255 thresholds
are ``k*(inputmax-inputmin)+inputmin`` for k = 1/256 ... 255/256. It preserves all
bytes outside those four expressions and fails closed on an unfamiliar structure.

The generated file is written as a new 0600 file into an existing 0700 directory.
The tool never overwrites an existing path and prints only hashes and structural
facts, not style content. Static PASS does not prove GeoTools/GeoServer rendering,
image equivalence, boundary behavior, NaN/division-by-zero behavior, or performance.
"""

from __future__ import annotations

import fractions
import hashlib
import os
import re
import stat
import sys

COLOURS = 256
OLD_ARGUMENT = "value"
NEW_ARGUMENT = "(value-inputmin)/(inputmax-inputmin)"
START = re.compile(r"^(\s*[a-z-]+: )\$\{categorize\(")
OLD_THRESHOLD = re.compile(
    r"^([0-9]+(?:\.[0-9]+)?)\*\(inputmax-inputmin\)\+inputmin$"
)
NEW_THRESHOLD = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")
COLOR = re.compile(r"^'#[0-9A-Fa-f]{6}'$")
OLD_EXPR = re.compile(
    r"([0-9]+(?:\.[0-9]+)?)\*\(inputmax-inputmin\)\+inputmin"
)


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as stream:
        return stream.read()


def split_lines(data: bytes) -> tuple[list[str], bytes, bool]:
    if b"\r\n" in data:
        remainder = data.replace(b"\r\n", b"")
        if b"\n" in remainder or b"\r" in remainder:
            raise ValueError("mixed line endings")
        ending = b"\r\n"
    else:
        if b"\r" in data:
            raise ValueError("bare CR line ending")
        ending = b"\n"
    terminated = data.endswith(ending)
    parts = data.split(ending)
    if terminated:
        parts.pop()
    try:
        lines = [part.decode("ascii") for part in parts]
    except UnicodeDecodeError as exc:
        raise ValueError("non-ASCII input is outside this tool's accepted format") from exc
    return lines, ending, terminated


def expression_spans(lines: list[str]) -> list[tuple[int, int]]:
    result: list[tuple[int, int]] = []
    index = 0
    while index < len(lines):
        if not START.match(lines[index]):
            index += 1
            continue
        end = index
        while end < len(lines) and not lines[end].rstrip().endswith(")}"):
            end += 1
        if end == len(lines):
            raise ValueError("unterminated categorize expression")
        result.append((index, end))
        index = end + 1
    return result


def expression_args(lines: list[str]) -> tuple[str, list[str]]:
    match = START.match(lines[0])
    if not match:
        raise ValueError("expression start")
    text = " ".join([lines[0][match.end():]] + [line.strip() for line in lines[1:]]).rstrip()
    if not text.endswith(")}"):
        raise ValueError("expression end")
    text = text[:-2]
    args: list[str] = []
    depth = 0
    quoted = False
    current: list[str] = []
    for char in text:
        if char == "'":
            quoted = not quoted
        elif not quoted and char == "(":
            depth += 1
        elif not quoted and char == ")":
            depth -= 1
        if char == "," and not quoted and depth == 0:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    args.append("".join(current).strip())
    return match.group(1), args


def continuation_shape(line: str) -> tuple[int, int]:
    commas = line.count(", ") + (1 if line.rstrip().endswith(",") else 0)
    return len(line) - len(line.lstrip(" ")), commas


def validate(data: bytes, *, normalized: bool) -> tuple[dict[str, object], list[str]]:
    facts: dict[str, object] = {}
    failures: list[str] = []
    try:
        lines, ending, terminated = split_lines(data)
        spans = expression_spans(lines)
    except ValueError as exc:
        return facts, [str(exc)]
    facts["line_ending"] = "CRLF" if ending == b"\r\n" else "LF"
    facts["trailing_newline"] = terminated
    facts["expression_count"] = len(spans)
    if len(spans) != 4:
        return facts, [f"expected 4 categorize expressions, found {len(spans)}"]
    marker = "${categorize("
    marker_count = sum(line.count(marker) for line in lines)
    if marker_count != 4 or any(lines[start].count(marker) != 1 for start, _ in spans):
        return facts, ["found an unrecognized categorize expression occurrence"]
    if any(marker in lines[index] for index in range(len(lines)) if index not in {start for start, _ in spans}):
        return facts, ["found an unrecognized categorize expression occurrence"]

    parsed: list[tuple[str, list[str]]] = []
    for first, last in spans:
        try:
            parsed.append(expression_args(lines[first:last + 1]))
        except ValueError as exc:
            failures.append(str(exc))
    if failures:
        return facts, failures

    all_args: list[tuple[str, ...]] = []
    colors_by_expression: list[tuple[str, ...]] = []
    expected_thresholds = [fractions.Fraction(n, COLOURS) for n in range(1, COLOURS)]
    for _, args in parsed:
        all_args.append(tuple(args))
        if len(args) != 2 * COLOURS:
            failures.append(f"expected {2 * COLOURS} arguments per expression")
            continue
        if args[0] != (NEW_ARGUMENT if normalized else OLD_ARGUMENT):
            failures.append("unexpected categorize input expression")
        colors, thresholds = args[1::2], args[2::2]
        colors_by_expression.append(tuple(colors))
        if not all(COLOR.fullmatch(color) for color in colors):
            failures.append("color literal is not a quoted #RRGGBB value")
        if normalized:
            if not all(NEW_THRESHOLD.fullmatch(threshold) for threshold in thresholds):
                failures.append("normalized threshold is not a decimal literal")
            else:
                try:
                    values = [fractions.Fraction(threshold) for threshold in thresholds]
                    if values != expected_thresholds:
                        failures.append("normalized thresholds are not n/256 for n=1..255")
                except (ValueError, ZeroDivisionError):
                    failures.append("invalid normalized threshold")
        else:
            matches = [OLD_THRESHOLD.fullmatch(threshold) for threshold in thresholds]
            if not all(matches):
                failures.append("source threshold has an unsupported expression")
            else:
                coefficients = [match.group(1) for match in matches if match]
                try:
                    values = [fractions.Fraction(value) for value in coefficients]
                    if values != expected_thresholds:
                        failures.append("source thresholds are not n/256 for n=1..255")
                except (ValueError, ZeroDivisionError):
                    failures.append("invalid source threshold")
    if len(set(all_args)) != 1:
        failures.append("the four categorize expressions are not identical")
    if len(set(colors_by_expression)) != 1:
        failures.append("the four color sequences are not identical")
    facts["colors_per_expression"] = len(colors_by_expression[0]) if colors_by_expression else 0
    facts["distinct_colors"] = len(set(colors_by_expression[0])) if colors_by_expression else 0
    facts["thresholds_per_expression"] = COLOURS - 1
    return facts, failures


def check_bytes(original: bytes, candidate: bytes) -> tuple[dict[str, object], list[str]]:
    facts: dict[str, object] = {}
    failures: list[str] = []
    outside_unchanged = False
    colors_unchanged = False
    original_facts, original_failures = validate(original, normalized=False)
    candidate_facts, candidate_failures = validate(candidate, normalized=True)
    if original_failures:
        failures.extend(f"source: {failure}" for failure in original_failures)
    if candidate_failures:
        failures.extend(f"candidate: {failure}" for failure in candidate_failures)
    try:
        old_lines, old_ending, old_terminated = split_lines(original)
        new_lines, new_ending, new_terminated = split_lines(candidate)
        old_spans, new_spans = expression_spans(old_lines), expression_spans(new_lines)
    except ValueError as exc:
        facts["lines_outside_expressions_unchanged"] = False
        facts["color_sequence_unchanged"] = False
        return facts, failures + [str(exc)]
    if old_ending != new_ending or old_terminated != new_terminated:
        failures.append("line ending or trailing newline changed")
    if len(old_lines) != len(new_lines) or old_spans != new_spans:
        failures.append("line count or expression locations changed")
    else:
        inside = {line for start, end in old_spans for line in range(start, end + 1)}
        outside_changes = [i for i in range(len(old_lines)) if i not in inside and old_lines[i] != new_lines[i]]
        if outside_changes:
            failures.append("bytes outside the four expressions changed")
        elif old_ending == new_ending and old_terminated == new_terminated:
            outside_unchanged = True
        colors_unchanged = True
        for (old_start, old_end), (new_start, new_end) in zip(old_spans, new_spans):
            old_key, old_args = expression_args(old_lines[old_start:old_end + 1])
            new_key, new_args = expression_args(new_lines[new_start:new_end + 1])
            if old_key != new_key:
                failures.append("expression property key or indentation changed")
            if len(old_args) == 2 * COLOURS and len(new_args) == 2 * COLOURS:
                if old_args[1::2] != new_args[1::2]:
                    failures.append("color sequence changed")
                    colors_unchanged = False
            else:
                colors_unchanged = False
            old_shape = [continuation_shape(line) for line in old_lines[old_start + 1:old_end + 1]]
            new_shape = [continuation_shape(line) for line in new_lines[new_start + 1:new_end + 1]]
            if old_shape != new_shape:
                failures.append("expression continuation layout changed")
    facts.update(
        source_line_ending=original_facts.get("line_ending"),
        source_expressions=original_facts.get("expression_count"),
        candidate_expressions=candidate_facts.get("expression_count"),
        lines_outside_expressions_unchanged=outside_unchanged,
        color_sequence_unchanged=colors_unchanged,
    )
    return facts, failures


def make_bytes(original: bytes) -> bytes:
    facts, failures = validate(original, normalized=False)
    if failures:
        raise ValueError("source rejected: " + "; ".join(failures))
    lines, ending, terminated = split_lines(original)
    for first, last in expression_spans(lines):
        lines[first] = lines[first].replace(
            "${categorize(value, ", f"${{categorize({NEW_ARGUMENT}, ", 1
        )
        for index in range(first, last + 1):
            lines[index] = OLD_EXPR.sub(lambda match: match.group(1), lines[index])
    output = ending.join(line.encode("ascii") for line in lines) + (ending if terminated else b"")
    _, check_failures = check_bytes(original, output)
    if check_failures:
        raise ValueError("generated candidate failed static check: " + "; ".join(check_failures))
    return output


def write_new(output_dir: str, filename: str, data: bytes) -> str:
    directory = os.lstat(output_dir)
    if not stat.S_ISDIR(directory.st_mode) or stat.S_IMODE(directory.st_mode) != 0o700:
        raise ValueError("output directory must be a real directory with mode 0700")
    path = os.path.join(output_dir, filename)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        remaining = memoryview(data)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if read_bytes(path) != data:
        raise OSError("read-back differs from generated bytes; inspect the new output file")
    if stat.S_IMODE(os.lstat(path).st_mode) != 0o600:
        raise OSError("output file mode is not 0600")
    return path


def candidate_filename(source_path: str) -> str:
    basename = os.path.basename(source_path)
    stem, extension = os.path.splitext(basename)
    if not extension:
        raise ValueError("source filename must have an extension")
    if stem.endswith("_i"):
        return f"{stem[:-2]}_n1_i{extension}"
    return f"{stem}_n1{extension}"


def main(argv: list[str]) -> int:
    if len(argv) == 3 and argv[0] == "make":
        source_path, output_dir = argv[1:]
        try:
            output = make_bytes(read_bytes(source_path))
            output_name = candidate_filename(source_path)
            path = write_new(output_dir, output_name, output)
        except (OSError, ValueError) as exc:
            print(f"STOP: {exc}", file=sys.stderr)
            return 1
        print(f"written {output_name}; sha256 {hashlib.sha256(output).hexdigest()}; mode 0600")
        return 0
    if len(argv) == 3 and argv[0] == "check":
        try:
            original, candidate = read_bytes(argv[1]), read_bytes(argv[2])
            facts, failures = check_bytes(original, candidate)
        except (OSError, ValueError) as exc:
            print(f"STOP: {exc}", file=sys.stderr)
            return 1
        print(f"source sha256 {hashlib.sha256(original).hexdigest()}")
        print(f"candidate sha256 {hashlib.sha256(candidate).hexdigest()}")
        for key in sorted(facts):
            print(f"{key}: {facts[key]}")
        for failure in failures:
            print(f"FAIL: {failure}")
        print("N1-STATIC-PASS" if not failures else "N1-STATIC-FAIL")
        return 0 if not failures else 1
    print(__doc__.strip())
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
