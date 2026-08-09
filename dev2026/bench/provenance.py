"""Provenance validation, shared by the gate and the sidecar.

These checks live outside both tools because both need them: `paired_bench.py`
validates the pre-run records before sampling, and `collect_backend_meta.py
--against` validates the same record again when it re-reads the store afterwards.
Importing one tool from the other would be circular; putting the rules in a third
place keeps a single definition of what a valid record is.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from bench.manifests import MANIFESTS, expand

# A pinned hash seed is what makes output ordering reproducible (spec 001 section
# 5.1), and section 5.2A's byte comparison depends on it.
#
# It cannot be required of every backend in every variant. Under 5.2B the reference
# *is* live production, which we are not allowed to restart — an unpinned seed there
# is the condition that makes 5.2B necessary, not a defect in the record. Requiring
# it unconditionally would have made the only variant D2a permits unrunnable, which
# is what the first D2a campaign discovered.
#
# So the requirement is per-arm and stated by the caller. It is never waived for a
# backend we start ourselves.
REQUIRED_HASH_SEED = "0"

#: What `collect_backend_meta.whitelisted_env` records when PYTHONHASHSEED is absent.
#: An unset seed is a finding, not a gap, so it is recorded as a value.
UNSET_HASH_SEED = "<unset — randomised>"

#: Which arms must have a pinned seed, by campaign. Three cases, not two — and the
#: third is what stopped C2 cycle 1.
#:
#:   both-pinned         D2b and C1. Both arms are ours and both are pinned; a
#:                       byte-exact comparison depends on it.
#:   reference-unpinned  5.2B against live production. The reference is production,
#:                       which we may not restart, so its seed is whatever it is —
#:                       "any", because we cannot assert either way.
#:   both-unpinned       C2. Both arms are ours and both are deliberately UNPINNED.
#:                       This one requires the seed to be absent rather than merely
#:                       tolerating it: a C2 cycle that ran with a pinned seed
#:                       observed nothing about unpinned behaviour, and would report
#:                       three identical seeds as if that were a fact about the
#:                       interpreter instead of about the launch.
SEED_POLICIES: dict[str, dict[str, str]] = {
    "both-pinned":        {"candidate": "pinned",   "reference": "pinned"},
    "reference-unpinned": {"candidate": "pinned",   "reference": "any"},
    "both-unpinned":      {"candidate": "unpinned", "reference": "unpinned"},
}


def seed_requirement_for(policy: str, label: str) -> str:
    """What `label` must show under `policy`. Unknown policies fail closed."""
    if policy not in SEED_POLICIES:
        raise ValueError(f"unknown seed policy {policy!r}; "
                         f"expected one of {sorted(SEED_POLICIES)}")
    return SEED_POLICIES[policy].get(label, "pinned")

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# Every field the sidecar must supply, with the type it must have. Checking only
# the interesting ones lets a stub through: an object carrying nothing but a fake
# hash and a hash seed would have passed the earlier version of this function.
REQUIRED_META_FIELDS: tuple[tuple[str, type | tuple[type, ...]], ...] = (
    ("kind", str), ("label", str), ("manifest_patterns", list),
    ("collected_at", str), ("host", str), ("kernel", str), ("cwd", str),
    ("executable", str), ("launch_argv", list), ("launch_command", str),
    ("master_pid", int), ("port", int), ("listener_pids", list),
    ("port_verified", bool), ("proc_starttime", int), ("boot_id", str),
    ("expect_argv_contains", list), ("worker_pids", list), ("env", dict),
    ("source_sha256", dict), ("env_python", str), ("env_python_source", str),
    ("env_python_version", str),
    ("store_path", str), ("store_source", str), ("store_path_literal", str),
    ("zmetadata_fingerprints", dict), ("dependencies", dict),
    # Emitted by the sidecar and therefore required: a field the collector always
    # writes but the validator never checks is a field nobody notices going missing.
    ("env_whitelist", list), ("collector_python", str),
)


def load_meta(path: Path | None, label: str) -> tuple[dict | None, list[str]]:
    """Read a sidecar file. A broken file is INVALID_METADATA, never a traceback.

    A malformed or unreadable provenance file means the run cannot be reproduced,
    which is exactly the condition the gate exists to catch — so it must flow into
    the verdict rather than crashing the harness after the samples were collected.
    """
    if path is None:
        return None, [f"{label}: no backend metadata "
                      f"(run bench.collect_backend_meta on the backend's host)"]
    try:
        raw = path.read_text()
    except OSError as exc:
        return None, [f"{label}: cannot read {path} ({exc.__class__.__name__})"]
    try:
        meta = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, [f"{label}: {path} is not valid JSON (line {exc.lineno})"]
    if not isinstance(meta, dict):
        return None, [f"{label}: {path} is {type(meta).__name__}, expected an object"]
    return meta, []


def _verify_source_set(meta: dict, label: str) -> list[str]:
    """The recorded sources must be exactly the manifest's files, with the right hashes.

    Two separate checks, and the second is the one that has teeth:

    1. **The file set.** The manifest is re-expanded against the backend's own cwd
       and the key sets compared, catching a fabricated entry and a subset that
       quietly omits the file that changed.
    2. **The digests.** Each file is re-hashed here and compared. Checking that a
       digest *looks* like a digest is not enough — a well-formed hash of the wrong
       content passes every syntactic rule, and until revision 21 it passed this
       function too. The test fixtures wrote `x` while recording `"a" * 64`, and
       nothing objected.

    This requires running on the backend's host — which the gate does by
    construction, since both arms are loopback. If the cwd is not reachable, that
    is reported rather than skipped: an unverifiable source set is not a verified
    one.

    A digest that disagrees is not necessarily tampering; a source file edited
    between provenance collection and the gate looks the same. Either way the record
    no longer describes what is on disk, and the run cannot be published.
    """
    cwd = Path(str(meta.get("cwd", "")))
    if not cwd.is_dir():
        return [f"{label}: cwd {cwd} is not reachable from here, so the hashed file "
                f"set cannot be checked against the manifest (run the gate on the "
                f"backend's host)"]
    try:
        expected = {str(p.relative_to(cwd)) for p in expand(label, cwd)}
    except SystemExit as exc:
        return [f"{label}: manifest does not resolve under {cwd}: {exc}"]

    hashes = meta.get("source_sha256") or {}
    recorded = set(hashes.keys())
    problems = []
    for extra in sorted(recorded - expected):
        problems.append(f"{label}: source_sha256 names {extra!r}, which the manifest "
                        f"does not match — fabricated or stale entry")
    for missing in sorted(expected - recorded):
        problems.append(f"{label}: source_sha256 omits {missing!r}, which the manifest "
                        f"resolves to")

    for name in sorted(recorded & expected):
        try:
            actual = hashlib.sha256((cwd / name).read_bytes()).hexdigest()
        except OSError as exc:
            problems.append(f"{label}: cannot re-hash {name} to check the recorded "
                            f"digest ({exc.__class__.__name__})")
            continue
        if hashes[name] != actual:
            problems.append(
                f"{label}: {name} does not match its recorded digest "
                f"(recorded {str(hashes[name])[:12]}…, on disk {actual[:12]}…) — the "
                f"record describes different content than the file has now")
    return problems


def validate_store_agreement(cand: dict | None, ref: dict | None) -> list[str]:
    """Both arms must be pointed at the same store. This is not optional.

    A latency comparison between two backends over different stores measures the
    stores, not the change.

    **What this compares is consolidated metadata, not data.** `.zmetadata` holds
    array shapes, chunk grids, compressors and attributes; it says nothing about the
    bytes in the chunks. Two stores with identical `.zmetadata` can hold different
    values. So a digest mismatch is reported as a *metadata* mismatch, and agreement
    here is evidence that the two arms were configured against the same store — not
    proof that they read identical data. That proof comes from the contract gate
    (section 5), which compares the responses themselves.

    **What this brackets is the two provenance collections, not the sampling run.**
    Both fingerprints are taken before the first request, so a change detected here
    happened between the two collections. Detecting a change *during* sampling needs
    a second collection afterwards — `collect_backend_meta.py --against` does that,
    and the procedure is in spec 001 section 5.3.3.

    Compared: the resolved store path, the set of groups found, and each group's
    `.zmetadata` digest and nanosecond mtime.
    """
    if not cand or not ref:
        return []          # absence is already reported by validate_meta

    problems = []
    # Canonical absolute paths, symlinks already followed by the sidecar. Each arm
    # reaches the store through its own staging symlink, so the literals can and do
    # match while the targets differ — only the resolved path settles it. A relative
    # value here is a defect in the record, not a store that happens to be relative:
    # it would have been fingerprinted against the collector's cwd rather than the
    # backend's.
    for label, m in (("candidate", cand), ("reference", ref)):
        sp = m.get("store_path")
        if not isinstance(sp, str) or not sp.startswith("/"):
            problems.append(f"{label}: store_path {sp!r} is not a canonical absolute "
                            f"path, so it does not identify the store the backend read")
    if not problems and cand.get("store_path") != ref.get("store_path"):
        problems.append(
            f"store mismatch: candidate reads {cand.get('store_path')!r}, reference "
            f"reads {ref.get('store_path')!r} — the two arms resolve to different "
            f"stores, so any comparison measures the stores")

    cfp = cand.get("zmetadata_fingerprints")
    rfp = ref.get("zmetadata_fingerprints")
    if not isinstance(cfp, dict) or not isinstance(rfp, dict):
        return problems

    for missing in sorted(set(rfp) - set(cfp)):
        problems.append(f"store mismatch: group {missing!r} seen by the reference "
                        f"but not the candidate")
    for extra in sorted(set(cfp) - set(rfp)):
        problems.append(f"store mismatch: group {extra!r} seen by the candidate "
                        f"but not the reference")

    for group in sorted(set(cfp) & set(rfp)):
        a, b = cfp[group], rfp[group]
        if not isinstance(a, dict) or not isinstance(b, dict):
            continue
        if a.get("sha256") != b.get("sha256"):
            problems.append(f"store metadata mismatch: {group} has different "
                            f"consolidated metadata in the two arms' views")
        elif a.get("mtime_ns") != b.get("mtime_ns"):
            problems.append(f"store touched between provenance collections: {group} "
                            f"has identical metadata but a different mtime")
    return problems


def validate_meta(meta: dict | None, label: str,
                  seed_requirement: str = "pinned") -> list[str]:
    """Reasons this run's provenance is not good enough to publish a number.

    Incompleteness here is not cosmetic. A result whose backend cannot be
    reconstructed cannot be re-checked by anyone later, and an unpinned hash seed
    means the ordering the contract gate compared is not the ordering a rerun would
    produce. Both invalidate the run rather than annotating it.
    """
    if meta is None:
        return [f"{label}: no backend metadata "
                f"(run bench.collect_backend_meta on the backend's host)"]
    problems = []

    for field, typ in REQUIRED_META_FIELDS:
        if field not in meta:
            problems.append(f"{label}: metadata is missing {field!r}")
        elif (not isinstance(meta[field], typ)
              or (typ is not bool and isinstance(meta[field], bool))):
            problems.append(f"{label}: {field!r} is {type(meta[field]).__name__}, "
                            f"expected {getattr(typ, '__name__', typ)}")
        elif isinstance(meta[field], (str, list, dict)) and len(meta[field]) == 0:
            problems.append(f"{label}: {field!r} is empty")

    if meta.get("kind") != "backend_meta":
        problems.append(f"{label}: kind is {meta.get('kind')!r}, expected 'backend_meta'")

    # The sidecar's label must be the one this arm was supposed to describe, and its
    # manifest must be the manifest currently in force — otherwise a stale or
    # mismatched provenance file silently vouches for the wrong thing.
    if meta.get("label") != label:
        problems.append(f"{label}: metadata is labelled {meta.get('label')!r}")
    elif list(meta.get("manifest_patterns") or []) != list(MANIFESTS[label]):
        problems.append(
            f"{label}: manifest_patterns {meta.get('manifest_patterns')} do not match "
            f"the manifest in force {list(MANIFESTS[label])}")

    pid = meta.get("master_pid")
    if isinstance(pid, int) and not isinstance(pid, bool) and pid <= 0:
        problems.append(f"{label}: master_pid is {pid}")
    port = meta.get("port")
    if isinstance(port, int) and not isinstance(port, bool) and not (0 < port < 65536):
        problems.append(f"{label}: port is {port}, outside 1-65535")

    # A record whose port could not be confirmed describes a process the benchmark
    # may never have talked to. Persisting the flag is only useful if the gate acts
    # on it.
    if meta.get("port_verified") is False:
        problems.append(
            f"{label}: port_verified is false — the sidecar could not confirm PID "
            f"{meta.get('master_pid')} holds port {meta.get('port')}, so this record "
            f"may describe a different process than the one measured")

    listeners = meta.get("listener_pids")
    if isinstance(listeners, list):
        if not all(isinstance(x, int) and not isinstance(x, bool) and x > 0
                   for x in listeners):
            problems.append(f"{label}: listener_pids must all be positive integers, "
                            f"got {listeners!r}")
        elif len(set(listeners)) != len(listeners):
            problems.append(f"{label}: listener_pids contains duplicates: {listeners}")
        elif isinstance(meta.get("master_pid"), int) \
                and meta["master_pid"] not in listeners:
            problems.append(
                f"{label}: master_pid {meta['master_pid']} is not among "
                f"listener_pids {listeners} — the record's own fields disagree about "
                f"which process held the port")

    start = meta.get("proc_starttime")
    if isinstance(start, int) and not isinstance(start, bool) and start <= 0:
        problems.append(f"{label}: proc_starttime is {start}")

    argv = meta.get("launch_argv")
    if isinstance(argv, list) and not all(isinstance(a, str) for a in argv):
        problems.append(f"{label}: launch_argv contains non-string entries")

    # Without this the post-run identity check silently has nothing to assert, so a
    # record lacking it must not be treated as complete.
    expect = meta.get("expect_argv_contains")
    if isinstance(expect, list) and not all(isinstance(a, str) and a for a in expect):
        problems.append(f"{label}: expect_argv_contains must be non-empty strings")

    hashes = meta.get("source_sha256")
    if isinstance(hashes, dict):
        for name, digest in hashes.items():
            if not isinstance(digest, str):
                problems.append(f"{label}: source {name} digest is not a string")
            elif digest.startswith("<"):
                problems.append(f"{label}: source {name} not hashed ({digest})")
            elif not SHA256_RE.match(digest):
                problems.append(f"{label}: source {name} digest is not 64 hex chars")

    deps = meta.get("dependencies")
    if isinstance(deps, dict):
        if deps.get("distributions_error"):
            problems.append(f"{label}: dependencies could not be listed "
                            f"({deps['distributions_error']})")
        elif not deps.get("name_version_set_sha256"):
            problems.append(f"{label}: dependencies record no name==version set digest")
    if meta.get("env_python_source") == "unresolved":
        problems.append(f"{label}: the package environment could not be resolved, so "
                        f"the dependency record describes no known interpreter")

    env = meta.get("env")
    seed = env.get("PYTHONHASHSEED") if isinstance(env, dict) else None
    if seed_requirement not in ("pinned", "unpinned", "any"):
        problems.append(f"{label}: unknown seed requirement {seed_requirement!r}")
    elif seed_requirement == "pinned" and seed != REQUIRED_HASH_SEED:
        problems.append(
            f"{label}: PYTHONHASHSEED is {seed!r}, must be {REQUIRED_HASH_SEED!r}")
    elif seed_requirement == "unpinned" and seed != UNSET_HASH_SEED:
        # Inverted on purpose. C2 exists to observe what an unpinned seed does; a
        # cycle that ran pinned answers a different question and must not be counted
        # as having answered this one.
        problems.append(
            f"{label}: PYTHONHASHSEED is {seed!r}, but this run requires it to be "
            f"unset — a pinned seed observes nothing about unpinned behaviour")

    expected_source = "hardcoded_relative" if label == "reference" else "env"
    if meta.get("store_source") not in (None, expected_source):
        problems.append(
            f"{label}: store_source is {meta.get('store_source')!r}, expected "
            f"{expected_source!r} — the reference reads a hard-coded relative path "
            f"and the candidate a mandatory environment variable")

    fps = meta.get("zmetadata_fingerprints")
    if isinstance(fps, dict):
        if "error" in fps:
            problems.append(f"{label}: store scan reports {fps['error']!r}")
        for group, fp in fps.items():
            if group == "error":
                continue
            if not isinstance(fp, dict) or "error" in fp:
                problems.append(f"{label}: zmetadata for {group} is {fp!r}")
            elif not SHA256_RE.match(str(fp.get("sha256", ""))):
                problems.append(f"{label}: zmetadata digest for {group} is not "
                                f"64 hex chars")
            elif not isinstance(fp.get("mtime_ns"), int):
                problems.append(f"{label}: zmetadata mtime_ns for {group} is "
                                f"{fp.get('mtime_ns')!r}")

    if isinstance(meta.get("source_sha256"), dict) and isinstance(meta.get("cwd"), str) \
            and meta.get("label") == label:
        problems.extend(_verify_source_set(meta, label))

    return problems


# --- escalation --------------------------------------------------------------

# The verdicts paired_bench can record. Anything else in a prior result means the
# file was written by a different tool or a different version of this one.
VALID_REGRESSION_VERDICTS = frozenset(
    {"REGRESSION", "NO_REGRESSION", "INCONCLUSIVE", "INVALID_STATUS"})

PRIOR_RUNG_FIELDS = ("kind", "gate", "gate_variant", "warm_samples_per_arm",
                     "metadata_complete", "post_run_drift", "results",
                     "candidate_url", "reference_url",
                     "candidate_meta", "reference_meta")


def verify_prior_rung(prior: dict | None, cand_meta: dict | None,
                      ref_meta: dict | None, expect_rung: int,
                      expect_variant: str,
                      expect_cases: set | None = None,
                      expect_candidate_url: str | None = None,
                      expect_reference_url: str | None = None) -> list[str]:
    """Is an earlier rung's result a sound basis for escalating from?

    Checking only that the file exists would let a rung-60 run escalate from a
    result produced by different code, against a different store, under a different
    gate variant, or from a run that was itself invalid — and then present the pair
    as one campaign. Escalation inherits everything the earlier rung established, so
    everything it established has to still hold.
    """
    if prior is None:
        return ["no prior rung result to escalate from"]
    problems = []

    # Presence before meaning. A truncated file can satisfy several checks below by
    # simply not containing the fields they look at — `.get()` returning None is not
    # the same as a value that passed.
    for field in PRIOR_RUNG_FIELDS:
        if field not in prior:
            problems.append(f"prior rung result has no {field!r} field — truncated or "
                            f"written by a different tool")
    if problems:
        return problems

    # Types before contents. `metadata_complete` in particular has to be exactly
    # True: a truthy string or 1 would sail through `if not prior.get(...)`.
    if prior.get("metadata_complete") is not True:
        problems.append(f"metadata_complete is {prior.get('metadata_complete')!r}, "
                        f"must be exactly True")
    if not isinstance(prior.get("post_run_drift"), list):
        problems.append(f"post_run_drift is "
                        f"{type(prior.get('post_run_drift')).__name__}, expected a list")
    if not isinstance(prior.get("results"), list):
        return problems + [f"results is {type(prior.get('results')).__name__}, "
                           f"expected a list"]

    # Every row must carry a usable verdict. Without this a prior rung whose rows
    # lack `regression_verdict` passes the case-set check, the escalation then finds
    # no INCONCLUSIVE case, and the script reports "no escalation needed" — turning
    # a malformed file into a clean bill of health.
    for i, row in enumerate(prior["results"]):
        if not isinstance(row, dict):
            problems.append(f"result {i} is {type(row).__name__}, expected an object")
            continue
        if not isinstance(row.get("id"), str) or not row["id"]:
            problems.append(f"result {i} has no usable 'id' ({row.get('id')!r})")
        v = row.get("regression_verdict")
        if v not in VALID_REGRESSION_VERDICTS:
            problems.append(f"result {row.get('id', i)!r} has regression_verdict "
                            f"{v!r}, not one of {sorted(VALID_REGRESSION_VERDICTS)}")

    if prior.get("kind") != "paired_latency":
        problems.append(f"prior result kind is {prior.get('kind')!r}, "
                        f"expected 'paired_latency'")
    if prior.get("gate_variant") != expect_variant:
        problems.append(f"prior rung ran variant {prior.get('gate_variant')!r}, "
                        f"this run is {expect_variant!r} — they are not comparable")
    if prior.get("warm_samples_per_arm") != expect_rung:
        problems.append(f"prior rung used {prior.get('warm_samples_per_arm')!r} warm "
                        f"samples, expected {expect_rung}")
    if prior.get("post_run_drift"):
        problems.append(f"prior rung recorded runtime drift: {prior['post_run_drift']}")
    if str(prior.get("gate", "")).startswith("INVALID"):
        problems.append(f"prior rung's gate was {prior.get('gate')!r}")
    if not prior.get("results"):
        problems.append("prior rung recorded no case results")

    # An established regression is not something to escalate past. More samples
    # would only make a confirmed failure more confident.
    if prior.get("gate") in ("FAIL", "INVALID_RUNTIME_DRIFT"):
        problems.append(f"prior rung's gate was {prior['gate']!r} — escalation cannot "
                        f"overturn an established failure, and re-running it as if it "
                        f"might would be looking for a better answer")
    if any(r.get("regression_verdict") == "REGRESSION"
           for r in prior.get("results", []) if isinstance(r, dict)):
        problems.append("prior rung established a regression on at least one case")

    if expect_cases is not None:
        ids = [r.get("id") for r in prior.get("results", []) if isinstance(r, dict)]
        if len(ids) != len(set(ids)):
            problems.append(f"prior rung has duplicate case ids: {sorted(ids)}")
        missing = expect_cases - set(ids)
        extra = set(ids) - expect_cases
        if missing:
            problems.append(f"prior rung is missing cases {sorted(missing)} — it did "
                            f"not cover what this escalation assumes it covered")
        if extra:
            problems.append(f"prior rung has unexpected cases {sorted(extra)}")

    for name, expected in (("candidate_url", expect_candidate_url),
                           ("reference_url", expect_reference_url)):
        if expected is not None and prior.get(name) != expected:
            problems.append(f"prior rung's {name} was {prior.get(name)!r}, this run "
                            f"uses {expected!r} — a different backend")

    # Same code, same data — or the two rungs are measuring different things.
    for label, now in (("candidate", cand_meta), ("reference", ref_meta)):
        then = prior.get(f"{label}_meta")
        if then is None or now is None:
            problems.append(f"{label}: cannot compare provenance across rungs "
                            f"({'prior' if then is None else 'current'} metadata absent)")
            continue
        if then.get("source_sha256") != now.get("source_sha256"):
            problems.append(f"{label}: source digests differ from the prior rung — "
                            f"the code under test changed between them")
        if then.get("store_path") != now.get("store_path"):
            problems.append(f"{label}: store path differs from the prior rung "
                            f"({then.get('store_path')!r} -> {now.get('store_path')!r})")
        if then.get("zmetadata_fingerprints") != now.get("zmetadata_fingerprints"):
            problems.append(f"{label}: store fingerprints differ from the prior rung — "
                            f"the data changed between them")
        if then.get("label") != now.get("label"):
            problems.append(f"{label}: prior metadata is labelled "
                            f"{then.get('label')!r}")
        # The embedded record has to be valid in its own right, not merely equal to
        # the current one — two identically broken records would otherwise agree.
        problems.extend(f"prior {m}" for m in validate_meta(then, label))

    return problems


# The only case failures a reusable contract result may carry. They failed on a
# comparator defect — `openapi.json` is a JSON object and the Swagger page is HTML,
# and `compare_semantic` treated every 200 body as a list of rows — not on any
# difference between the two backends. Any other failure means the contract gate
# genuinely disagreed and cannot be carried forward.
KNOWN_HARNESS_FAILURES = frozenset({"C20a", "C20b"})


# The comparator defect those two failed on left a specific note. Matching the case
# id alone would let any future failure of C20a/C20b through under the same excuse.
KNOWN_HARNESS_NOTE = "unparseable body"

# A contract run reaches a verdict only when it actually compared things. Anything
# beginning INVALID_ means it refused to, and an unknown value means the file was
# written by a tool this one does not know.
VALID_CONTRACT_GATES = frozenset({"PASS", "FAIL"})
VALID_CONTRACT_VERDICTS = frozenset({"MATCH", "DIFFER", "ERROR"})


def _contract_case_ids() -> set:
    """The case list this repository defines, fetched rather than supplied.

    An `expect_case_ids` a caller may omit is one a caller will omit, and the check
    then silently covers nothing. The parameter remains only so tests can supply a
    smaller list deliberately.
    """
    from bench.contract_cases import all_cases
    return {c.id for c in all_cases()}


def verify_prior_contract(prior: dict | None, cand_meta: dict | None,
                          expect_variant: str,
                          ref_meta: dict | None = None,
                          expect_case_ids: set | None = None,
                          allowed_failures: frozenset = KNOWN_HARNESS_FAILURES
                          ) -> list[str]:
    """Is an earlier contract result safe to carry into a latency-only run?

    Reusing it means not paying production for 64 requests again. That is only
    legitimate if the result still describes the same code against the same data,
    and if the reason it did not pass is one we have since fixed in the harness
    rather than a real disagreement between the backends.
    """
    if prior is None:
        return ["no prior contract result to reuse"]
    problems = []

    for field in ("kind", "gate", "variant", "results", "candidate_meta",
                  "reference_meta"):
        if field not in prior:
            problems.append(f"prior contract result has no {field!r} field")
    if problems:
        return problems

    if expect_case_ids is None:
        expect_case_ids = _contract_case_ids()

    if prior.get("kind") != "contract_diff":
        problems.append(f"prior contract result kind is {prior.get('kind')!r}")

    gate = prior.get("gate")
    if gate not in VALID_CONTRACT_GATES:
        problems.append(
            f"prior contract gate is {gate!r}; only {sorted(VALID_CONTRACT_GATES)} "
            f"describe a run that actually compared anything. An INVALID_* gate means "
            f"the run refused to compare, so it establishes nothing to carry forward.")
    if prior.get("variant") != expect_variant:
        problems.append(f"prior contract result ran variant {prior.get('variant')!r}, "
                        f"this run is {expect_variant!r}")
    if not isinstance(prior.get("results"), list) or not prior["results"]:
        return problems + ["prior contract result has no case results"]

    failed = {}
    ids = []
    for i, row in enumerate(prior["results"]):
        if not isinstance(row, dict):
            problems.append(f"contract result {i} is {type(row).__name__}")
            continue
        if not isinstance(row.get("id"), str) or not row["id"]:
            problems.append(f"contract result {i} has no usable 'id'")
            continue
        ids.append(row["id"])
        verdict = row.get("verdict")
        if verdict not in VALID_CONTRACT_VERDICTS:
            problems.append(f"contract case {row['id']!r} has verdict {verdict!r}, "
                            f"not one of {sorted(VALID_CONTRACT_VERDICTS)}")
            continue
        if verdict != "MATCH":
            failed[row["id"]] = row.get("notes") or []

    if len(ids) != len(set(ids)):
        problems.append("contract result has duplicate case ids")
    if expect_case_ids is not None:
        # The identities, not the count. Sixty-four fabricated ids satisfied a count
        # check while covering none of the case list — the reuse would then have
        # been justified by a result about nothing.
        missing = expect_case_ids - set(ids)
        extra = set(ids) - expect_case_ids
        if missing:
            problems.append(f"contract result is missing {len(missing)} case(s): "
                            f"{sorted(missing)[:6]}"
                            f"{' …' if len(missing) > 6 else ''}")
        if extra:
            problems.append(f"contract result has {len(extra)} case(s) that are not "
                            f"in the case list: {sorted(extra)[:6]}"
                            f"{' …' if len(extra) > 6 else ''}")

    # An allowed id is not an excuse on its own: the failure has to be the defect we
    # fixed, not a new one that happens to land on the same case.
    for cid, notes in failed.items():
        if cid in allowed_failures and not any(
                KNOWN_HARNESS_NOTE in str(n) for n in notes):
            problems.append(
                f"{cid} failed, but not with the known comparator defect "
                f"({KNOWN_HARNESS_NOTE!r}) — notes were {notes}. A permitted case id "
                f"does not make an unrelated failure permitted.")

    # The gate and the rows have to agree. A PASS carrying failures, or a FAIL
    # carrying none, means the file does not describe its own contents.
    if gate == "PASS" and failed:
        problems.append(f"prior contract gate is PASS but {sorted(failed)} did not "
                        f"match — the recorded verdict contradicts the recorded rows")
    if gate == "FAIL" and not failed:
        problems.append("prior contract gate is FAIL but every case matched")

    unexpected = set(failed) - allowed_failures
    if unexpected:
        problems.append(
            f"prior contract result failed on {sorted(unexpected)}, which is not the "
            f"known harness defect — those are real disagreements and cannot be "
            f"carried forward")

    # Same code, same data, same dependencies, or the result describes a different
    # thing than this run is about to measure.
    # Both arms, not just the candidate. A contract result is a statement about a
    # comparison; carrying it forward while the other side may have changed would
    # reuse half a fact.
    for label, now in (("candidate", cand_meta), ("reference", ref_meta)):
        then = prior.get(f"{label}_meta")
        if not isinstance(then, dict) or not isinstance(now, dict):
            problems.append(f"cannot compare {label} provenance across runs "
                            f"({'prior' if not isinstance(then, dict) else 'current'} "
                            f"metadata absent)")
            continue
        for field, note in (("source_sha256", "the code under test changed"),
                            ("store_path", "the store path changed"),
                            ("zmetadata_fingerprints", "the data changed"),
                            ("dependencies", "the installed dependencies changed")):
            if then.get(field) != now.get(field):
                problems.append(f"{label}: {note} since the contract gate ran "
                                f"({field})")
        req = "pinned" if label == "candidate" else "any"
        problems.extend(f"prior contract {m}" for m in
                        validate_meta(then, label, seed_requirement=req))

    return problems


# --- controlled two-arm comparison (spec 001 section 5.2A / D2b) --------------

#: Digest pairs that must agree between the arms, by campaign. The anchor differs
#: because the environments are built differently: D2b resolves a lockfile, S2 copies
#: a package tree and anchors on that tree's manifest. Hard-coding `lockfile_sha256`
#: here made every S2 run fail with "lockfile digest missing on candidate", which is
#: true and irrelevant — there is no lockfile to be missing.
ARM_MATCH_DIGESTS = (("name_version_set_sha256", "installed name==version set"),
                     ("lockfile_sha256", "lockfile"))
S2_ARM_MATCH_DIGESTS = (("name_version_set_sha256", "installed name==version set"),
                        ("clone_manifest_sha256", "package-tree clone manifest"),
                        ("package_tree_digest", "package-tree digest"),
                        ("runtime_distribution_digest", "runtime distribution digest"))


def verify_environment_match(cand_meta: dict | None, ref_meta: dict | None,
                             digests=ARM_MATCH_DIGESTS) -> list[str]:
    """Do both arms run the *same* interpreter and the *same* installed packages?

    Variant 5.2A exists to remove the variables 5.2B could not. The 2026-08-07
    campaign compared a candidate against live production and found that, while the
    twelve pinned packages agreed, 23 shared transitive dependencies did not — so
    the measurement carried an uncontrolled variable nobody had looked for.

    This is the check that makes the controlled run controlled. It compares the
    resolved interpreter version and the digest of the full distribution list, not a
    hand-picked subset: a subset is how the last discrepancy stayed invisible.
    """
    if not isinstance(cand_meta, dict) or not isinstance(ref_meta, dict):
        return ["cannot compare arm environments: metadata missing"]

    problems = []
    for field, note in (
            ("env_python_version", "interpreter version"),
            ("env_python", "package environment path"),
    ):
        a, b = cand_meta.get(field), ref_meta.get(field)
        if a != b:
            problems.append(f"{note} differs between arms: candidate {a!r} vs "
                            f"reference {b!r}")

    ca = (cand_meta.get("dependencies") or {})
    ra = (ref_meta.get("dependencies") or {})
    for field, note in digests:
        a, b = ca.get(field), ra.get(field)
        if a is None or b is None:
            problems.append(f"{note} digest missing on "
                            f"{'candidate' if a is None else 'reference'}")
        elif a != b:
            problems.append(f"{note} differs between arms ({field})")

    # When the digests disagree, name the packages. "The sets differ" is not
    # actionable; "fsspec 2026.7.0 vs 2025.10.0" is.
    if ca.get("distributions") and ra.get("distributions") and \
            ca.get("name_version_set_sha256") != ra.get("name_version_set_sha256"):
        am = {x.split("==")[0]: x.split("==")[1] for x in ca["distributions"] if "==" in x}
        bm = {x.split("==")[0]: x.split("==")[1] for x in ra["distributions"] if "==" in x}
        for name in sorted(set(am) | set(bm)):
            if am.get(name) != bm.get(name):
                problems.append(f"  {name}: candidate {am.get(name)} vs "
                                f"reference {bm.get(name)}")
    return problems


def verify_group_path_agreement(cand_meta: dict | None, ref_meta: dict | None
                                ) -> list[str]:
    """Do both arms interpolate the *same string* when building `zarr_group_paths`?

    `zarr_group_paths` is a `set` of path strings, so its iteration order depends on
    the hash of those strings. With `PYTHONHASHSEED` pinned the order is
    deterministic per string, but two arms building different strings for the same
    logical group can still iterate them in different orders — which reorders
    `result_list`, and so the rows of the response.

This is the strongly supported mechanism behind the 2026-08-08 5.2A failure.
    The reference interpolated `"data/"` (giving `data//1_degree/...`) while the
    candidate interpolated an absolute path, so the arms iterate the set in different
    orders; the only two cases whose query spans more than one Zarr group — C16 and
    C16-csv — are exactly the two that differed, and the other 62 matched byte for
    byte because they touch a single group where order cannot differ.

    **The actual decomposition of those bodies is not established.** Only an offline
    synthetic reproducer has been run; C16's real responses were never captured, so
    which of row order, key order or something else accounts for the difference
    remains unproven.

    This does not check that the strings are *correct*, only that they are the same.
    Whether they point at the same data is `validate_store_agreement`'s job.
    """
    if not isinstance(cand_meta, dict) or not isinstance(ref_meta, dict):
        return ["cannot compare group paths: metadata missing"]
    a = cand_meta.get("store_path_literal")
    b = ref_meta.get("store_path_literal")
    problems = []
    for label, v in (("candidate", a), ("reference", b)):
        if not isinstance(v, str) or not v:
            problems.append(f"{label}: no usable 'store_path_literal'")
    if problems:
        return problems
    if a != b:
        return [f"the arms build zarr_group_paths from different strings: "
                f"candidate {a!r} vs reference {b!r}. Set iteration order depends on "
                f"the string, so result_list is concatenated in a different order "
                f"for any query spanning more than one group."]
    return []


# The environment record a controlled run writes before starting anything, and the
# meta field each of its keys must equal. `dependencies.` marks a nested lookup.
ENVIRONMENT_RECORD_FIELDS = (
    ("env_python", "env_python", "package environment path"),
    ("python_version", "env_python_version", "interpreter version"),
    ("lockfile_sha256", "dependencies.lockfile_sha256", "lockfile digest"),
    ("name_version_set_sha256", "dependencies.name_version_set_sha256",
     "installed name==version set digest"),
)

# The S2 modes have no lockfile: the arms do not run an environment this campaign
# resolved and installed, they run a read-only copy of production's package tree.
# The anchor is therefore the clone's own manifest digest — the artefact that says
# *this* clone is the one that was built and verified against production — and it
# occupies exactly the position `lockfile_sha256` holds under D2b. Nothing else
# changes: the interpreter, its version and the distribution set are still compared
# field by field, because two arms agreeing with each other has never been evidence
# that they agree with the environment the run intended.
S2_ENVIRONMENT_RECORD_FIELDS = (
    ("env_python", "env_python", "package environment interpreter"),
    ("python_version", "env_python_version", "interpreter version"),
    ("clone_manifest_sha256", "dependencies.clone_manifest_sha256",
     "package-tree clone manifest digest"),
    ("name_version_set_sha256", "dependencies.name_version_set_sha256",
     "installed name==version set digest"),
    ("package_tree_digest", "dependencies.package_tree_digest",
     "package-tree digest (240 dist-info directories, s4.1.2b)"),
    ("runtime_distribution_digest", "dependencies.runtime_distribution_digest",
     "runtime distribution digest (236 with METADATA, s4.1.2b)"),
)


def verify_environment_record(env_record: dict | None, meta: dict | None,
                              label: str,
                              fields=ENVIRONMENT_RECORD_FIELDS) -> list[str]:
    """Is this arm running the environment the run built, or merely *an* environment?

    `verify_environment_match` only asks whether the two arms agree with each other.
    Two arms can agree perfectly while both run a venv that has nothing to do with
    the one this run prepared and recorded — a stale `.venv` from a previous
    invocation satisfies it exactly. The environment record is the anchor, so every
    field it carries is compared, not just the distribution digest.

    Fails closed: a missing record, a missing field on either side, or a value that
    is not a string is a problem, never a pass.
    """
    if not isinstance(env_record, dict):
        return [f"{label}: no environment record to compare against"]
    if not isinstance(meta, dict):
        return [f"{label}: metadata missing, cannot compare to the environment record"]

    problems = []
    if not fields:
        return [f"{label}: no fields to compare — an empty field list would pass "
                f"every environment, including the wrong one"]
    for env_key, meta_path, note in fields:
        want = env_record.get(env_key)
        got: object = meta
        for part in meta_path.split("."):
            got = (got or {}).get(part) if isinstance(got, dict) else None
        if not isinstance(want, str) or not want:
            problems.append(f"{label}: environment record has no usable {env_key!r}")
        elif not isinstance(got, str) or not got:
            problems.append(f"{label}: metadata has no usable {meta_path!r}")
        elif want != got:
            problems.append(f"{label}: {note} is not the environment this run built "
                            f"({meta_path}={got!r}, record {env_key}={want!r})")
    return problems


def compare_arms(cand_meta: dict | None, ref_meta: dict | None,
                 env_record: dict | None, *, s2: bool,
                 seed_policy: str = "both-pinned") -> list[str]:
    """Everything that must hold before the two arms may be compared at all.

    This lives here, and not inline in the runner, because it was inline in the
    runner. The S2 branch computed the right field list into a variable and then
    called `verify_environment_record` without it — a dead assignment that reads
    exactly like the working code — so every S2 run failed on the D2b field list
    complaining about a lockfile the campaign does not have. Nothing offline caught
    it: the pieces each had tests, and the composition had none because it was not a
    function.

    Now it is one, and the test drives it with the artefacts a real run produced.

    The four questions, in order:

    1. is each record internally well-formed (`validate_meta`);
    2. do the two arms agree with each other (`verify_environment_match`);
    3. is what they agree on the environment this run actually prepared
       (`verify_environment_record`) — two arms sharing a stale venv, or a clone
       nobody verified, agree perfectly and prove nothing;
    `seed_policy` decides what each arm's `PYTHONHASHSEED` must be. It is not
    inferred from the variant: 5.2B was written for a pinned candidate against live
    production, and C2 is a third arrangement — both arms ours, both unpinned — that
    a two-valued rule reported as a defect. See `SEED_POLICIES`.

    4. do they build `zarr_group_paths` from the same string
       (`verify_group_path_agreement`) — different strings hash differently, so the
       set iterates in a different order for any query spanning more than one group,
       which is exactly how C16 and C16-csv differed in the 2026-08-08 run.
    """
    fields = S2_ENVIRONMENT_RECORD_FIELDS if s2 else ENVIRONMENT_RECORD_FIELDS
    digests = S2_ARM_MATCH_DIGESTS if s2 else ARM_MATCH_DIGESTS
    return (validate_meta(cand_meta, "candidate",
                          seed_requirement=seed_requirement_for(seed_policy, "candidate"))
            + validate_meta(ref_meta, "reference",
                            seed_requirement=seed_requirement_for(seed_policy, "reference"))
            + verify_environment_match(cand_meta, ref_meta, digests=digests)
            + verify_environment_record(env_record, cand_meta, "candidate", fields)
            + verify_environment_record(env_record, ref_meta, "reference", fields)
            + verify_group_path_agreement(cand_meta, ref_meta))
