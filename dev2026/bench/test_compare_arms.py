"""The pre-contract gate, driven by the artefacts a real C1 run produced.

`compare_arms` is what decides whether two arms may be compared at all. It used to
be spelled out inline in a shell heredoc, where the S2 branch computed the correct
field list into a variable and then called `verify_environment_record` without it.
The dead assignment reads exactly like working code. Every piece it composed had
tests; the composition had none, because it was not a function — and the second
authorised C1 attempt died on it, complaining that a lockfile was missing from a
campaign that has no lockfile.

So the fixtures here are not invented. They are the real `c1_meta_candidate.json`,
`c1_meta_reference.json` and `c1_environment.json` written by that run on VM24, with
only the 236-entry distribution lists trimmed — every field the gate reads is
verbatim. A test built from hand-written records would have agreed with whatever the
code did.

    uv run python -m bench.test_compare_arms
"""

import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.provenance import (  # noqa: E402
    ARM_MATCH_DIGESTS, S2_ARM_MATCH_DIGESTS, SEED_POLICIES,
    SEED_POLICY_MEANING, compare_arms, seed_requirement_for,
    verify_environment_match,
)
from bench.suite_summary import summary          # noqa: E402

PASS = 0
FAIL = 0
FIX = Path(__file__).resolve().parent / "fixtures"


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


def load():
    return (json.loads((FIX / "c1_meta_candidate.json").read_text()),
            json.loads((FIX / "c1_meta_reference.json").read_text()),
            json.loads((FIX / "c1_environment.json").read_text()))


def any_mentions(problems, text):
    return any(text in p for p in problems)


def offhost(problems):
    """Drop the two problems that are artefacts of running this off VM24.

    `validate_meta` re-hashes each arm's source files from the cwd recorded in the
    record, which is a staging directory on the host. The gate runs there by
    construction — both arms are loopback — so on VM24 these do not appear. They are
    filtered here rather than suppressed in the code: an unverifiable source set must
    stay a problem for the real run.
    """
    return [p for p in problems if "is not reachable from here" not in p]


print("the real C1 artefacts pass the S2 gate")
cand, ref, env = load()
problems = compare_arms(cand, ref, env, s2=True)
check("nothing fails except the off-host source re-hash", [], offhost(problems))
check("and that is exactly two problems, one per arm", 2, len(problems))
check("both of them the unreachable staging cwd", True,
      all("is not reachable from here" in p for p in problems))

# What the run actually reported, and why.
print()
print("and fail the D2b gate — which is the bug that stopped the run")
d2b = offhost(compare_arms(cand, ref, env, s2=False))
check("the D2b field list rejects them", True, bool(d2b))
check("complaining about a missing lockfile digest", True,
      any_mentions(d2b, "lockfile"))
check("which is true and irrelevant: S2 has no lockfile", None,
      cand["dependencies"].get("lockfile_sha256"))
check("the anchor it does have is the clone manifest", 64,
      len(cand["dependencies"]["clone_manifest_sha256"]))

print()
print("the fixtures record what the run established about the arms")
check("both arms ran production's interpreter",
      "/home/odbadmin/.pyenv/versions/py311/bin/python3.11", cand["env_python"])
check("named explicitly, not derived", "explicit", cand["env_python_source"])
check("the reference too", cand["env_python"], ref["env_python"])
check("both at 3.11.4", ("3.11.4", "3.11.4"),
      (cand["env_python_version"], ref["env_python_version"]))
check("both saw the same distribution set",
      cand["dependencies"]["name_version_set_sha256"],
      ref["dependencies"]["name_version_set_sha256"])
check("both anchored on the same clone manifest",
      cand["dependencies"]["clone_manifest_sha256"],
      ref["dependencies"]["clone_manifest_sha256"])
check("and it is the manifest verified on the host",
      "f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4",
      cand["dependencies"]["clone_manifest_sha256"])
check("both build group paths from the same literal", ("data/", "data/"),
      (cand["store_path_literal"], ref["store_path_literal"]))
check("PYTHONHASHSEED was pinned on the candidate", "0",
      cand["env"]["PYTHONHASHSEED"])
check("and on the reference", "0", ref["env"]["PYTHONHASHSEED"])
check("each arm is an arbiter plus one worker",
      (1, 1), (len(cand["worker_pids"]), len(ref["worker_pids"])))

print()
print("every check the gate composes is still reachable")
cand, ref, env = load()
bad = copy.deepcopy(cand)
bad["dependencies"]["name_version_set_sha256"] = "z" * 64
check("a differing distribution set between arms is caught", True,
      any_mentions(offhost(compare_arms(bad, ref, env, s2=True)), "installed name==version set"))

bad = copy.deepcopy(cand)
bad["dependencies"]["clone_manifest_sha256"] = "z" * 64
p = offhost(compare_arms(bad, ref, env, s2=True))
check("a differing clone manifest between arms is caught", True,
      any_mentions(p, "package-tree clone manifest"))
check("and it is also caught against the environment record", True,
      any_mentions(p, "clone manifest digest is not the environment"))

bad = copy.deepcopy(cand)
del bad["dependencies"]["clone_manifest_sha256"]
check("a missing clone manifest digest is caught, not skipped", True,
      bool(offhost(compare_arms(bad, ref, env, s2=True))))

bad = copy.deepcopy(cand)
bad["env_python"] = "/home/odbadmin/.pyenv/versions/py311/bin/python"
check("an arm on a different interpreter is caught", True,
      any_mentions(offhost(compare_arms(bad, ref, env, s2=True)), "package environment"))

bad = copy.deepcopy(cand)
bad["store_path_literal"] = "/home/odbadmin/python/woa23/data"
check("arms building group paths from different strings are caught", True,
      bool(offhost(compare_arms(bad, ref, env, s2=True))))

stale = copy.deepcopy(env)
stale["name_version_set_sha256"] = "z" * 64
check("an environment record that is not what the arms ran is caught", True,
      any_mentions(offhost(compare_arms(cand, ref, stale, s2=True)),
                   "installed name==version set digest is not the environment"))

check("a missing environment record fails closed", True,
      bool(offhost(compare_arms(cand, ref, None, s2=True))))
check("a missing candidate record fails closed", True,
      bool(offhost(compare_arms(None, ref, env, s2=True))))
check("a missing reference record fails closed", True,
      bool(offhost(compare_arms(cand, None, env, s2=True))))

print()
print("the digest sets differ by campaign, and neither is empty")
check("D2b anchors on the lockfile", True,
      any(f == "lockfile_sha256" for f, _ in ARM_MATCH_DIGESTS))
check("S2 anchors on the clone manifest", True,
      any(f == "clone_manifest_sha256" for f, _ in S2_ARM_MATCH_DIGESTS))
check("both compare the name==version set", True,
      all(any(f == "name_version_set_sha256" for f, _ in d)
          for d in (ARM_MATCH_DIGESTS, S2_ARM_MATCH_DIGESTS)))
check("and S2 also compares both dist-info-keyed digests", True,
      {"package_tree_digest", "runtime_distribution_digest"}.issubset(
          {f for f, _ in S2_ARM_MATCH_DIGESTS}))
check("the default is unchanged for D2b callers", ARM_MATCH_DIGESTS,
      verify_environment_match.__defaults__[0])

print()
print("C2: both arms ours, both deliberately unpinned — a third seed arrangement")
# The fixtures are the real c2_cycle1 records from VM24, where both arms ran with
# PYTHONHASHSEED unset and two workers each. The C1 fixtures are pinned, so nothing
# built on them could have caught this: the gate applied the pinned rule to a run
# whose entire purpose is to be unpinned, and stopped cycle 1 before the contract.
def load_c2():
    return (json.loads((FIX / "c2_cycle1_meta_candidate.json").read_text()),
            json.loads((FIX / "c2_cycle1_meta_reference.json").read_text()),
            json.loads((FIX / "c2_cycle1_environment.json").read_text()))

c2c, c2r, c2e = load_c2()
check("both arms really are unpinned in the fixture",
      ("<unset — randomised>", "<unset — randomised>"),
      (c2c["env"]["PYTHONHASHSEED"], c2r["env"]["PYTHONHASHSEED"]))
check("and each ran production's two workers", (2, 2),
      (len(c2c["worker_pids"]), len(c2r["worker_pids"])))

check("the both-unpinned policy accepts them", [],
      offhost(compare_arms(c2c, c2r, c2e, s2=True, seed_policy="both-unpinned")))
# What actually happened on 2026-08-09.
under_pinned = offhost(compare_arms(c2c, c2r, c2e, s2=True, seed_policy="both-pinned"))
check("the both-pinned policy rejects them — the bug that stopped cycle 1", True,
      any("PYTHONHASHSEED" in p for p in under_pinned))
check("and it rejects BOTH arms, as it did", 2,
      len([p for p in under_pinned if "PYTHONHASHSEED" in p]))
# 5.2B's own default is not right either: it was written for a pinned candidate
# against live production.
under_5_2b = offhost(compare_arms(c2c, c2r, c2e, s2=True,
                                  seed_policy="reference-unpinned"))
check("5.2B's historical default also rejects them", True,
      any("PYTHONHASHSEED" in p for p in under_5_2b))
check("because it still requires the candidate to be pinned", 1,
      len([p for p in under_5_2b if "PYTHONHASHSEED" in p]))

# Inverted, and that inversion is the point: a C2 cycle that ran pinned observed
# nothing about unpinned behaviour and must not be counted as if it had.
pinned_c2 = copy.deepcopy(c2c)
pinned_c2["env"]["PYTHONHASHSEED"] = "0"
probs = offhost(compare_arms(pinned_c2, c2r, c2e, s2=True, seed_policy="both-unpinned"))
check("a pinned arm in a C2 cycle is rejected", True,
      any("requires it to be unset" in p for p in probs))
check("and says why it is not merely tolerated", True,
      any("observes nothing about unpinned" in p for p in probs))

check("the C1 fixtures still pass under both-pinned", [],
      offhost(compare_arms(*load(), s2=True, seed_policy="both-pinned")))
check("and are rejected under both-unpinned", True,
      bool(offhost(compare_arms(*load(), s2=True, seed_policy="both-unpinned"))))

print()
print("the full policy x arm-shape matrix, on real records throughout")
# Three real shapes, none invented:
#   pinned/pinned      the C1 run                     -> both-pinned
#   pinned/unpinned    5.2B's arrangement, assembled from C1's candidate and C2's
#                      reference (a pinned candidate against an unpinned reference,
#                      which is what live production is)
#   unpinned/unpinned  the C2 cycle                   -> both-unpinned
c1c, c1r, c1e = load()
#   unpinned/pinned    the fourth corner: an unpinned candidate against a pinned
#                      reference. No campaign produces it on purpose, which is
#                      exactly why it is here — it is what a half-applied change
#                      looks like (C2's launch on one arm, C1's on the other), and
#                      every policy must refuse it.
SHAPES = {
    "pinned/pinned":     (c1c, c1r, c1e),
    "pinned/unpinned":   (c1c, c2r, c1e),
    "unpinned/pinned":   (c2c, c1r, c2e),
    "unpinned/unpinned": (c2c, c2r, c2e),
}
# accepted[policy][shape]: does the seed rule let it through?
EXPECTED = {
    "both-pinned":        {"pinned/pinned": True,  "pinned/unpinned": False,
                           "unpinned/pinned": False, "unpinned/unpinned": False},
    "reference-unpinned": {"pinned/pinned": True,  "pinned/unpinned": True,
                           "unpinned/pinned": False, "unpinned/unpinned": False},
    "both-unpinned":      {"pinned/pinned": False, "pinned/unpinned": False,
                           "unpinned/pinned": False, "unpinned/unpinned": True},
}
for policy, row in EXPECTED.items():
    for shape, want_ok in row.items():
        cand, ref, env = SHAPES[shape]
        probs = [p for p in offhost(compare_arms(cand, ref, env, s2=True,
                                                 seed_policy=policy))
                 if "PYTHONHASHSEED" in p]
        check(f"{policy:18s} x {shape:18s} -> {'accept' if want_ok else 'reject'}",
              want_ok, not probs)

# The two ways C2 could be waved through without anyone deciding to.
# The fourth corner is rejected by every policy, and for two different reasons —
# the candidate is unpinned where it must be pinned, or the reference is pinned
# where it must be unset. Both are checked, so neither arm can be the only one
# holding the line.
for policy in SEED_POLICIES:
    probs = [p for p in offhost(compare_arms(c2c, c1r, c2e, s2=True,
                                             seed_policy=policy))
             if "PYTHONHASHSEED" in p]
    check(f"unpinned candidate / pinned reference is refused under {policy}", True,
          bool(probs))
check("under both-pinned it is the candidate that is named", True,
      any("candidate" in p for p in offhost(
          compare_arms(c2c, c1r, c2e, s2=True, seed_policy="both-pinned"))
          if "PYTHONHASHSEED" in p))
check("under both-unpinned it is the reference that is named", True,
      any("reference" in p and "requires it to be unset" in p for p in offhost(
          compare_arms(c2c, c1r, c2e, s2=True, seed_policy="both-unpinned"))))

print()
print("reference-unpinned does not mean the reference must be unpinned")
# The name describes the situation that motivated the policy, not the rule. Read as
# a requirement it would invert the check on the one arm it deliberately leaves
# unconstrained, so the semantics are asserted directly rather than left to the name.
check("it accepts a PINNED reference", [],
      [p for p in offhost(compare_arms(c1c, c1r, c1e, s2=True,
                                       seed_policy="reference-unpinned"))
       if "PYTHONHASHSEED" in p])
check("and an UNPINNED reference", [],
      [p for p in offhost(compare_arms(c1c, c2r, c1e, s2=True,
                                       seed_policy="reference-unpinned"))
       if "PYTHONHASHSEED" in p])
check("while still requiring a pinned candidate", True,
      bool([p for p in offhost(compare_arms(c2c, c1r, c2e, s2=True,
                                            seed_policy="reference-unpinned"))
            if "PYTHONHASHSEED" in p]))
check("the reference is literally unconstrained under it", "any",
      seed_requirement_for("reference-unpinned", "reference"))
check("every policy carries its meaning in words", set(SEED_POLICIES),
      set(SEED_POLICY_MEANING))
check("and the misleading name is corrected in its own text", True,
      "NOT 'the reference must be unpinned'" in SEED_POLICY_MEANING["reference-unpinned"])
check("which says the reference MAY be either", True,
      "MAY be either" in SEED_POLICY_MEANING["reference-unpinned"])

check("no policy lets an unpinned CANDIDATE through as 'any'", True,
      all(reqs["candidate"] != "any" for reqs in SEED_POLICIES.values()))
check("only the reference is ever 'any', and only under reference-unpinned",
      {"reference-unpinned"},
      {name for name, reqs in SEED_POLICIES.items() if "any" in reqs.values()})
check("the 5.2B variant default alone does NOT accept the C2 shape", False,
      not [p for p in offhost(compare_arms(c2c, c2r, c2e, s2=True,
                                           seed_policy="reference-unpinned"))
           if "PYTHONHASHSEED" in p])
_scripts = Path(__file__).resolve().parent.parent / "scripts"
# The runner plus the chain it sources: the stage invocations moved into
# lib_s2perf.sh, and "the harness does X" is a claim about both files together.
runner_src = ((_scripts / "run_controlled.sh").read_text()
              + (_scripts / "lib_s2perf.sh").read_text())
check("the runner states both-unpinned for C2 rather than inheriting a default", True,
      "SEED_POLICY=both-unpinned" in runner_src)
check("and passes it to the gate explicitly", True,
      '"$VARIANT" "$SEED_POLICY"' in runner_src
      and '--seed-policy "$policy"' in runner_src)
check("C1 and D2b state both-pinned rather than relying on the variant", True,
      "SEED_POLICY=both-pinned" in runner_src)

print()
print("the three policies are distinct and fail closed")
check("there are exactly three", 3, len(SEED_POLICIES))
check("both-pinned pins both", ("pinned", "pinned"),
      (seed_requirement_for("both-pinned", "candidate"),
       seed_requirement_for("both-pinned", "reference")))
check("reference-unpinned pins only the candidate", ("pinned", "any"),
      (seed_requirement_for("reference-unpinned", "candidate"),
       seed_requirement_for("reference-unpinned", "reference")))
check("both-unpinned requires both to be unset", ("unpinned", "unpinned"),
      (seed_requirement_for("both-unpinned", "candidate"),
       seed_requirement_for("both-unpinned", "reference")))
try:
    seed_requirement_for("whatever", "candidate")
    check("an unknown policy raises", True, False)
except ValueError as exc:
    check("an unknown policy raises rather than defaulting", True, "unknown seed policy" in str(exc))

print()
raise SystemExit(summary(PASS, FAIL))