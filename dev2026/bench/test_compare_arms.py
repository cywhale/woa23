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
    ARM_MATCH_DIGESTS, S2_ARM_MATCH_DIGESTS, compare_arms,
    verify_environment_match,
)

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
if FAIL:
    print(f"FAILED {FAIL}/{PASS + FAIL}")
    raise SystemExit(1)
print(f"all passed ({PASS} assertions)")
