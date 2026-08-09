"""Tests for resolving the package environment a backend is actually using.

The first campaign recorded fastapi 0.115.2 / polars 1.10.0 / xarray 2024.9.0 for
both arms while both were running 0.115.12 / 1.27.1 / 2025.3.1. `/proc/<pid>/exe`
follows a venv's symlink to the base interpreter, so it names a real executable whose
site-packages belong to something else entirely — and the record looked plausible.

These use the repository's own venv, so they run offline and need no backend.

    uv run python -m bench.test_environment
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

from bench.collect_backend_meta import dependencies, resolve_env_python

HERE = Path(__file__).resolve().parent.parent
VENV_PY = HERE / ".venv" / "bin" / "python"

failures: list[str] = []
passed: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    (passed if cond else failures).append(name)
    print(f"  {'ok  ' if cond else 'FAIL'} {name}" + (f"  {detail}" if not cond else ""))


def test_resolve_prefers_virtual_env() -> None:
    r = resolve_env_python(Path("/anywhere"), ["/usr/bin/gunicorn"],
                           {"VIRTUAL_ENV": str(HERE / ".venv")})
    check("VIRTUAL_ENV wins when it points at a real interpreter",
          r == {"env_python": str(VENV_PY), "env_python_source": "VIRTUAL_ENV"}, str(r))


def test_resolve_falls_back_to_launcher_sibling() -> None:
    r = resolve_env_python(HERE, [str(HERE / ".venv" / "bin" / "gunicorn")], {})
    check("the launcher's sibling python is used when VIRTUAL_ENV is absent",
          r["env_python_source"] == "argv0_sibling"
          and r["env_python"] == str(VENV_PY), str(r))


def test_resolve_reports_failure_rather_than_guessing() -> None:
    r = resolve_env_python(Path("/tmp"), ["/bin/false"], {})
    check("an unresolvable environment says so", r == {"env_python": None,
                                                       "env_python_source": "unresolved"})
    r = resolve_env_python(Path("/tmp"), [], {"VIRTUAL_ENV": "/no/such/venv"})
    check("a VIRTUAL_ENV that does not exist is not trusted",
          r["env_python_source"] == "unresolved", str(r))


def test_resolve_override_outranks_both_heuristics() -> None:
    """The S2 case: both heuristics would name production's own environment.

    An S2 arm runs `~/.pyenv/versions/py311/bin/python3.11 -S -m gunicorn` with the
    package clone on PYTHONPATH. `VIRTUAL_ENV` is unset by construction, and argv[0]'s
    sibling is `.../py311/bin/python` — production's interpreter, whose site-packages
    are the one thing the arm is arranged not to use. A confident wrong answer is the
    failure mode this whole function exists to have fixed, so the override wins even
    when a heuristic would have produced something.
    """
    r = resolve_env_python(Path("/anywhere"), ["/usr/bin/gunicorn"],
                           {"VIRTUAL_ENV": str(HERE / ".venv")},
                           override="/clone/bin/python3.11")
    check("an explicit interpreter outranks VIRTUAL_ENV",
          r == {"env_python": "/clone/bin/python3.11",
                "env_python_source": "explicit"}, str(r))
    r = resolve_env_python(HERE, [str(HERE / ".venv" / "bin" / "gunicorn")], {},
                           override="/clone/bin/python3.11")
    check("and outranks the argv0 sibling",
          r["env_python_source"] == "explicit", str(r))
    check("no override leaves the existing behaviour alone",
          resolve_env_python(HERE, [str(HERE / ".venv" / "bin" / "gunicorn")], {},
                             override=None)["env_python_source"] == "argv0_sibling")
    check("an empty override is not an override",
          resolve_env_python(Path("/tmp"), ["/bin/false"], {},
                             override="")["env_python_source"] == "unresolved")


def test_dependencies_answers_for_the_launch_not_the_binary() -> None:
    """Under -S with a controlled PYTHONPATH, the same binary lists a different set.

    This is the S2 arrangement in miniature: production's binary lists production's
    236 distributions when run normally, and the clone's when run the way the arm is
    started. Listing it without reproducing the launch would describe production's
    environment while the record claimed it described the clone's.
    """
    if not VENV_PY.exists():
        check("venv present for the launch-sensitivity test", False, "no .venv")
        return
    plain = dependencies(str(VENV_PY), None)
    with tempfile.TemporaryDirectory() as d:
        env = dict(os.environ)
        env.pop("VIRTUAL_ENV", None)
        env["PYTHONPATH"] = d
        env["PYTHONNOUSERSITE"] = "1"
        isolated = dependencies(str(VENV_PY), None, interp_args=("-S",), env=env)
    n_plain = len(plain.get("distributions", []))
    n_iso = len(isolated.get("distributions", []))
    check("the same interpreter lists a different set under the S2 launch",
          n_plain != n_iso, f"plain={n_plain} isolated={n_iso}")
    check("an empty stand-in clone yields no distributions", n_iso == 0, f"{n_iso}")
    check("the launch arguments are recorded with the answer",
          isolated.get("interpreter_args") == ["-S"], str(isolated.get("interpreter_args")))
    check("so is the environment that decided it",
          (isolated.get("interpreter_env") or {}).get("PYTHONNOUSERSITE") == "1",
          str(isolated.get("interpreter_env")))
    check("VIRTUAL_ENV is recorded as absent rather than omitted",
          "VIRTUAL_ENV" in (isolated.get("interpreter_env") or {})
          and isolated["interpreter_env"]["VIRTUAL_ENV"] is None,
          str(isolated.get("interpreter_env")))
    check("a plain listing carries no launch fields to be misread",
          "interpreter_args" not in plain and "interpreter_env" not in plain)


def test_resolve_never_follows_the_symlink() -> None:
    """The whole point: the venv python and its target are different environments."""
    if not VENV_PY.exists():
        check("venv present for the symlink test", False, "no .venv")
        return
    target = VENV_PY.resolve()
    check("the venv interpreter is a symlink to a different path", target != VENV_PY,
          f"{VENV_PY} -> {target}")

    via_venv = dependencies(str(VENV_PY), None)
    via_target = dependencies(str(target), None)
    n_venv = len(via_venv.get("distributions", []))
    n_target = len(via_target.get("distributions", []))
    check("the venv lists its own distributions", n_venv > 10, f"{n_venv} found")
    check("the symlink target lists a different set — which is the bug",
          n_venv != n_target, f"venv={n_venv} target={n_target}")


def test_dependencies_lists_the_pinned_versions() -> None:
    d = dependencies(str(VENV_PY), HERE / "uv.lock")
    check("no error listing distributions", "distributions_error" not in d,
          str(d.get("distributions_error")))
    got = {x.split("==")[0].lower(): x.split("==")[1] for x in d.get("distributions", [])
           if "==" in x}
    for name, want in (("fastapi", "0.115.12"), ("starlette", "0.46.2"),
                       ("uvicorn", "0.34.1"), ("pydantic", "2.11.3"),
                       ("polars", "1.27.1"), ("xarray", "2025.3.1"),
                       ("zarr", "2.18.6")):
        check(f"{name} is the pinned {want}", got.get(name) == want,
              f"got {got.get(name)}")
    check("a distributions digest is produced", len(d.get("distributions_sha256", "")) == 64)
    check("the lockfile digest is recorded", len(d.get("lockfile_sha256", "")) == 64)
    # The two arms of a 5.2A run must agree on the interpreter version, so the probe
    # reports it from inside the environment rather than leaving it to be inferred.
    check("the interpreter version is reported from the environment itself",
          d.get("python_version", "").startswith("3.11"), str(d.get("python_version")))


def test_dependencies_fails_loudly() -> None:
    check("an unusable interpreter is an error, not an empty list",
          "distributions_error" in dependencies("/bin/false", None))
    check("an unresolved environment is an error",
          "distributions_error" in dependencies(None, None))
    with tempfile.TemporaryDirectory() as d:
        stub = Path(d) / "python"
        stub.write_text("#!/bin/sh\nexit 3\n")
        stub.chmod(0o755)
        out = dependencies(str(stub), None)
        check("a non-zero exit is reported", "distributions_error" in out, str(out))


def test_probe_runs_under_the_target_interpreter() -> None:
    """The listing must come from the backend's interpreter, not the harness's."""
    r = subprocess.run([str(VENV_PY), "-c", "import sys; print(sys.executable)"],
                       capture_output=True, text=True)
    check("the venv interpreter reports itself as sys.executable",
          r.stdout.strip() == str(VENV_PY), r.stdout.strip())


def main() -> int:
    for fn in (test_resolve_prefers_virtual_env,
               test_resolve_falls_back_to_launcher_sibling,
               test_resolve_reports_failure_rather_than_guessing,
               test_resolve_override_outranks_both_heuristics,
               test_dependencies_answers_for_the_launch_not_the_binary,
               test_resolve_never_follows_the_symlink,
               test_dependencies_lists_the_pinned_versions,
               test_dependencies_fails_loudly,
               test_probe_runs_under_the_target_interpreter):
        print(f"\n{fn.__name__}")
        fn()
    total = len(passed) + len(failures)
    if failures:
        print(f"\nFAILED {len(failures)}/{total}: {', '.join(failures)}")
    else:
        print(f"\nall passed ({total} assertions)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
