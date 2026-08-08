"""Tests for resolving the package environment a backend is actually using.

The first campaign recorded fastapi 0.115.2 / polars 1.10.0 / xarray 2024.9.0 for
both arms while both were running 0.115.12 / 1.27.1 / 2025.3.1. `/proc/<pid>/exe`
follows a venv's symlink to the base interpreter, so it names a real executable whose
site-packages belong to something else entirely — and the record looked plausible.

These use the repository's own venv, so they run offline and need no backend.

    uv run python -m bench.test_environment
"""

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
