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


def test_dash_valued_options_reach_the_parser() -> None:
    """`--env-python-arg=-S` parses; `--env-python-arg -S` does not.

    argparse reads a value beginning with a dash as the next *option*, so the
    separated form exits 2 with "expected one argument". The runner used it, and the
    first real C1 attempt died there — after both arms were up, the process trees
    verified and the store probed, with the contract gate never reached.

    Nothing offline caught it: every CLI test stops at argument validation, and a
    grep for the right spelling only checks the spelling. This runs the parser.
    """
    def parse(*args):
        r = subprocess.run(
            [sys.executable, "-m", "bench.collect_backend_meta",
             "--port", "18061", "--manifest", "candidate",
             "--expect-argv-contains", "api.app:app",
             "--env-python", sys.executable, "--env-python-pythonpath", "/tmp",
             *args],
            cwd=str(HERE), capture_output=True, text=True)
        return r.returncode, r.stderr

    rc, err = parse("--env-python-arg=-S")
    check("the = form gets past argument parsing",
          "expected one argument" not in err, err[-200:])
    # It still fails, on the host check rather than the parser: nothing is listening
    # on 18061 here. That is the proof it got past argparse.
    check("and fails later, on the host, not the parser",
          "nothing is listening" in (err or ""), err[-200:])

    rc, err = parse("--env-python-arg", "-S")
    check("the separated form is the failure that was hit",
          "expected one argument" in err, err[-200:])
    check("and it exits 2, which reads like a configuration error", rc == 2, str(rc))

    # A value that does not start with a dash works either way — which is why the
    # bug survived: every other option in the command line looked fine.
    rc, err = parse("--env-python-arg", "-X")
    check("any dash-leading value has the same problem",
          "expected one argument" in err, err[-200:])


def test_worker_count_survives_hostile_argv() -> None:
    """argv is NUL-separated because an argument may hold anything but NUL.

    The version this replaces did `tr '\\0' '\\n'` and read the result line by line.
    That is wrong twice over. An argument containing a newline becomes two, so every
    position after it shifts and the token *after* the real worker count gets read
    as the worker count — a wrong number that decides how many processes the run
    believes it is authorised to start. And embedding a live process's command line
    in shell or Python source text is a quoting problem waiting for an argument with
    a quote in it.
    """
    from bench.collect_backend_meta import worker_count

    def n(argv):
        return worker_count(argv)[0]

    def why(argv):
        return worker_count(argv)[1] or ""

    # The three spellings gunicorn accepts.
    base = ["gunicorn", "woa23_app:app", "-k", "uvicorn.workers.UvicornWorker"]
    check("-w N is read", n(base + ["-w", "2"]) == 2)
    check("--workers N is read", n(base + ["--workers", "4"]) == 4)
    check("--workers=N is read", n(base + ["--workers=8"]) == 8)
    check("production's actual shape gives 2",
          n(["/home/odbadmin/.pyenv/versions/py311/bin/python3.11",
             "/home/odbadmin/.pyenv/versions/py311/bin/gunicorn", "woa23_app:app",
             "-w", "2", "-b", "127.0.0.1:8050", "--timeout", "120"]) == 2)

    # The whole point: these are single arguments, not separators.
    check("an argument containing a newline does not split",
          n(["gunicorn", "--access-logformat", "line1\nline2", "-w", "3"]) == 3)
    check("a newline BEFORE the flag does not shift the pairing",
          n(["gunicorn", "--name", "a\n-w\n99", "-w", "3"]) == 3)
    check("a newline-embedded '-w 99' is not mistaken for the flag",
          n(["gunicorn", "--name", "x\n-w\n99"]) is None)
    check("an argument containing a quote is one argument",
          n(["gunicorn", "--name", "it's \"quoted\"", "-w", "5"]) == 5)
    check("an argument containing backticks is inert",
          n(["gunicorn", "--name", "`touch /tmp/pwned`", "-w", "6"]) == 6)
    check("so is one containing a command substitution",
          n(["gunicorn", "--name", "$(touch /tmp/pwned)", "-w", "7"]) == 7)
    check("and one containing a NUL-looking escape",
          n(["gunicorn", "--name", "\\0-w\\0 99", "-w", "1"]) == 1)
    check("a semicolon does not terminate anything",
          n(["gunicorn", "--name", "; rm -rf /", "-w", "2"]) == 2)
    check("a value that merely contains -w is not the flag",
          n(["gunicorn", "--log-file", "/var/log/-w-2.log"]) is None)

    # Fail closed rather than guess.
    check("no flag at all is an error, not a default", n(base) is None)
    check("and says so", "no -w/--workers" in why(base))
    check("a trailing -w with no value is an error",
          n(base + ["-w"]) is None)
    check("and says the value is missing", "no value after it" in why(base + ["-w"]))
    check("a non-numeric worker count is an error",
          n(base + ["-w", "two"]) is None)
    check("a negative worker count is an error", n(base + ["-w", "-2"]) is None)
    check("an empty worker count is an error", n(base + ["-w", ""]) is None)
    check("conflicting counts are an error, not a precedence guess",
          n(base + ["-w", "2", "--workers", "4"]) is None)
    check("and the conflict is spelled out",
          "more than once" in why(base + ["-w", "2", "--workers=4"]))
    check("but a repeat of the SAME value is fine",
          n(base + ["-w", "2", "--workers=2"]) == 2)
    check("an empty argv is an error", n([]) is None)


def test_graceful_timeout_is_read_not_assumed() -> None:
    """The value C2 cycle 1 was never asked for.

    The arms' launch argv was already being recorded when that cycle stranded an
    arbiter. What was missing was anything that read it: gunicorn's default of 30 s
    was in force, `STOP_WAIT_SECS` was 20, and both facts were sitting in the
    evidence unexamined. So absence is an error here, never a silent fallback to
    whatever the library would have done.
    """
    from bench.collect_backend_meta import graceful_timeout

    def n(argv):
        return graceful_timeout(argv)[0]

    def why(argv):
        return graceful_timeout(argv)[1] or ""

    base = ["gunicorn", "api.app:app", "-k", "uvicorn.workers.UvicornWorker"]
    check("--graceful-timeout N is read", n(base + ["--graceful-timeout", "10"]) == 10)
    check("--graceful-timeout=N is read", n(base + ["--graceful-timeout=10"]) == 10)
    check("the arms' actual shape gives 10",
          n(["/home/odbadmin/.pyenv/versions/py311/bin/python3.11", "-S", "-m",
             "gunicorn", "api.app:app", "-w", "2", "-k",
             "uvicorn.workers.UvicornWorker", "--graceful-timeout", "10",
             "-b", "127.0.0.1:18071", "--timeout", "120"]) == 10)

    check("absence is an error, not gunicorn's 30-second default",
          n(base) is None)
    check("and the error says so rather than naming a number",
          "default" in why(base), why(base))

    # --timeout is a different flag with a different meaning, and the arms carry
    # both. Reading one for the other would size the stop window against the
    # request timeout, which is 120 and would hide any shutdown problem entirely.
    check("--timeout is not --graceful-timeout",
          n(base + ["--timeout", "120"]) is None)
    check("and the pairing is not confused when both are present",
          n(base + ["--timeout", "120", "--graceful-timeout", "10"]) == 10)

    check("a trailing flag with no value is an error",
          n(base + ["--graceful-timeout"]) is None)
    check("two different values are ambiguous, not first-wins",
          n(base + ["--graceful-timeout", "10", "--graceful-timeout=30"]) is None)
    check("but the same value twice is not ambiguous",
          n(base + ["--graceful-timeout", "10", "--graceful-timeout=10"]) == 10)
    check("a non-integer value is rejected",
          n(base + ["--graceful-timeout", "10s"]) is None)

    # Same hostile-argv properties as the worker count, and for the same reason:
    # this argv comes from /proc and may contain anything but NUL.
    check("an argument containing a newline does not split",
          n(["gunicorn", "--name", "a\n--graceful-timeout\n99",
             "--graceful-timeout", "10"]) == 10)
    check("a newline-embedded flag is not mistaken for the real one",
          n(["gunicorn", "--name", "x\n--graceful-timeout\n99"]) is None)


def test_argv_of_splits_on_nul_only() -> None:
    """Read back from a real file, in the exact /proc/<pid>/cmdline format."""
    from bench.collect_backend_meta import argv_of, worker_count

    hostile = ["gunicorn", "woa23_app:app", "--name", "a\nb`c`\"d'e; f",
               "-w", "2", "-b", "127.0.0.1:8050"]
    with tempfile.TemporaryDirectory() as d:
        fake = Path(d) / "cmdline"
        fake.write_bytes(b"\0".join(a.encode() for a in hostile) + b"\0")
        raw = fake.read_bytes()
        parts = raw.split(b"\0")
        if parts and parts[-1] == b"":
            parts.pop()
        argv = [p.decode("utf-8", "surrogateescape") for p in parts]
    check("every argument round-trips intact", argv == hostile, str(argv))
    check("including the one with a newline, backticks and quotes",
          argv[3] == "a\nb`c`\"d'e; f", repr(argv[3]))
    check("and the worker count is still right", worker_count(argv)[0] == 2)
    # What the old newline-based reader would have seen: one argument became three,
    # so the token after "-w" is no longer at the position the pairing expects.
    naive = "\n".join(hostile).split("\n")
    check("the newline-based reader sees a different argv", len(naive) != len(argv),
          f"{len(naive)} vs {len(argv)}")

    argv, err = argv_of(999999999)
    check("an unreadable pid is an error, not an empty argv",
          argv is None and err is not None, str(err))


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
    check("a distributions digest is produced", len(d.get("name_version_set_sha256", "")) == 64)
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
               test_dash_valued_options_reach_the_parser,
               test_worker_count_survives_hostile_argv,
               test_graceful_timeout_is_read_not_assumed,
               test_argv_of_splits_on_nul_only,
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
