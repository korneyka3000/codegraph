"""CLI misc smoke tests (M4 T9): `--version`.

Eager option on the top-level `app` callback (`_callback` in cli.py) -- reads the
REAL installed package version via `importlib.metadata.version("codegraph")`, not
the separate `codegraph.__version__` constant in `__init__.py` (that constant can
drift from what's actually installed; importlib.metadata reads the package's own
installed distribution metadata, sourced from pyproject.toml's `[project].version`
at build/install time -- the same thing `uv pip show codegraph` would report).
`_pyproject_version()` below reads pyproject.toml directly (not the `__init__.py`
constant either) so this test can never silently pass by comparing two copies of
the same hardcoded string that both happened to drift together.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

from typer.testing import CliRunner

from codegraph.cli import app

runner = CliRunner()
REPO_ROOT = Path(__file__).parents[2]


def _pyproject_version() -> str:
    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    return data["project"]["version"]


def test_version_flag_prints_pyproject_version_and_exits_0():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0, result.output
    assert _pyproject_version() in result.output


def test_version_flag_short_circuits_before_subcommand_argument_validation():
    # Eager-option contract (`is_eager=True`): --version must be handled BEFORE
    # Typer/Click validates a subcommand's own required arguments -- `trace`
    # requires a SELECTOR argument, which is deliberately omitted here. If the
    # eager wiring were broken (e.g. a plain, non-eager option), this would
    # instead fail with a "Missing argument 'SELECTOR'" usage error.
    result = runner.invoke(app, ["--version", "trace"])
    assert result.exit_code == 0, result.output
    assert _pyproject_version() in result.output
    assert "Missing argument" not in result.output


def test_no_version_flag_bare_invocation_still_shows_help_not_a_version():
    # Regression: adding the --version option must not change the pre-existing
    # `no_args_is_help=True` contract for a bare `codegraph` invocation (see
    # cli.py's own `_callback` comment) -- still prints help (exit 2, Click's own
    # `no_args_is_help` contract -- verified unchanged, not something this task
    # touches), and must NOT print a bare version string on its own.
    result = runner.invoke(app, [])
    assert result.exit_code == 2, result.output
    assert "Usage" in result.output
