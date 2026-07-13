"""Запуск scip-python через npx: pinned-версия, venv-окружение, таймаут, кэш по tree-hash."""

from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path

from codegraph.constants import SCIP_PYTHON_VERSION
from codegraph.core.errors import CodegraphError


class ScipRunError(CodegraphError):
    pass


@dataclass(frozen=True)
class ScipRunResult:
    scip_path: Path
    from_cache: bool


class ScipRunner:
    def __init__(self, version: str = SCIP_PYTHON_VERSION, timeout_s: int = 1200,
                 node_options: str = "--max-old-space-size=8192", npx: str = "npx"):
        self.version = version
        self.timeout_s = timeout_s
        self.node_options = node_options
        self.npx = npx

    def run(self, service_name: str, service_path: Path, venv: Path | None,
            cache_dir: Path, tree_hash: str) -> ScipRunResult:
        cache_dir.mkdir(parents=True, exist_ok=True)
        out = cache_dir / f"{service_name}-{tree_hash}.scip"
        if out.exists():
            return ScipRunResult(scip_path=out, from_cache=True)

        cmd = [self.npx, "--yes", f"@sourcegraph/scip-python@{self.version}",
               "index", ".", "--project-name", service_name, "--output", str(out)]
        env = os.environ.copy()
        env["NODE_OPTIONS"] = self.node_options
        if venv is not None:
            env["VIRTUAL_ENV"] = str(venv)
            env["PATH"] = f"{venv / 'bin'}{os.pathsep}{env.get('PATH', '')}"

        proc = subprocess.Popen(
            cmd, cwd=service_path, env=env, start_new_session=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            output, _ = proc.communicate(timeout=self.timeout_s)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
            raise ScipRunError(
                f"scip-python timeout after {self.timeout_s}s for {service_name!r}"
            ) from None
        if proc.returncode != 0 or not out.exists():
            tail = (output or "")[-2000:]
            raise ScipRunError(
                f"scip-python failed for {service_name!r} "
                f"(exit {proc.returncode}):\n{tail}"
            )
        return ScipRunResult(scip_path=out, from_cache=False)
