import stat
import time

import pytest

from codegraph.resolvers.scip.runner import ScipRunError, ScipRunner

FAKE_OK = """#!/usr/bin/env python3
import sys, pathlib
args = sys.argv[1:]
out = args[args.index("--output") + 1]
pathlib.Path(out).write_bytes(b"FAKE-SCIP")
marker = pathlib.Path(__file__).parent / "invocations.log"
marker.open("a").write("run\\n")
"""

FAKE_SLEEP = """#!/usr/bin/env python3
import time
time.sleep(60)
"""

FAKE_FAIL = """#!/usr/bin/env python3
import sys
print("boom: cannot resolve environment")
sys.exit(3)
"""


def _mk_fake(tmp_path, body, name="fake-npx"):
    p = tmp_path / name
    p.write_text(body)
    p.chmod(p.stat().st_mode | stat.S_IEXEC)
    return p


def test_run_creates_scip_and_caches(tmp_path):
    fake = _mk_fake(tmp_path, FAKE_OK)
    svc = tmp_path / "svc"
    svc.mkdir()
    r = ScipRunner(npx=str(fake))
    res1 = r.run("svc", svc, None, tmp_path / "cache", "h1")
    assert res1.scip_path.read_bytes() == b"FAKE-SCIP" and not res1.from_cache
    res2 = r.run("svc", svc, None, tmp_path / "cache", "h1")
    assert res2.from_cache
    assert (tmp_path / "invocations.log").read_text().count("run") == 1


def test_new_tree_hash_reruns(tmp_path):
    fake = _mk_fake(tmp_path, FAKE_OK)
    svc = tmp_path / "svc"
    svc.mkdir()
    r = ScipRunner(npx=str(fake))
    r.run("svc", svc, None, tmp_path / "cache", "h1")
    r.run("svc", svc, None, tmp_path / "cache", "h2")
    assert (tmp_path / "invocations.log").read_text().count("run") == 2


def test_timeout_kills_process_group(tmp_path):
    fake = _mk_fake(tmp_path, FAKE_SLEEP)
    svc = tmp_path / "svc"
    svc.mkdir()
    r = ScipRunner(npx=str(fake), timeout_s=1)
    t0 = time.monotonic()
    with pytest.raises(ScipRunError, match="timeout"):
        r.run("svc", svc, None, tmp_path / "cache", "h1")
    assert time.monotonic() - t0 < 10


def test_nonzero_exit_raises_with_output_tail(tmp_path):
    fake = _mk_fake(tmp_path, FAKE_FAIL)
    svc = tmp_path / "svc"
    svc.mkdir()
    with pytest.raises(ScipRunError, match="cannot resolve environment"):
        ScipRunner(npx=str(fake)).run("svc", svc, None, tmp_path / "cache", "h1")


def test_venv_env_injected(tmp_path):
    probe = tmp_path / "probe-npx"
    probe.write_text(
        """#!/usr/bin/env python3
import os, sys, pathlib
args = sys.argv[1:]
out = args[args.index("--output") + 1]
pathlib.Path(out).write_text(os.environ.get("VIRTUAL_ENV", "") + "|" +
                             os.environ["PATH"].split(os.pathsep)[0])
"""
    )
    probe.chmod(probe.stat().st_mode | stat.S_IEXEC)
    svc = tmp_path / "svc"
    svc.mkdir()
    venv = tmp_path / ".venv"
    (venv / "bin").mkdir(parents=True)
    res = ScipRunner(npx=str(probe)).run("svc", svc, venv, tmp_path / "c", "h")
    env_dump = res.scip_path.read_text()
    assert str(venv) in env_dump and str(venv / "bin") in env_dump
