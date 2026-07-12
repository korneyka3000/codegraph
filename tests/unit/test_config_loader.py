from pathlib import Path

import pytest

from codegraph.config.loader import (
    ConfigError,
    effective_idioms,
    load_workspace,
    synth_zero_config,
)

MINIMAL = """
version: 1
graph_name: demo
services:
  - name: svc-a
    path: ./svc-a
"""


def _mk_ws(tmp_path: Path, yaml_text: str) -> Path:
    (tmp_path / "svc-a").mkdir()
    p = tmp_path / "codegraph.yaml"
    p.write_text(yaml_text)
    return p


def test_load_explicit_yaml_resolves_paths(tmp_path):
    p = _mk_ws(tmp_path, MINIMAL)
    cfg = load_workspace(p)
    assert cfg.services[0].path == (tmp_path / "svc-a").resolve()


def test_load_directory_finds_yaml(tmp_path):
    _mk_ws(tmp_path, MINIMAL)
    cfg = load_workspace(tmp_path)
    assert cfg.graph_name == "demo"


def test_zero_config_synthesis(tmp_path):
    repo = tmp_path / "my-repo"
    repo.mkdir()
    cfg = load_workspace(repo)
    assert cfg.graph_name == "my-repo"
    assert len(cfg.services) == 1
    assert cfg.services[0].path == repo.resolve()
    assert synth_zero_config(repo).services[0].name == "my-repo"


def test_missing_service_path_raises(tmp_path):
    p = tmp_path / "codegraph.yaml"
    p.write_text(MINIMAL)  # svc-a не создана
    with pytest.raises(ConfigError, match="svc-a"):
        load_workspace(p)


def test_unknown_builtin_idiom_raises(tmp_path):
    p = _mk_ws(
        tmp_path,
        MINIMAL + "builtin_idioms: [fastapi, nosuch]\n",
    )
    with pytest.raises(ConfigError, match="nosuch"):
        load_workspace(p)


def test_effective_idioms_merges_builtin_and_service(tmp_path):
    p = _mk_ws(
        tmp_path,
        """
version: 1
graph_name: demo
builtin_idioms: [aiokafka]
services:
  - name: svc-a
    path: ./svc-a
    idioms:
      producers:
        - name: outbox
          call: "app.outbox.add_event"
          channel: { kind: event_type, event_type_from: { arg: 0 } }
""",
    )
    cfg = load_workspace(p)
    idioms = effective_idioms(cfg, cfg.services[0])
    names = {pr.name for pr in idioms.producers}
    assert "outbox" in names and "aiokafka-send" in names
