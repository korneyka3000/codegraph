"""Загрузка workspace-конфига: явный YAML, поиск в директории, zero-config."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from codegraph.config.builtin_idioms import resolve_builtins
from codegraph.config.models import ServiceConfig, ServiceIdioms, WorkspaceConfig

CONFIG_FILENAME = "codegraph.yaml"


class ConfigError(Exception):
    pass


def synth_zero_config(repo: Path) -> WorkspaceConfig:
    repo = repo.resolve()
    return WorkspaceConfig(
        graph_name=repo.name,
        services=[ServiceConfig(name=repo.name, path=repo)],
    )


def load_workspace(target: Path) -> WorkspaceConfig:
    target = target.resolve()
    if target.is_dir():
        candidate = target / CONFIG_FILENAME
        if not candidate.exists():
            return synth_zero_config(target)
        target = candidate
    if not target.exists():
        raise ConfigError(f"config file not found: {target}")

    try:
        raw = yaml.safe_load(target.read_text()) or {}
        cfg = WorkspaceConfig.model_validate(raw)
    except yaml.YAMLError as e:
        raise ConfigError(f"invalid YAML in {target}: {e}") from e
    except ValidationError as e:
        raise ConfigError(f"invalid config {target}:\n{e}") from e

    base = target.parent
    resolved_services = []
    for svc in cfg.services:
        path = svc.path if svc.path.is_absolute() else (base / svc.path)
        path = path.resolve()
        if not path.is_dir():
            raise ConfigError(f"service {svc.name!r}: path does not exist: {path}")
        resolved_services.append(svc.model_copy(update={"path": path}))
    cfg = cfg.model_copy(update={"services": resolved_services})

    try:
        resolve_builtins(cfg.builtin_idioms)
    except KeyError as e:
        raise ConfigError(str(e)) from e
    return cfg


def effective_idioms(cfg: WorkspaceConfig, svc: ServiceConfig) -> ServiceIdioms:
    merged = resolve_builtins(cfg.builtin_idioms)
    merged.producers.extend(svc.idioms.producers)
    merged.consumers.extend(svc.idioms.consumers)
    merged.http_clients.extend(svc.idioms.http_clients)
    return merged
