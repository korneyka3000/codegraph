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
    # M7 T3 (OPEN R1): env_sources paths resolve relative to the workspace yaml's own
    # dir, mirroring ServiceConfig.path's exact contract just above (relative-vs-
    # absolute check, .resolve(), loud ConfigError on a missing file) -- validating
    # existence HERE (config load time) rather than defensively degrading in
    # linking/env_map.py keeps a typo'd path a loud, immediate signal instead of a
    # silently-empty env->service map three stages later.
    resolved_env_sources = []
    for p in cfg.env_sources:
        path = p if p.is_absolute() else (base / p)
        path = path.resolve()
        if not path.is_file():
            raise ConfigError(f"env_sources entry not found: {path}")
        resolved_env_sources.append(path)
    cfg = cfg.model_copy(
        update={"services": resolved_services, "env_sources": resolved_env_sources},
    )

    try:
        resolve_builtins(cfg.builtin_idioms)
    except KeyError as e:
        raise ConfigError(e.args[0]) from e
    return cfg


def effective_idioms(cfg: WorkspaceConfig, svc: ServiceConfig) -> ServiceIdioms:
    """Effective per-service idioms: сервисные идиомы идут ПЕРВЫМИ в каждом merged-
    списке, builtin -- после. Порядок значим, не только состав (T6-ревью фикс):
    экстракторы (kafka_ext producers/consumers, http_client_ext) дедупят call-сайты по
    принципу «первая идиома в списке побеждает», поэтому собственная (более специфичная)
    идиома сервиса должна затенять builtin-конвенцию, когда обе матчат один call-сайт --
    напр. кастомный default-sdk http-client kyc-worker'а (base_url_env=
    DOCUMENT_MANAGEMENT_URL) против безадресного builtin aiohttp-client-convention,
    оба глоба которых матчат один и тот же клиент-класс."""
    builtins = resolve_builtins(cfg.builtin_idioms)
    return ServiceIdioms(
        producers=[*svc.idioms.producers, *builtins.producers],
        consumers=[*svc.idioms.consumers, *builtins.consumers],
        http_clients=[*svc.idioms.http_clients, *builtins.http_clients],
    )
