"""S1 discover / S2 scan: обход .py-файлов сервиса с pathspec-фильтрацией.

DEFAULT_EXCLUDES применяются всегда (venv/vcs/кэши/state, паттерны "**/"-префиксные --
матчатся на любой глубине, не только в корне сервиса), поверх них слоятся .gitignore
сервиса (если есть, корневой файл, gitignore-синтаксис через pathspec) и явные excludes
из конфига сервиса.
relpath'ы отсортированы — единственный источник детерминизма для всех последующих стадий
(facts/extract/join итерируют файлы в этом же порядке, см. analyze.py); tree_hash — вход
для кэша ScipRunner (S3): sha256 по отсортированному списку "relpath:sha256" пар, поэтому
меняется при любом изменении набора файлов сервиса или содержимого хотя бы одного из них.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pathspec

DEFAULT_EXCLUDES = [
    "**/.venv/**", "**/.git/**", "**/__pycache__/**", "**/.codegraph/**", "**/node_modules/**",
]


def _gitignore_lines(service_root: Path) -> list[str]:
    gitignore = service_root / ".gitignore"
    if not gitignore.is_file():
        return []
    return gitignore.read_text().splitlines()


def scan_service(
    path: Path, excludes: list[str]
) -> tuple[list[tuple[str, str, int]], str]:
    spec = pathspec.PathSpec.from_lines(
        "gitignore", [*_gitignore_lines(path), *excludes, *DEFAULT_EXCLUDES]
    )

    rows: list[tuple[str, str, int]] = []
    for f in path.rglob("*.py"):
        relpath = f.relative_to(path).as_posix()
        if spec.match_file(relpath):
            continue
        data = f.read_bytes()
        rows.append((relpath, hashlib.sha256(data).hexdigest(), len(data)))

    rows.sort(key=lambda r: r[0])
    tree_hash = hashlib.sha256(
        "\n".join(f"{rp}:{sha}" for rp, sha, _ in rows).encode()
    ).hexdigest()
    return rows, tree_hash
