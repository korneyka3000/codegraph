"""S1/S2 scan_service: pathspec-фильтрация (.gitignore сервиса + excludes + DEFAULT_EXCLUDES),
детерминизм relpath-сортировки и чувствительность tree_hash к содержимому/набору файлов."""

from __future__ import annotations

import hashlib
import warnings

from codegraph.pipeline.scan import scan_service


def _tree(tmp_path):
    (tmp_path / "app" / "sub").mkdir(parents=True)
    (tmp_path / ".venv" / "lib").mkdir(parents=True)
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "app" / "main.py").write_text("A = 1\n")
    (tmp_path / "app" / "sub" / "mod.py").write_text("B = 2\n")
    (tmp_path / ".venv" / "lib" / "pkg.py").write_text("VENDORED = 1\n")
    (tmp_path / "__pycache__" / "cached.py").write_text("CACHED = 1\n")
    (tmp_path / "ignored.py").write_text("IGNORED = 1\n")
    (tmp_path / ".gitignore").write_text("ignored.py\n")
    return tmp_path


def test_scan_filters_default_excludes_and_gitignore(tmp_path):
    root = _tree(tmp_path)
    rows, _ = scan_service(root, [])
    relpaths = [r for r, _, _ in rows]
    assert relpaths == ["app/main.py", "app/sub/mod.py"]


def test_scan_relpaths_sorted(tmp_path):
    root = _tree(tmp_path)
    rows, _ = scan_service(root, [])
    relpaths = [r for r, _, _ in rows]
    assert relpaths == sorted(relpaths)


def test_scan_excludes_param_filters_additional_paths(tmp_path):
    root = _tree(tmp_path)
    rows, _ = scan_service(root, ["app/sub/**"])
    assert [r for r, _, _ in rows] == ["app/main.py"]


def test_scan_missing_gitignore_is_fine(tmp_path):
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "main.py").write_text("A = 1\n")
    rows, _ = scan_service(tmp_path, [])
    assert [r for r, _, _ in rows] == ["app/main.py"]


def test_scan_row_shape_sha256_and_size(tmp_path):
    (tmp_path / "m.py").write_bytes(b"x = 1\n")
    rows, _ = scan_service(tmp_path, [])
    assert rows == [("m.py", hashlib.sha256(b"x = 1\n").hexdigest(), 6)]


def test_two_calls_are_equal_rows_and_hash(tmp_path):
    root = _tree(tmp_path)
    rows1, hash1 = scan_service(root, [])
    rows2, hash2 = scan_service(root, [])
    assert rows1 == rows2
    assert hash1 == hash2


def test_tree_hash_changes_when_file_byte_changes(tmp_path):
    root = _tree(tmp_path)
    _, hash_before = scan_service(root, [])
    (root / "app" / "main.py").write_text("A = 2\n")
    _, hash_after = scan_service(root, [])
    assert hash_before != hash_after


def test_tree_hash_changes_when_file_set_changes(tmp_path):
    root = _tree(tmp_path)
    _, hash_before = scan_service(root, [])
    (root / "app" / "new_mod.py").write_text("C = 3\n")
    _, hash_after = scan_service(root, [])
    assert hash_before != hash_after


def test_scan_default_excludes_match_nested_not_only_root(tmp_path):
    # live-verified leak: unprefixed DEFAULT_EXCLUDES patterns (e.g. ".venv/**") only
    # matched a dir named ".venv" at the service root -- a nested ".venv" a few levels
    # down (e.g. inside a sub-package checked out its own vendored copy) slipped
    # through unfiltered. "**/"-prefixed patterns must match at any depth.
    (tmp_path / "svc" / "sub" / ".venv" / "lib").mkdir(parents=True)
    (tmp_path / "svc" / "sub" / ".venv" / "lib" / "x.py").write_text("VENDORED = 1\n")
    (tmp_path / "svc" / "sub" / "app.py").write_text("A = 1\n")

    rows, _ = scan_service(tmp_path, [])
    relpaths = [r for r, _, _ in rows]
    assert "svc/sub/.venv/lib/x.py" not in relpaths
    assert "svc/sub/app.py" in relpaths


def test_scan_uses_gitignore_pathspec_factory_no_deprecation_warning(tmp_path):
    # regression: PathSpec.from_lines("gitwildmatch", ...) raises pathspec's
    # DeprecationWarning ("'gitwildmatch' is deprecated ... use 'gitignore' instead")
    # on every single scan_service() call -- switching the factory name to
    # "gitignore" must make scanning warning-free.
    root = _tree(tmp_path)
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        scan_service(root, [])  # must not raise
