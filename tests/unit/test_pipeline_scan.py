"""S1/S2 scan_service: pathspec-фильтрация (.gitignore сервиса + excludes + DEFAULT_EXCLUDES),
детерминизм relpath-сортировки и чувствительность tree_hash к содержимому/набору файлов."""

from __future__ import annotations

import hashlib

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
