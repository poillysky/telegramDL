"""Tag-only folder layout (no date / media_type / flat subdirs)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.organizer import flatten_date_dirs_under_group, resolve_media_subdir


def _msg(text: str = "", message_id: int = 1):
    return SimpleNamespace(id=message_id, message=text, raw_text=text, media=None)


def test_resolve_media_subdir_tags_only_no_date():
    msg = _msg("#demo 7.11-14 自录")
    rel = resolve_media_subdir(msg, use_caption_folders=True, folder_mode="caption")
    assert rel == "#demo"
    assert "/" not in rel


def test_resolve_media_subdir_uncategorized():
    msg = _msg("no hashtag here 7.11")
    assert resolve_media_subdir(msg) == "_未分类"


def test_resolve_ignores_media_type_and_flat_modes():
    msg = _msg("#x video")
    assert resolve_media_subdir(msg, folder_mode="media_type") == "#x"
    assert resolve_media_subdir(msg, folder_mode="flat") == "#x"


def test_flatten_date_dirs_under_tag(tmp_path: Path):
    tag = tmp_path / "#demo"
    dated = tag / "2025.7.2-2025.7.4"
    dated.mkdir(parents=True)
    (dated / "a.mp4").write_bytes(b"a")
    (dated / "b.mp4.part").write_bytes(b"p")
    moves = flatten_date_dirs_under_group(tmp_path)
    assert moves
    assert (tag / "a.mp4").is_file()
    assert (tag / "b.mp4.part").is_file()
    assert not dated.exists()


def test_flatten_date_dirs_under_uncategorized(tmp_path: Path):
    root = tmp_path / "_未分类"
    dated = root / "7.11-7.14"
    dated.mkdir(parents=True)
    (dated / "c.mp4").write_bytes(b"c")
    moves = flatten_date_dirs_under_group(tmp_path)
    assert moves
    assert (root / "c.mp4").is_file()
    assert not dated.exists()


def test_flatten_noop_when_already_flat(tmp_path: Path):
    tag = tmp_path / "#demo"
    tag.mkdir()
    (tag / "a.mp4").write_bytes(b"a")
    assert flatten_date_dirs_under_group(tmp_path) == []
