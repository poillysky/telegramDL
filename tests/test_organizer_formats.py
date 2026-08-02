"""Unit tests for format / size filters (no Telegram network)."""

from __future__ import annotations

from types import SimpleNamespace

from app.organizer import matches_file_formats, matches_file_size


def _msg(*, name: str = "", size: int = 0):
    # Provide both name and ext (Telethon File-like)
    ext = ""
    if name and "." in name:
        ext = "." + name.rsplit(".", 1)[-1]
    file = SimpleNamespace(name=name, size=size, ext=ext) if (name or size) else None
    return SimpleNamespace(
        file=file,
        media=None,
        photo=None,
        document=None,
        video=None,
        audio=None,
        voice=None,
        video_note=None,
        gif=None,
        sticker=None,
    )


def test_global_formats_list():
    msg = _msg(name="clip.MP4")
    assert matches_file_formats(msg, ["mp4", "jpg"], "video") is True
    assert matches_file_formats(msg, ["mkv"], "video") is False
    assert matches_file_formats(msg, [], "video") is True


def test_per_type_formats_dict():
    video = _msg(name="a.mp4")
    photo = _msg(name="b.jpg")
    rules = {"video": ["mp4"], "photo": ["png"]}
    assert matches_file_formats(video, rules, "video") is True
    assert matches_file_formats(photo, rules, "photo") is False
    # No rule for document → allow
    assert matches_file_formats(video, rules, "document") is True


def test_file_size_bounds():
    assert matches_file_size(0, min_bytes=100, max_bytes=200) is True  # unknown
    assert matches_file_size(150, min_bytes=100, max_bytes=200) is True
    assert matches_file_size(50, min_bytes=100, max_bytes=0) is False
    assert matches_file_size(250, min_bytes=0, max_bytes=200) is False
    assert matches_file_size(250, min_bytes=0, max_bytes=0) is True
