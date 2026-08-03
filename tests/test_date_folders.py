"""Date token / file YMD range / caption fallback unit tests."""

from __future__ import annotations

from pathlib import Path

from app.organizer import (
    build_date_folder_repairs,
    date_token_from_filenames,
    extract_date_token,
    extract_ymd_from_filename,
    migrate_legacy_date_dirs,
    repair_date_folders,
)


def test_same_month_shorthand():
    assert extract_date_token("#t 7.11-14自录") == "7.11-7.14"
    assert extract_date_token("7.24-8.25") == "7.24-8.25"
    assert extract_date_token("7.11") == "7.11"


def test_ymd_from_filename():
    assert extract_ymd_from_filename("VID_20250711_120000.mp4") == (2025, 7, 11)
    assert extract_ymd_from_filename("2025-07-14.mp4") == (2025, 7, 14)
    assert extract_ymd_from_filename("clip_2024.mp4") is None
    assert extract_ymd_from_filename("a.mp4") is None


def test_date_token_from_filenames_range():
    assert (
        date_token_from_filenames(
            ["VID_20250711_1.mp4", "VID_20250714_2.mp4", "nope.mp4"]
        )
        == "2025.7.11-2025.7.14"
    )
    assert date_token_from_filenames(["2025.07.11.mp4"]) == "2025.7.11"
    assert date_token_from_filenames(["a.mp4", "b.mp4"]) is None


def test_build_repairs_from_captions():
    mapping = build_date_folder_repairs(
        ["#a 7.11-14", "#b 7.24-8.25", "#c 7.11自录"]
    )
    assert mapping["7.11"] == "7.11-7.14"
    assert mapping["7.24"] == "7.24-8.25"


def test_repair_prefers_file_ymd(tmp_path: Path):
    tag = tmp_path / "#demo"
    old = tag / "7.11"
    old.mkdir(parents=True)
    (old / "VID_20250711_001.mp4").write_bytes(b"x")
    (old / "VID_20250714_002.mp4").write_bytes(b"y")
    moves = repair_date_folders(tmp_path, ["#demo 7.11-14"])
    assert moves
    dst = tag / "2025.7.11-2025.7.14"
    assert dst.is_dir()
    assert (dst / "VID_20250711_001.mp4").is_file()
    assert not old.exists()


def test_repair_caption_fallback_without_file_dates(tmp_path: Path):
    tag = tmp_path / "#demo"
    old = tag / "7.11"
    old.mkdir(parents=True)
    (old / "a.mp4").write_bytes(b"x")
    moves = repair_date_folders(tmp_path, ["#demo 7.11-14"])
    assert moves
    dst = tag / "7.11-7.14"
    assert dst.is_dir()
    assert (dst / "a.mp4").is_file()


def test_migrate_moves_caption_folder_into_file_ymd(tmp_path: Path):
    tag = tmp_path / "#demo"
    old = tag / "7.11"
    old.mkdir(parents=True)
    (old / "vlc_record_2026_04_11_01h00m00s.mp4").write_bytes(b"a")
    (old / "vlc_record-2026-07-21-21h09m00s.mp4").write_bytes(b"b")
    new = tag / "2026.4.11-2026.7.21"
    new.mkdir()
    (new / "already.mp4").write_bytes(b"c")
    moves = migrate_legacy_date_dirs(
        tag, "2026.4.11-2026.7.21", caption="#demo 7.11-14"
    )
    assert moves
    assert (new / "vlc_record_2026_04_11_01h00m00s.mp4").is_file()
    assert (new / "vlc_record-2026-07-21-21h09m00s.mp4").is_file()
    assert (new / "already.mp4").is_file()
    assert not old.exists()


def test_repair_without_captions_still_uses_files(tmp_path: Path):
    tag = tmp_path / "#demo"
    old = tag / "4.11"
    old.mkdir(parents=True)
    (old / "vlc_record_2026_04_11_x.mp4").write_bytes(b"a")
    (old / "vlc_record_2026_04_26_y.mp4").write_bytes(b"b")
    moves = repair_date_folders(tmp_path, [])
    assert moves
    dst = tag / "2026.4.11-2026.4.26"
    assert dst.is_dir()
    assert (dst / "vlc_record_2026_04_11_x.mp4").is_file()
