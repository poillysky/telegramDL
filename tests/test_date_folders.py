"""Caption MD + file year / cross-year / nearby year unit tests."""

from __future__ import annotations

from pathlib import Path

from app.organizer import (
    apply_years_to_caption_date,
    build_date_folder_repairs,
    extract_date_token,
    extract_years_from_filename,
    migrate_legacy_date_dirs,
    repair_date_folders,
    resolve_caption_date_token,
    strip_years_from_date_token,
)


def test_same_month_shorthand():
    assert extract_date_token("#t 7.11-14自录") == "7.11-7.14"
    assert extract_date_token("7.24-8.25") == "7.24-8.25"
    assert extract_date_token("7.11") == "7.11"


def test_years_from_filename():
    assert extract_years_from_filename("VID_20250711_120000.mp4") == [2025]
    assert extract_years_from_filename("vlc_record_2026_04_12_01h13m16s.mp4") == [2026]
    assert extract_years_from_filename("clip_2024.mp4") == [2024]


def test_apply_years_same_and_cross():
    assert apply_years_to_caption_date("7.11-7.14", [2026]) == "2026.7.11-2026.7.14"
    assert apply_years_to_caption_date("7.11", [2026]) == "2026.7.11"
    # Cross-year caption span
    assert apply_years_to_caption_date("12.20-1.5", [2025]) == "2025.12.20-2026.1.5"
    assert (
        apply_years_to_caption_date("12.20-1.5", [2025, 2026]) == "2025.12.20-2026.1.5"
    )
    # No year → bare MD
    assert apply_years_to_caption_date("7.11-7.14", []) == "7.11-7.14"
    assert apply_years_to_caption_date("7.11", [], fallback_year=2026) == "2026.7.11"


def test_resolve_caption_date_token():
    assert (
        resolve_caption_date_token(
            "#t 7.11-14",
            ["vlc_record_2026_04_11_x.mp4", "vlc_record_2026_04_14_y.mp4"],
        )
        == "2026.7.11-2026.7.14"
    )
    # No file year → nearby caption / hint
    assert (
        resolve_caption_date_token(
            "#t 7.11",
            ["a.mp4"],
            nearby_texts=["#other 2025年合集"],
        )
        == "2025.7.11"
    )
    assert (
        resolve_caption_date_token("#t 7.11", ["a.mp4"], hint_year=2024) == "2024.7.11"
    )


def test_strip_years():
    assert strip_years_from_date_token("2026.7.11-2026.7.14") == "7.11-7.14"
    assert strip_years_from_date_token("2026.7.11") == "7.11"


def test_build_repairs_from_captions():
    mapping = build_date_folder_repairs(
        ["#a 7.11-14", "#b 7.24-8.25", "#c 7.11自录"]
    )
    assert mapping["7.11"] == "7.11-7.14"
    assert mapping["7.24"] == "7.24-8.25"


def test_repair_caption_md_plus_file_year(tmp_path: Path):
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


def test_repair_caption_fallback_without_file_years(tmp_path: Path):
    tag = tmp_path / "#demo"
    old = tag / "7.11"
    old.mkdir(parents=True)
    (old / "a.mp4").write_bytes(b"x")
    moves = repair_date_folders(tmp_path, ["#demo 7.11-14"])
    assert moves
    # No year on files → caption range without inventing year
    dst = tag / "7.11-7.14"
    assert dst.is_dir()


def test_migrate_moves_caption_folder(tmp_path: Path):
    tag = tmp_path / "#demo"
    old = tag / "7.11"
    old.mkdir(parents=True)
    (old / "vlc_record_2026_04_11_01h00m00s.mp4").write_bytes(b"a")
    new = tag / "2026.7.11-2026.7.14"
    new.mkdir()
    (new / "already.mp4").write_bytes(b"c")
    moves = migrate_legacy_date_dirs(
        tag, "2026.7.11-2026.7.14", caption="#demo 7.11-14"
    )
    assert moves
    assert (new / "vlc_record_2026_04_11_01h00m00s.mp4").is_file()
    assert not old.exists()
