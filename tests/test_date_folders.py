"""Date token / legacy folder repair unit tests."""

from __future__ import annotations

from pathlib import Path

from app.organizer import (
    build_date_folder_repairs,
    extract_date_token,
    repair_date_folders,
)


def test_same_month_shorthand():
    assert extract_date_token("#t 7.11-14自录") == "7.11-7.14"
    assert extract_date_token("7.24-8.25") == "7.24-8.25"
    assert extract_date_token("7.11") == "7.11"


def test_build_repairs_from_captions():
    mapping = build_date_folder_repairs(
        ["#a 7.11-14", "#b 7.24-8.25", "#c 7.11自录"]
    )
    assert mapping["7.11"] == "7.11-7.14"
    assert mapping["7.24"] == "7.24-8.25"
    assert mapping.get("7.11-14") == "7.11-7.14"


def test_repair_date_folders_merges(tmp_path: Path):
    tag = tmp_path / "#demo"
    old = tag / "7.11"
    old.mkdir(parents=True)
    (old / "a.mp4").write_bytes(b"x")
    moves = repair_date_folders(tmp_path, ["#demo 7.11-14"])
    assert moves
    dst = tag / "7.11-7.14"
    assert dst.is_dir()
    assert (dst / "a.mp4").is_file()
    assert not old.exists()
