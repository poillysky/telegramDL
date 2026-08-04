"""Temp cleanup: zombies out, resumable .part kept."""

from __future__ import annotations

from pathlib import Path

from app.temp_cleanup import cleanup_temp_group, format_temp_cleanup_log, part_to_final_name


def test_part_to_final_name():
    assert part_to_final_name("a.mp4.part") == "a.mp4"
    assert part_to_final_name("x.part") == "x"


def test_cleanup_removes_empty_and_done_keeps_resume(tmp_path: Path):
    dl = tmp_path / "downloads" / "G"
    tmp = tmp_path / "temp" / "G"
    tag = "#t"
    date = "2025.6.12-2025.6.18"
    (dl / tag / date).mkdir(parents=True)
    (tmp / tag / date).mkdir(parents=True)

    # Finished download
    (dl / tag / date / "done.mp4").write_bytes(b"finished-content")
    (tmp / tag / date / "done.mp4.part").write_bytes(b"stale-part")

    # Empty zombie
    (tmp / tag / date / "empty.mp4.part").write_bytes(b"")

    # Completed in queue but path drifted
    (tmp / tag / date / "queued_done.mp4.part").write_bytes(b"abc")

    # Still needed — large resumable
    (tmp / tag / date / "resume.mp4.part").write_bytes(b"x" * 5000)

    # Duplicate smaller resume
    other = tmp / tag / "other-date"
    other.mkdir(parents=True)
    (other / "resume.mp4.part").write_bytes(b"tiny")

    # Empty folder to prune
    (tmp / tag / "empty-dir").mkdir(parents=True)

    stats = cleanup_temp_group(
        tmp,
        dl,
        completed_basenames={"queued_done.mp4"},
    )
    assert not (tmp / tag / date / "empty.mp4.part").exists()
    assert not (tmp / tag / date / "done.mp4.part").exists()
    assert not (tmp / tag / date / "queued_done.mp4.part").exists()
    assert (tmp / tag / date / "resume.mp4.part").exists()
    assert (tmp / tag / date / "resume.mp4.part").stat().st_size == 5000
    assert not (other / "resume.mp4.part").exists()  # dup removed
    assert not (tmp / tag / "empty-dir").exists()
    assert stats["kept_resume"] == 1
    assert stats["removed_empty"] >= 1
    assert stats["removed_done"] >= 2
    line = format_temp_cleanup_log(stats)
    assert "临时目录整理" in line
    assert "保留续传" in line
