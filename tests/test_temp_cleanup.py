"""Temp cleanup: zombies out, resumable .part kept."""

from __future__ import annotations

from pathlib import Path

from app.temp_cleanup import cleanup_temp_group, format_temp_cleanup_log, part_to_final_name


def test_message_id_from_part_name():
    from app.temp_cleanup import message_id_from_part_name

    assert message_id_from_part_name("video_12345.mp4.part") == 12345
    assert message_id_from_part_name("clip_99_1.mp4.part") == 99
    assert message_id_from_part_name("vlc_record_no_id.mp4.part") is None


def test_list_resumable_part_entries_prefers_largest(tmp_path: Path):
    from app.temp_cleanup import list_resumable_part_entries

    g = tmp_path / "G"
    (g / "a").mkdir(parents=True)
    (g / "b").mkdir(parents=True)
    (g / "a" / "file_100.mp4.part").write_bytes(b"x" * 100)
    (g / "b" / "file_100.mp4.part").write_bytes(b"y" * 5000)
    (g / "a" / "empty.mp4.part").write_bytes(b"")
    (g / "a" / "orig.mp4.part").write_bytes(b"z" * 200)

    entries = list_resumable_part_entries(g)
    bases = {e["basename"]: e for e in entries}
    assert "file_100.mp4" in bases
    assert bases["file_100.mp4"]["size"] == 5000
    assert bases["file_100.mp4"]["message_id"] == 100
    assert "empty.mp4" not in bases
    assert bases["orig.mp4"]["message_id"] is None
    assert entries[0]["size"] >= entries[-1]["size"]



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

    # Still needed — large resumable (known to resume set)
    (tmp / tag / date / "resume_42.mp4.part").write_bytes(b"x" * 5000)

    # Duplicate smaller resume
    other = tmp / tag / "other-date"
    other.mkdir(parents=True)
    (other / "resume_42.mp4.part").write_bytes(b"tiny")

    # Unmapped orphan (no msgid, not in resume list) → drop
    (tmp / tag / date / "orphan_vlc.mp4.part").write_bytes(b"orphan" * 100)

    # Empty folder to prune
    (tmp / tag / "empty-dir").mkdir(parents=True)

    stats = cleanup_temp_group(
        tmp,
        dl,
        completed_basenames={"queued_done.mp4"},
        resume_message_ids={42},
        resume_basenames={"resume_42.mp4"},
        drop_unmapped=True,
    )
    assert not (tmp / tag / date / "empty.mp4.part").exists()
    assert not (tmp / tag / date / "done.mp4.part").exists()
    assert not (tmp / tag / date / "queued_done.mp4.part").exists()
    assert (tmp / tag / date / "resume_42.mp4.part").exists()
    assert (tmp / tag / date / "resume_42.mp4.part").stat().st_size == 5000
    assert not (other / "resume_42.mp4.part").exists()  # dup removed
    assert not (tmp / tag / date / "orphan_vlc.mp4.part").exists()
    assert not (tmp / tag / "empty-dir").exists()
    assert stats["kept_resume"] == 1
    assert stats["removed_orphan"] >= 1


def test_purge_parts_under(tmp_path: Path):
    from app.temp_cleanup import purge_parts_under

    g = tmp_path / "G"
    (g / "t").mkdir(parents=True)
    (g / "t" / "a_9.mp4.part").write_bytes(b"x")
    (g / "t" / "keep.mp4.part").write_bytes(b"y")
    n = purge_parts_under(g, message_ids={9}, basenames={"nope.mp4"})
    assert n == 1
    assert not (g / "t" / "a_9.mp4.part").exists()
    assert (g / "t" / "keep.mp4.part").exists()


def test_format_temp_cleanup_log_includes_orphan():
    line = format_temp_cleanup_log(
        {
            "kept_resume": 2,
            "removed_empty": 1,
            "removed_done": 3,
            "removed_dup": 0,
            "removed_orphan": 4,
            "dirs_pruned": 1,
        }
    )
    assert "临时目录整理" in line
    assert "保留续传 2" in line
    assert "无法续传 4" in line
