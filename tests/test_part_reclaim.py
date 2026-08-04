"""Incomplete .part must stay in temp; stray download parts are reclaimed."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.downloader import DownloadScheduler


def test_reclaim_stray_parts_from_download(tmp_path: Path, monkeypatch):
    dl = tmp_path / "downloads"
    tmp = tmp_path / "temp"
    group = dl / "ChatA" / "#tag"
    group.mkdir(parents=True)
    stray = group / "clip.mp4.part"
    stray.write_bytes(b"partial-bytes")

    monkeypatch.setattr(
        "app.downloader.get_settings",
        lambda: SimpleNamespace(download_dir=dl, temp_dir=tmp),
    )
    sched = DownloadScheduler.__new__(DownloadScheduler)
    n = sched._reclaim_stray_parts_from_download(dl / "ChatA")
    assert n == 1
    assert not stray.exists()
    dest = tmp / "ChatA" / "#tag" / "clip.mp4.part"
    assert dest.is_file()
    assert dest.read_bytes() == b"partial-bytes"
