"""Keep temp_dir clean: drop zombies, keep resumable .part files."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from app.organizer import file_looks_complete, prune_empty_dirs


def part_to_final_name(part_name: str) -> str:
    """``video.mp4.part`` → ``video.mp4``."""
    name = str(part_name or "")
    if name.endswith(".part"):
        return name[: -len(".part")]
    return name


def cleanup_temp_group(
    temp_group: Path,
    download_group: Path,
    *,
    completed_basenames: Optional[set[str]] = None,
    min_keep_bytes: int = 1,
) -> dict[str, int]:
    """
    Scan temp mirror of one chat group and tidy it.

    Rules:
    - 0-byte ``.part`` → delete (zombie)
    - Final file already complete under downloads → delete ``.part``
    - Basename listed in ``completed_basenames`` (queue/chat_completed) → delete
    - Duplicate ``.part`` same basename → keep largest only
    - Otherwise keep (resume later — especially large parts)
    - Then prune empty directories under temp_group
    """
    stats = {
        "parts_seen": 0,
        "removed_empty": 0,
        "removed_done": 0,
        "removed_dup": 0,
        "kept_resume": 0,
        "dirs_pruned": 0,
    }
    temp_group = Path(temp_group)
    download_group = Path(download_group)
    if not temp_group.is_dir():
        return stats

    completed = {str(x) for x in (completed_basenames or set()) if x}
    # Finished files on disk (basename → best path)
    finished: dict[str, Path] = {}
    if download_group.is_dir():
        try:
            for p in download_group.rglob("*"):
                try:
                    if not p.is_file() or p.name.endswith(".part"):
                        continue
                    if file_looks_complete(p):
                        prev = finished.get(p.name)
                        if prev is None or p.stat().st_size >= prev.stat().st_size:
                            finished[p.name] = p
                except OSError:
                    continue
        except OSError:
            pass

    # Group .part by final basename
    by_base: dict[str, list[Path]] = {}
    try:
        parts = [p for p in temp_group.rglob("*.part") if p.is_file()]
    except OSError:
        parts = []

    for part in parts:
        stats["parts_seen"] += 1
        base = part_to_final_name(part.name)
        by_base.setdefault(base, []).append(part)

    for base, group in by_base.items():
        # Prefer largest when choosing which duplicate to keep
        sized: list[tuple[int, Path]] = []
        for p in group:
            try:
                sized.append((int(p.stat().st_size), p))
            except OSError:
                sized.append((0, p))
        sized.sort(key=lambda t: t[0], reverse=True)

        done = base in completed or base in finished
        # Mirrored final path for the largest candidate
        keep_path: Optional[Path] = None
        for sz, part in sized:
            if sz <= 0:
                try:
                    part.unlink(missing_ok=True)
                    stats["removed_empty"] += 1
                except OSError:
                    pass
                continue
            if done:
                try:
                    part.unlink(missing_ok=True)
                    stats["removed_done"] += 1
                except OSError:
                    pass
                continue
            # Also drop if mirrored final exists next to expected download path
            try:
                rel = part.resolve().relative_to(temp_group.resolve())
                final = download_group / rel
                # part is .../file.mp4.part → final file is .../file.mp4
                if str(final).endswith(".part"):
                    final = Path(str(final)[: -len(".part")])
                if file_looks_complete(final):
                    part.unlink(missing_ok=True)
                    stats["removed_done"] += 1
                    continue
            except (ValueError, OSError):
                pass

            if keep_path is None:
                keep_path = part
                stats["kept_resume"] += 1
            else:
                # Duplicate smaller/same — remove
                try:
                    part.unlink(missing_ok=True)
                    stats["removed_dup"] += 1
                except OSError:
                    pass

        # If we kept a part but a finished file exists under another folder name,
        # still remove (done already handled). If min_keep_bytes and somehow 0 left…
        _ = min_keep_bytes

    stats["dirs_pruned"] = prune_empty_dirs(temp_group, stop_at=temp_group)
    return stats


def format_temp_cleanup_log(stats: dict[str, Any]) -> str:
    """One short Chinese log line for task activity."""
    kept = int(stats.get("kept_resume") or 0)
    empty = int(stats.get("removed_empty") or 0)
    done = int(stats.get("removed_done") or 0)
    dup = int(stats.get("removed_dup") or 0)
    dirs = int(stats.get("dirs_pruned") or 0)
    bits = ["临时目录整理"]
    if empty:
        bits.append(f"空文件 {empty}")
    if done:
        bits.append(f"已完成残留 {done}")
    if dup:
        bits.append(f"重复 {dup}")
    if dirs:
        bits.append(f"空目录 {dirs}")
    if kept:
        bits.append(f"保留续传 {kept}")
    if len(bits) == 1:
        bits.append("无需清理")
    return " · ".join(bits)
