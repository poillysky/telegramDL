"""Keep temp_dir clean: drop zombies, keep resumable .part files."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Optional

from app.organizer import file_looks_complete, prune_empty_dirs

# ``name_123.mp4`` / ``video_123.mp4`` / ``name_123_1.mp4``
_PART_MSGID_RE = re.compile(r"_(\d+)(?:_\d+)?\.[^.]+$", re.IGNORECASE)


def part_to_final_name(part_name: str) -> str:
    """``video.mp4.part`` → ``video.mp4``."""
    name = str(part_name or "")
    if name.endswith(".part"):
        return name[: -len(".part")]
    return name


def message_id_from_part_name(part_name: str) -> Optional[int]:
    """Parse Telegram message id embedded in a ``.part`` / final filename."""
    final = part_to_final_name(part_name)
    m = _PART_MSGID_RE.search(final)
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def list_resumable_part_entries(
    temp_group: Path, *, min_bytes: int = 1
) -> list[dict[str, Any]]:
    """
    Scan temp mirror for keepable ``.part`` files.

    Each entry: ``path``, ``basename`` (final name), ``message_id`` (if parseable), ``size``.
    Duplicate basenames keep the largest part only.
    """
    temp_group = Path(temp_group)
    if not temp_group.is_dir():
        return []
    by_base: dict[str, Path] = {}
    try:
        parts = [p for p in temp_group.rglob("*.part") if p.is_file()]
    except OSError:
        parts = []
    for part in parts:
        try:
            size = part.stat().st_size
        except OSError:
            continue
        if size < int(min_bytes or 1):
            continue
        base = part_to_final_name(part.name)
        prev = by_base.get(base)
        if prev is None:
            by_base[base] = part
            continue
        try:
            if size >= prev.stat().st_size:
                by_base[base] = part
        except OSError:
            by_base[base] = part

    out: list[dict[str, Any]] = []
    for base, part in by_base.items():
        try:
            size = part.stat().st_size
        except OSError:
            size = 0
        out.append(
            {
                "path": part,
                "basename": base,
                "message_id": message_id_from_part_name(part.name),
                "size": size,
            }
        )
    # Largest first — finish big residues before tiny scraps
    out.sort(key=lambda e: int(e.get("size") or 0), reverse=True)
    return out


def purge_parts_under(
    temp_group: Path,
    *,
    message_ids: Optional[set[int]] = None,
    basenames: Optional[set[str]] = None,
) -> int:
    """Delete ``.part`` files matching message ids and/or final basenames. Returns count."""
    temp_group = Path(temp_group)
    if not temp_group.is_dir():
        return 0
    mids = {int(x) for x in (message_ids or set()) if x is not None}
    bases = {str(b).strip().lower() for b in (basenames or set()) if str(b).strip()}
    if not mids and not bases:
        return 0
    removed = 0
    try:
        parts = [p for p in temp_group.rglob("*.part") if p.is_file()]
    except OSError:
        parts = []
    for part in parts:
        try:
            base = part_to_final_name(part.name)
            mid = message_id_from_part_name(part.name)
            hit = (mid is not None and int(mid) in mids) or (
                base.lower() in bases
            )
            if not hit:
                continue
            part.unlink(missing_ok=True)
            removed += 1
        except OSError:
            continue
    if removed:
        prune_empty_dirs(temp_group, stop_at=temp_group)
    return removed


def cleanup_temp_group(
    temp_group: Path,
    download_group: Path,
    *,
    completed_basenames: Optional[set[str]] = None,
    resume_message_ids: Optional[set[int]] = None,
    resume_basenames: Optional[set[str]] = None,
    drop_unmapped: bool = True,
    min_keep_bytes: int = 1,
) -> dict[str, int]:
    """
    Scan temp mirror of one chat group and tidy it.

    Rules:
    - 0-byte ``.part`` → delete (zombie)
    - Final file already complete under downloads → delete ``.part``
    - Basename listed in ``completed_basenames`` (queue/chat_completed) → delete
    - Duplicate ``.part`` same basename → keep largest only
    - Keep only if consumable: message_id in ``resume_message_ids`` OR
      basename in ``resume_basenames`` (when those sets are provided)
    - If ``drop_unmapped`` and not consumable → delete as orphan zombie
    - Then prune empty directories under temp_group
    """
    stats = {
        "parts_seen": 0,
        "removed_empty": 0,
        "removed_done": 0,
        "removed_dup": 0,
        "removed_orphan": 0,
        "kept_resume": 0,
        "dirs_pruned": 0,
    }
    temp_group = Path(temp_group)
    download_group = Path(download_group)
    if not temp_group.is_dir():
        return stats

    completed = {str(x) for x in (completed_basenames or set()) if x}
    resume_mids = {int(x) for x in (resume_message_ids or set()) if x is not None}
    resume_bases = {
        str(b).strip().lower() for b in (resume_basenames or set()) if str(b).strip()
    }
    # When caller passes neither set, keep all non-done (legacy); when either is
    # passed (even empty), enforce consumable-only so orphans can be dropped.
    filter_resume = resume_message_ids is not None or resume_basenames is not None

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

            mid = message_id_from_part_name(part.name)
            consumable = True
            if filter_resume:
                consumable = (mid is not None and int(mid) in resume_mids) or (
                    base.lower() in resume_bases
                )
            if not consumable and drop_unmapped:
                try:
                    part.unlink(missing_ok=True)
                    stats["removed_orphan"] += 1
                except OSError:
                    pass
                continue

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

        _ = min_keep_bytes

    stats["dirs_pruned"] = prune_empty_dirs(temp_group, stop_at=temp_group)
    return stats


def format_temp_cleanup_log(stats: dict[str, Any]) -> str:
    """One short Chinese log line for task activity."""
    kept = int(stats.get("kept_resume") or 0)
    empty = int(stats.get("removed_empty") or 0)
    done = int(stats.get("removed_done") or 0)
    dup = int(stats.get("removed_dup") or 0)
    orphan = int(stats.get("removed_orphan") or 0)
    dirs = int(stats.get("dirs_pruned") or 0)
    bits = ["临时目录整理"]
    if empty:
        bits.append(f"空文件 {empty}")
    if done:
        bits.append(f"已完成残留 {done}")
    if dup:
        bits.append(f"重复 {dup}")
    if orphan:
        bits.append(f"无法续传 {orphan}")
    if dirs:
        bits.append(f"空目录 {dirs}")
    if kept:
        bits.append(f"保留续传 {kept}")
    if len(bits) == 1:
        bits.append("无需清理")
    return " · ".join(bits)
