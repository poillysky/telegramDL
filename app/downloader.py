"""Per-chat download workers with optional in-task concurrency and live speed."""

from __future__ import annotations

import asyncio
import logging
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from telethon.errors import FloodWaitError

from app.config import get_settings
from app.db import Database, db, normalize_download_mode
from app.indexer import indexer
from app.notify import notify_event
from app.organizer import (
    build_filename,
    detect_media_type,
    extract_tags,
    file_looks_complete,
    has_media,
    matches_caption_keywords,
    matches_file_formats,
    matches_file_size,
    matches_include_tags,
    merge_related_tag_folders,
    message_text,
    next_folder_state,
    normalize_keyword_list,
    normalize_tag_list,
    resolve_caption_text,
    resolve_download_path,
    resolve_media_subdir,
    sanitize_name,
    unique_path,
)
from app.telegram_client import DownloadPaused, tg_manager

logger = logging.getLogger(__name__)


@dataclass
class DownloadJob:
    message: Any
    message_id: int
    target_path: Path
    caption: str = ""
    media_type: Optional[str] = None
    rel_dir: str = "_未分类"
    tags: list[str] = field(default_factory=list)


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        try:
            dt = datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _msg_date_utc(message) -> Optional[datetime]:
    if not message or not message.date:
        return None
    dt = message.date
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _chunked(items: list[int], size: int = 80):
    for i in range(0, len(items), size):
        yield items[i : i + size]


class DownloadScheduler:
    def __init__(self, database: Database | None = None):
        self.db = database or db
        self._tasks: dict[int, asyncio.Task] = {}
        self._stop_flags: dict[int, asyncio.Event] = {}
        # Monotonic generation so a dying worker never pops its replacement
        self._worker_gen: dict[int, int] = {}
        self._lock = asyncio.Lock()
        # Dynamic parallel-chat slots (limit from app_meta / env)
        self._active_slots = 0
        self._slot_cond: Optional[asyncio.Condition] = None
        # Per-task format/size filters (avoids threading through every helper)
        self._task_filters: dict[int, dict[str, Any]] = {}
        # Live progress: task_id -> {files: {msg_id: slot}, ...}
        self._progress: dict[int, dict[str, Any]] = {}
        self._progress_lock = threading.Lock()
        # chat_id -> bumped after index scan so local monitor wakes
        self._index_bumps: set[str] = set()
        self._index_wake: dict[str, asyncio.Event] = {}
        self._index_bump_lock = threading.Lock()
        # task_id -> pending count when we last returned gap_found (blocks spin loops)
        self._gap_pending_sticky: dict[int, int] = {}
        # task_id -> monotonic time sticky was set (cooldown before auto re-try)
        self._gap_sticky_at: dict[int, float] = {}
        # task ids that were running across process restart (auto-resume after TG)
        self._startup_resume_ids: list[int] = []
        # Delayed auto-retry handles for sequential tasks with leftover failures
        self._failed_retry_handles: dict[int, asyncio.Task] = {}
        # task_id -> live download pool (for hot concurrency changes)
        self._live_pools: dict[int, dict[str, Any]] = {}

    def _chat_wake_event(self, chat_id: str | int) -> asyncio.Event:
        """Lazy Event for this chat (must run on the asyncio loop)."""
        key = str(chat_id)
        with self._index_bump_lock:
            ev = self._index_wake.get(key)
            if ev is None:
                ev = asyncio.Event()
                self._index_wake[key] = ev
            return ev

    def wake_local_monitor(self, chat_id: str | int) -> None:
        """Wake idle local-monitor loops (tag edits, etc.). Event.set is thread-safe."""
        key = str(chat_id)
        with self._index_bump_lock:
            ev = self._index_wake.get(key)
        if ev is not None:
            ev.set()

    async def apply_live_concurrency(
        self, task_id: int, concurrency: int | None = None
    ) -> None:
        """Hot-apply task concurrency to a running download pool (if any)."""
        tid = int(task_id)
        pool = self._live_pools.get(tid)
        if not pool or not pool.get("started"):
            return
        if concurrency is None:
            try:
                task = await self.db.get_task(tid) or {}
                concurrency = int(task.get("concurrency") or 1)
            except Exception:
                return
        await self._apply_pool_concurrency(pool, int(concurrency))

    def notify_index_updated(self, chat_id: str | int) -> None:
        """After manual/auto/full index scan: mark bump and wake monitor."""
        key = str(chat_id)
        with self._index_bump_lock:
            self._index_bumps.add(key)
            ev = self._index_wake.get(key)
        if ev is not None:
            ev.set()

    def peek_index_bump(self, chat_id: str | int) -> bool:
        with self._index_bump_lock:
            return str(chat_id) in self._index_bumps

    def consume_index_bump(self, chat_id: str | int) -> bool:
        with self._index_bump_lock:
            key = str(chat_id)
            if key in self._index_bumps:
                self._index_bumps.discard(key)
                return True
            return False

    async def _wait_monitor_idle(
        self,
        *,
        chat_id: int,
        stop_event: asyncio.Event,
        test_deadline: float | None,
        heartbeat_sec: float,
    ) -> str:
        """Sleep until index update, stop, test deadline, or rare heartbeat.

        Does not query the DB. Returns ``bump`` | ``stop`` | ``heartbeat``.
        """
        if stop_event.is_set() or self._test_time_up(test_deadline):
            return "stop"
        wake = self._chat_wake_event(chat_id)
        # Avoid lost wake: bump already set → return immediately
        if self.peek_index_bump(chat_id):
            return "bump"
        wake.clear()
        if self.peek_index_bump(chat_id):
            return "bump"

        stop_task = asyncio.create_task(stop_event.wait())
        wake_task = asyncio.create_task(wake.wait())
        tasks = {stop_task, wake_task}
        try:
            done, pending = await asyncio.wait(
                tasks,
                timeout=max(60.0, float(heartbeat_sec)),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        except Exception:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        if stop_event.is_set() or self._test_time_up(test_deadline):
            return "stop"
        if self.peek_index_bump(chat_id) or wake_task in done:
            return "bump" if self.peek_index_bump(chat_id) else "wake"
        return "heartbeat"

    def _slot_condition(self) -> asyncio.Condition:
        if self._slot_cond is None:
            self._slot_cond = asyncio.Condition()
        return self._slot_cond

    async def _parallel_limit(self) -> int:
        try:
            return await self.db.get_max_parallel_chats()
        except Exception:
            return max(1, int(get_settings().max_parallel_chats or 1))

    async def _acquire_slot(self) -> None:
        cond = self._slot_condition()
        async with cond:
            while self._active_slots >= await self._parallel_limit():
                await cond.wait()
            self._active_slots += 1

    async def _release_slot(self) -> None:
        cond = self._slot_condition()
        async with cond:
            self._active_slots = max(0, self._active_slots - 1)
            cond.notify_all()

    def _set_task_filters(
        self,
        task_id: int,
        *,
        file_formats: Any = None,
        min_file_bytes: int = 0,
        max_file_bytes: int = 0,
    ) -> None:
        self._task_filters[task_id] = {
            "file_formats": file_formats or [],
            "min_file_bytes": max(0, int(min_file_bytes or 0)),
            "max_file_bytes": max(0, int(max_file_bytes or 0)),
        }

    def _clear_task_filters(self, task_id: int) -> None:
        self._task_filters.pop(task_id, None)

    def _passes_file_filters(self, task_id: int, message, media_type: Optional[str]) -> bool:
        f = self._task_filters.get(task_id) or {}
        formats = f.get("file_formats") or []
        if not matches_file_formats(message, formats, media_type):
            return False
        return matches_file_size(
            self._expected_size(message),
            min_bytes=int(f.get("min_file_bytes") or 0),
            max_bytes=int(f.get("max_file_bytes") or 0),
        )

    @staticmethod
    def _formats_log_text(file_formats: Any) -> str:
        if not file_formats:
            return ""
        if isinstance(file_formats, dict):
            parts = []
            for k, v in file_formats.items():
                if not v:
                    continue
                parts.append(f"{k}:{'/'.join(v)}")
            return "; ".join(parts)
        return ", ".join(str(x) for x in file_formats)

    def get_live_progress(self, task_id: int) -> Optional[dict[str, Any]]:
        with self._progress_lock:
            p = self._progress.get(task_id)
            if not p:
                return None
            if p.get("phase") == "indexing":
                scanned = int(p.get("scanned") or 0)
                media = int(p.get("media") or 0)
                indexed_last = int(p.get("indexed_last") or 0)
                chat_latest = int(p.get("chat_latest") or 0)
                behind = int(p.get("behind") or 0)
                if chat_latest > 0:
                    percent = round(
                        min(100.0, max(0.0, 100.0 * indexed_last / chat_latest)), 1
                    )
                else:
                    percent = None
                return {
                    "phase": "indexing",
                    "file": p.get("title") or "文案索引处理中…",
                    "title": p.get("title") or "文案索引处理中…",
                    "detail": p.get("detail") or "",
                    "scanned": scanned,
                    "media": media,
                    "indexed_last": indexed_last,
                    "chat_latest": chat_latest,
                    "behind": behind,
                    "error": p.get("error") or "",
                    "received": indexed_last,
                    "total": chat_latest,
                    "speed": 0,
                    "percent": percent,
                    "active_count": 0,
                    "files": [],
                    "workers": [],
                    "worker_count": 0,
                }

            paused_phase = p.get("phase") == "paused"
            worker_count = int(p.get("worker_count") or 0)
            workers_map = p.get("workers") or {}
            files_map = p.get("files") or {}

            workers: list[dict[str, Any]] = []
            files: list[dict[str, Any]] = []
            total_speed = 0.0
            total_received = 0
            total_size = 0
            active = 0

            if worker_count > 0:
                for wid in range(1, worker_count + 1):
                    slot = workers_map.get(wid) or {}
                    status = str(slot.get("status") or "idle")
                    received = int(slot.get("received") or 0)
                    total = int(slot.get("total") or 0)
                    mid = int(slot.get("message_id") or 0)
                    speed = (
                        float(slot.get("speed") or 0)
                        if status == "busy" and not paused_phase
                        else 0.0
                    )
                    # Keep lane visible while switching to the next file
                    show_bar = status in ("busy", "paused", "switching") or (
                        status == "idle" and bool(slot.get("file"))
                    )
                    if show_bar:
                        if status in ("busy", "switching"):
                            active += 1
                            total_speed += speed
                        total_received += received
                        total_size += total
                        files.append(
                            {
                                "id": mid,
                                "worker": wid,
                                "file": slot.get("file") or "",
                                "received": received,
                                "total": total,
                                "speed": speed,
                                "percent": (
                                    round(100.0 * received / total, 1) if total else None
                                ),
                            }
                        )
                    workers.append(
                        {
                            "id": wid,
                            "status": status,
                            "message_id": mid or None,
                            "file": slot.get("file") or "",
                            "received": received,
                            "total": total,
                            "speed": speed,
                            "percent": (
                                round(100.0 * received / total, 1)
                                if total and show_bar
                                else None
                            ),
                        }
                    )
            else:
                for mid, slot in files_map.items():
                    received = int(slot.get("received") or 0)
                    total = int(slot.get("total") or 0)
                    speed = 0.0 if paused_phase else float(slot.get("speed") or 0)
                    total_speed += speed
                    total_received += received
                    total_size += total
                    files.append(
                        {
                            "id": int(mid),
                            "file": slot.get("file") or "",
                            "received": received,
                            "total": total,
                            "speed": speed,
                            "percent": (
                                round(100.0 * received / total, 1) if total else None
                            ),
                        }
                    )
                files.sort(key=lambda x: x["id"])
                active = len(files)

            # Keep showing worker slots while pool is alive / paused snapshot
            if active == 0 and worker_count <= 0 and not paused_phase:
                return None
            if paused_phase:
                summary_file = p.get("title") or "已暂停"
            elif active == 0 and worker_count > 0:
                summary_file = f"{worker_count} 路 Worker 待命"
            elif active == 1 and files:
                summary_file = files[0]["file"]
            else:
                summary_file = f"{active} 路 Worker 下载中"
            return {
                "phase": "paused" if paused_phase else "download",
                "file": summary_file,
                "received": total_received,
                "total": total_size,
                "speed": 0.0 if paused_phase else total_speed,
                "percent": (
                    round(100.0 * total_received / total_size, 1)
                    if total_size
                    else None
                ),
                "active_count": 0 if paused_phase else active,
                "worker_count": worker_count,
                "workers": workers,
                "files": files,
            }

    def _set_index_progress(
        self,
        task_id: int,
        *,
        scanned: int = 0,
        media: int = 0,
        title: str = "",
        detail: str = "",
        indexed_last: int = 0,
        chat_latest: int = 0,
        behind: int = 0,
        error: str = "",
    ) -> None:
        with self._progress_lock:
            prev = self._progress.get(task_id) or {}
            if prev.get("phase") != "indexing":
                prev = {}
            # Keep prior tip if this update omits it (0), so % bar stays stable
            tip = int(chat_latest or 0) or int(prev.get("chat_latest") or 0)
            last = int(indexed_last or 0)
            if last <= 0:
                last = int(prev.get("indexed_last") or 0)
            self._progress[task_id] = {
                "phase": "indexing",
                "scanned": int(scanned or 0),
                "media": int(media or 0),
                "title": (title or prev.get("title") or "文案索引处理中…"),
                "detail": detail if detail is not None else (prev.get("detail") or ""),
                "indexed_last": last,
                "chat_latest": tip,
                "behind": max(0, tip - last) if tip else int(behind or 0),
                "error": error if error is not None else (prev.get("error") or ""),
                "files": {},
            }

    def _clear_index_progress(self, task_id: int) -> None:
        with self._progress_lock:
            p = self._progress.get(task_id)
            if p and p.get("phase") == "indexing":
                self._progress.pop(task_id, None)

    def _ensure_worker_progress(self, task_id: int, worker_count: int) -> None:
        """Create fixed Worker 1..N idle slots for live UI."""
        n = max(1, min(5, int(worker_count or 1)))
        with self._progress_lock:
            bucket = self._progress.get(task_id)
            if bucket and bucket.get("phase") == "indexing":
                bucket = None
            # Resume from pause: keep frozen file/bars so UI does not jump
            if bucket and bucket.get("phase") == "paused":
                bucket.pop("phase", None)
                bucket["worker_count"] = max(n, int(bucket.get("worker_count") or 0))
                workers = bucket.setdefault("workers", {})
                for wid in range(1, n + 1):
                    if wid not in workers:
                        workers[wid] = {
                            "status": "idle",
                            "message_id": 0,
                            "file": "",
                            "received": 0,
                            "total": 0,
                            "speed": 0.0,
                        }
                return
            if not bucket:
                bucket = {"files": {}, "workers": {}, "worker_count": n}
                self._progress[task_id] = bucket
            bucket.pop("phase", None)
            bucket["worker_count"] = n
            workers = bucket.setdefault("workers", {})
            bucket.setdefault("files", {})
            for wid in range(1, n + 1):
                if wid not in workers:
                    workers[wid] = {
                        "status": "idle",
                        "message_id": 0,
                        "file": "",
                        "received": 0,
                        "total": 0,
                        "speed": 0.0,
                    }

    def _set_worker_idle(self, task_id: int, worker_id: int) -> None:
        if not worker_id:
            return
        with self._progress_lock:
            bucket = self._progress.get(task_id)
            if not bucket:
                return
            slot = (bucket.get("workers") or {}).get(int(worker_id))
            if not slot:
                return
            # Keep last file/bytes so the lane stays visible until the next job
            # binds — avoids progress-bar flicker between multi-worker files.
            # Do NOT inflate received→total here; pause must keep real stop %.
            if slot.get("file") or int(slot.get("received") or 0) > 0:
                if bucket.get("phase") == "paused":
                    slot["status"] = "paused"
                else:
                    slot["status"] = "switching"
            else:
                slot["status"] = "idle"
                slot["message_id"] = 0
                slot["file"] = ""
                slot["received"] = 0
                slot["total"] = 0
            slot["speed"] = 0.0

    def _begin_file_progress(
        self,
        task_id: int,
        message_id: int,
        rel_file: str,
        total: int = 0,
        *,
        worker_id: int | None = None,
    ) -> None:
        now = time.monotonic()
        with self._progress_lock:
            bucket = self._progress.setdefault(
                task_id, {"files": {}, "workers": {}, "worker_count": 0}
            )
            if bucket.get("phase") == "indexing":
                bucket = {"files": {}, "workers": {}, "worker_count": 0}
                self._progress[task_id] = bucket
            slot = {
                "file": rel_file,
                "received": 0,
                "total": int(total or 0),
                "speed": 0.0,
                "worker": int(worker_id or 0),
                "_t": now,
                "_bytes": 0,
                "_started": now,
                "_base_bytes": 0,
                "_seeded": False,
                "_speed_ready": False,
            }
            bucket.setdefault("files", {})[int(message_id)] = slot
            wid = int(worker_id or 0)
            if wid > 0:
                workers = bucket.setdefault("workers", {})
                wslot = workers.setdefault(
                    wid,
                    {
                        "status": "idle",
                        "message_id": 0,
                        "file": "",
                        "received": 0,
                        "total": 0,
                        "speed": 0.0,
                    },
                )
                wslot["status"] = "busy"
                wslot["message_id"] = int(message_id)
                wslot["file"] = rel_file
                wslot["received"] = 0
                wslot["total"] = int(total or 0)
                wslot["speed"] = 0.0
                # Mirror speed fields onto worker slot (same object refs via sync in on_bytes)
                wslot["_file_mid"] = int(message_id)

    # Speed meter: ignore the first short burst (Telethon often dumps a large
    # received jump in a few ms → hundreds of MB/s), then EMA with a hard cap.
    _SPEED_WARMUP_S = 1.0
    _SPEED_MIN_DT_S = 0.5
    _SPEED_MAX_BPS = 80.0 * 1024 * 1024  # 80 MB/s — above this is almost always a spike

    def _on_bytes_progress(
        self,
        task_id: int,
        message_id: int,
        received: int,
        total: int,
    ) -> None:
        """Update live progress. Throttled to cut CPU under pipelined downloads."""
        with self._progress_lock:
            bucket = self._progress.get(task_id)
            if not bucket:
                return
            p = (bucket.get("files") or {}).get(int(message_id))
            if not p:
                return

            def _mirror_worker() -> None:
                wid = int(p.get("worker") or 0)
                if wid <= 0:
                    return
                wslot = (bucket.get("workers") or {}).get(wid)
                if wslot is None:
                    return
                wslot["status"] = "busy"
                wslot["message_id"] = int(message_id)
                wslot["file"] = p.get("file") or wslot.get("file") or ""
                wslot["received"] = int(p.get("received") or 0)
                wslot["total"] = int(p.get("total") or 0)
                wslot["speed"] = float(p.get("speed") or 0)

            now = time.monotonic()
            recv = int(received or 0)
            # Always remember latest bytes for resume/UI, but skip heavy updates
            last_ui = float(p.get("_ui_t") or 0)
            last_ui_bytes = int(p.get("_ui_bytes") or 0)
            byte_delta = recv - last_ui_bytes
            if (
                last_ui
                and (now - last_ui) < 0.35
                and byte_delta < 256 * 1024
                and recv < int(p.get("total") or total or 0)
            ):
                p["_pending_recv"] = recv
                if total:
                    p["_pending_total"] = int(total)
                return

            prev_bytes = int(p.get("_bytes") or 0)
            if "_pending_recv" in p:
                # Pending was only for UI coalesce — don't use it as the speed baseline
                # or a burst of buffered callbacks looks like a huge instant spike.
                p.pop("_pending_recv", None)
            if "_pending_total" in p:
                total = int(p.pop("_pending_total") or total or 0)

            started = float(p.get("_started") or now)
            # First sample often arrives with a big recv and tiny dt — seed baseline
            # without computing speed yet (avoids the "几百 MB/s" flash).
            if not p.get("_speed_ready"):
                if not p.get("_seeded"):
                    p["_t"] = now
                    p["_bytes"] = recv
                    p["_base_bytes"] = recv  # exclude resume / already-on-disk bytes
                    p["_seeded"] = True
                    p["speed"] = 0.0
                    p["received"] = recv
                    if total:
                        p["total"] = int(total)
                    p["_ui_t"] = now
                    p["_ui_bytes"] = recv
                    _mirror_worker()
                    return

                elapsed = now - started
                sample_dt = now - float(p.get("_t") or started)
                if elapsed < self._SPEED_WARMUP_S or sample_dt < self._SPEED_MIN_DT_S:
                    p["speed"] = 0.0
                    p["received"] = recv
                    if total:
                        p["total"] = int(total)
                    p["_ui_t"] = now
                    p["_ui_bytes"] = recv
                    _mirror_worker()
                    return

                base = int(p.get("_base_bytes") or 0)
                full_dt = max(self._SPEED_MIN_DT_S, now - started)
                full_delta = max(0, recv - base)
                first = full_delta / full_dt
                p["speed"] = min(first, self._SPEED_MAX_BPS)
                p["_speed_ready"] = True
                p["_t"] = now
                p["_bytes"] = recv
                p["received"] = recv
                if total:
                    p["total"] = int(total)
                p["_ui_t"] = now
                p["_ui_bytes"] = recv
                _mirror_worker()
                return

            dt = now - float(p.get("_t") or now)
            delta = recv - prev_bytes
            if dt >= self._SPEED_MIN_DT_S and delta >= 0:
                inst = (delta / dt) if dt > 0 else 0.0
                # Cap absurd spikes (connection pool catch-up / callback bunching)
                if inst > self._SPEED_MAX_BPS:
                    inst = self._SPEED_MAX_BPS
                prev = float(p.get("speed") or 0)
                if prev <= 0:
                    p["speed"] = inst
                else:
                    # Smooth toward new rate; slightly heavier on history to avoid jitter
                    p["speed"] = prev * 0.82 + inst * 0.18
                p["_t"] = now
                p["_bytes"] = recv
            p["received"] = recv
            if total:
                p["total"] = int(total)
            p["_ui_t"] = now
            p["_ui_bytes"] = recv
            _mirror_worker()

    def _finish_file_progress(
        self,
        task_id: int,
        message_id: int,
        *,
        received: int = 0,
        total: int = 0,
        remove: bool = True,
        worker_id: int | None = None,
        completed: bool = False,
    ) -> None:
        with self._progress_lock:
            bucket = self._progress.get(task_id)
            if not bucket:
                return
            files = bucket.get("files") or {}
            p = files.get(int(message_id))
            wid = int(worker_id or 0) or int((p or {}).get("worker") or 0)
            if p:
                if received or total:
                    p["received"] = int(received or p.get("received") or 0)
                    if total:
                        p["total"] = int(total)
                if remove:
                    files.pop(int(message_id), None)
            if wid > 0:
                wslot = (bucket.get("workers") or {}).get(wid)
                if wslot is not None:
                    last_file = wslot.get("file") or (p or {}).get("file") or ""
                    last_total = int(
                        total
                        or wslot.get("total")
                        or (p or {}).get("total")
                        or 0
                    )
                    # Keep real bytes unless the file actually finished
                    if int(received or 0) > 0:
                        last_recv = int(received)
                    else:
                        last_recv = int(
                            wslot.get("received")
                            or (p or {}).get("received")
                            or 0
                        )
                    if completed and last_total > 0:
                        last_recv = last_total
                    paused = bucket.get("phase") == "paused"
                    if last_file or last_recv > 0:
                        wslot["status"] = "paused" if paused else "switching"
                        wslot["file"] = last_file
                        if last_total > 0:
                            wslot["total"] = last_total
                        wslot["received"] = last_recv
                    else:
                        wslot["status"] = "idle"
                        wslot["message_id"] = 0
                        wslot["file"] = ""
                        wslot["received"] = 0
                        wslot["total"] = 0
                    wslot["speed"] = 0.0
            if remove and not files and not int(bucket.get("worker_count") or 0):
                self._progress.pop(task_id, None)

    def _clear_progress(self, task_id: int) -> None:
        with self._progress_lock:
            self._progress.pop(task_id, None)

    def _freeze_progress_paused(self, task_id: int) -> None:
        """Keep the live progress box after pause (frozen bars, no speed)."""
        with self._progress_lock:
            bucket = self._progress.get(task_id)
            if not bucket or bucket.get("phase") == "indexing":
                self._progress[task_id] = {
                    "phase": "paused",
                    "title": "已暂停",
                    "worker_count": 0,
                    "workers": {},
                    "files": {},
                }
                return
            bucket["phase"] = "paused"
            for slot in (bucket.get("workers") or {}).values():
                st = str(slot.get("status") or "")
                if st in ("busy", "switching", "paused") or slot.get("file"):
                    slot["status"] = "paused"
                    # Never rewrite received/total — freeze true stop-time %
                else:
                    slot["status"] = "idle"
                slot["speed"] = 0.0
            for slot in (bucket.get("files") or {}).values():
                slot["speed"] = 0.0

    async def _watch_part_file(
        self,
        task_id: int,
        message_id: int,
        part_path: Path,
        stop: asyncio.Event,
        total: int,
    ) -> None:
        """Fallback .part size poll when Telethon progress callbacks go silent."""
        try:
            # Give pipelined callbacks a head start — avoid double-updating
            await asyncio.sleep(2.0)
            while not stop.is_set():
                # Skip if callback already refreshed recently
                with self._progress_lock:
                    bucket = self._progress.get(task_id) or {}
                    p = (bucket.get("files") or {}).get(int(message_id)) or {}
                    last_ui = float(p.get("_ui_t") or 0)
                if last_ui and (time.monotonic() - last_ui) < 1.2:
                    await asyncio.sleep(1.2)
                    continue
                if part_path.exists():
                    try:
                        size = part_path.stat().st_size
                    except OSError:
                        size = 0
                    self._on_bytes_progress(task_id, message_id, size, total)
                await asyncio.sleep(1.2)
        except asyncio.CancelledError:
            return

    def is_worker_alive(self, task_id: int) -> bool:
        existing = self._tasks.get(int(task_id))
        return bool(existing and not existing.done())

    async def heal_stale_running(
        self, task_id: int | None = None, *, quiet: bool = False
    ) -> list[int]:
        """Mark DB 'running' as paused when the worker is already gone."""
        healed: list[int] = []
        tasks = await self.db.list_tasks()
        for t in tasks:
            tid = int(t["id"])
            if task_id is not None and tid != int(task_id):
                continue
            if t.get("status") == "running" and not self.is_worker_alive(tid):
                await self.db.update_task(tid, status="paused")
                if not quiet:
                    await self.db.append_log(
                        tid, "检测到任务状态卡住，已恢复为暂停，可点继续"
                    )
                healed.append(tid)
        return healed

    async def _stop_worker(
        self,
        task_id: int,
        *,
        grace_s: float = 1.5,
        cancel_s: float = 1.0,
        detach: bool = True,
    ) -> None:
        """Ask a worker to stop; never block longer than grace+cancel.

        Telethon downloads often ignore CancelledError until the current chunk
        finishes — awaiting them without a timeout freezes「继续」and hot-reload.
        """
        task_id = int(task_id)
        flag = self._stop_flags.get(task_id)
        worker = self._tasks.get(task_id)
        if flag:
            flag.set()
        if worker and not worker.done():
            try:
                await asyncio.wait_for(asyncio.shield(worker), timeout=grace_s)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                worker.cancel()
                try:
                    await asyncio.wait_for(worker, timeout=cancel_s)
                except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                    logger.warning(
                        "task %s worker still running after stop timeout — detaching",
                        task_id,
                    )
        if detach:
            # Bump generation so a late finally on the old worker cannot clear
            # a newly started replacement.
            self._worker_gen[task_id] = int(self._worker_gen.get(task_id) or 0) + 1
            cur = self._tasks.get(task_id)
            if cur is worker:
                self._tasks.pop(task_id, None)
            fl = self._stop_flags.get(task_id)
            if fl is flag:
                self._stop_flags.pop(task_id, None)
            # Keep frozen progress box after user pause
            self._freeze_progress_paused(task_id)

    async def start_task(self, task_id: int) -> None:
        """Start or force-restart a task worker."""
        task_id = int(task_id)
        self._cancel_failed_retry(task_id)
        # Stop any live worker first (bounded wait — must not hang HTTP).
        if self.is_worker_alive(task_id):
            await self._stop_worker(task_id, grace_s=1.5, cancel_s=1.0, detach=True)

        async with self._lock:
            if self.is_worker_alive(task_id):
                await self._stop_worker(task_id, grace_s=0.2, cancel_s=0.5, detach=True)
            stop_event = asyncio.Event()
            gen = int(self._worker_gen.get(task_id) or 0) + 1
            self._worker_gen[task_id] = gen
            self._stop_flags[task_id] = stop_event
            self._tasks[task_id] = asyncio.create_task(
                self._run_task(task_id, stop_event, gen)
            )

    async def pause_task(self, task_id: int) -> None:
        flag = self._stop_flags.get(task_id)
        if flag:
            flag.set()
        # Freeze live box immediately so pause/resume UI stays on the same panel
        self._freeze_progress_paused(int(task_id))
        self._cancel_failed_retry(int(task_id))
        await self.db.update_task(task_id, status="paused")

    async def cancel_and_wait(self, task_id: int, timeout: float = 120) -> None:
        """Pause a running worker and wait until it exits before deleting."""
        # Cap wait so delete API cannot hang forever on a stuck download.
        await self._stop_worker(
            task_id,
            grace_s=min(8.0, max(1.0, float(timeout))),
            cancel_s=2.0,
            detach=True,
        )

    async def stop_all(self) -> None:
        """Shutdown helper — must return quickly so uvicorn reload cannot wedge."""
        for _tid, flag in list(self._stop_flags.items()):
            flag.set()
        tasks = [t for t in self._tasks.values() if not t.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=3.0,
                )
            except (asyncio.TimeoutError, Exception):
                logger.warning(
                    "stop_all: workers did not finish within 3s — continuing shutdown"
                )
        self._tasks.clear()
        self._stop_flags.clear()
        self._worker_gen.clear()
        for h in list(self._failed_retry_handles.values()):
            if h and not h.done():
                h.cancel()
        self._failed_retry_handles.clear()
        # Reset slots so a wedged worker cannot poison shutdown bookkeeping
        self._active_slots = 0

    async def resume_running_on_startup(self) -> None:
        """Park formerly-running tasks; they auto-start after Telegram reconnects."""
        tasks = await self.db.list_tasks()
        resume_ids: list[int] = []
        for t in tasks:
            if t["status"] == "running":
                tid = int(t["id"])
                resume_ids.append(tid)
                await self.db.update_task(tid, status="paused")
                await self.db.append_log(
                    tid, "服务重启，等待 Telegram 连接后自动恢复…"
                )
        self._startup_resume_ids = resume_ids

    async def auto_resume_after_telegram(self) -> int:
        """Start tasks interrupted by process restart once Telegram is ready."""
        ids = list(self._startup_resume_ids or [])
        self._startup_resume_ids = []
        started = 0
        for tid in ids:
            try:
                t = await self.db.get_task(tid)
                if not t:
                    continue
                # User may have deleted / already continued / completed
                if str(t.get("status") or "") not in ("paused", "pending"):
                    continue
                if self.is_worker_alive(tid):
                    continue
                await self.db.append_log(tid, "Telegram 已连接，自动恢复任务")
                await self.start_task(tid)
                started += 1
            except Exception:
                logger.exception("auto-resume task %s failed", tid)
                try:
                    await self.db.append_log(
                        tid, "自动恢复失败，请手动点继续"
                    )
                except Exception:
                    pass
        return started

    def abandon_startup_resume(self, reason: str = "") -> None:
        """If Telegram cannot connect, leave parked tasks for manual continue."""
        ids = list(self._startup_resume_ids or [])
        self._startup_resume_ids = []
        if not ids:
            return

        async def _note() -> None:
            msg = "Telegram 未能自动连接，任务保持暂停，请连接后点继续"
            if reason:
                msg = f"{msg}（{reason}）"
            for tid in ids:
                try:
                    await self.db.append_log(tid, msg)
                except Exception:
                    pass

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_note())
        except RuntimeError:
            pass

    def _schedule_failed_retry(self, task_id: int) -> None:
        """After sequential finish with failures, retry once the cooldown elapses."""
        task_id = int(task_id)
        old = self._failed_retry_handles.pop(task_id, None)
        if old and not old.done():
            old.cancel()

        async def _run() -> None:
            try:
                sec = float(await self.db.get_failed_retry_interval_sec())
            except Exception:
                sec = float(get_settings().failed_retry_interval_sec or 900)
            sec = max(120.0, sec)
            try:
                await asyncio.sleep(sec)
            except asyncio.CancelledError:
                return
            try:
                if self.is_worker_alive(task_id):
                    return
                t = await self.db.get_task(task_id)
                if not t or str(t.get("status") or "") != "paused":
                    return
                failed = int(await self.db.count_failed(task_id) or 0)
                if failed <= 0:
                    return
                await self.db.append_log(
                    task_id, f"定时自动重试 {failed} 条失败项"
                )
                await self.start_task(task_id)
            except Exception:
                logger.exception("delayed failed-retry task %s", task_id)
            finally:
                cur = self._failed_retry_handles.get(task_id)
                if cur is asyncio.current_task():
                    self._failed_retry_handles.pop(task_id, None)

        try:
            self._failed_retry_handles[task_id] = asyncio.create_task(
                _run(), name=f"failed-retry-{task_id}"
            )
        except RuntimeError:
            pass

    def _cancel_failed_retry(self, task_id: int) -> None:
        old = self._failed_retry_handles.pop(int(task_id), None)
        if old and not old.done():
            old.cancel()

    async def _run_task(
        self, task_id: int, stop_event: asyncio.Event, gen: int
    ) -> None:
        me = asyncio.current_task()
        await self._acquire_slot()
        try:
            # Superseded while waiting for a parallel slot
            if int(self._worker_gen.get(task_id) or 0) != int(gen):
                return
            try:
                await self._download_loop(task_id, stop_event)
            except asyncio.CancelledError:
                # Hot-reload / process stop cancels the worker; avoid zombie "running"
                try:
                    if int(self._worker_gen.get(task_id) or 0) == int(gen):
                        task = await self.db.get_task(task_id)
                        if task and task.get("status") == "running":
                            await self.db.update_task(task_id, status="paused")
                            await self.db.append_log(
                                task_id, "任务被中断（服务重载或停止），请点继续"
                            )
                except Exception:
                    logger.exception("failed to mark task paused after cancel")
                raise
            except Exception as e:
                logger.exception("task %s failed", task_id)
                if int(self._worker_gen.get(task_id) or 0) == int(gen):
                    await self.db.update_task(
                        task_id, status="failed", last_error=str(e)
                    )
                    await self.db.append_log(task_id, f"任务失败: {e}")
                    try:
                        failed_task = await self.db.get_task(task_id)
                        await notify_event(
                            "task_failed",
                            task_id=task_id,
                            title=str((failed_task or {}).get("chat_title") or ""),
                            message=str(e),
                        )
                    except Exception:
                        logger.debug("task_failed notify failed", exc_info=True)
            finally:
                # Only the current generation may clear registry / UI state
                if int(self._worker_gen.get(task_id) or 0) == int(gen):
                    try:
                        st = await self.db.get_task(task_id)
                    except Exception:
                        st = None
                    if st and str(st.get("status") or "") == "paused":
                        self._freeze_progress_paused(task_id)
                    else:
                        self._clear_progress(task_id)
                    self._clear_task_filters(task_id)
                    if self._tasks.get(task_id) is me:
                        self._tasks.pop(task_id, None)
                    if self._stop_flags.get(task_id) is stop_event:
                        self._stop_flags.pop(task_id, None)
        finally:
            await self._release_slot()

    async def _wait_flood(
        self, task_id: int, seconds: int, stop_event: asyncio.Event
    ) -> bool:
        """Wait for FloodWait; return False only if user paused during wait."""
        wait_s = max(1, int(seconds) + 1)
        try:
            max_wait = max(60, int(await self.db.get_max_flood_wait()))
        except Exception:
            max_wait = max(60, int(get_settings().max_flood_wait or 1800))
        capped = False
        if wait_s > max_wait:
            capped = True
            await self.db.append_log(
                task_id,
                f"限流需等待 {wait_s}s，先自动等待上限 {max_wait}s 后重试",
            )
            wait_s = max_wait

        await self.db.append_log(
            task_id,
            f"触发限流，自动等待 {wait_s}s 后继续"
            + ("（已封顶）" if capped else ""),
        )
        await self.db.update_task(task_id, last_error=f"FloodWait {wait_s}s，自动等待中")
        task = await self.db.get_task(task_id)
        await notify_event(
            "flood_wait",
            task_id=task_id,
            title=str((task or {}).get("chat_title") or ""),
            message=f"FloodWait {wait_s}s，自动等待中",
            extra={"seconds": wait_s, "paused": False, "capped": capped},
        )
        remaining = wait_s
        while remaining > 0:
            if stop_event.is_set():
                return False
            step = min(5, remaining)
            await asyncio.sleep(step)
            remaining -= step
        await self.db.update_task(task_id, last_error=None)
        await self.db.append_log(task_id, "限流等待结束，继续下载")
        return True

    async def _download_loop(self, task_id: int, stop_event: asyncio.Event) -> None:
        settings = get_settings()
        task = await self.db.get_task(task_id)
        if not task:
            return

        await self.db.update_task(task_id, status="running", last_error=None)

        client = await tg_manager.ensure_client()
        if not await client.is_user_authorized():
            await self.db.update_task(task_id, status="failed", last_error="未登录 Telegram")
            await self.db.append_log(task_id, "未登录 Telegram")
            return

        chat_id = int(task["chat_id"])
        try:
            title = task["chat_title"] or await tg_manager.get_chat_title(chat_id)
        except Exception:
            title = task["chat_title"] or str(chat_id)
        await self.db.update_task(task_id, chat_title=title)

        group_dir = settings.download_dir / sanitize_name(title)
        group_dir.mkdir(parents=True, exist_ok=True)

        media_types = set(task["media_types"] or [])
        use_text_as_folder = bool(task["use_text_as_folder"])
        min_len = int(task["min_folder_title_len"] or settings.min_folder_title_len)
        current_folder: Optional[str] = task.get("current_folder")
        album_captions: dict = {}
        last_id = int(task.get("last_message_id") or 0)
        start_id = int(task.get("start_message_id") or 0)
        end_id = task.get("end_message_id")

        start_date = _parse_date(task.get("start_date"))
        end_date = _parse_date(task.get("end_date"))

        processed = int(task.get("processed_count") or 0)
        downloaded = int(task.get("downloaded_count") or 0)
        failed = int(task.get("failed_count") or 0)
        skipped = int(task.get("skipped_count") or 0)

        download_order = (task.get("download_order") or "added_first").strip()
        if download_order not in ("added_first", "oldest_first", "newest_first"):
            download_order = "added_first"
        # sequential Telethon scan: added_first ≈ oldest→newest in chat
        newest_first = download_order == "newest_first"
        index_order_by = "id" if download_order == "added_first" else "message_id"
        order_label = {
            "added_first": "先入库先下载",
            "oldest_first": "从旧到新（按消息）",
            "newest_first": "从最近往前",
        }.get(download_order, download_order)

        test_mode = bool(task.get("test_mode"))
        test_deadline = None
        if test_mode:
            dur = float(settings.test_duration_sec or 10)
            test_deadline = asyncio.get_event_loop().time() + max(1.0, dur)

        # Official help.getAppConfig: large_queue=2, small_queue=5
        large_cap = int(getattr(settings, "large_file_concurrency", 2) or 2)
        small_cap = int(getattr(settings, "small_file_concurrency", 5) or 5)
        concurrency = max(1, min(small_cap, int(task.get("concurrency") or large_cap)))
        file_formats = task.get("file_formats") or []
        min_file_bytes = max(0, int(task.get("min_file_bytes") or 0))
        max_file_bytes = max(0, int(task.get("max_file_bytes") or 0))
        self._set_task_filters(
            task_id,
            file_formats=file_formats,
            min_file_bytes=min_file_bytes,
            max_file_bytes=max_file_bytes,
        )
        max_messages = task.get("max_messages")
        if max_messages is not None:
            try:
                max_messages = int(max_messages)
                if max_messages <= 0:
                    max_messages = None
            except (TypeError, ValueError):
                max_messages = None
        delay_min = float(task.get("delay_min") if task.get("delay_min") is not None else settings.download_delay)
        delay_max = float(task.get("delay_max") if task.get("delay_max") is not None else delay_min)
        if delay_max < delay_min:
            delay_max = delay_min
        folder_mode = task.get("folder_mode") or ("caption" if use_text_as_folder else "flat")
        include_tags = normalize_tag_list(task.get("include_tags") or [])
        caption_keywords = normalize_keyword_list(task.get("caption_keywords") or [])
        tag_blacklist = await self.db.get_tag_relation_blacklist()
        # Tag filter is always OR: hit any selected tag → download
        tag_match_mode = "any"
        # Cap ≤8; safe operating range is 2–3 (see 运行设置 → 媒体连接数)
        from app import runtime_tune

        media_conn = runtime_tune.media_connections()
        try:
            from app.telegram_client import install_media_connection_pool

            install_media_connection_pool(client, pool_size=media_conn)
        except Exception:
            logger.debug("media pool resize failed", exc_info=True)
        download_mode = normalize_download_mode(task.get("download_mode"))
        mode_labels = {
            "sequential": "按时间顺序",
            "monitor": "监控模式",
        }
        # One startup line — progress UI covers the rest (no per-setting spam)
        is_resume = bool(processed or downloaded or last_id)
        verb = "继续下载" if is_resume else "开始下载"
        bits = [
            verb,
            mode_labels.get(download_mode, download_mode),
            order_label,
            f"并发{concurrency}",
        ]
        if test_mode:
            bits.append(f"测试{float(settings.test_duration_sec or 10):.0f}s")
        if include_tags:
            bits.append(f"{len(include_tags)}个标签")
        if max_messages:
            bits.append(f"上限{max_messages}")
        await self.db.append_log(task_id, " · ".join(bits))

        try:
            if download_mode == "monitor":
                await self._download_by_index_body(
                    task_id=task_id,
                    stop_event=stop_event,
                    settings=settings,
                    client=client,
                    chat_id=chat_id,
                    chat_title=title,
                    group_dir=group_dir,
                    media_types=media_types,
                    use_text_as_folder=use_text_as_folder,
                    current_folder=current_folder,
                    album_captions=album_captions,
                    processed=processed,
                    downloaded=downloaded,
                    failed=failed,
                    skipped=skipped,
                    newest_first=newest_first,
                    index_order_by=index_order_by,
                    test_mode=test_mode,
                    test_deadline=test_deadline,
                    concurrency=concurrency,
                    file_formats=file_formats,
                    max_messages=max_messages,
                    delay_min=delay_min,
                    delay_max=delay_max,
                    folder_mode=folder_mode,
                    include_tags=include_tags,
                    caption_keywords=caption_keywords,
                    tag_match_mode=tag_match_mode,
                    tag_blacklist=tag_blacklist,
                )
            else:
                await self._download_loop_body(
                    task_id=task_id,
                    stop_event=stop_event,
                    settings=settings,
                    client=client,
                    chat_id=chat_id,
                    group_dir=group_dir,
                    media_types=media_types,
                    use_text_as_folder=use_text_as_folder,
                    min_len=min_len,
                    current_folder=current_folder,
                    album_captions=album_captions,
                    last_id=last_id,
                    start_id=start_id,
                    end_id=end_id,
                    start_date=start_date,
                    end_date=end_date,
                    processed=processed,
                    downloaded=downloaded,
                    failed=failed,
                    skipped=skipped,
                    newest_first=newest_first,
                    test_mode=test_mode,
                    test_deadline=test_deadline,
                    concurrency=concurrency,
                    file_formats=file_formats,
                    max_messages=max_messages,
                    delay_min=delay_min,
                    delay_max=delay_max,
                    folder_mode=folder_mode,
                    include_tags=include_tags,
                    caption_keywords=caption_keywords,
                    tag_match_mode=tag_match_mode,
                    tag_blacklist=tag_blacklist,
                )
        finally:
            try:
                done = await self.db.get_task(task_id)
            except Exception:
                done = None
            if done and str(done.get("status") or "") == "paused":
                self._freeze_progress_paused(task_id)
            else:
                self._clear_progress(task_id)
            if use_text_as_folder:
                await self._merge_tag_folders_after(
                    task_id,
                    group_dir,
                    chat_id=chat_id,
                    tag_blacklist=tag_blacklist,
                )
            try:
                done = await self.db.get_task(task_id)
                if done and done.get("status") == "completed":
                    await notify_event(
                        "task_completed",
                        task_id=task_id,
                        title=str(done.get("chat_title") or ""),
                        message=(
                            f"下载完成：成功 {done.get('downloaded_count') or 0}，"
                            f"失败 {done.get('failed_count') or 0}"
                        ),
                        extra={
                            "downloaded": done.get("downloaded_count"),
                            "failed": done.get("failed_count"),
                            "skipped": done.get("skipped_count"),
                        },
                    )
            except Exception:
                logger.debug("task_completed notify failed", exc_info=True)

    async def _ensure_caption(
        self,
        client,
        chat_id: int,
        message,
        album_captions: dict,
        caption_override: Optional[str] = None,
    ) -> str:
        """Read message 文案; for albums, pull caption from sibling messages if needed."""
        text = resolve_caption_text(
            message,
            album_captions=album_captions,
            caption_override=caption_override,
        )
        if text:
            return text

        grouped_id = getattr(message, "grouped_id", None)
        if not grouped_id:
            return ""

        try:
            around = list(range(max(1, message.id - 12), message.id + 13))
            msgs = await asyncio.wait_for(
                client.get_messages(chat_id, ids=around),
                timeout=20,
            )
            if not isinstance(msgs, list):
                msgs = [msgs]
            for m in msgs:
                if not m or getattr(m, "grouped_id", None) != grouped_id:
                    continue
                t = message_text(m)
                if t:
                    album_captions[grouped_id] = t
                    return t
        except Exception:
            logger.debug("album caption lookup failed", exc_info=True)
        return ""

    async def _download_by_index_body(
        self,
        *,
        task_id: int,
        stop_event: asyncio.Event,
        settings,
        client,
        chat_id: int,
        chat_title: str,
        group_dir: Path,
        media_types: set[str],
        use_text_as_folder: bool,
        current_folder: Optional[str],
        album_captions: dict,
        processed: int,
        downloaded: int,
        failed: int,
        skipped: int,
        newest_first: bool,
        index_order_by: str = "id",
        test_mode: bool = False,
        test_deadline: float | None = None,
        concurrency: int = 1,
        file_formats: list | None = None,
        max_messages: int | None = None,
        delay_min: float = 0.5,
        delay_max: float = 0.5,
        folder_mode: str = "caption",
        include_tags: list | None = None,
        caption_keywords: list | None = None,
        tag_match_mode: str = "any",
        tag_blacklist: frozenset | set | None = None,
    ) -> None:
        """Download media by querying the chat caption index (scan-once reuse)."""
        file_formats = file_formats or []
        include_tags = include_tags or []
        caption_keywords = caption_keywords or []
        if tag_blacklist is None:
            tag_blacklist = await self.db.get_tag_relation_blacklist()
        tag_folder_map = {}
        if use_text_as_folder:
            tag_folder_map = await self._load_tag_folder_map(
                task_id, chat_id, group_dir, folder_mode=folder_mode
            )
        counters = {
            "processed": processed,
            "downloaded": downloaded,
            "failed": failed,
            "skipped": skipped,
            "current_folder": current_folder,
            "last_id": 0,
            "tag_folder_map": tag_folder_map,
            "tag_blacklist": tag_blacklist,
            "chat_id": chat_id,
        }
        # Filename→message_id local sync only seeds the queue; skip uses queue/DB
        try:
            n_local = await self.db.sync_local_completed_from_dir(chat_id, group_dir)
            if n_local:
                await self.db.append_log(
                    task_id, f"本地文件写入队列已处理 {n_local} 条"
                )
        except Exception:
            logger.debug("sync_local_completed_from_dir failed", exc_info=True)
        await self._log_queue_skip_stats(
            task_id,
            chat_id,
            include_tags=include_tags,
            caption_keywords=caption_keywords,
            tag_match_mode=tag_match_mode,
            media_types=media_types,
        )

        # Empty tags/keywords allowed: build index then index-only monitor;
        # downloads start when tags are added in task settings.

        # Monitor uses the local caption index only. Index growth is handled by
        # settings auto/manual incremental scans — do not scan Telegram here.
        media_count = await self.db.count_media_index(chat_id)
        await self.db.append_log(task_id, f"本地索引 {media_count} 条")

        # Reload filters (user may edit tags on the tasks page while indexing)
        fresh = await self.db.get_task(task_id) or {}
        include_tags = normalize_tag_list(fresh.get("include_tags") or [])
        caption_keywords = normalize_keyword_list(fresh.get("caption_keywords") or [])
        tag_match_mode = "any"
        if not include_tags and not caption_keywords:
            # Index-only: no tags yet — wait for settings; download after tags are set
            await self.db.append_log(task_id, "未选标签，等待配置")
            meta = await self.db.get_index_meta(chat_id) or {}
            last_seen = max(
                int(meta.get("last_message_id") or 0),
                int(counters.get("last_id") or 0),
            )
            reason = await self._monitor_tagged_messages(
                task_id=task_id,
                stop_event=stop_event,
                settings=settings,
                client=client,
                chat_id=chat_id,
                group_dir=group_dir,
                media_types=media_types,
                use_text_as_folder=use_text_as_folder,
                album_captions=album_captions,
                counters=counters,
                last_seen=last_seen,
                concurrency=concurrency,
                file_formats=file_formats,
                max_messages=max_messages,
                delay_min=delay_min,
                delay_max=delay_max,
                folder_mode=folder_mode,
                include_tags=[],
                caption_keywords=[],
                tag_match_mode=tag_match_mode,
                test_mode=test_mode,
                test_deadline=test_deadline,
                newest_first=newest_first,
                index_order_by=index_order_by,
            )
            if reason not in ("filters_ready", "gap_found") or stop_event.is_set():
                return
            fresh = await self.db.get_task(task_id) or {}
            include_tags = normalize_tag_list(fresh.get("include_tags") or [])
            caption_keywords = normalize_keyword_list(
                fresh.get("caption_keywords") or []
            )
            if not include_tags and not caption_keywords and reason != "gap_found":
                return
            await self.db.append_log(task_id, "标签已配置，开始补下")
            # Fall through to tagged backlog + monitor
        # Expand with direct + indirect related tags from caption co-occurrence
        expanded_tags = await self.db.expand_related_tags(chat_id, include_tags)
        if len(expanded_tags) > len(include_tags):
            extra = [t for t in expanded_tags if t not in include_tags]
            await self.db.append_log(
                task_id,
                "已自动关联相关标签: "
                + " ".join(f"#{t}" for t in extra[:20])
                + (" …" if len(extra) > 20 else ""),
            )
            # Related expansion is OR-semantics
            include_tags = expanded_tags

        # Per-tag backlog: finish one tag completely before the next.
        # Order within a tag follows index_order_by (default: 先入库先下载).
        tag_passes: list[str | None] = (
            list(include_tags) if include_tags else [None]
        )
        per_tag_ids: list[tuple[str | None, list[int]]] = []
        seen_ids: set[int] = set()
        for tag in tag_passes:
            ids = await self.db.list_index_message_ids(
                chat_id,
                tags=[tag] if tag else None,
                tag_match_mode=tag_match_mode,
                keywords=caption_keywords,
                newest_first=newest_first,
                order_by=index_order_by,
            )
            ids = [mid for mid in ids if mid not in seen_ids]
            for mid in ids:
                seen_ids.add(mid)
            per_tag_ids.append((tag, ids))
        message_ids = [mid for _, ids in per_tag_ids for mid in ids]
        tag_n = len([t for t, _ in per_tag_ids if t])
        await self.db.append_log(
            task_id,
            (
                f"索引命中 {len(message_ids)} 条 · 按 {tag_n} 个标签依次下载"
                if include_tags
                else f"索引命中 {len(message_ids)} 条 · 先补历史再监控"
            ),
        )

        # Retry failed first (same as sequential mode)
        ok = await self._retry_failed_messages(
            task_id=task_id,
            chat_id=chat_id,
            group_dir=group_dir,
            media_types=media_types,
            current_folder=current_folder,
            use_text_as_folder=use_text_as_folder,
            min_len=2,
            album_captions=album_captions,
            stop_event=stop_event,
            counters=counters,
            test_mode=test_mode,
            test_deadline=test_deadline,
            concurrency=concurrency,
            file_formats=file_formats,
            folder_mode=folder_mode,
            delay_min=delay_min,
            delay_max=delay_max,
            max_messages=max_messages,
            include_tags=include_tags,
            caption_keywords=caption_keywords,
            tag_match_mode=tag_match_mode,
        )
        processed = int(counters["processed"])
        downloaded = int(counters["downloaded"])
        failed = await self.db.count_failed(task_id)
        skipped = int(counters["skipped"])
        current_folder = counters.get("current_folder") or current_folder
        if not ok:
            await self.db.update_task(
                task_id,
                status="paused",
                current_folder=current_folder,
                processed_count=processed,
                downloaded_count=downloaded,
                failed_count=failed,
                skipped_count=skipped,
            )
            return

        if max_messages and downloaded >= max_messages:
            await self.db.update_task(
                task_id,
                status="completed",
                current_folder=current_folder,
                processed_count=processed,
                downloaded_count=downloaded,
                failed_count=failed,
                skipped_count=skipped,
            )
            await self.db.append_log(
                task_id, f"已达上限 {max_messages} 个文件，任务完成"
            )
            return

        pool = self._new_download_pool(concurrency)

        async def _pause_and_return(reason: str) -> None:
            if self._pool_busy(pool):
                await self._pool_drain(
                    pool,
                    task_id=task_id,
                    settings=settings,
                    group_dir=group_dir,
                    use_text_as_folder=use_text_as_folder,
                    counters=counters,
                    test_mode=test_mode,
                )
            await self.db.update_task(
                task_id,
                status="paused",
                current_folder=counters.get("current_folder") or current_folder,
                last_message_id=int(counters.get("last_id") or 0),
                processed_count=int(counters["processed"]),
                downloaded_count=int(counters["downloaded"]),
                failed_count=await self.db.count_failed(task_id),
                skipped_count=int(counters["skipped"]),
            )
            # Skip generic pause — file path already logged「已暂停，保留进度」
            if reason and reason not in ("任务已暂停", "监控已暂停"):
                await self.db.append_log(task_id, reason)

        for tag, tag_message_ids in per_tag_ids:
            if not tag_message_ids:
                continue
            if tag:
                await self.db.append_log(
                    task_id,
                    f"下载标签 #{tag} · {len(tag_message_ids)} 条",
                )

            for chunk in _chunked(tag_message_ids, 80):
                if stop_event.is_set() or self._test_time_up(test_deadline):
                    reason = (
                        "测试模式时间到，已停止（未下完整文件）"
                        if self._test_time_up(test_deadline) and not stop_event.is_set()
                        else "任务已暂停"
                    )
                    await _pause_and_return(reason)
                    return

                captions_map = await self.db.get_index_captions(chat_id, chunk)

                while True:
                    if stop_event.is_set():
                        await _pause_and_return("任务已暂停")
                        return
                    try:
                        messages = await client.get_messages(chat_id, ids=chunk)
                        break
                    except FloodWaitError as e:
                        if not await self._wait_flood(task_id, e.seconds, stop_event):
                            await _pause_and_return("任务已暂停")
                            return

                if not isinstance(messages, list):
                    messages = [messages]

                # Preserve order of chunk (get_messages may reorder)
                by_id = {m.id: m for m in messages if m}
                ordered = [by_id[i] for i in chunk if i in by_id]

                for message in ordered:
                    if self._pool_busy(pool) and await self._pool_reap(
                        pool,
                        task_id=task_id,
                        settings=settings,
                        group_dir=group_dir,
                        use_text_as_folder=use_text_as_folder,
                        counters=counters,
                        test_mode=test_mode,
                        wait=False,
                    ):
                        await _pause_and_return("任务已暂停")
                        return

                    if stop_event.is_set() or self._test_time_up(test_deadline):
                        reason = (
                            "测试模式时间到，已停止（未下完整文件）"
                            if self._test_time_up(test_deadline) and not stop_event.is_set()
                            else "任务已暂停"
                        )
                        await _pause_and_return(reason)
                        return

                    if max_messages and int(counters["downloaded"]) >= max_messages:
                        if self._pool_busy(pool):
                            await self._pool_drain(
                                pool,
                                task_id=task_id,
                                settings=settings,
                                group_dir=group_dir,
                                use_text_as_folder=use_text_as_folder,
                                counters=counters,
                                test_mode=test_mode,
                            )
                        await self.db.update_task(
                            task_id,
                            status="completed",
                            current_folder=counters.get("current_folder") or current_folder,
                            last_message_id=int(counters.get("last_id") or 0),
                            processed_count=int(counters["processed"]),
                            downloaded_count=int(counters["downloaded"]),
                            failed_count=await self.db.count_failed(task_id),
                            skipped_count=int(counters["skipped"]),
                        )
                        await self.db.append_log(
                            task_id, f"已达上限 {max_messages} 个文件，任务完成"
                        )
                        return

                    if await self.db.is_message_done(task_id, message.id):
                        # Already counted on first pass — don't inflate 跳过 on pause/续跑
                        counters["last_id"] = message.id
                        continue

                    if not has_media(message):
                        await self.db.mark_message(task_id, message.id, status="done")
                        counters["last_id"] = message.id
                        counters["processed"] = int(counters["processed"]) + 1
                        counters["skipped"] = int(counters["skipped"]) + 1
                        continue

                    media_type = detect_media_type(message)
                    if media_type == "sticker":
                        media_type = "document"
                    if not media_type or media_type not in media_types:
                        await self.db.mark_message(task_id, message.id, status="done")
                        counters["last_id"] = message.id
                        counters["processed"] = int(counters["processed"]) + 1
                        counters["skipped"] = int(counters["skipped"]) + 1
                        continue

                    if not self._passes_file_filters(task_id, message, media_type):
                        await self.db.mark_message(task_id, message.id, status="done")
                        counters["last_id"] = message.id
                        counters["processed"] = int(counters["processed"]) + 1
                        counters["skipped"] = int(counters["skipped"]) + 1
                        continue

                    indexed_caption = captions_map.get(message.id, "")
                    caption = await self._ensure_caption(
                        client,
                        chat_id,
                        message,
                        album_captions,
                        caption_override=indexed_caption or None,
                    )
                    tags = extract_tags(caption or "")
                    subdir = resolve_media_subdir(
                        message,
                        album_captions=album_captions,
                        use_caption_folders=use_text_as_folder,
                        group_dir=group_dir if use_text_as_folder else None,
                        folder_mode=folder_mode,
                        caption_override=caption or None,
                        tag_folder_map=counters.get("tag_folder_map") or None,
                        tag_blacklist=counters.get("tag_blacklist") or None,
                    )
                    filename = build_filename(
                        message,
                        media_type,
                        album_captions=album_captions,
                        caption=caption,
                    )
                    rel_dir = subdir if subdir is not None else "_未分类"
                    target_dir = group_dir / rel_dir if rel_dir else group_dir
                    target_dir.mkdir(parents=True, exist_ok=True)
                    if counters.get("current_folder") != (rel_dir or "."):
                        await self.db.append_log(
                            task_id, f"目录: {rel_dir or '（群根目录）'}"
                        )
                    counters["current_folder"] = rel_dir or "."

                    existing = await self._skip_if_already_downloaded(
                        task_id=task_id,
                        chat_id=chat_id,
                        message=message,
                        target_dir=target_dir,
                        filename=filename,
                        settings=settings,
                    )
                    if existing is not None:
                        counters["last_id"] = message.id
                        counters["processed"] = int(counters["processed"]) + 1
                        counters["downloaded"] = int(counters["downloaded"]) + 1
                        try:
                            rel = existing.relative_to(settings.download_dir)
                        except ValueError:
                            rel = existing
                        await self.db.append_log(task_id, f"队列已处理，跳过: {rel}")
                        await self.db.update_task(
                            task_id,
                            current_folder=counters["current_folder"],
                            last_message_id=message.id,
                            processed_count=int(counters["processed"]),
                            downloaded_count=int(counters["downloaded"]),
                            failed_count=await self.db.count_failed(task_id),
                            skipped_count=int(counters["skipped"]),
                        )
                        continue

                    target_path, _ = resolve_download_path(
                        target_dir,
                        filename,
                        message.id,
                        self._expected_size(message),
                    )
                    job = DownloadJob(
                        message=message,
                        message_id=message.id,
                        target_path=target_path,
                        caption=caption or "",
                        media_type=media_type,
                        rel_dir=rel_dir or "",
                        tags=tags,
                    )
                    status = await self._pool_submit(
                        pool,
                        job,
                        task_id=task_id,
                        stop_event=stop_event,
                        settings=settings,
                        group_dir=group_dir,
                        use_text_as_folder=use_text_as_folder,
                        counters=counters,
                        concurrency=concurrency,
                        max_messages=max_messages,
                        delay_min=delay_min,
                        delay_max=delay_max,
                        test_mode=test_mode,
                    )
                    if status == "paused":
                        await _pause_and_return("任务已暂停")
                        return
                    counters["last_id"] = message.id

            # Finish current tag's in-flight jobs before starting the next tag
            if self._pool_busy(pool):
                paused = await self._pool_drain(
                    pool,
                    task_id=task_id,
                    settings=settings,
                    group_dir=group_dir,
                    use_text_as_folder=use_text_as_folder,
                    counters=counters,
                    test_mode=test_mode,
                )
                if paused:
                    await self.db.update_task(
                        task_id,
                        status="paused",
                        current_folder=counters.get("current_folder") or current_folder,
                        last_message_id=int(counters.get("last_id") or 0),
                        processed_count=int(counters["processed"]),
                        downloaded_count=int(counters["downloaded"]),
                        failed_count=await self.db.count_failed(task_id),
                        skipped_count=int(counters["skipped"]),
                    )
                    return
            if tag:
                await self.db.append_log(task_id, f"标签 #{tag} 已下完，进入下一标签")

        if max_messages and int(counters["downloaded"]) >= max_messages:
            await self.db.update_task(
                task_id,
                status="completed",
                current_folder=counters.get("current_folder") or current_folder,
                last_message_id=int(counters.get("last_id") or 0),
                processed_count=int(counters["processed"]),
                downloaded_count=int(counters["downloaded"]),
                failed_count=await self.db.count_failed(task_id),
                skipped_count=int(counters["skipped"]),
                last_error=None,
            )
            await self.db.append_log(
                task_id, f"已达上限 {max_messages} 个文件，任务完成"
            )
            return

        if test_mode or self._test_time_up(test_deadline):
            await self.db.update_task(
                task_id,
                status="completed",
                current_folder=counters.get("current_folder") or current_folder,
                last_message_id=int(counters.get("last_id") or 0),
                processed_count=int(counters["processed"]),
                downloaded_count=int(counters["downloaded"]),
                failed_count=await self.db.count_failed(task_id),
                skipped_count=int(counters["skipped"]),
                last_error=None,
            )
            await self.db.append_log(task_id, "测试模式结束（未进入持续监控）")
            return

        meta = await self.db.get_index_meta(chat_id) or {}
        last_seen = max(
            int(meta.get("last_message_id") or 0),
            int(counters.get("last_id") or 0),
            max(message_ids) if message_ids else 0,
        )
        reason = await self._monitor_tagged_messages(
            task_id=task_id,
            stop_event=stop_event,
            settings=settings,
            client=client,
            chat_id=chat_id,
            group_dir=group_dir,
            media_types=media_types,
            use_text_as_folder=use_text_as_folder,
            album_captions=album_captions,
            counters=counters,
            last_seen=last_seen,
            concurrency=concurrency,
            file_formats=file_formats,
            max_messages=max_messages,
            delay_min=delay_min,
            delay_max=delay_max,
            folder_mode=folder_mode,
            include_tags=include_tags,
            caption_keywords=caption_keywords,
            tag_match_mode=tag_match_mode,
            test_mode=test_mode,
            test_deadline=test_deadline,
            newest_first=newest_first,
            index_order_by=index_order_by,
        )
        if reason in ("filters_ready", "gap_found") and not stop_event.is_set():
            await self.db.append_log(
                task_id,
                "标签已更新，重新补下" if reason == "filters_ready" else "发现未下载项，开始补下",
            )
            await self._download_by_index_body(
                task_id=task_id,
                stop_event=stop_event,
                settings=settings,
                client=client,
                chat_id=chat_id,
                chat_title=chat_title,
                group_dir=group_dir,
                media_types=media_types,
                use_text_as_folder=use_text_as_folder,
                current_folder=counters.get("current_folder") or current_folder,
                album_captions=album_captions,
                processed=int(counters["processed"]),
                downloaded=int(counters["downloaded"]),
                failed=await self.db.count_failed(task_id),
                skipped=int(counters["skipped"]),
                newest_first=newest_first,
                index_order_by=index_order_by,
                test_mode=test_mode,
                test_deadline=test_deadline,
                concurrency=concurrency,
                file_formats=file_formats,
                max_messages=max_messages,
                delay_min=delay_min,
                delay_max=delay_max,
                folder_mode=folder_mode,
                include_tags=None,  # reload from DB inside
                caption_keywords=None,
                tag_match_mode=tag_match_mode,
                tag_blacklist=tag_blacklist,
            )

    async def _monitor_tagged_messages(
        self,
        *,
        task_id: int,
        stop_event: asyncio.Event,
        settings,
        client,
        chat_id: int,
        group_dir: Path,
        media_types: set[str],
        use_text_as_folder: bool,
        album_captions: dict,
        counters: dict[str, Any],
        last_seen: int,
        concurrency: int = 1,
        file_formats: list | None = None,
        max_messages: int | None = None,
        delay_min: float = 0.5,
        delay_max: float = 0.5,
        folder_mode: str = "caption",
        include_tags: list | None = None,
        caption_keywords: list | None = None,
        tag_match_mode: str = "any",
        test_mode: bool = False,
        test_deadline: float | None = None,
        poll_sec: float | None = None,
        newest_first: bool = False,
        index_order_by: str = "id",
    ) -> str | None:
        """Local-only monitor: compare caption index vs downloaded.

        Does not poll Telegram. Idle until index scan notifies a bump (or a
        rare heartbeat / tag change). Then checks local gaps and returns
        ``gap_found`` so the caller downloads missing matches.

        Sticky pending queues are re-tried after ``failed_retry_interval_sec``.

        Returns ``filters_ready`` | ``gap_found`` | ``None`` (paused/stopped).
        """
        _ = (
            client,
            album_captions,
            concurrency,
            file_formats,
            delay_min,
            delay_max,
            folder_mode,
            use_text_as_folder,
            group_dir,
            newest_first,
            index_order_by,
        )
        include_tags = include_tags or []
        caption_keywords = caption_keywords or []
        last_seen = max(0, int(last_seen or 0))
        media_types_list = sorted(media_types) if media_types else None
        # Ensure Event exists before first wait (also for late notifies)
        self._chat_wake_event(chat_id)
        if poll_sec is None:
            try:
                poll_sec = float(await self.db.get_monitor_heartbeat_sec())
            except Exception:
                poll_sec = float(
                    getattr(get_settings(), "monitor_heartbeat_sec", 600) or 600
                )
        heartbeat_sec = max(60.0, float(poll_sec or 600.0))
        try:
            failed_retry_sec = max(
                120.0, float(await self.db.get_failed_retry_interval_sec())
            )
        except Exception:
            failed_retry_sec = max(
                120.0,
                float(getattr(get_settings(), "failed_retry_interval_sec", 900) or 900),
            )
        status_line = (
            "进入监控 · 未选标签"
            if not include_tags and not caption_keywords
            else "进入监控"
        )
        await self.db.append_log(task_id, status_line)
        await self.db.update_task(
            task_id,
            status="running",
            last_error=None,
            last_message_id=last_seen,
            current_folder=counters.get("current_folder"),
            processed_count=int(counters["processed"]),
            downloaded_count=int(counters["downloaded"]),
            failed_count=await self.db.count_failed(task_id),
            skipped_count=int(counters["skipped"]),
        )

        # First pass: catch gaps left after backlog / restart without a new bump
        check_pending = True

        while True:
            if stop_event.is_set() or self._test_time_up(test_deadline):
                reason = (
                    "测试模式时间到，已停止"
                    if self._test_time_up(test_deadline) and not stop_event.is_set()
                    else "监控已暂停"
                )
                await self.db.update_task(
                    task_id,
                    status="paused",
                    last_message_id=last_seen,
                    current_folder=counters.get("current_folder"),
                    processed_count=int(counters["processed"]),
                    downloaded_count=int(counters["downloaded"]),
                    failed_count=await self.db.count_failed(task_id),
                    skipped_count=int(counters["skipped"]),
                )
                await self.db.append_log(task_id, reason)
                return None

            fresh = await self.db.get_task(task_id) or {}
            prev_tags = set(include_tags)
            prev_kws = set(caption_keywords)
            include_tags = normalize_tag_list(fresh.get("include_tags") or [])
            caption_keywords = normalize_keyword_list(
                fresh.get("caption_keywords") or []
            )
            tag_match_mode = "any"
            filters_changed = set(include_tags) != prev_tags or set(
                caption_keywords
            ) != prev_kws
            if filters_changed:
                if not include_tags and not caption_keywords:
                    await self.db.append_log(task_id, "标签已清空，等待配置")
                else:
                    await self.db.append_log(task_id, "标签已更新，开始补下")
                    await self.db.update_task(
                        task_id,
                        status="running",
                        last_message_id=last_seen,
                        current_folder=counters.get("current_folder"),
                        processed_count=int(counters["processed"]),
                        downloaded_count=int(counters["downloaded"]),
                        failed_count=await self.db.count_failed(task_id),
                        skipped_count=int(counters["skipped"]),
                        last_error=None,
                    )
                    return "filters_ready"

            if max_messages and int(counters["downloaded"]) >= max_messages:
                await self.db.update_task(
                    task_id,
                    status="completed",
                    last_message_id=last_seen,
                    current_folder=counters.get("current_folder"),
                    processed_count=int(counters["processed"]),
                    downloaded_count=int(counters["downloaded"]),
                    failed_count=await self.db.count_failed(task_id),
                    skipped_count=int(counters["skipped"]),
                    last_error=None,
                )
                await self.db.append_log(
                    task_id, f"已达上限 {max_messages} 个文件，监控结束"
                )
                return None

            bumped = self.peek_index_bump(chat_id)
            if filters_changed or bumped:
                self._gap_pending_sticky.pop(task_id, None)
                self._gap_sticky_at.pop(task_id, None)

            pending = 0
            failed_n = 0
            # Count when woken / first pass / bump / heartbeat — not every idle tick
            if (include_tags or caption_keywords) and (bumped or check_pending):
                try:
                    tags_for_q = await self.db.expand_related_tags(
                        chat_id, include_tags
                    )
                except Exception:
                    tags_for_q = include_tags
                try:
                    pending = await self.db.count_index_pending(
                        task_id,
                        chat_id,
                        tags=tags_for_q or None,
                        tag_match_mode=tag_match_mode,
                        keywords=caption_keywords or None,
                        media_types=media_types_list,
                    )
                except Exception:
                    logger.debug("count_index_pending failed", exc_info=True)
                    pending = 0
                try:
                    failed_n = int(await self.db.count_failed(task_id) or 0)
                except Exception:
                    failed_n = 0

            if (include_tags or caption_keywords) and (
                bumped or pending > 0 or failed_n > 0
            ):
                sticky = self._gap_pending_sticky.get(task_id)
                sticky_at = float(self._gap_sticky_at.get(task_id) or 0.0)
                sticky_age = (
                    (time.monotonic() - sticky_at) if sticky_at > 0 else 0.0
                )
                cooldown_ok = sticky_age >= failed_retry_sec
                # Same pending after a gap pass → standby, but re-try after cooldown
                # (covers failed / abnormal files that need another download pass).
                blocked = (
                    pending > 0
                    and sticky is not None
                    and pending >= int(sticky)
                    and not bumped
                    and not filters_changed
                    and not cooldown_ok
                    and failed_n <= 0
                )
                # Failed rows always get a cooldown-gated retry even if pending sticky
                if (
                    failed_n > 0
                    and sticky is not None
                    and not bumped
                    and not filters_changed
                    and not cooldown_ok
                ):
                    blocked = True
                if blocked:
                    if check_pending:
                        wait_m = max(1, int((failed_retry_sec - sticky_age) / 60))
                        await self.db.append_log(
                            task_id,
                            f"队列仍有 {pending} 条"
                            + (f"·失败 {failed_n}" if failed_n else "")
                            + f"，待命约 {wait_m} 分钟后自动重试",
                        )
                else:
                    if sticky is not None and cooldown_ok and not bumped:
                        await self.db.append_log(
                            task_id,
                            f"定时自动重试失败/待补项"
                            + (f"（失败 {failed_n}）" if failed_n else ""),
                        )
                    if pending > 0 or failed_n > 0:
                        self._gap_pending_sticky[task_id] = int(pending or failed_n)
                        self._gap_sticky_at[task_id] = time.monotonic()
                    else:
                        self._gap_pending_sticky.pop(task_id, None)
                        self._gap_sticky_at.pop(task_id, None)
                    self.consume_index_bump(chat_id)
                    if pending > 0:
                        gap_msg = f"待补下 {pending} 条"
                    elif failed_n > 0:
                        gap_msg = f"重试失败 {failed_n} 条"
                    else:
                        gap_msg = "索引已更新"
                    await self.db.append_log(task_id, gap_msg)
                    await self.db.update_task(
                        task_id,
                        status="running",
                        last_message_id=last_seen,
                        current_folder=counters.get("current_folder"),
                        processed_count=int(counters["processed"]),
                        downloaded_count=int(counters["downloaded"]),
                        failed_count=await self.db.count_failed(task_id),
                        skipped_count=int(counters["skipped"]),
                        last_error=None,
                    )
                    return "gap_found"

            check_pending = False
            # True idle: drop worker shell so UI shows「监控中」not empty lanes
            self._clear_progress(task_id)
            await self.db.update_task(
                task_id,
                status="running",
                last_message_id=last_seen,
                current_folder=counters.get("current_folder"),
                processed_count=int(counters["processed"]),
                downloaded_count=int(counters["downloaded"]),
                failed_count=await self.db.count_failed(task_id),
                skipped_count=int(counters["skipped"]),
                last_error=None,
            )

            reason = await self._wait_monitor_idle(
                chat_id=chat_id,
                stop_event=stop_event,
                test_deadline=test_deadline,
                heartbeat_sec=heartbeat_sec,
            )
            # Heartbeat / external wake: light recheck; index bump handled above
            if reason in ("heartbeat", "wake"):
                check_pending = True
    async def _load_tag_folder_map(
        self, task_id: int, chat_id: int, group_dir: Path, *, folder_mode: str
    ) -> dict[str, str]:
        """Build tag → full related multi-tag folder map from index + disk."""
        if (folder_mode or "caption") != "caption":
            return {}
        try:
            mapping = await self.db.get_tag_folder_map(chat_id, group_dir)
        except Exception:
            logger.exception("build tag folder map failed")
            await self.db.append_log(task_id, "关联目录映射构建失败，回退为单条文案标签")
            return {}
        if mapping:
            await self.db.append_log(
                task_id, f"关联目录映射已启用（{len(mapping)} 个标签）"
            )
        return mapping

    async def _merge_tag_folders_after(
        self,
        task_id: int,
        group_dir: Path,
        *,
        chat_id: int | None = None,
        extra_tag_groups: list[list[str]] | None = None,
        quiet: bool = False,
        tag_blacklist: frozenset | set | None = None,
    ) -> None:
        """Merge related #tag folders into full multi-tag names."""
        groups: list[list[str]] = list(extra_tag_groups or [])
        if chat_id is not None:
            try:
                groups.extend(
                    await self.db.list_index_tag_groups_for_merge(chat_id)
                )
            except Exception:
                logger.debug("index tag groups for merge failed", exc_info=True)
        if tag_blacklist is None:
            tag_blacklist = await self.db.get_tag_relation_blacklist()
        try:
            logs = await asyncio.to_thread(
                merge_related_tag_folders,
                group_dir,
                extra_tag_groups=groups or None,
                blacklist=tag_blacklist,
            )
        except Exception as e:
            logger.exception("merge tag folders failed")
            await self.db.append_log(task_id, f"同类目录合并失败: {e}")
            return
        if not logs:
            return
        if not quiet:
            await self.db.append_log(task_id, "同类标签目录已合并")
        for line in logs:
            await self.db.append_log(task_id, line)

    async def _download_loop_body(
        self,
        *,
        task_id: int,
        stop_event: asyncio.Event,
        settings,
        client,
        chat_id: int,
        group_dir: Path,
        media_types: set[str],
        use_text_as_folder: bool,
        min_len: int,
        current_folder: Optional[str],
        album_captions: dict,
        last_id: int,
        start_id: int,
        end_id,
        start_date,
        end_date,
        processed: int,
        downloaded: int,
        failed: int,
        skipped: int,
        newest_first: bool,
        test_mode: bool = False,
        test_deadline: float | None = None,
        concurrency: int = 1,
        file_formats: list | None = None,
        max_messages: int | None = None,
        delay_min: float = 0.5,
        delay_max: float = 0.5,
        folder_mode: str = "caption",
        include_tags: list | None = None,
        caption_keywords: list | None = None,
        tag_match_mode: str = "any",
        tag_blacklist: frozenset | set | None = None,
    ) -> None:
        file_formats = file_formats or []
        include_tags = include_tags or []
        caption_keywords = caption_keywords or []
        if tag_blacklist is None:
            tag_blacklist = await self.db.get_tag_relation_blacklist()
        tag_folder_map = {}
        if use_text_as_folder:
            tag_folder_map = await self._load_tag_folder_map(
                task_id, chat_id, group_dir, folder_mode=folder_mode
            )
        counters = {
            "processed": processed,
            "downloaded": downloaded,
            "failed": failed,
            "skipped": skipped,
            "current_folder": current_folder,
            "last_id": last_id,
            "tag_folder_map": tag_folder_map,
            "tag_blacklist": tag_blacklist,
            "chat_id": chat_id,
        }
        try:
            n_local = await self.db.sync_local_completed_from_dir(chat_id, group_dir)
            if n_local:
                await self.db.append_log(
                    task_id, f"本地文件写入队列已处理 {n_local} 条"
                )
        except Exception:
            logger.debug("sync_local_completed_from_dir failed", exc_info=True)
        await self._log_queue_skip_stats(
            task_id,
            chat_id,
            include_tags=include_tags,
            caption_keywords=caption_keywords,
            tag_match_mode=tag_match_mode,
            media_types=media_types,
        )
        ok = await self._retry_failed_messages(
            task_id=task_id,
            chat_id=chat_id,
            group_dir=group_dir,
            media_types=media_types,
            current_folder=current_folder,
            use_text_as_folder=use_text_as_folder,
            min_len=min_len,
            album_captions=album_captions,
            stop_event=stop_event,
            counters=counters,
            test_mode=test_mode,
            test_deadline=test_deadline,
            concurrency=concurrency,
            file_formats=file_formats,
            folder_mode=folder_mode,
            delay_min=delay_min,
            delay_max=delay_max,
            max_messages=max_messages,
            include_tags=include_tags,
            caption_keywords=caption_keywords,
            tag_match_mode=tag_match_mode,
        )
        processed = counters["processed"]
        downloaded = counters["downloaded"]
        failed = await self.db.count_failed(task_id)
        skipped = counters["skipped"]
        current_folder = counters.get("current_folder") or current_folder
        last_id = int(counters.get("last_id") or last_id)

        if max_messages and downloaded >= max_messages:
            await self.db.update_task(
                task_id,
                status="completed",
                current_folder=current_folder,
                last_message_id=last_id,
                processed_count=processed,
                downloaded_count=downloaded,
                failed_count=failed,
                skipped_count=skipped,
                last_error=None,
            )
            await self.db.append_log(task_id, f"已达上限 {max_messages} 个文件，任务完成")
            return

        if not ok:
            await self.db.update_task(
                task_id,
                status="paused",
                current_folder=current_folder,
                last_message_id=last_id,
                processed_count=processed,
                downloaded_count=downloaded,
                failed_count=failed,
                skipped_count=skipped,
            )
            return

        # Phase 2: scan from checkpoint; sliding-window concurrency (free slot → next)
        pool = self._new_download_pool(concurrency)
        counters = {
            "processed": processed,
            "downloaded": downloaded,
            "failed": failed,
            "skipped": skipped,
            "current_folder": current_folder,
            "last_id": last_id,
            "tag_folder_map": tag_folder_map,
            "tag_blacklist": tag_blacklist,
            "chat_id": chat_id,
        }

        async def _sync_from_counters() -> None:
            nonlocal processed, downloaded, failed, skipped, current_folder, last_id
            processed = int(counters["processed"])
            downloaded = int(counters["downloaded"])
            failed = int(counters["failed"])
            skipped = int(counters["skipped"])
            current_folder = counters.get("current_folder")
            last_id = int(counters.get("last_id") or 0)

        async def _pause_and_return(reason: str) -> None:
            await _sync_from_counters()
            if self._pool_busy(pool):
                await self._pool_drain(
                    pool,
                    task_id=task_id,
                    settings=settings,
                    group_dir=group_dir,
                    use_text_as_folder=use_text_as_folder,
                    counters=counters,
                    test_mode=test_mode,
                )
                await _sync_from_counters()
            await self.db.update_task(
                task_id,
                status="paused",
                current_folder=current_folder,
                last_message_id=last_id,
                processed_count=processed,
                downloaded_count=downloaded,
                failed_count=failed,
                skipped_count=skipped,
            )
            if reason and reason not in ("任务已暂停", "监控已暂停"):
                await self.db.append_log(task_id, reason)

        async def _complete_limit() -> None:
            await _sync_from_counters()
            if self._pool_busy(pool):
                await self._pool_drain(
                    pool,
                    task_id=task_id,
                    settings=settings,
                    group_dir=group_dir,
                    use_text_as_folder=use_text_as_folder,
                    counters=counters,
                    test_mode=test_mode,
                )
                await _sync_from_counters()
            await self.db.update_task(
                task_id,
                status="completed",
                current_folder=current_folder,
                last_message_id=last_id,
                processed_count=processed,
                downloaded_count=downloaded,
                failed_count=failed,
                skipped_count=skipped,
            )
            await self.db.append_log(
                task_id, f"已达上限 {max_messages} 个文件，任务完成"
            )

        while True:
            if stop_event.is_set() or self._test_time_up(test_deadline):
                reason = (
                    "测试模式时间到，已停止（未下完整文件）"
                    if self._test_time_up(test_deadline) and not stop_event.is_set()
                    else "任务已暂停"
                )
                await _pause_and_return(reason)
                return

            iter_kwargs: dict[str, Any] = {"reverse": not newest_first}
            if newest_first:
                if last_id > 0:
                    iter_kwargs["max_id"] = last_id
                elif end_id:
                    iter_kwargs["max_id"] = int(end_id) + 1
                if start_id:
                    iter_kwargs["min_id"] = start_id
            else:
                iter_kwargs["min_id"] = max(last_id, start_id)
                if end_id:
                    iter_kwargs["max_id"] = int(end_id) + 1

            try:
                async for message in client.iter_messages(chat_id, **iter_kwargs):
                    # Opportunistically reap finished downloads while scanning
                    if self._pool_busy(pool) and await self._pool_reap(
                        pool,
                        task_id=task_id,
                        settings=settings,
                        group_dir=group_dir,
                        use_text_as_folder=use_text_as_folder,
                        counters=counters,
                        test_mode=test_mode,
                        wait=False,
                    ):
                        await _pause_and_return("任务已暂停")
                        return
                    await _sync_from_counters()

                    if stop_event.is_set() or self._test_time_up(test_deadline):
                        reason = (
                            "测试模式时间到，已停止（未下完整文件）"
                            if self._test_time_up(test_deadline)
                            and not stop_event.is_set()
                            else "任务已暂停"
                        )
                        await _pause_and_return(reason)
                        return

                    msg_dt = _msg_date_utc(message)
                    if end_date and msg_dt and msg_dt > end_date:
                        continue
                    if start_date and msg_dt and msg_dt < start_date:
                        last_id = message.id
                        processed += 1
                        counters["last_id"] = last_id
                        counters["processed"] = processed
                        continue

                    if await self.db.is_message_done(task_id, message.id):
                        # Already counted — ignore on resume/rescan
                        last_id = message.id
                        counters["last_id"] = last_id
                        continue

                    resolve_caption_text(message, album_captions=album_captions)

                    current_folder, subdir, is_title = next_folder_state(
                        current_folder,
                        message,
                        use_text_as_folder=use_text_as_folder,
                        min_len=min_len,
                        album_captions=album_captions,
                        group_dir=group_dir if use_text_as_folder else None,
                        folder_mode=folder_mode,
                        tag_folder_map=counters.get("tag_folder_map") or None,
                        tag_blacklist=counters.get("tag_blacklist") or None,
                    )
                    counters["current_folder"] = current_folder

                    if is_title or not has_media(message):
                        await self.db.mark_message(task_id, message.id, status="done")
                        last_id = message.id
                        processed += 1
                        skipped += 1
                        counters["last_id"] = last_id
                        counters["processed"] = processed
                        counters["skipped"] = skipped
                        continue

                    media_type = detect_media_type(message)
                    if media_type == "sticker":
                        media_type = "document"
                    if not media_type or media_type not in media_types:
                        await self.db.mark_message(task_id, message.id, status="done")
                        last_id = message.id
                        processed += 1
                        skipped += 1
                        counters["last_id"] = last_id
                        counters["processed"] = processed
                        counters["skipped"] = skipped
                        continue

                    if not self._passes_file_filters(task_id, message, media_type):
                        await self.db.mark_message(task_id, message.id, status="done")
                        last_id = message.id
                        processed += 1
                        skipped += 1
                        counters["last_id"] = last_id
                        counters["processed"] = processed
                        counters["skipped"] = skipped
                        continue

                    if max_messages and int(counters["downloaded"]) >= max_messages:
                        await _complete_limit()
                        return

                    caption = await self._ensure_caption(
                        client, chat_id, message, album_captions
                    )
                    tags = extract_tags(caption or "")
                    if not matches_include_tags(
                        tags, include_tags, mode=tag_match_mode
                    ) or not matches_caption_keywords(caption, caption_keywords):
                        await self.db.mark_message(task_id, message.id, status="done")
                        last_id = message.id
                        processed += 1
                        skipped += 1
                        counters["last_id"] = last_id
                        counters["processed"] = processed
                        counters["skipped"] = skipped
                        continue

                    subdir = resolve_media_subdir(
                        message,
                        album_captions=album_captions,
                        use_caption_folders=use_text_as_folder,
                        group_dir=group_dir if use_text_as_folder else None,
                        folder_mode=folder_mode,
                        tag_folder_map=counters.get("tag_folder_map") or None,
                        tag_blacklist=counters.get("tag_blacklist") or None,
                    )
                    filename = build_filename(
                        message,
                        media_type,
                        album_captions=album_captions,
                        caption=caption,
                    )
                    rel_dir = subdir if subdir is not None else "_未分类"
                    target_dir = group_dir / rel_dir if rel_dir else group_dir
                    target_dir.mkdir(parents=True, exist_ok=True)
                    if current_folder != (rel_dir or "."):
                        await self.db.append_log(
                            task_id, f"目录: {rel_dir or '（群根目录）'}"
                        )
                    current_folder = rel_dir or "."
                    counters["current_folder"] = current_folder

                    existing = await self._skip_if_already_downloaded(
                        task_id=task_id,
                        chat_id=chat_id,
                        message=message,
                        target_dir=target_dir,
                        filename=filename,
                        settings=settings,
                    )
                    if existing is not None:
                        last_id = message.id
                        processed += 1
                        downloaded += 1
                        counters["last_id"] = last_id
                        counters["processed"] = processed
                        counters["downloaded"] = downloaded
                        try:
                            rel = existing.relative_to(settings.download_dir)
                        except ValueError:
                            rel = existing
                        await self.db.append_log(task_id, f"队列已处理，跳过: {rel}")
                        await self.db.update_task(
                            task_id,
                            current_folder=current_folder,
                            last_message_id=last_id,
                            processed_count=processed,
                            downloaded_count=downloaded,
                            failed_count=failed,
                            skipped_count=skipped,
                        )
                        if max_messages and downloaded >= max_messages:
                            await _complete_limit()
                            return
                        continue

                    target_path, _ = resolve_download_path(
                        target_dir,
                        filename,
                        message.id,
                        self._expected_size(message),
                    )

                    job = DownloadJob(
                        message=message,
                        message_id=message.id,
                        target_path=target_path,
                        caption=caption or "",
                        media_type=media_type,
                        rel_dir=rel_dir or "",
                        tags=tags,
                    )
                    status = await self._pool_submit(
                        pool,
                        job,
                        task_id=task_id,
                        stop_event=stop_event,
                        settings=settings,
                        group_dir=group_dir,
                        use_text_as_folder=use_text_as_folder,
                        counters=counters,
                        concurrency=concurrency,
                        max_messages=max_messages,
                        delay_min=delay_min,
                        delay_max=delay_max,
                        test_mode=test_mode,
                    )
                    await _sync_from_counters()
                    if status == "paused":
                        await _pause_and_return("任务已暂停")
                        return
                    if status == "limit":
                        await _complete_limit()
                        return

                # End of this iter page — drain in-flight then finish or continue after flood
                if await self._pool_drain(
                    pool,
                    task_id=task_id,
                    settings=settings,
                    group_dir=group_dir,
                    use_text_as_folder=use_text_as_folder,
                    counters=counters,
                    test_mode=test_mode,
                ):
                    await _pause_and_return("任务已暂停")
                    return
                await _sync_from_counters()
                if max_messages and downloaded >= max_messages:
                    await _complete_limit()
                    return
                break  # finished iteration without FloodWait

            except FloodWaitError as e:
                if self._pool_busy(pool):
                    await self._pool_drain(
                        pool,
                        task_id=task_id,
                        settings=settings,
                        group_dir=group_dir,
                        use_text_as_folder=use_text_as_folder,
                        counters=counters,
                        test_mode=test_mode,
                    )
                await _sync_from_counters()
                await self.db.update_task(
                    task_id,
                    current_folder=current_folder,
                    last_message_id=last_id,
                    processed_count=processed,
                    downloaded_count=downloaded,
                    failed_count=failed,
                    skipped_count=skipped,
                )
                continued = await self._wait_flood(task_id, e.seconds, stop_event)
                if not continued:
                    await self.db.update_task(
                        task_id,
                        status="paused",
                        last_error=f"FloodWait {e.seconds}s",
                        current_folder=current_folder,
                        last_message_id=last_id,
                        processed_count=processed,
                        downloaded_count=downloaded,
                        failed_count=failed,
                        skipped_count=skipped,
                    )
                    return
                continue

        failed = await self.db.count_failed(task_id)
        if failed == 0:
            await self.db.update_task(
                task_id,
                status="completed",
                current_folder=current_folder,
                last_message_id=last_id,
                processed_count=processed,
                downloaded_count=downloaded,
                failed_count=0,
                skipped_count=skipped,
                last_error=None,
            )
            await self.db.append_log(task_id, "任务完成")
        else:
            # Keep paused so「继续」can retry; also schedule a delayed auto-retry
            await self.db.update_task(
                task_id,
                status="paused",
                current_folder=current_folder,
                last_message_id=last_id,
                processed_count=processed,
                downloaded_count=downloaded,
                failed_count=failed,
                skipped_count=skipped,
                last_error=f"{failed} 条失败待重试",
            )
            await self.db.append_log(
                task_id,
                f"仍有 {failed} 条失败，已暂停；将定时自动重试，也可手动继续",
            )
            self._schedule_failed_retry(int(task_id))

    def _test_time_up(self, test_deadline: float | None) -> bool:
        if test_deadline is None:
            return False
        return asyncio.get_event_loop().time() >= test_deadline

    def _expected_size(self, message) -> int:
        if message and message.file and getattr(message.file, "size", None):
            try:
                return int(message.file.size)
            except (TypeError, ValueError):
                return 0
        return 0

    def _part_path(self, target_path: Path) -> Path:
        """Stage incomplete downloads under temp_dir (mirror of download_dir tree)."""
        settings = get_settings()
        target_path = Path(target_path)
        try:
            rel = target_path.resolve().relative_to(Path(settings.download_dir).resolve())
        except ValueError:
            rel = Path(target_path.name)
        part = (Path(settings.temp_dir) / rel).with_suffix(
            target_path.suffix + ".part"
        )
        # Migrate legacy sidecar next to final file (pre-temp_dir layout)
        legacy = target_path.with_suffix(target_path.suffix + ".part")
        if legacy.exists() and legacy.is_file() and not part.exists():
            try:
                part.parent.mkdir(parents=True, exist_ok=True)
                self._move_path(legacy, part)
            except OSError:
                logger.debug(
                    "migrate legacy .part failed: %s -> %s", legacy, part, exc_info=True
                )
        return part

    @staticmethod
    def _move_path(src: Path, dst: Path) -> None:
        """Rename when possible; fall back to copy+delete across volumes."""
        import shutil

        src = Path(src)
        dst = Path(dst)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            dst.unlink()
        try:
            src.rename(dst)
        except OSError:
            shutil.move(str(src), str(dst))

    async def _salvage_complete_file(
        self,
        *,
        task_id: int,
        message_id: int,
        target_path: Path,
        expected_size: int = 0,
        message=None,
    ) -> bool:
        """
        If target (or a finished .part) is already a complete file, mark done.
        Used after pause/cancel so resume won't re-download finished media.
        """
        if expected_size <= 0 and message is not None:
            expected_size = self._expected_size(message)
        if file_looks_complete(target_path, expected_size):
            await self.db.mark_message(
                task_id, message_id, status="done", file_path=str(target_path)
            )
            return True
        part = self._part_path(target_path)
        try:
            if part.exists() and part.is_file():
                size = part.stat().st_size
                if size > 0 and (not expected_size or size == expected_size):
                    self._move_path(part, target_path)
                    if file_looks_complete(target_path, expected_size):
                        await self.db.mark_message(
                            task_id,
                            message_id,
                            status="done",
                            file_path=str(target_path),
                        )
                        return True
        except OSError:
            logger.debug("salvage part failed for %s", target_path, exc_info=True)
        return False

    async def _log_queue_skip_stats(
        self,
        task_id: int,
        chat_id,
        *,
        include_tags: list | None,
        caption_keywords: list | None,
        tag_match_mode: str,
        media_types: set[str] | list[str] | None,
    ) -> None:
        """Log how many matched items are already done in the queue (not disk scan)."""
        include_tags = include_tags or []
        caption_keywords = caption_keywords or []
        if not include_tags and not caption_keywords:
            return
        media_list = sorted(media_types) if media_types else None
        try:
            tags_for_q = await self.db.expand_related_tags(chat_id, include_tags)
        except Exception:
            tags_for_q = include_tags
        try:
            done_n = await self.db.count_index_done(
                task_id,
                chat_id,
                tags=tags_for_q or None,
                tag_match_mode=tag_match_mode or "any",
                keywords=caption_keywords or None,
                media_types=media_list,
            )
            pending_n = await self.db.count_index_pending(
                task_id,
                chat_id,
                tags=tags_for_q or None,
                tag_match_mode=tag_match_mode or "any",
                keywords=caption_keywords or None,
                media_types=media_list,
            )
        except Exception:
            logger.debug("queue skip stats failed", exc_info=True)
            return
        if done_n > 0:
            await self.db.append_log(
                task_id,
                f"队列已处理 {done_n} 条，下载时将跳过（待下 {pending_n}）",
            )

    async def _skip_if_already_downloaded(
        self,
        *,
        task_id: int,
        chat_id,
        message,
        target_dir: Path,
        filename: str,
        settings,
        group_dir: Optional[Path] = None,
        dup_index: Optional[dict] = None,
    ) -> Optional[Path]:
        """Skip when this message_id is already done in the queue.

        Uses chat_completed / downloaded by message_id. Also skips if the exact
        target path is already complete. Does not scan for same-name/size dupes.
        """
        _ = (group_dir, dup_index)  # legacy kwargs ignored
        mid = int(message.id)
        expected = self._expected_size(message)

        if await self.db.is_message_done(task_id, mid, chat_id=chat_id):
            reused = await self.db.find_chat_completed_file(chat_id, mid)
            p: Optional[Path] = None
            if reused:
                p = Path(reused)
                if not p.is_absolute():
                    p = settings.download_dir / p
                if not file_looks_complete(p, expected):
                    p = None
            if p is None:
                await self.db.mark_message(task_id, mid, status="done", file_path=None)
                return target_dir / filename
            await self.db.mark_message(
                task_id, mid, status="done", file_path=str(p)
            )
            return p

        path, done = resolve_download_path(target_dir, filename, mid, expected)
        if done:
            await self.db.mark_message(
                task_id, mid, status="done", file_path=str(path)
            )
            try:
                await self.db.mark_chat_completed(chat_id, mid, file_path=str(path))
            except Exception:
                pass
            return path
        return None

    async def _sleep_delay(
        self, delay_min: float, delay_max: float, stop_event: asyncio.Event
    ) -> None:
        lo = max(0.0, float(delay_min or 0))
        hi = max(lo, float(delay_max or lo))
        if hi <= 0:
            return
        delay = lo if hi == lo else random.uniform(lo, hi)
        remaining = delay
        while remaining > 0:
            if stop_event.is_set():
                return
            step = min(0.25, remaining)
            await asyncio.sleep(step)
            remaining -= step

    def _new_download_pool(self, concurrency: int = 1) -> dict[str, Any]:
        """Tangyoha-style pool: N resident workers pull from a shared queue."""
        n = max(1, min(5, int(concurrency or 1)))
        return {
            "concurrency": n,
            "queue": asyncio.Queue(),
            "priority": deque(),  # deferred jobs — resume ASAP under new concurrency
            "workers": [],  # asyncio.Task list (legacy order)
            "worker_tasks": {},  # worker_id -> Task
            "started": False,
            "in_flight": 0,
            "blocked": False,
            "_stopping": False,
            "done_event": asyncio.Event(),
            "lock": asyncio.Lock(),
            "order": [],  # scan order for checkpoint
            "settled": {},  # message_id -> True(ok) | False(blocked)
            "order_idx": 0,
            "active": {},  # worker_id -> {yield: Event, job}
            # settle kwargs filled by _ensure_pool_workers
            "_ctx": None,
            "_task_id": None,
        }

    def _pool_busy(self, pool: dict[str, Any]) -> bool:
        pri = pool.get("priority")
        return bool(pool.get("started")) and (
            int(pool.get("in_flight") or 0) > 0
            or not pool["queue"].empty()
            or bool(pri)
            or bool(pool.get("workers"))
        )

    def _pool_pending_count(self, pool: dict[str, Any]) -> int:
        pri = pool.get("priority")
        return int(pool["queue"].qsize()) + (len(pri) if pri else 0)

    def _pool_signal_done(self, pool: dict[str, Any]) -> None:
        ev = pool.get("done_event")
        if ev is not None:
            ev.set()

    async def _apply_pool_concurrency(self, pool: dict[str, Any], desired: int) -> None:
        """Raise/lower live concurrency; excess in-flight downloads yield and requeue."""
        desired = max(1, min(5, int(desired or 1)))
        prev = max(1, int(pool.get("concurrency") or 1))
        if not pool.get("started"):
            pool["concurrency"] = desired
            return
        pool["concurrency"] = desired
        ctx = pool.get("_ctx") or {}
        task_id = int(ctx.get("task_id") or pool.get("_task_id") or 0)

        # Shrink: ask workers above the new limit to yield their current file
        yielded = 0
        for wid, info in list((pool.get("active") or {}).items()):
            if int(wid) > desired:
                ev = info.get("yield")
                if isinstance(ev, asyncio.Event) and not ev.is_set():
                    ev.set()
                    yielded += 1
        if yielded and task_id:
            try:
                asyncio.get_running_loop().create_task(
                    self.db.append_log(
                        task_id,
                        f"并发 {prev}→{desired}：多余下载改排队续传（当前文件下完后立即处理）",
                    )
                )
            except Exception:
                pass

        # Grow / refill: spawn missing workers
        if desired > len(pool.get("worker_tasks") or {}):
            await self._spawn_missing_pool_workers(pool)

        if task_id:
            self._ensure_worker_progress(task_id, desired)
        self._pool_signal_done(pool)

    async def _spawn_missing_pool_workers(self, pool: dict[str, Any]) -> None:
        ctx = pool.get("_ctx")
        if not ctx or not pool.get("started"):
            return
        n = max(1, min(5, int(pool.get("concurrency") or 1)))
        tasks_map: dict[int, asyncio.Task] = pool.setdefault("worker_tasks", {})
        workers_list: list = pool.setdefault("workers", [])
        task_id = int(ctx["task_id"])
        for wid in range(1, n + 1):
            existing = tasks_map.get(wid)
            if existing is not None and not existing.done():
                continue
            t = asyncio.create_task(
                self._download_worker_loop(wid, pool),
                name=f"dl-worker-{task_id}-{wid}",
            )
            tasks_map[wid] = t
            workers_list.append(t)
        self._ensure_worker_progress(task_id, n)

    async def _ensure_pool_workers(
        self,
        pool: dict[str, Any],
        *,
        task_id: int,
        stop_event: asyncio.Event,
        settings,
        group_dir: Path,
        use_text_as_folder: bool,
        counters: dict[str, Any],
        test_mode: bool = False,
        log_prefix: str = "",
    ) -> None:
        n = max(1, min(5, int(pool.get("concurrency") or 1)))
        pool["concurrency"] = n
        pool["_ctx"] = {
            "task_id": task_id,
            "stop_event": stop_event,
            "settings": settings,
            "group_dir": group_dir,
            "use_text_as_folder": use_text_as_folder,
            "counters": counters,
            "test_mode": test_mode,
            "log_prefix": log_prefix,
        }
        pool["_task_id"] = int(task_id)
        if pool.get("started"):
            # Already running — only sync concurrency / spawn missing workers
            await self._apply_pool_concurrency(pool, n)
            return
        pool["started"] = True
        pool["blocked"] = False
        pool["_stopping"] = False
        pool.setdefault("priority", deque())
        pool.setdefault("active", {})
        pool.setdefault("worker_tasks", {})
        self._live_pools[int(task_id)] = pool
        self._ensure_worker_progress(task_id, n)
        await self._spawn_missing_pool_workers(pool)

    async def _download_worker_loop(self, worker_id: int, pool: dict[str, Any]) -> None:
        """Resident worker: pull jobs until sentinel None; park when above concurrency."""
        q: asyncio.Queue = pool["queue"]
        wid = int(worker_id)
        while True:
            if pool.get("_stopping"):
                return

            # Above current concurrency → park (do not pull new work)
            while wid > int(pool.get("concurrency") or 1) and not pool.get("_stopping"):
                await asyncio.sleep(0.12)
            if pool.get("_stopping"):
                return

            job = None
            from_queue = False
            pri = pool.get("priority")
            if pri:
                try:
                    job = pri.popleft()
                except IndexError:
                    job = None
            if job is None:
                try:
                    job = await asyncio.wait_for(q.get(), timeout=0.35)
                    from_queue = True
                except asyncio.TimeoutError:
                    continue

            try:
                if job is None:
                    return

                # Became excess while waiting — put job back for lower workers
                if wid > int(pool.get("concurrency") or 1):
                    pool.setdefault("priority", deque()).appendleft(job)
                    continue

                ctx = pool.get("_ctx") or {}
                task_id = int(ctx["task_id"])
                stop_event: asyncio.Event = ctx["stop_event"]
                yield_ev = asyncio.Event()
                pool.setdefault("active", {})[wid] = {"yield": yield_ev, "job": job}
                async with pool["lock"]:
                    pool["in_flight"] = int(pool.get("in_flight") or 0) + 1
                result: bool | str = False
                try:
                    result = await self._download_with_retry(
                        task_id,
                        job.message,
                        job.target_path,
                        stop_event,
                        test_mode=bool(ctx.get("test_mode")),
                        caption=job.caption,
                        message_id=job.message_id,
                        worker_id=wid,
                        yield_event=yield_ev,
                    )
                except Exception:
                    logger.exception(
                        "download worker %s crashed: msg %s", wid, job.message_id
                    )
                    result = False
                    pool["blocked"] = True
                finally:
                    pool.get("active", {}).pop(wid, None)
                    self._set_worker_idle(task_id, wid)
                    async with pool["lock"]:
                        pool["in_flight"] = max(0, int(pool.get("in_flight") or 0) - 1)
                    self._pool_signal_done(pool)

                try:
                    await self._settle_pool_job(
                        pool,
                        job,
                        result,
                        task_id=task_id,
                        settings=ctx["settings"],
                        group_dir=ctx["group_dir"],
                        use_text_as_folder=bool(ctx.get("use_text_as_folder")),
                        counters=ctx["counters"],
                        test_mode=bool(ctx.get("test_mode")),
                        log_prefix=str(ctx.get("log_prefix") or ""),
                    )
                except Exception:
                    logger.exception("settle failed msg %s", job.message_id)
                    pool["settled"][job.message_id] = False
                    pool["blocked"] = True
                    self._pool_signal_done(pool)
            finally:
                if from_queue:
                    q.task_done()

    async def _settle_pool_job(
        self,
        pool: dict[str, Any],
        job: DownloadJob,
        result: bool | str,
        *,
        task_id: int,
        settings,
        group_dir: Path,
        use_text_as_folder: bool,
        counters: dict[str, Any],
        test_mode: bool = False,
        log_prefix: str = "",
    ) -> None:
        """Apply one finished job to counters / checkpoint (worker-safe)."""
        merge_groups: list[list[str]] = []
        deferred_rel = None
        async with pool["lock"]:
            if result == "deferred":
                # Concurrency shrink: keep .part, resume ASAP on a free slot
                pool["settled"].pop(job.message_id, None)
                pool.setdefault("priority", deque()).appendleft(job)
                try:
                    deferred_rel = str(job.target_path.relative_to(settings.download_dir))
                except Exception:
                    deferred_rel = str(job.target_path)
                self._pool_signal_done(pool)
            elif result == "paused":
                if await self._salvage_complete_file(
                    task_id=task_id,
                    message_id=job.message_id,
                    target_path=job.target_path,
                    message=job.message,
                ):
                    result = True
                else:
                    pool["settled"][job.message_id] = False
                    pool["blocked"] = True
                    return

        if deferred_rel is not None:
            await self.db.append_log(
                task_id, f"排队续传（并发已调整）: {deferred_rel}"
            )
            return

        async with pool["lock"]:
            if result == "paused":
                # already handled above
                return
            pool["settled"][job.message_id] = True
            downloaded = int(counters["downloaded"])
            failed = int(counters["failed"])
            current_folder = counters.get("current_folder")

            if result is True:
                downloaded += 1
                await self.db.mark_message(
                    task_id,
                    job.message_id,
                    status="done",
                    file_path=str(job.target_path),
                    chat_id=counters.get("chat_id"),
                    commit=False,
                )
                label = "测试占位" if test_mode else (log_prefix or "已下载")
                try:
                    rel = job.target_path.relative_to(settings.download_dir)
                except ValueError:
                    rel = job.target_path
                await self.db.append_log(task_id, f"{label}: {rel}")
                if use_text_as_folder and len(job.tags) >= 2:
                    merge_groups.append(job.tags)
            else:
                failed += 1
                await self.db.mark_message(
                    task_id,
                    job.message_id,
                    status="failed",
                    error="download failed",
                    commit=False,
                )
                await self.db.append_log(
                    task_id,
                    f"下载失败: message {job.message_id}（下次继续时重试）",
                )
            current_folder = job.rel_dir or current_folder

            processed = int(counters["processed"])
            last_id = int(counters.get("last_id") or 0)
            order: list[DownloadJob] = pool["order"]
            idx = int(pool["order_idx"])
            settled: dict = pool["settled"]
            while idx < len(order):
                mid = order[idx].message_id
                if mid not in settled:
                    break
                if not settled[mid]:
                    break
                last_id = mid
                processed += 1
                idx += 1
            pool["order_idx"] = idx

            counters["processed"] = processed
            counters["downloaded"] = downloaded
            counters["failed"] = failed
            counters["current_folder"] = current_folder
            counters["last_id"] = last_id

            now_m = time.monotonic()
            last_flush = float(counters.get("_db_flush_t") or 0)
            force_flush = bool(pool.get("blocked")) or int(pool.get("in_flight") or 0) == 0
            if force_flush or (now_m - last_flush) >= 0.8:
                counters["_db_flush_t"] = now_m
                await self.db.update_task(
                    task_id,
                    commit=False,
                    current_folder=current_folder,
                    last_message_id=last_id,
                    processed_count=processed,
                    downloaded_count=downloaded,
                    failed_count=failed,
                    skipped_count=int(counters.get("skipped") or 0),
                )
            await self.db.commit()

        if merge_groups:
            cid = counters.get("chat_id")
            await self._merge_tag_folders_after(
                task_id,
                group_dir,
                chat_id=int(cid) if cid is not None else None,
                extra_tag_groups=merge_groups,
                quiet=True,
            )

    async def _pool_reap(
        self,
        pool: dict[str, Any],
        *,
        task_id: int,
        settings,
        group_dir: Path,
        use_text_as_folder: bool,
        counters: dict[str, Any],
        test_mode: bool = False,
        log_prefix: str = "",
        wait: bool = True,
    ) -> bool:
        """
        Wait for progress from resident workers.
        Returns True if the pool is blocked (pause/error).
        """
        if pool.get("blocked"):
            return True
        if not pool.get("started"):
            return False
        if not wait:
            return bool(pool.get("blocked"))
        if int(pool.get("in_flight") or 0) <= 0 and self._pool_pending_count(pool) <= 0:
            return bool(pool.get("blocked"))
        ev: asyncio.Event = pool["done_event"]
        while True:
            if pool.get("blocked"):
                return True
            if int(pool.get("in_flight") or 0) <= 0 and self._pool_pending_count(pool) <= 0:
                return bool(pool.get("blocked"))
            ev.clear()
            try:
                await asyncio.wait_for(ev.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            return bool(pool.get("blocked"))

    async def _pool_drain(
        self,
        pool: dict[str, Any],
        *,
        task_id: int,
        settings,
        group_dir: Path,
        use_text_as_folder: bool,
        counters: dict[str, Any],
        test_mode: bool = False,
        log_prefix: str = "",
    ) -> bool:
        """Finish queued+in-flight work, then stop workers. Returns True if paused/blocked."""
        if not pool.get("started"):
            return bool(pool.get("blocked"))

        # Normal path: let workers drain the queue. Pause/error: stop waiting early.
        while True:
            if pool.get("blocked"):
                break
            if int(pool.get("in_flight") or 0) <= 0 and self._pool_pending_count(pool) <= 0:
                break
            await self._pool_reap(
                pool,
                task_id=task_id,
                settings=settings,
                group_dir=group_dir,
                use_text_as_folder=use_text_as_folder,
                counters=counters,
                test_mode=test_mode,
                log_prefix=log_prefix,
                wait=True,
            )

        # On pause/error, discard jobs that never started
        if pool.get("blocked"):
            while True:
                try:
                    job = pool["queue"].get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    if job is not None:
                        pool["settled"][job.message_id] = False
                finally:
                    pool["queue"].task_done()
            pri = pool.get("priority")
            if pri:
                while pri:
                    job = pri.popleft()
                    if job is not None:
                        pool["settled"][job.message_id] = False
            # Wait for the file currently inside each worker (bounded)
            wait_rounds = 0
            while int(pool.get("in_flight") or 0) > 0:
                wait_rounds += 1
                if wait_rounds > 40:  # ~20s at 0.5s reap timeout
                    logger.warning(
                        "pool drain: in_flight=%s after timeout — forcing stop",
                        pool.get("in_flight"),
                    )
                    break
                await self._pool_reap(
                    pool,
                    task_id=task_id,
                    settings=settings,
                    group_dir=group_dir,
                    use_text_as_folder=use_text_as_folder,
                    counters=counters,
                    test_mode=test_mode,
                    log_prefix=log_prefix,
                    wait=True,
                )

        # Stop resident workers with sentinels (include parked excess workers)
        pool["_stopping"] = True
        n_workers = max(
            len(pool.get("worker_tasks") or {}),
            len(pool.get("workers") or {}),
            int(pool.get("concurrency") or 1),
        )
        # Unpark excess workers so they can receive sentinels
        pool["concurrency"] = max(n_workers, int(pool.get("concurrency") or 1))
        for _ in range(n_workers):
            await pool["queue"].put(None)
        workers = list(pool.get("workers") or [])
        if workers:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*workers, return_exceptions=True),
                    timeout=8.0,
                )
            except asyncio.TimeoutError:
                logger.warning("pool drain: workers did not stop within 8s")
                for w in workers:
                    if not w.done():
                        w.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
        pool["workers"] = []
        pool["worker_tasks"] = {}
        pool["active"] = {}
        pool["priority"] = deque()
        pool["started"] = False
        pool["_stopping"] = False
        pool["_ctx"] = None
        self._live_pools.pop(int(task_id), None)
        blocked = bool(pool.get("blocked"))
        if blocked:
            # Pause: keep frozen progress on the live box
            self._freeze_progress_paused(task_id)
        else:
            # Normal drain between batches: clear worker shell
            with self._progress_lock:
                bucket = self._progress.get(task_id)
                if bucket and int(bucket.get("worker_count") or 0):
                    if not (bucket.get("files") or {}):
                        self._progress.pop(task_id, None)
                    else:
                        bucket["worker_count"] = 0
                        bucket["workers"] = {}
        return blocked

    async def _pool_submit(
        self,
        pool: dict[str, Any],
        job: DownloadJob,
        *,
        task_id: int,
        stop_event: asyncio.Event,
        settings,
        group_dir: Path,
        use_text_as_folder: bool,
        counters: dict[str, Any],
        concurrency: int,
        max_messages: int | None,
        delay_min: float,
        delay_max: float,
        test_mode: bool = False,
        log_prefix: str = "",
    ) -> str:
        """
        Enqueue a job for a resident worker.
        Returns: 'ok' | 'paused' | 'limit'
        """
        # Prefer live task setting so mid-run concurrency edits apply immediately
        try:
            fresh = await self.db.get_task(task_id) or {}
            live_c = int(fresh.get("concurrency") or concurrency or 1)
        except Exception:
            live_c = int(concurrency or pool.get("concurrency") or 1)
        conc = max(1, min(5, live_c))
        await self._apply_pool_concurrency(pool, conc)
        await self._ensure_pool_workers(
            pool,
            task_id=task_id,
            stop_event=stop_event,
            settings=settings,
            group_dir=group_dir,
            use_text_as_folder=use_text_as_folder,
            counters=counters,
            test_mode=test_mode,
            log_prefix=log_prefix,
        )
        waited_for_slot = False
        while True:
            if stop_event.is_set() or pool.get("blocked"):
                return "paused"
            downloaded = int(counters["downloaded"])
            if max_messages and downloaded >= max_messages:
                return "limit"
            limit_cap = int(pool.get("concurrency") or conc)
            if max_messages is not None:
                limit_cap = min(limit_cap, max(0, max_messages - downloaded))
            if limit_cap <= 0:
                return "limit"
            in_flight = int(pool.get("in_flight") or 0)
            pending = self._pool_pending_count(pool)
            if in_flight + pending < limit_cap:
                break
            waited_for_slot = True
            if await self._pool_reap(
                pool,
                task_id=task_id,
                settings=settings,
                group_dir=group_dir,
                use_text_as_folder=use_text_as_folder,
                counters=counters,
                test_mode=test_mode,
                log_prefix=log_prefix,
                wait=True,
            ):
                return "paused"

        if waited_for_slot:
            await self._sleep_delay(delay_min, delay_max, stop_event)
            if stop_event.is_set() or pool.get("blocked"):
                return "paused"

        pool["order"].append(job)
        await pool["queue"].put(job)
        return "ok"

    async def _flush_download_batch(
        self,
        *,
        task_id: int,
        batch: list[DownloadJob],
        stop_event: asyncio.Event,
        settings,
        group_dir: Path,
        use_text_as_folder: bool,
        counters: dict[str, Any],
        test_mode: bool = False,
        log_prefix: str = "",
        concurrency: int | None = None,
        delay_min: float = 0,
        delay_max: float = 0,
        max_messages: int | None = None,
    ) -> dict[str, Any]:
        """Run jobs via N resident workers (queue)."""
        if not batch:
            return {**counters, "paused": False}
        conc = concurrency or len(batch)
        pool = self._new_download_pool(conc)
        for job in batch:
            status = await self._pool_submit(
                pool,
                job,
                task_id=task_id,
                stop_event=stop_event,
                settings=settings,
                group_dir=group_dir,
                use_text_as_folder=use_text_as_folder,
                counters=counters,
                concurrency=conc,
                max_messages=max_messages,
                delay_min=delay_min,
                delay_max=delay_max,
                test_mode=test_mode,
                log_prefix=log_prefix,
            )
            if status == "paused":
                await self._pool_drain(
                    pool,
                    task_id=task_id,
                    settings=settings,
                    group_dir=group_dir,
                    use_text_as_folder=use_text_as_folder,
                    counters=counters,
                    test_mode=test_mode,
                    log_prefix=log_prefix,
                )
                return {**counters, "paused": True}
            if status == "limit":
                break
        paused = await self._pool_drain(
            pool,
            task_id=task_id,
            settings=settings,
            group_dir=group_dir,
            use_text_as_folder=use_text_as_folder,
            counters=counters,
            test_mode=test_mode,
            log_prefix=log_prefix,
        )
        return {**counters, "paused": paused}

    async def _retry_failed_messages(
        self,
        *,
        task_id: int,
        chat_id: int,
        group_dir: Path,
        media_types: set[str],
        current_folder: Optional[str],
        use_text_as_folder: bool,
        min_len: int,
        album_captions: dict,
        stop_event: asyncio.Event,
        counters: dict[str, Any],
        test_mode: bool = False,
        test_deadline: float | None = None,
        concurrency: int = 1,
        file_formats: list | None = None,
        folder_mode: str = "caption",
        delay_min: float = 0.5,
        delay_max: float = 0.5,
        max_messages: int | None = None,
        include_tags: list | None = None,
        caption_keywords: list | None = None,
        tag_match_mode: str = "any",
    ) -> bool:
        """Return False if paused during retry."""
        settings = get_settings()
        file_formats = file_formats or []
        include_tags = include_tags or []
        caption_keywords = caption_keywords or []
        client = await tg_manager.ensure_client()
        failed_ids = await self.db.list_failed_message_ids(task_id)
        if not failed_ids:
            return True

        await self.db.append_log(
            task_id, f"优先重试失败消息 {len(failed_ids)} 条（并发 {concurrency}）"
        )
        concurrency = max(1, min(5, int(concurrency or 1)))

        for chunk in _chunked(failed_ids, 80):
            while True:
                if stop_event.is_set():
                    return False
                try:
                    messages = await client.get_messages(chat_id, ids=chunk)
                    break
                except FloodWaitError as e:
                    if not await self._wait_flood(task_id, e.seconds, stop_event):
                        return False

            if not isinstance(messages, list):
                messages = [messages]

            pool = self._new_download_pool(concurrency)
            for message in messages:
                if self._pool_busy(pool) and await self._pool_reap(
                    pool,
                    task_id=task_id,
                    settings=settings,
                    group_dir=group_dir,
                    use_text_as_folder=use_text_as_folder,
                    counters=counters,
                    test_mode=test_mode,
                    log_prefix="重试成功",
                    wait=False,
                ):
                    await self._pool_drain(
                        pool,
                        task_id=task_id,
                        settings=settings,
                        group_dir=group_dir,
                        use_text_as_folder=use_text_as_folder,
                        counters=counters,
                        test_mode=test_mode,
                        log_prefix="重试成功",
                    )
                    return False
                if stop_event.is_set() or self._test_time_up(test_deadline):
                    if self._pool_busy(pool):
                        await self._pool_drain(
                            pool,
                            task_id=task_id,
                            settings=settings,
                            group_dir=group_dir,
                            use_text_as_folder=use_text_as_folder,
                            counters=counters,
                            test_mode=test_mode,
                            log_prefix="重试成功",
                        )
                    return False
                if max_messages and int(counters.get("downloaded") or 0) >= max_messages:
                    if self._pool_busy(pool):
                        await self._pool_drain(
                            pool,
                            task_id=task_id,
                            settings=settings,
                            group_dir=group_dir,
                            use_text_as_folder=use_text_as_folder,
                            counters=counters,
                            test_mode=test_mode,
                            log_prefix="重试成功",
                        )
                    return True
                if not message:
                    continue
                if await self.db.is_message_done(task_id, message.id):
                    continue
                if not has_media(message):
                    await self.db.mark_message(task_id, message.id, status="done")
                    continue

                media_type = detect_media_type(message)
                if media_type == "sticker":
                    media_type = "document"
                if not media_type or media_type not in media_types:
                    await self.db.mark_message(task_id, message.id, status="done")
                    continue
                if not self._passes_file_filters(task_id, message, media_type):
                    await self.db.mark_message(task_id, message.id, status="done")
                    continue

                caption = await self._ensure_caption(
                    client, chat_id, message, album_captions
                )
                tags = extract_tags(caption or "")
                if not matches_include_tags(
                    tags, include_tags, mode=tag_match_mode
                ) or not matches_caption_keywords(caption, caption_keywords):
                    await self.db.mark_message(task_id, message.id, status="done")
                    counters["skipped"] = int(counters.get("skipped") or 0) + 1
                    counters["processed"] = int(counters.get("processed") or 0) + 1
                    continue
                subdir = resolve_media_subdir(
                    message,
                    album_captions=album_captions,
                    use_caption_folders=use_text_as_folder,
                    group_dir=group_dir if use_text_as_folder else None,
                    folder_mode=folder_mode,
                    tag_folder_map=counters.get("tag_folder_map") or None,
                    tag_blacklist=counters.get("tag_blacklist") or None,
                )
                filename = build_filename(
                    message,
                    media_type,
                    album_captions=album_captions,
                    caption=caption,
                )
                rel_dir = subdir if subdir is not None else (current_folder or "_未分类")
                target_dir = group_dir / rel_dir if rel_dir else group_dir
                target_dir.mkdir(parents=True, exist_ok=True)
                existing = await self._skip_if_already_downloaded(
                    task_id=task_id,
                    chat_id=chat_id,
                    message=message,
                    target_dir=target_dir,
                    filename=filename,
                    settings=settings,
                )
                if existing is not None:
                    counters["downloaded"] = int(counters.get("downloaded") or 0) + 1
                    counters["processed"] = int(counters.get("processed") or 0) + 1
                    try:
                        rel = existing.relative_to(settings.download_dir)
                    except ValueError:
                        rel = existing
                    await self.db.append_log(task_id, f"队列已处理，跳过: {rel}")
                    continue
                target_path, _ = resolve_download_path(
                    target_dir,
                    filename,
                    message.id,
                    self._expected_size(message),
                )
                job = DownloadJob(
                    message=message,
                    message_id=message.id,
                    target_path=target_path,
                    caption=caption or "",
                    media_type=media_type,
                    rel_dir=rel_dir or "",
                    tags=tags,
                )
                status = await self._pool_submit(
                    pool,
                    job,
                    task_id=task_id,
                    stop_event=stop_event,
                    settings=settings,
                    group_dir=group_dir,
                    use_text_as_folder=use_text_as_folder,
                    counters=counters,
                    concurrency=concurrency,
                    max_messages=max_messages,
                    delay_min=delay_min,
                    delay_max=delay_max,
                    test_mode=test_mode,
                    log_prefix="重试成功",
                )
                if status == "paused":
                    await self._pool_drain(
                        pool,
                        task_id=task_id,
                        settings=settings,
                        group_dir=group_dir,
                        use_text_as_folder=use_text_as_folder,
                        counters=counters,
                        test_mode=test_mode,
                        log_prefix="重试成功",
                    )
                    return False
                if status == "limit":
                    await self._pool_drain(
                        pool,
                        task_id=task_id,
                        settings=settings,
                        group_dir=group_dir,
                        use_text_as_folder=use_text_as_folder,
                        counters=counters,
                        test_mode=test_mode,
                        log_prefix="重试成功",
                    )
                    return True

            if await self._pool_drain(
                pool,
                task_id=task_id,
                settings=settings,
                group_dir=group_dir,
                use_text_as_folder=use_text_as_folder,
                counters=counters,
                test_mode=test_mode,
                log_prefix="重试成功",
            ):
                return False

        return True

    async def _download_with_retry(
        self,
        task_id: int,
        message,
        target_path: Path,
        stop_event: asyncio.Event,
        *,
        test_mode: bool = False,
        caption: str = "",
        message_id: Optional[int] = None,
        worker_id: int | None = None,
        yield_event: asyncio.Event | None = None,
    ) -> bool | str:
        """Return True/False, 'paused' (task stop), or 'deferred' (concurrency yield)."""
        settings = get_settings()
        mid = int(message_id or getattr(message, "id", 0) or 0)
        try:
            rel = str(target_path.relative_to(settings.download_dir))
        except ValueError:
            rel = str(target_path)

        def _stop_or_yield() -> str | None:
            if stop_event.is_set():
                return "paused"
            if yield_event is not None and yield_event.is_set():
                return "deferred"
            return None

        async def _combined_stop() -> tuple[asyncio.Event, asyncio.Task | None]:
            """Bridge task-stop + concurrency-yield into one Event for Telethon."""
            if yield_event is None:
                return stop_event, None
            combined = asyncio.Event()
            if stop_event.is_set() or yield_event.is_set():
                combined.set()
                return combined, None

            async def _bridge() -> None:
                t_stop = asyncio.create_task(stop_event.wait())
                t_yield = asyncio.create_task(yield_event.wait())
                try:
                    await asyncio.wait(
                        {t_stop, t_yield}, return_when=asyncio.FIRST_COMPLETED
                    )
                    combined.set()
                finally:
                    for t in (t_stop, t_yield):
                        if not t.done():
                            t.cancel()
                            try:
                                await t
                            except asyncio.CancelledError:
                                pass

            bridge_task = asyncio.create_task(_bridge())
            return combined, bridge_task

        expected_size = 0
        if message.file and getattr(message.file, "size", None):
            expected_size = int(message.file.size)

        if test_mode:
            reason = _stop_or_yield()
            if reason:
                return reason
            try:
                self._begin_file_progress(
                    task_id, mid, rel, total=len(caption or "") or 1, worker_id=worker_id
                )
                target_path.parent.mkdir(parents=True, exist_ok=True)
                body = (
                    "TEST_PLACEHOLDER\n"
                    f"message_id={mid}\n"
                    f"caption={caption or ''}\n"
                )
                data = body.encode("utf-8")
                target_path.write_bytes(data)
                self._on_bytes_progress(task_id, mid, len(data), len(data))
                await asyncio.sleep(0.05)
                reason = _stop_or_yield()
                if reason:
                    self._finish_file_progress(
                        task_id, mid, remove=True, completed=False, worker_id=worker_id
                    )
                    return reason
                self._finish_file_progress(
                    task_id,
                    mid,
                    received=len(data),
                    total=len(data),
                    remove=True,
                    worker_id=worker_id,
                    completed=True,
                )
                return True
            except Exception as e:
                await self.db.append_log(task_id, f"测试占位失败: {e}")
                self._finish_file_progress(task_id, mid, remove=True)
                return False

        # Final guard: file already complete on disk → do not download again
        if file_looks_complete(target_path, expected_size):
            await self.db.append_log(task_id, f"相同文件已存在，跳过: {rel}")
            return True

        retries = max(1, settings.max_retries)
        for attempt in range(1, retries + 1):
            reason = _stop_or_yield()
            if reason:
                self._finish_file_progress(task_id, mid, remove=True)
                if await self._salvage_complete_file(
                    task_id=task_id,
                    message_id=mid,
                    target_path=target_path,
                    expected_size=expected_size,
                    message=message,
                ):
                    await self.db.append_log(task_id, f"暂停前已下完，保留: {rel}")
                    return True
                return reason
            watch_stop = asyncio.Event()
            watch_task: asyncio.Task | None = None
            bridge_task: asyncio.Task | None = None
            try:
                self._begin_file_progress(task_id, mid, rel, total=expected_size, worker_id=worker_id)
                # No "正在下载"/"断点续传" logs — live progress already shows that.

                def _cb(
                    received: int,
                    total: int,
                    _tid=task_id,
                    _mid=mid,
                    _exp=expected_size,
                ) -> None:
                    self._on_bytes_progress(
                        _tid, _mid, int(received or 0), int(total or _exp or 0)
                    )

                tmp_path = self._part_path(target_path)
                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                # Keep incomplete .part in temp for Telegram ranged resume
                if tmp_path.exists():
                    try:
                        psz = tmp_path.stat().st_size
                    except OSError:
                        psz = 0
                    if expected_size and psz >= expected_size > 0:
                        self._move_path(tmp_path, target_path)
                        self._finish_file_progress(
                            task_id,
                            mid,
                            received=expected_size,
                            total=expected_size,
                            remove=True,
                            completed=True,
                        )
                        return True
                    if psz == 0:
                        tmp_path.unlink(missing_ok=True)

                watch_task = asyncio.create_task(
                    self._watch_part_file(
                        task_id, mid, tmp_path, watch_stop, expected_size
                    )
                )
                combined_stop, bridge_task = await _combined_stop()
                try:
                    result = await asyncio.wait_for(
                        tg_manager.download_media_to(
                            message,
                            tmp_path,
                            progress_callback=_cb,
                            resume=True,
                            stop_event=combined_stop,
                        ),
                        timeout=7200,
                    )
                finally:
                    watch_stop.set()
                    if bridge_task and not bridge_task.done():
                        bridge_task.cancel()
                        try:
                            await bridge_task
                        except asyncio.CancelledError:
                            pass
                    if watch_task and not watch_task.done():
                        watch_task.cancel()
                        try:
                            await watch_task
                        except asyncio.CancelledError:
                            pass

                if not result or not result.exists():
                    raise IOError("empty download result")
                actual_size = result.stat().st_size
                if actual_size <= 0:
                    result.unlink(missing_ok=True)
                    raise IOError("empty download result (0 bytes)")
                if expected_size and actual_size != expected_size:
                    # Keep .part in temp for resume — do not delete on size mismatch
                    raise IOError(
                        f"size mismatch: got {actual_size}, expected {expected_size}"
                    )
                # Finished in temp → promote into downloads/
                self._move_path(result, target_path)
                actual_size = target_path.stat().st_size
                if actual_size <= 0:
                    target_path.unlink(missing_ok=True)
                    raise IOError("empty file after move")
                if expected_size and actual_size != expected_size:
                    # move back to temp part so resume can continue
                    try:
                        self._move_path(target_path, tmp_path)
                    except OSError:
                        target_path.unlink(missing_ok=True)
                    raise IOError(
                        f"size mismatch: got {actual_size}, expected {expected_size}"
                    )
                self._finish_file_progress(
                    task_id,
                    mid,
                    received=actual_size,
                    total=expected_size or actual_size,
                    remove=True,
                    completed=True,
                )
                return True
            except DownloadPaused:
                watch_stop.set()
                if bridge_task and not bridge_task.done():
                    bridge_task.cancel()
                if watch_task:
                    watch_task.cancel()
                # Preserve real stop-time bytes (do not bump bar to 100%)
                self._finish_file_progress(task_id, mid, remove=True, completed=False)
                if await self._salvage_complete_file(
                    task_id=task_id,
                    message_id=mid,
                    target_path=target_path,
                    expected_size=expected_size,
                    message=message,
                ):
                    await self.db.append_log(task_id, f"暂停前已下完，保留: {rel}")
                    return True
                reason = _stop_or_yield() or "paused"
                if reason == "deferred":
                    await self.db.append_log(
                        task_id, f"并发让出，保留进度: {rel}（优先续传）"
                    )
                    return "deferred"
                await self.db.append_log(
                    task_id, f"已暂停，保留进度: {rel}（可继续续传）"
                )
                return "paused"
            except asyncio.CancelledError:
                watch_stop.set()
                if bridge_task and not bridge_task.done():
                    bridge_task.cancel()
                if watch_task:
                    watch_task.cancel()
                self._finish_file_progress(task_id, mid, remove=True, completed=False)
                if await self._salvage_complete_file(
                    task_id=task_id,
                    message_id=mid,
                    target_path=target_path,
                    expected_size=expected_size,
                    message=message,
                ):
                    await self.db.append_log(task_id, f"中断前已下完，保留: {rel}")
                    return True
                raise
            except FloodWaitError as e:
                watch_stop.set()
                if bridge_task and not bridge_task.done():
                    bridge_task.cancel()
                if watch_task:
                    watch_task.cancel()
                self._finish_file_progress(task_id, mid, remove=True, completed=False)
                if not await self._wait_flood(task_id, e.seconds, stop_event):
                    if await self._salvage_complete_file(
                        task_id=task_id,
                        message_id=mid,
                        target_path=target_path,
                        expected_size=expected_size,
                        message=message,
                    ):
                        return True
                    return "paused"
            except Exception as e:
                watch_stop.set()
                if bridge_task and not bridge_task.done():
                    bridge_task.cancel()
                if watch_task:
                    watch_task.cancel()
                self._finish_file_progress(task_id, mid, remove=True, completed=False)
                await self.db.append_log(
                    task_id, f"下载重试 {attempt}/{retries} (msg {mid}): {e}"
                )
                await asyncio.sleep(min(30, 2**attempt))
        self._finish_file_progress(task_id, mid, remove=True, completed=False)
        return False


scheduler = DownloadScheduler()
