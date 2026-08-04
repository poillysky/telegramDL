import time
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api.deps import require_web_auth
from app.config import get_settings
from app.db import db, normalize_download_mode, normalize_file_formats
from app.downloader import scheduler
from app.organizer import normalize_keyword_list, normalize_tag_list
from app.telegram_client import tg_manager

router = APIRouter(prefix="/api/tasks", tags=["tasks"])

DEFAULT_MEDIA = ["photo", "video", "document", "audio", "voice", "video_note"]


class CreateTaskBody(BaseModel):
    chat_id: int | str
    chat_title: str = ""
    media_types: List[str] = Field(default_factory=lambda: list(DEFAULT_MEDIA))
    use_text_as_folder: bool = True
    min_folder_title_len: int = 2
    start_message_id: int = 0
    end_message_id: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    download_order: str = "added_first"  # added_first | oldest_first | newest_first
    test_mode: bool = False  # 不下载完整文件，只测文案建夹
    concurrency: int = Field(default=2, ge=1, le=5)  # 官方 large_queue=2 / small_queue=5
    file_formats: Any = Field(default_factory=list)  # list or {video:["mp4"],...}
    min_file_bytes: int = Field(default=0, ge=0)
    max_file_bytes: int = Field(default=0, ge=0)
    max_messages: Optional[int] = Field(default=None, ge=1)  # 最多下载条数
    delay_min: float = Field(default=0.5, ge=0)
    delay_max: float = Field(default=0.5, ge=0)
    folder_mode: str = "tag"  # always #标签 (legacy caption/media_type/flat ignored)
    include_tags: List[str] = Field(default_factory=list)  # e.g. ["风流狗尾巴","1"] empty=all
    caption_keywords: List[str] = Field(default_factory=list)  # caption substring filter
    tag_match_mode: str = "any"  # any | all
    download_mode: str = "sequential"  # sequential | monitor (legacy: all/date/tags)
    use_index: bool = False
    auto_start: bool = True


class BatchCreateBody(BaseModel):
    chats: List[dict] = Field(default_factory=list)  # [{chat_id, chat_title}]
    media_types: List[str] = Field(default_factory=lambda: list(DEFAULT_MEDIA))
    use_text_as_folder: bool = True
    min_folder_title_len: int = 2
    start_message_id: int = 0
    end_message_id: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    download_order: str = "added_first"
    test_mode: bool = False
    concurrency: int = Field(default=2, ge=1, le=5)
    file_formats: Any = Field(default_factory=list)
    min_file_bytes: int = Field(default=0, ge=0)
    max_file_bytes: int = Field(default=0, ge=0)
    max_messages: Optional[int] = Field(default=None, ge=1)
    delay_min: float = Field(default=0.5, ge=0)
    delay_max: float = Field(default=0.5, ge=0)
    folder_mode: str = "tag"
    include_tags: List[str] = Field(default_factory=list)
    caption_keywords: List[str] = Field(default_factory=list)
    tag_match_mode: str = "any"
    download_mode: str = "sequential"
    use_index: bool = False
    auto_start: bool = True


# task_id -> (monotonic_ts, tag_match_count)
_tag_match_cache: dict[int, tuple[float, int]] = {}
_tag_match_refreshing: set[int] = set()
_TAG_MATCH_TTL = 12.0  # seconds — avoid heavy index counts on every 0.8s poll
# task_id -> (monotonic_ts, tag_done_count) — chat_completed / local aware
_tag_done_cache: dict[int, tuple[float, int]] = {}
_tag_done_refreshing: set[int] = set()
# chat_id -> (monotonic_ts, media_count)
_index_count_cache: dict[str, tuple[float, int]] = {}
_INDEX_COUNT_TTL = 20.0
# task_id -> chat_id (for cache invalidation after index scan)
_task_chat_map: dict[int, str] = {}
_local_sync_at: dict[str, float] = {}
_LOCAL_SYNC_TTL = 60.0


async def _chat_index_media_count(chat_id) -> int:
    """Local caption-index size for this chat (meta, with COUNT fallback)."""
    import time

    key = str(chat_id or "").strip()
    if not key:
        return 0
    now = time.monotonic()
    cached = _index_count_cache.get(key)
    if cached and (now - cached[0]) < _INDEX_COUNT_TTL and int(cached[1]) > 0:
        return int(cached[1])
    n = 0
    try:
        meta = await db.get_index_meta(key) or {}
        n = int(meta.get("media_count") or 0)
    except Exception:
        n = 0
    # Meta can lag / miss; COUNT is authoritative when meta says 0
    if n <= 0:
        try:
            n = await db.count_media_index(key)
        except Exception:
            n = 0
    _index_count_cache[key] = (now, n)
    return n


def invalidate_index_count_cache(chat_id=None) -> None:
    """Drop cached index totals and match/done counts (call after index scan)."""
    try:
        db.invalidate_tag_groups_cache(chat_id)
    except Exception:
        pass
    if chat_id is None:
        _index_count_cache.clear()
        _tag_match_cache.clear()
        _tag_done_cache.clear()
        return
    key = str(chat_id)
    _index_count_cache.pop(key, None)
    for tid, cid in list(_task_chat_map.items()):
        if str(cid) == key:
            _tag_match_cache.pop(tid, None)
            _tag_done_cache.pop(tid, None)


async def _refresh_tag_match_count(
    tid: int,
    chat_id,
    tags: list,
    keywords: list,
    media_types: list,
    mode: str,
    *,
    download_mode: str = "sequential",
) -> None:
    """Background COUNT — never block /api/tasks first paint."""
    import asyncio
    import time

    if tid in _tag_match_refreshing:
        return
    _tag_match_refreshing.add(tid)
    try:
        # Monitor with no tags/keywords must not treat whole index as "hits"
        if normalize_download_mode(download_mode) == "monitor" and not tags and not keywords:
            tag_match = 0
        else:
            tag_match = await db.count_media_index_filtered(
                chat_id,
                tags=tags or None,
                tag_match_mode=mode,
                media_types=media_types or None,
                keywords=keywords or None,
            )
        _tag_match_cache[tid] = (time.monotonic(), int(tag_match))
    except Exception:
        pass
    finally:
        _tag_match_refreshing.discard(tid)


async def _refresh_tag_done_count(
    tid: int,
    chat_id,
    tags: list,
    keywords: list,
    media_types: list,
    mode: str,
    *,
    download_mode: str = "sequential",
) -> None:
    """Background done COUNT against chat_completed + task records."""
    import time

    if tid in _tag_done_refreshing:
        return
    _tag_done_refreshing.add(tid)
    try:
        if normalize_download_mode(download_mode) == "monitor" and not tags and not keywords:
            tag_done = 0
        else:
            tag_done = await db.count_index_done(
                tid,
                chat_id,
                tags=tags or None,
                tag_match_mode=mode,
                media_types=media_types or None,
                keywords=keywords or None,
            )
        _tag_done_cache[tid] = (time.monotonic(), int(tag_done))
    except Exception:
        pass
    finally:
        _tag_done_refreshing.discard(tid)


async def _maybe_sync_local_completed(task: dict, *, force: bool = False) -> None:
    """Throttle local-dir → chat_completed sync (queue/已处理 use disk, not only task records)."""
    import time

    from app.config import get_settings
    from app.organizer import sanitize_name

    chat_id = task.get("chat_id")
    if chat_id is None:
        return
    key = str(chat_id)
    now = time.monotonic()
    prev = _local_sync_at.get(key) or 0.0
    if not force and now - prev < _LOCAL_SYNC_TTL:
        return
    _local_sync_at[key] = now
    title = (task.get("chat_title") or "").strip() or str(chat_id)
    group_dir = get_settings().download_dir / sanitize_name(title)
    try:
        await db.sync_local_completed_from_dir(chat_id, group_dir)
        # Done counts may change after disk sync
        tid = int(task.get("id") or 0)
        if tid:
            _tag_done_cache.pop(tid, None)
            _tag_match_cache.pop(tid, None)
    except Exception:
        pass


async def _with_tag_progress(task: dict) -> dict:
    """Attach tag-match / processed counts (stale-while-revalidate, non-blocking)."""
    import asyncio
    import time

    chat_id = task.get("chat_id")
    # Stored tags already expanded when saved
    tags = normalize_tag_list(task.get("include_tags") or [])
    keywords = normalize_keyword_list(task.get("caption_keywords") or [])
    media_types = list(task.get("media_types") or [])
    mode = str(task.get("tag_match_mode") or "any")
    download_mode = normalize_download_mode(task.get("download_mode") or "")
    tid = int(task["id"])
    if chat_id is not None:
        _task_chat_map[tid] = str(chat_id)

    now = time.monotonic()
    # Monitor + 未选标签：命中/队列/已处理应为 0
    if download_mode == "monitor" and not tags and not keywords:
        tag_match = 0
        tag_done = 0
        _tag_match_cache[tid] = (now, 0)
        _tag_done_cache[tid] = (now, 0)
    else:
        cached = _tag_match_cache.get(tid)
        if cached and (now - cached[0]) < _TAG_MATCH_TTL:
            tag_match = cached[1]
        else:
            tag_match = int(cached[1]) if cached else 0
            try:
                asyncio.get_running_loop().create_task(
                    _refresh_tag_match_count(
                        tid,
                        chat_id,
                        tags,
                        keywords,
                        media_types,
                        mode,
                        download_mode=download_mode,
                    )
                )
            except RuntimeError:
                pass

        done_cached = _tag_done_cache.get(tid)
        if done_cached and (now - done_cached[0]) < _TAG_MATCH_TTL:
            tag_done = done_cached[1]
        else:
            # Prefer last known done; fall back to task counter until refresh lands
            tag_done = (
                int(done_cached[1])
                if done_cached
                else int(task.get("downloaded_count") or 0)
            )
            try:
                asyncio.get_running_loop().create_task(
                    _refresh_tag_done_count(
                        tid,
                        chat_id,
                        tags,
                        keywords,
                        media_types,
                        mode,
                        download_mode=download_mode,
                    )
                )
            except RuntimeError:
                pass
        # Opportunistic local sync (throttled) so recreate-task sees disk files
        try:
            asyncio.get_running_loop().create_task(_maybe_sync_local_completed(task))
        except RuntimeError:
            pass

    # Truncate log for list payload — full log is huge and slows first paint.
    # last_log is newest-first, so keep the HEAD (not the tail). Tail-trim used
    # to chop "[HH:MM:SS]" off the newest line → UI showed「无时间」+ half a filename.
    last_log = task.get("last_log") or ""
    if isinstance(last_log, str) and len(last_log) > 2500:
        chunk = last_log[:2500]
        nl = chunk.rfind("\n")
        if nl >= 800:
            chunk = chunk[:nl]
        last_log = chunk

    out = {
        **task,
        "last_log": last_log,
        "tag_match_count": tag_match,
        "tag_processed_count": tag_done,
        "index_media_count": await _chat_index_media_count(chat_id),
    }
    prog = scheduler.get_live_progress(tid)
    if prog:
        out = {**out, "live": prog}
    return out


def _task_list_snapshot(task: dict) -> dict:
    """Fast task payload for settings save — cache only, no DB COUNT / index probes."""
    tid = int(task["id"])
    chat_id = task.get("chat_id")
    if chat_id is not None:
        _task_chat_map[tid] = str(chat_id)

    match_cached = _tag_match_cache.get(tid)
    done_cached = _tag_done_cache.get(tid)
    tag_match = int(match_cached[1]) if match_cached else 0
    tag_done = (
        int(done_cached[1])
        if done_cached
        else int(task.get("downloaded_count") or 0)
    )
    key = str(chat_id or "").strip()
    idx_cached = _index_count_cache.get(key) if key else None
    index_n = int(idx_cached[1]) if idx_cached else int(
        (task.get("index_media_count") or 0)
    )

    last_log = task.get("last_log") or ""
    if isinstance(last_log, str) and len(last_log) > 2500:
        chunk = last_log[:2500]
        nl = chunk.rfind("\n")
        if nl >= 800:
            chunk = chunk[:nl]
        last_log = chunk

    out = {
        **task,
        "last_log": last_log,
        "tag_match_count": tag_match,
        "tag_processed_count": tag_done,
        "index_media_count": index_n,
    }
    prog = scheduler.get_live_progress(tid)
    if prog:
        out = {**out, "live": prog}
    return out


_last_heal_mono: float = 0.0


@router.get("")
async def list_tasks(_: None = Depends(require_web_auth)):
    global _last_heal_mono
    # Unstick「继续」when DB says running but worker is dead — not every poll tick
    now = time.monotonic()
    if now - _last_heal_mono >= 15.0:
        _last_heal_mono = now
        await scheduler.heal_stale_running(quiet=True)
    tasks = await db.list_tasks()
    # Sequential on shared SQLite connection (gather can pile up waits)
    out = [await _with_tag_progress(t) for t in tasks]
    return {"ok": True, "tasks": out}


@router.get("/settings/defaults")
async def get_settings_defaults(_: None = Depends(require_web_auth)):
    s = get_settings()
    parallel = await db.get_max_parallel_chats()
    return {
        "ok": True,
        "settings": {
            "max_parallel_chats": parallel,
            "download_delay": s.download_delay,
            "download_delay_min": s.download_delay_min,
            "download_delay_max": s.download_delay_max,
            "max_retries": s.max_retries,
            "min_folder_title_len": s.min_folder_title_len,
            "max_flood_wait": await db.get_max_flood_wait(),
            "failed_retry_interval_sec": await db.get_failed_retry_interval_sec(),
            "monitor_heartbeat_sec": await db.get_monitor_heartbeat_sec(),
            "download_dir": str(s.download_dir),
            "test_mode": s.test_mode,
            "test_duration_sec": s.test_duration_sec,
        },
    }


def _task_payload_from_body(body: CreateTaskBody | BatchCreateBody, chat_id, title: str) -> dict:
    media = [m for m in body.media_types if m in DEFAULT_MEDIA]
    if not media:
        media = list(DEFAULT_MEDIA)
    delay_min = float(body.delay_min)
    delay_max = float(body.delay_max)
    if delay_max < delay_min:
        delay_max = delay_min
    folder_mode = "tag"
    use_text = True
    download_mode = normalize_download_mode(body.download_mode)
    use_index = bool(body.use_index) or download_mode == "monitor"
    return {
        "chat_id": chat_id,
        "chat_title": title,
        "media_types": media,
        "use_text_as_folder": use_text,
        "min_folder_title_len": body.min_folder_title_len,
        "start_message_id": body.start_message_id,
        "end_message_id": body.end_message_id,
        "start_date": body.start_date,
        "end_date": body.end_date,
        "download_order": (
            body.download_order
            if body.download_order
            in ("added_first", "newest_first", "oldest_first")
            else "added_first"
        ),
        # 仅以任务参数为准；全局 TEST_MODE 只通过 /settings/defaults 给前端默认勾选
        "test_mode": bool(body.test_mode),
        "concurrency": max(1, min(5, int(body.concurrency or 2))),
        "file_formats": normalize_file_formats(body.file_formats),
        "min_file_bytes": max(0, int(body.min_file_bytes or 0)),
        "max_file_bytes": max(0, int(body.max_file_bytes or 0)),
        "max_messages": body.max_messages,
        "delay_min": delay_min,
        "delay_max": delay_max,
        "folder_mode": folder_mode,
        "include_tags": normalize_tag_list(body.include_tags),
        "caption_keywords": normalize_keyword_list(body.caption_keywords),
        "tag_match_mode": "any",
        "download_mode": download_mode,
        "use_index": use_index,
    }


@router.post("")
async def create_task(body: CreateTaskBody, _: None = Depends(require_web_auth)):
    title = body.chat_title
    if not title:
        try:
            title = await tg_manager.get_chat_title(body.chat_id)
        except Exception:
            title = str(body.chat_id)

    task = await db.create_task(_task_payload_from_body(body, body.chat_id, title))
    if body.auto_start:
        await scheduler.start_task(task["id"])
        task = await db.get_task(task["id"])
    return {"ok": True, "task": task}


@router.post("/batch")
async def create_tasks_batch(body: BatchCreateBody, _: None = Depends(require_web_auth)):
    if not body.chats:
        return {"ok": False, "message": "请至少选择一个群组"}
    created = []
    for item in body.chats[:30]:
        chat_id = item.get("chat_id") or item.get("id")
        if chat_id is None:
            continue
        title = item.get("chat_title") or item.get("title") or ""
        if not title:
            try:
                title = await tg_manager.get_chat_title(chat_id)
            except Exception:
                title = str(chat_id)
        task = await db.create_task(_task_payload_from_body(body, chat_id, title))
        if body.auto_start:
            await scheduler.start_task(task["id"])
            task = await db.get_task(task["id"])
        created.append(task)
    return {"ok": True, "tasks": created, "count": len(created)}


@router.get("/{task_id}")
async def get_task(task_id: int, _: None = Depends(require_web_auth)):
    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    return {"ok": True, "task": await _with_tag_progress(task)}


class UpdateTaskTagsBody(BaseModel):
    include_tags: List[str] = Field(default_factory=list)
    tag_match_mode: str = "any"
    expand_related: bool = True


class UpdateTaskSettingsBody(BaseModel):
    include_tags: Optional[List[str]] = None
    tag_match_mode: str = "any"
    expand_related: bool = True
    caption_keywords: Optional[List[str]] = None
    concurrency: Optional[int] = Field(default=None, ge=1, le=5)
    delay_min: Optional[float] = Field(default=None, ge=0)
    delay_max: Optional[float] = Field(default=None, ge=0)
    media_types: Optional[List[str]] = None
    folder_mode: Optional[str] = None
    use_text_as_folder: Optional[bool] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    clear_start_date: bool = False
    clear_end_date: bool = False
    download_order: Optional[str] = None
    max_messages: Optional[int] = Field(default=None, ge=1)
    clear_max_messages: bool = False
    file_formats: Optional[Any] = None
    min_file_bytes: Optional[int] = Field(default=None, ge=0)
    max_file_bytes: Optional[int] = Field(default=None, ge=0)


@router.patch("/{task_id}/tags")
async def update_task_tags(
    task_id: int,
    body: UpdateTaskTagsBody,
    _: None = Depends(require_web_auth),
):
    """Update monitor tags; new tags auto-start index backlog download."""
    return await update_task_settings(
        task_id,
        UpdateTaskSettingsBody(
            include_tags=body.include_tags,
            tag_match_mode=body.tag_match_mode,
            expand_related=body.expand_related,
        ),
        _,
    )


@router.patch("/{task_id}/settings")
async def update_task_settings(
    task_id: int,
    body: UpdateTaskSettingsBody,
    _: None = Depends(require_web_auth),
):
    """Update tags / concurrency / delay. Monitor: tag changes immediately re-check gaps."""
    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")

    old_tags = normalize_tag_list(task.get("include_tags") or [])
    old_kws = normalize_keyword_list(task.get("caption_keywords") or [])

    fields: dict = {}
    log_bits: list[str] = []

    if body.include_tags is not None:
        tags = normalize_tag_list(body.include_tags)
        mode = "any"
        # Skip write when unchanged (common: only concurrency / delay / media).
        # Do NOT expand related into stored tags — worker expands at query time.
        same_tags = {t.lower() for t in tags} == {t.lower() for t in old_tags}
        if not same_tags:
            # 监控任务允许清空标签（仅索引）；有标签时才按标签下载
            fields["include_tags"] = tags
            fields["tag_match_mode"] = mode
            joined = " ".join(f"#{t}" for t in tags) if tags else "（无）"
            log_bits.append(f"标签 {joined}")

    if body.caption_keywords is not None:
        kws = normalize_keyword_list(body.caption_keywords)
        fields["caption_keywords"] = kws
        log_bits.append("关键词 " + ("、".join(kws) if kws else "（无）"))

    if body.concurrency is not None:
        conc = max(1, min(5, int(body.concurrency)))
        old_conc = max(1, min(5, int(task.get("concurrency") or 1)))
        if conc != old_conc:
            fields["concurrency"] = conc
            log_bits.append(f"并发 {conc}")
            concurrency_changed = True
        else:
            concurrency_changed = False
    else:
        concurrency_changed = False

    if body.delay_min is not None or body.delay_max is not None:
        dmin = float(
            body.delay_min
            if body.delay_min is not None
            else (task.get("delay_min") if task.get("delay_min") is not None else 0.5)
        )
        dmax = float(
            body.delay_max
            if body.delay_max is not None
            else (task.get("delay_max") if task.get("delay_max") is not None else dmin)
        )
        if dmax < dmin:
            dmax = dmin
        old_dmin = float(
            task.get("delay_min") if task.get("delay_min") is not None else 0.5
        )
        old_dmax = float(
            task.get("delay_max") if task.get("delay_max") is not None else old_dmin
        )
        if abs(dmin - old_dmin) > 1e-9 or abs(dmax - old_dmax) > 1e-9:
            fields["delay_min"] = dmin
            fields["delay_max"] = dmax
            log_bits.append(f"延迟 {dmin:g}–{dmax:g}s")

    if body.media_types is not None:
        allowed = {"photo", "video", "document", "audio", "voice", "video_note"}
        media = [str(x) for x in body.media_types if str(x) in allowed]
        if not media:
            media = list(DEFAULT_MEDIA)
        old_media = [str(x) for x in (task.get("media_types") or [])]
        if set(media) != set(old_media) or len(media) != len(old_media):
            fields["media_types"] = media
            media_zh = {
                "photo": "图片",
                "video": "视频",
                "document": "文件",
                "audio": "音频",
                "voice": "语音",
                "video_note": "圆视频",
            }
            log_bits.append("媒体 " + "、".join(media_zh.get(m, m) for m in media))

    if body.folder_mode is not None or body.use_text_as_folder is not None:
        # Single layout only — keep #标签 folders; ignore client mode switches
        old_folder = str(task.get("folder_mode") or "tag")
        old_use = bool(task.get("use_text_as_folder"))
        if old_folder != "tag" or not old_use:
            fields["folder_mode"] = "tag"
            fields["use_text_as_folder"] = True
            log_bits.append("按标签建目录")

    if body.clear_start_date:
        fields["start_date"] = None
        log_bits.append("清空起始日期")
    elif body.start_date is not None:
        sd = str(body.start_date).strip() or None
        fields["start_date"] = sd
        log_bits.append(f"起始 {sd}" if sd else "清空起始日期")

    if body.clear_end_date:
        fields["end_date"] = None
        log_bits.append("清空结束日期")
    elif body.end_date is not None:
        ed = str(body.end_date).strip() or None
        fields["end_date"] = ed
        log_bits.append(f"结束 {ed}" if ed else "清空结束日期")

    if body.download_order is not None:
        order = str(body.download_order or "added_first")
        if order not in ("added_first", "oldest_first", "newest_first"):
            order = "added_first"
        old_order = str(task.get("download_order") or "added_first")
        if order != old_order:
            fields["download_order"] = order
            order_zh = {
                "added_first": "先入库先下",
                "oldest_first": "从旧到新",
                "newest_first": "从新到旧",
            }
            log_bits.append(order_zh.get(order, "方向已改"))

    if body.clear_max_messages:
        fields["max_messages"] = None
        log_bits.append("数量不限")
    elif body.max_messages is not None:
        fields["max_messages"] = int(body.max_messages)
        log_bits.append(f"上限 {fields['max_messages']}")

    if body.file_formats is not None:
        formats = normalize_file_formats(body.file_formats)
        fields["file_formats"] = formats
        if isinstance(formats, dict):
            n = sum(len(v or []) for v in formats.values())
        else:
            n = len(formats or [])
        log_bits.append(f"扩展名 {n or '全部'}")

    if body.min_file_bytes is not None:
        fields["min_file_bytes"] = max(0, int(body.min_file_bytes))
        mb = fields["min_file_bytes"]
        if mb >= 1024 * 1024:
            log_bits.append(f"最小 {mb / (1024 * 1024):g}MB")
        elif mb >= 1024:
            log_bits.append(f"最小 {mb / 1024:g}KB")
        else:
            log_bits.append(f"最小 {mb}B" if mb else "最小不限")
    if body.max_file_bytes is not None:
        fields["max_file_bytes"] = max(0, int(body.max_file_bytes))
        mb = fields["max_file_bytes"]
        if mb <= 0:
            log_bits.append("最大不限")
        elif mb >= 1024 * 1024:
            log_bits.append(f"最大 {mb / (1024 * 1024):g}MB")
        elif mb >= 1024:
            log_bits.append(f"最大 {mb / 1024:g}KB")
        else:
            log_bits.append(f"最大 {mb}B")

    if not fields:
        return {"ok": True, "task": _task_list_snapshot(task)}

    await db.update_task(task_id, **fields)
    # Log in background — append_log contends with downloaders on SQLite
    settings_line = "已改设置 · " + " · ".join(log_bits) if log_bits else "已改设置"
    try:
        import asyncio

        asyncio.get_running_loop().create_task(
            db.append_log(task_id, settings_line)
        )
    except Exception:
        await db.append_log(task_id, settings_line)
    _tag_match_cache.pop(int(task_id), None)
    _tag_done_cache.pop(int(task_id), None)
    task = await db.get_task(task_id)

    # Hot-apply concurrency without blocking the HTTP response
    if concurrency_changed:
        try:
            import asyncio

            conc_now = int(task.get("concurrency") or 1)

            async def _hot_conc() -> None:
                try:
                    await scheduler.apply_live_concurrency(task_id, conc_now)
                except Exception:
                    pass

            asyncio.get_running_loop().create_task(_hot_conc())
        except Exception:
            pass

    # Hot-apply file filters / wake monitor without restarting the worker
    try:
        scheduler.apply_live_task_settings(task_id, task)
    except Exception:
        try:
            scheduler.wake_local_monitor(task.get("chat_id"))
        except Exception:
            pass

    # Monitor: tag/keyword change → soft wake when running; never flash pending if paused
    mode = normalize_download_mode(task.get("download_mode") or "")
    if mode == "monitor" and (
        body.include_tags is not None or body.caption_keywords is not None
    ):
        new_tags = normalize_tag_list(task.get("include_tags") or [])
        new_kws = normalize_keyword_list(task.get("caption_keywords") or [])
        changed = set(new_tags) != set(old_tags) or set(new_kws) != set(old_kws)
        status_now = str(task.get("status") or "").lower()
        if changed and (new_tags or new_kws):
            try:
                import asyncio

                async def _kick_tags() -> None:
                    try:
                        chat_id = task.get("chat_id")
                        # Running worker → wake only (no pause / pending)
                        if scheduler.is_worker_alive(task_id):
                            await db.append_log(task_id, "标签已更新，开始补下")
                            scheduler.wake_local_monitor(chat_id)
                            return
                        # Stale "running" without worker → soft restart, stay running
                        if status_now == "running":
                            await db.append_log(task_id, "标签已更新，开始补下")
                            await db.update_task(
                                task_id, status="running", last_error=None
                            )
                            await scheduler.start_task(task_id)
                            return
                        # Paused / completed / failed: persist only; user clicks 继续
                        await db.append_log(
                            task_id, "标签已更新（点继续后按新标签补下）"
                        )
                    except Exception:
                        pass

                asyncio.get_running_loop().create_task(_kick_tags())
            except Exception:
                if scheduler.is_worker_alive(task_id):
                    await db.append_log(task_id, "标签已更新，开始补下")
                    scheduler.wake_local_monitor(task.get("chat_id"))
                elif status_now == "running":
                    await db.append_log(task_id, "标签已更新，开始补下")
                    await db.update_task(task_id, status="running", last_error=None)
                    await scheduler.start_task(task_id)
                    task = await db.get_task(task_id)
                else:
                    await db.append_log(
                        task_id, "标签已更新（点继续后按新标签补下）"
                    )
        elif changed:
            try:
                import asyncio

                asyncio.get_running_loop().create_task(
                    db.append_log(task_id, "标签已清空，等待配置")
                )
            except Exception:
                await db.append_log(task_id, "标签已清空，等待配置")

    return {"ok": True, "task": _task_list_snapshot(task)}


@router.post("/{task_id}/start")
async def start_task(task_id: int, _: None = Depends(require_web_auth)):
    import asyncio

    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    await scheduler.heal_stale_running(task_id)
    prev = str(task.get("status") or "")
    mon = normalize_download_mode(task.get("download_mode") or "") == "monitor"
    # Mark running immediately so UI never sticks on「等待中」
    await db.update_task(task_id, status="running", last_error=None)
    if mon:
        boot_msg = "正在恢复监控…" if prev == "paused" else "正在启动监控…"
    else:
        boot_msg = "正在继续下载…" if prev == "paused" else "正在启动下载…"
    await db.append_log(task_id, boot_msg)
    await scheduler.start_task(task_id)
    # Brief wait for worker to settle — keep short so UI stays responsive
    for _ in range(15):
        await asyncio.sleep(0.1)
        task = await db.get_task(task_id)
        if task and task.get("status") in ("running", "paused", "completed", "failed"):
            break
    task = await db.get_task(task_id)
    return {"ok": True, "task": task}


@router.post("/{task_id}/pause")
async def pause_task(task_id: int, _: None = Depends(require_web_auth)):
    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    mon = normalize_download_mode(task.get("download_mode") or "") == "monitor"
    await scheduler.pause_task(task_id)
    await db.append_log(task_id, "已暂停监控" if mon else "已暂停下载")
    task = await db.get_task(task_id)
    return {"ok": True, "task": task}


@router.post("/{task_id}/clear-log")
async def clear_task_log(task_id: int, _: None = Depends(require_web_auth)):
    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    await db.clear_log(task_id)
    task = await db.get_task(task_id)
    return {"ok": True, "task": task}


@router.post("/{task_id}/reorganize-local")
async def reorganize_task_local(task_id: int, _: None = Depends(require_web_auth)):
    """
    Adjust existing download/temp folders for this task to #标签 layout:
    merge related tags, flatten date subdirs, collapse legacy media-type dirs.
    """
    from app.downloader import scheduler

    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    try:
        result = await scheduler.reorganize_local(task_id)
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    except LookupError as e:
        raise HTTPException(404, str(e)) from e
    except Exception as e:
        raise HTTPException(500, f"整理失败: {e}") from e
    task = await db.get_task(task_id)
    return {"ok": True, "task": task, **(result or {})}


@router.get("/{task_id}/queue")
async def get_task_queue(
    task_id: int,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _: None = Depends(require_web_auth),
):
    """Backward-compatible alias → kind=queue."""
    return await get_task_files(task_id, kind="queue", limit=limit, offset=offset, _=_)


@router.get("/{task_id}/files")
async def get_task_files(
    task_id: int,
    kind: str = Query(default="queue"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _: None = Depends(require_web_auth),
):
    """
    kind:
      - matches: 标签命中（索引匹配）
      - done: 已处理（本群本地/完成记录，不单看本任务队列）
      - queue: 待下载（匹配且本地未完成）+ 正在下载
    """
    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    kind_n = (kind or "queue").strip().lower()
    if kind_n not in ("matches", "done", "queue"):
        raise HTTPException(400, "kind 须为 matches / done / queue")

    tags = normalize_tag_list(task.get("include_tags") or [])
    keywords = normalize_keyword_list(task.get("caption_keywords") or [])
    media_types = list(task.get("media_types") or [])
    mode = str(task.get("tag_match_mode") or "any")
    order = (task.get("download_order") or "added_first").strip()
    newest_first = order == "newest_first"
    download_mode = normalize_download_mode(task.get("download_mode") or "")
    # Monitor without filters: not a download queue (whole index ≠ hits)
    no_monitor_filters = download_mode == "monitor" and not tags and not keywords

    # Refresh chat_completed from local filenames before queue/done listing
    if kind_n in ("done", "queue") and not no_monitor_filters:
        await _maybe_sync_local_completed(task, force=True)

    active_files: list = []
    if kind_n == "done":
        items, total = await db.list_task_done_items(
            task_id,
            task["chat_id"],
            tags=tags or None,
            tag_match_mode=mode,
            media_types=media_types or None,
            keywords=keywords or None,
            limit=limit,
            offset=offset,
        )
    elif no_monitor_filters and kind_n in ("matches", "queue"):
        items, total = [], 0
    elif kind_n == "matches":
        items, total = await db.list_task_match_items(
            task["chat_id"],
            tags=tags or None,
            tag_match_mode=mode,
            media_types=media_types or None,
            keywords=keywords or None,
            limit=limit,
            offset=offset,
            newest_first=newest_first,
        )
    else:
        items, total = await db.list_task_queue_items(
            task_id,
            task["chat_id"],
            tags=tags or None,
            tag_match_mode=mode,
            media_types=media_types or None,
            keywords=keywords or None,
            limit=limit,
            offset=offset,
            newest_first=newest_first,
        )
        live = scheduler.get_live_progress(task_id) or {}
        if live.get("phase") == "download":
            active_files = list(live.get("files") or [])

    return {
        "ok": True,
        "kind": kind_n,
        "task_id": task_id,
        "chat_title": task.get("chat_title") or "",
        "total": total,
        "limit": limit,
        "offset": offset,
        "active_count": len(active_files),
        "active": active_files,
        "items": items,
    }


@router.delete("/{task_id}")
async def delete_task(task_id: int, _: None = Depends(require_web_auth)):
    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    await scheduler.cancel_and_wait(task_id)
    await db.delete_task(task_id)
    return {"ok": True}
