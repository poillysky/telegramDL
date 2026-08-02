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
    folder_mode: str = "caption"  # caption | media_type | flat
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
    folder_mode: str = "caption"
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


async def _refresh_tag_match_count(
    tid: int,
    chat_id,
    tags: list,
    keywords: list,
    media_types: list,
    mode: str,
) -> None:
    """Background COUNT — never block /api/tasks first paint."""
    import asyncio
    import time

    if tid in _tag_match_refreshing:
        return
    _tag_match_refreshing.add(tid)
    try:
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
    tid = int(task["id"])
    # "已处理" uses the task counter — skip expensive join on every poll
    tag_done = int(task.get("downloaded_count") or 0)

    now = time.monotonic()
    cached = _tag_match_cache.get(tid)
    if cached and (now - cached[0]) < _TAG_MATCH_TTL:
        tag_match = cached[1]
    else:
        # Serve stale/0 immediately; refresh COUNT in background
        tag_match = int(cached[1]) if cached else 0
        try:
            asyncio.get_running_loop().create_task(
                _refresh_tag_match_count(
                    tid, chat_id, tags, keywords, media_types, mode
                )
            )
        except RuntimeError:
            pass

    # Truncate log for list payload — full log is huge and slows first paint
    last_log = task.get("last_log") or ""
    if isinstance(last_log, str) and len(last_log) > 2500:
        last_log = last_log[-2500:]

    out = {
        **task,
        "last_log": last_log,
        "tag_match_count": tag_match,
        "tag_processed_count": tag_done,
    }
    prog = scheduler.get_live_progress(tid)
    if prog:
        out = {**out, "live": prog}
    return out


@router.get("")
async def list_tasks(_: None = Depends(require_web_auth)):
    # Unstick「继续」disabled when DB says running but worker is dead
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
            "max_flood_wait": s.max_flood_wait,
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
    folder_mode = body.folder_mode if body.folder_mode in ("caption", "media_type", "flat") else "caption"
    if folder_mode == "caption":
        use_text = bool(body.use_text_as_folder)
    else:
        use_text = False
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


@router.patch("/{task_id}/tags")
async def update_task_tags(
    task_id: int,
    body: UpdateTaskTagsBody,
    _: None = Depends(require_web_auth),
):
    """Update monitor tags on an existing task (applies on next poll / continue)."""
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
    """Update tags / concurrency / delay etc. (tags apply next poll; concurrency after 继续)."""
    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")

    fields: dict = {}
    log_bits: list[str] = []

    if body.include_tags is not None:
        tags = normalize_tag_list(body.include_tags)
        mode = "any"
        if body.expand_related and tags:
            try:
                expanded = await db.expand_related_tags(task["chat_id"], tags)
                if len(expanded) > len(tags):
                    tags = expanded
            except Exception:
                pass
        kws_for_check = (
            normalize_keyword_list(body.caption_keywords)
            if body.caption_keywords is not None
            else (task.get("caption_keywords") or [])
        )
        if normalize_download_mode(task.get("download_mode")) == "monitor" and not tags:
            if not kws_for_check:
                return {
                    "ok": False,
                    "message": "监控模式请至少保留一个标签，或先填关键词",
                }
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
        fields["concurrency"] = conc
        log_bits.append(f"并发 {conc}")

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
        fields["delay_min"] = dmin
        fields["delay_max"] = dmax
        log_bits.append(f"延迟 {dmin:g}–{dmax:g}s")

    if not fields:
        return {"ok": True, "task": await _with_tag_progress(task)}

    await db.update_task(task_id, **fields)
    await db.append_log(task_id, "已更新任务设置: " + " · ".join(log_bits))
    _tag_match_cache.pop(int(task_id), None)
    task = await db.get_task(task_id)
    return {"ok": True, "task": await _with_tag_progress(task)}


@router.post("/{task_id}/start")
async def start_task(task_id: int, _: None = Depends(require_web_auth)):
    import asyncio

    task = await db.get_task(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    await scheduler.heal_stale_running(task_id)
    await db.update_task(task_id, status="pending", last_error=None)
    await db.append_log(task_id, "收到继续指令，正在启动…")
    await scheduler.start_task(task_id)
    # Brief wait for worker to flip status — keep short so UI stays responsive
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
    await scheduler.pause_task(task_id)
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
      - done: 已处理（本任务已下载完成）
      - queue: 待下载（匹配且未完成）+ 正在下载
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

    active_files: list = []
    if kind_n == "done":
        items, total = await db.list_task_done_items(
            task_id,
            task["chat_id"],
            limit=limit,
            offset=offset,
        )
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
