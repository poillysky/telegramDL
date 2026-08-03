"""Global app settings (tag relation blacklist, runtime, notify)."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.api.deps import require_web_auth
from app.config import get_settings
from app.db import DEFAULT_TAG_RELATION_BLACKLIST, db, normalize_blacklist_tag
from app.notify import get_notify_config, save_notify_config

router = APIRouter(prefix="/api/settings", tags=["settings"])


class TagBlacklistBody(BaseModel):
    tags: list[str] = Field(default_factory=list)


class TagBlacklistItemBody(BaseModel):
    tag: str = ""


class RuntimeSettingsBody(BaseModel):
    max_parallel_chats: int = Field(default=1, ge=1, le=10)
    failed_retry_interval_sec: int = Field(default=900, ge=120, le=86400)
    monitor_heartbeat_sec: int = Field(default=600, ge=60, le=86400)
    max_flood_wait: int = Field(default=1800, ge=60, le=86400)
    media_connections: int = Field(default=3, ge=1, le=8)
    download_pipeline: int = Field(default=4, ge=1, le=8)
    download_part_size: int = Field(default=1048576, ge=524288, le=1048576)
    notify_enabled: bool = False
    notify_webhook: str = ""


@router.get("/tag-blacklist")
async def get_tag_blacklist(_user=Depends(require_web_auth)):
    tags = sorted(await db.get_tag_relation_blacklist(), key=lambda x: x.lower())
    defaults = sorted(DEFAULT_TAG_RELATION_BLACKLIST, key=lambda x: x.lower())
    return {
        "tags": tags,
        "count": len(tags),
        "defaults": defaults,
        "default_count": len(defaults),
    }


@router.put("/tag-blacklist")
async def put_tag_blacklist(body: TagBlacklistBody, _user=Depends(require_web_auth)):
    tags = await db.set_tag_relation_blacklist(body.tags or [])
    return {"ok": True, "tags": tags, "count": len(tags)}


@router.post("/tag-blacklist")
async def add_tag_blacklist(body: TagBlacklistItemBody, _user=Depends(require_web_auth)):
    name = normalize_blacklist_tag(body.tag)
    if not name:
        raise HTTPException(status_code=400, detail="标签名不能为空")
    try:
        tags = await db.add_tag_relation_blacklist(name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, "tag": name, "tags": tags, "count": len(tags)}


@router.delete("/tag-blacklist/{tag}")
async def delete_tag_blacklist(tag: str, _user=Depends(require_web_auth)):
    name = normalize_blacklist_tag(tag)
    if not name:
        raise HTTPException(status_code=400, detail="标签名不能为空")
    try:
        tags = await db.remove_tag_relation_blacklist(name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, "tag": name, "tags": tags, "count": len(tags)}


@router.post("/tag-blacklist/reset")
async def reset_tag_blacklist(_user=Depends(require_web_auth)):
    tags = await db.reset_tag_relation_blacklist()
    return {"ok": True, "tags": tags, "count": len(tags)}


@router.get("/runtime")
async def get_runtime_settings(_user=Depends(require_web_auth)):
    s = get_settings()
    parallel = await db.get_max_parallel_chats()
    failed_retry = await db.get_failed_retry_interval_sec()
    heartbeat = await db.get_monitor_heartbeat_sec()
    flood_wait = await db.get_max_flood_wait()
    media_conn = await db.get_media_connections()
    pipeline = await db.get_download_pipeline()
    part_size = await db.get_download_part_size()
    notify = await get_notify_config()
    return {
        "ok": True,
        "max_parallel_chats": parallel,
        "max_parallel_chats_env_default": max(1, int(s.max_parallel_chats or 1)),
        "failed_retry_interval_sec": failed_retry,
        "monitor_heartbeat_sec": heartbeat,
        "max_flood_wait": flood_wait,
        "media_connections": media_conn,
        "download_pipeline": pipeline,
        "download_part_size": part_size,
        "notify_enabled": bool(notify.get("enabled")),
        "notify_webhook": notify.get("webhook") or "",
        "download_dir": str(s.download_dir),
        "log_dir": str(Path(s.data_dir) / "logs"),
    }


@router.put("/runtime")
async def put_runtime_settings(body: RuntimeSettingsBody, _user=Depends(require_web_auth)):
    from app import runtime_tune

    parallel = await db.set_max_parallel_chats(body.max_parallel_chats)
    failed_retry = await db.set_failed_retry_interval_sec(body.failed_retry_interval_sec)
    heartbeat = await db.set_monitor_heartbeat_sec(body.monitor_heartbeat_sec)
    flood_wait = await db.set_max_flood_wait(body.max_flood_wait)
    media_conn = await db.set_media_connections(body.media_connections)
    pipeline = await db.set_download_pipeline(body.download_pipeline)
    part_size = await db.set_download_part_size(body.download_part_size)
    runtime_tune.apply_overrides(
        media_connections=media_conn,
        download_pipeline=pipeline,
        download_part_size=part_size,
    )
    notify = await save_notify_config(
        enabled=bool(body.notify_enabled),
        webhook=(body.notify_webhook or "").strip(),
    )
    # Wake waiters so a raised limit can admit queued tasks sooner
    try:
        from app.downloader import scheduler

        cond = scheduler._slot_condition()
        async with cond:
            cond.notify_all()
    except Exception:
        pass
    # Hot-resize media pool on the live Telegram client
    try:
        from app.telegram_client import install_media_connection_pool, tg_manager

        client = tg_manager.client
        if client is not None:
            install_media_connection_pool(client, pool_size=media_conn)
    except Exception:
        pass
    return {
        "ok": True,
        "max_parallel_chats": parallel,
        "failed_retry_interval_sec": failed_retry,
        "monitor_heartbeat_sec": heartbeat,
        "max_flood_wait": flood_wait,
        "media_connections": media_conn,
        "download_pipeline": pipeline,
        "download_part_size": part_size,
        "notify_enabled": bool(notify.get("enabled")),
        "notify_webhook": notify.get("webhook") or "",
    }
