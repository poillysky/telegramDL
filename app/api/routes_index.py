"""Chat media caption index: scan once, reuse by tag / keyword."""

from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.api.deps import require_web_auth
from app.db import db
from app.indexer import indexer
from app.organizer import normalize_tag_list
from app.telegram_client import tg_manager

router = APIRouter(prefix="/api/index", tags=["index"])


class ScanBody(BaseModel):
    full: bool = False
    chat_title: str = ""


class AutoScanBody(BaseModel):
    enabled: bool = False
    interval_min: int = Field(default=60, ge=5, le=1440)  # 5 min … 24 h
    chat_title: str = ""


@router.patch("/{chat_id}/auto-scan")
async def set_auto_scan(
    chat_id: int | str,
    body: AutoScanBody,
    _: None = Depends(require_web_auth),
):
    """Enable/disable timed incremental index updates for a chat."""
    meta = await db.set_auto_index(
        chat_id,
        enabled=bool(body.enabled),
        interval_min=int(body.interval_min or 60),
        chat_title=(body.chat_title or "").strip(),
    )
    return {
        "ok": True,
        "meta": meta,
        "auto_incremental": bool(meta.get("auto_incremental")),
        "auto_interval_min": int(meta.get("auto_interval_min") or 60),
    }


@router.get("/{chat_id}/tags")
async def get_index_tags_only(
    chat_id: int | str, _: None = Depends(require_web_auth)
):
    """Lightweight tag list for pickers — no Telegram coverage probe.

    Skip full related-map build (O(n·k²) over all captions); picker only needs
    tags + a capped set of co-occurrence bundles.
    """
    tags = await db.list_index_tags(chat_id)
    # Cap bundles so mobile open stays responsive
    bundles = await db.list_tag_cooccur_bundles(chat_id, min_count=1, limit=400)
    return {"ok": True, "tags": tags, "bundles": bundles, "related": {}}


@router.get("/{chat_id}")
async def get_index(chat_id: int | str, _: None = Depends(require_web_auth)):
    meta = await db.get_index_meta(chat_id)
    if not meta:
        meta = {
            "chat_id": str(chat_id),
            "chat_title": "",
            "last_scan_at": None,
            "last_message_id": 0,
            "media_count": 0,
            "scanned_count": 0,
            "status": "idle",
            "last_error": None,
        }
    tags = await db.list_index_tags(chat_id)
    related = await db.get_tag_relations(chat_id)
    try:
        coverage = await indexer.assess_index_coverage(chat_id)
    except Exception:
        coverage = {
            "complete": None,
            "action": "none",
            "reason": "无法核对覆盖范围",
            "indexed_last": int(meta.get("last_message_id") or 0),
            "chat_latest": 0,
            "media_count": int(meta.get("media_count") or 0),
            "behind": 0,
        }
    return {
        "ok": True,
        "meta": meta,
        "tags": tags,
        "related": related,
        "coverage": coverage,
        "scanning": indexer.is_scanning(chat_id),
    }


@router.post("/{chat_id}/scan")
async def start_scan(
    chat_id: int | str,
    body: ScanBody = ScanBody(),
    _: None = Depends(require_web_auth),
):
    try:
        client = await tg_manager.ensure_client()
        if not await client.is_user_authorized():
            return {"ok": False, "message": "请先登录 Telegram"}
    except Exception as e:
        return {"ok": False, "message": str(e) or "请先登录 Telegram"}
    title = (body.chat_title or "").strip()
    if not title:
        try:
            title = await tg_manager.get_chat_title(chat_id)
        except Exception:
            title = str(chat_id)
    result = await indexer.start_scan(chat_id, chat_title=title, full=bool(body.full))
    tags = await db.list_index_tags(chat_id)
    return {
        "ok": True,
        "already_running": result.get("already_running"),
        "meta": result.get("meta"),
        "tags": tags,
        "scanning": indexer.is_scanning(chat_id),
    }


@router.post("/{chat_id}/stop")
async def stop_scan(chat_id: int | str, _: None = Depends(require_web_auth)):
    result = await indexer.stop_scan(chat_id)
    tags = await db.list_index_tags(chat_id)
    return {
        "ok": True,
        "meta": result.get("meta"),
        "tags": tags,
        "scanning": indexer.is_scanning(chat_id),
    }


@router.get("/{chat_id}/tags/suggest")
async def suggest_tags(
    chat_id: int | str,
    q: str = Query(default=""),
    limit: int = Query(default=20, ge=1, le=50),
    _: None = Depends(require_web_auth),
):
    """Autocomplete tags from index; includes direct/indirect related tags."""
    items = await db.suggest_index_tags(chat_id, q=q, limit=limit)
    return {"ok": True, "items": items, "q": q}


@router.get("/{chat_id}/tags/related")
async def related_tags(
    chat_id: int | str,
    tag: str = Query(default=""),
    tags: Optional[str] = None,
    _: None = Depends(require_web_auth),
):
    """Expand seed tag(s) with all transitively related tags."""
    seeds: List[str] = []
    if tags:
        seeds = normalize_tag_list(
            [x for x in tags.replace("，", ",").split(",") if x.strip()]
        )
    elif tag:
        seeds = normalize_tag_list([tag])
    expanded = await db.expand_related_tags(chat_id, seeds)
    related_only = [t for t in expanded if t.lower() not in {s.lower() for s in seeds}]
    return {
        "ok": True,
        "seeds": seeds,
        "expanded": expanded,
        "related": related_only,
    }


@router.get("/{chat_id}/items")
async def list_items(
    chat_id: int | str,
    tag: Optional[str] = None,
    tags: Optional[str] = None,
    q: Optional[str] = None,
    tag_match_mode: str = Query(default="any"),
    limit: int = Query(default=40, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _: None = Depends(require_web_auth),
):
    tag_list: List[str] = []
    if tags:
        tag_list = normalize_tag_list(
            [x for x in tags.replace("，", ",").split(",") if x.strip()]
        )
    elif tag:
        tag_list = normalize_tag_list([tag])
    items, total = await db.list_index_items(
        chat_id,
        tags=tag_list,
        tag_match_mode=tag_match_mode,
        q=q,
        limit=limit,
        offset=offset,
    )
    return {"ok": True, "items": items, "total": total, "limit": limit, "offset": offset}
