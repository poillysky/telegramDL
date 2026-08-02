"""Optional outbound notifications (webhook)."""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from app.db import db

logger = logging.getLogger(__name__)

_META_WEBHOOK = "notify_webhook"
_META_ENABLED = "notify_enabled"


async def get_notify_config() -> dict[str, Any]:
    enabled_raw = await db.get_meta(_META_ENABLED, "0")
    webhook = (await db.get_meta(_META_WEBHOOK, "")) or ""
    return {
        "enabled": str(enabled_raw).strip() in ("1", "true", "yes", "on"),
        "webhook": webhook.strip(),
    }


async def save_notify_config(*, enabled: bool, webhook: str) -> dict[str, Any]:
    await db.set_meta(_META_ENABLED, "1" if enabled else "0")
    await db.set_meta(_META_WEBHOOK, (webhook or "").strip())
    return await get_notify_config()


async def notify_event(
    event: str,
    *,
    task_id: Optional[int] = None,
    title: str = "",
    message: str = "",
    extra: Optional[dict[str, Any]] = None,
) -> None:
    """Fire-and-forget webhook POST. Never raises to caller."""
    try:
        cfg = await get_notify_config()
        url = cfg.get("webhook") or ""
        if not cfg.get("enabled") or not url:
            return
        payload = {
            "event": event,
            "task_id": task_id,
            "title": title,
            "message": message,
            "extra": extra or {},
        }
        async with httpx.AsyncClient(timeout=8.0) as client:
            await client.post(url, json=payload)
    except Exception:
        logger.debug("notify_event failed event=%s", event, exc_info=True)
