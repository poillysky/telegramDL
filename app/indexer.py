"""Background scan: index full captions (+ derived tags) for a chat's media messages."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

from telethon.errors import FloodWaitError

from app.db import db
from app.organizer import detect_media_type, extract_tags, has_media, message_text
from app.telegram_client import tg_manager

logger = logging.getLogger(__name__)

ProgressCb = Callable[[dict[str, Any]], Awaitable[None] | None]


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _msg_date_iso(message) -> Optional[str]:
    if not message or not message.date:
        return None
    dt = message.date
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


class ChatIndexer:
    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task] = {}
        self._stops: dict[str, asyncio.Event] = {}
        self._auto_task: Optional[asyncio.Task] = None
        self._auto_stop = asyncio.Event()

    def is_scanning(self, chat_id: str | int) -> bool:
        key = str(chat_id)
        if key in self._stops:
            return True
        t = self._tasks.get(key)
        return bool(t and not t.done())

    async def start_scan(
        self,
        chat_id: str | int,
        *,
        chat_title: str = "",
        full: bool = False,
    ) -> dict[str, Any]:
        key = str(chat_id)
        if self.is_scanning(key):
            meta = await db.get_index_meta(key)
            return {"ok": True, "already_running": True, "meta": meta}

        if full:
            await db.clear_chat_index(key)

        stop = asyncio.Event()
        self._stops[key] = stop
        meta_fields: dict[str, Any] = {
            "chat_title": chat_title or "",
            "status": "scanning",
            "last_error": None,
        }
        if full:
            meta_fields.update(
                scanned_count=0, media_count=0, last_message_id=0
            )
        await db.upsert_index_meta(key, **meta_fields)

        task = asyncio.create_task(
            self._run_scan(key, chat_title=chat_title or "", full=full, stop_event=stop)
        )
        self._tasks[key] = task

        def _done(t: asyncio.Task) -> None:
            self._tasks.pop(key, None)
            self._stops.pop(key, None)

        task.add_done_callback(_done)
        meta = await db.get_index_meta(key)
        return {"ok": True, "already_running": False, "meta": meta}

    async def stop_scan(self, chat_id: str | int) -> dict[str, Any]:
        key = str(chat_id)
        ev = self._stops.get(key)
        if ev:
            ev.set()
        t = self._tasks.get(key)
        if t and not t.done():
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=8)
            except Exception:
                pass
        meta = await db.get_index_meta(key)
        return {"ok": True, "meta": meta}

    async def has_usable_index(self, chat_id: str | int) -> bool:
        """True if index has media rows, or a successful scan finished at least once."""
        key = str(chat_id)
        if await db.count_media_index(key) > 0:
            return True
        meta = await db.get_index_meta(key)
        if not meta:
            return False
        if meta.get("status") == "error":
            return False
        # Completed scan (even if the chat had zero media)
        return bool(meta.get("last_scan_at"))

    async def get_chat_latest_message_id(self, chat_id: str | int) -> int:
        """Newest accessible message id in the chat (0 if empty / unavailable)."""
        try:
            client = await tg_manager.ensure_client()
            if not await client.is_user_authorized():
                return 0
            msgs = await asyncio.wait_for(
                client.get_messages(int(chat_id), limit=1),
                timeout=30,
            )
            if not msgs:
                return 0
            m = msgs[0] if isinstance(msgs, list) else msgs
            return int(getattr(m, "id", 0) or 0)
        except Exception:
            logger.debug("get chat latest message id failed chat=%s", chat_id, exc_info=True)
            return 0

    async def assess_index_coverage(self, chat_id: str | int) -> dict[str, Any]:
        """
        Compare local index high-water mark with the chat's latest message.
        Scan walks oldest→newest and stores last_message_id; if that is behind
        the chat tip (or never finished), the index is incomplete.
        """
        key = str(chat_id)
        meta = await db.get_index_meta(key) or {}
        media_count = await db.count_media_index(key)
        indexed_last = int(meta.get("last_message_id") or 0)
        last_scan_at = meta.get("last_scan_at")
        status = meta.get("status") or "idle"
        chat_latest = await self.get_chat_latest_message_id(key)

        base = {
            "indexed_last": indexed_last,
            "chat_latest": chat_latest,
            "media_count": media_count,
        }

        if chat_latest <= 0:
            # Cannot verify against Telegram tip — keep existing index if any
            if last_scan_at or media_count > 0 or indexed_last > 0:
                return {
                    **base,
                    "complete": True,
                    "action": "none",
                    "reason": "无法核对群最新消息，沿用现有索引",
                    "behind": 0,
                }
            return {
                **base,
                "complete": False,
                "action": "full",
                "reason": "尚未建立索引",
                "behind": 0,
            }

        if status == "error" and media_count <= 0 and indexed_last <= 0:
            return {
                **base,
                "complete": False,
                "action": "full",
                "reason": "索引出错且无可用数据",
                "behind": chat_latest,
            }

        if indexed_last <= 0 and media_count <= 0 and not last_scan_at:
            return {
                **base,
                "complete": False,
                "action": "full",
                "reason": "尚未建立索引",
                "behind": chat_latest,
            }

        if indexed_last < chat_latest:
            behind = chat_latest - indexed_last
            return {
                **base,
                "complete": False,
                "action": "incremental" if indexed_last > 0 or media_count > 0 else "full",
                "reason": (
                    f"索引未覆盖全群（已到消息 {indexed_last}，群最新 {chat_latest}，落后 {behind}）"
                ),
                "behind": behind,
            }

        return {
            **base,
            "complete": True,
            "action": "none",
            "reason": "索引已覆盖当前可访问的全部消息",
            "behind": 0,
        }

    async def ensure_index(
        self,
        chat_id: str | int,
        *,
        chat_title: str = "",
        stop_event: Optional[asyncio.Event] = None,
        on_progress: Optional[ProgressCb] = None,
    ) -> bool:
        """
        Ensure caption index covers the whole accessible chat history.
        Incomplete / stale indexes are auto catch-up scanned before returning.
        Returns False if stopped / failed.
        """
        key = str(chat_id)

        async def _emit(meta: Optional[dict[str, Any]] = None) -> None:
            if not on_progress:
                return
            data = meta or (await db.get_index_meta(key)) or {}
            try:
                result = on_progress(data)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.debug("index progress callback failed", exc_info=True)

        # Wait if another scan is already running
        if self.is_scanning(key):
            while self.is_scanning(key):
                if stop_event and stop_event.is_set():
                    return False
                await _emit()
                await asyncio.sleep(1.0)
            if stop_event and stop_event.is_set():
                return False

        coverage = await self.assess_index_coverage(key)
        if coverage.get("complete"):
            return True

        action = coverage.get("action") or "full"
        full = action == "full"

        stop = stop_event or asyncio.Event()
        # Reuse external stop_event when provided (task pause); still register for is_scanning
        self._stops[key] = stop

        meta_fields: dict[str, Any] = {
            "chat_title": chat_title or "",
            "status": "scanning",
            "last_error": None,
        }
        if full:
            # First-time / empty rebuild only — do not wipe a partial index on catch-up
            meta_fields.update(scanned_count=0, media_count=0, last_message_id=0)
        await db.upsert_index_meta(key, **meta_fields)
        await _emit(await db.get_index_meta(key))

        try:
            await self._run_scan(
                key,
                chat_title=chat_title or "",
                full=full,
                stop_event=stop,
                on_progress=on_progress,
            )
        finally:
            self._stops.pop(key, None)

        if stop.is_set():
            return False
        meta = await db.get_index_meta(key)
        if meta and meta.get("status") == "error":
            return False

        # Re-check against live tip (new messages may have arrived during scan)
        coverage_after = await self.assess_index_coverage(key)
        if coverage_after.get("complete"):
            return True
        # One more incremental pass if still behind
        if coverage_after.get("action") == "incremental" and not stop.is_set():
            self._stops[key] = stop
            try:
                await db.upsert_index_meta(
                    key,
                    chat_title=chat_title or "",
                    status="scanning",
                    last_error=None,
                )
                await self._run_scan(
                    key,
                    chat_title=chat_title or "",
                    full=False,
                    stop_event=stop,
                    on_progress=on_progress,
                )
            finally:
                self._stops.pop(key, None)
            if stop.is_set():
                return False
            meta = await db.get_index_meta(key)
            if meta and meta.get("status") == "error":
                return False
            return bool(
                (await self.assess_index_coverage(key)).get("complete")
                or (meta and (meta.get("last_scan_at") or meta.get("status") == "idle"))
            )

        return bool(meta and (meta.get("last_scan_at") or meta.get("status") == "idle"))

    async def _run_scan(
        self,
        chat_id: str,
        *,
        chat_title: str,
        full: bool,
        stop_event: asyncio.Event,
        on_progress: Optional[ProgressCb] = None,
    ) -> None:
        try:
            client = await tg_manager.ensure_client()
            if not await client.is_user_authorized():
                await db.upsert_index_meta(
                    chat_id,
                    status="error",
                    last_error="未登录 Telegram",
                )
                return

            if not chat_title:
                try:
                    chat_title = await tg_manager.get_chat_title(int(chat_id))
                except Exception:
                    chat_title = chat_id

            meta = await db.get_index_meta(chat_id) or {}
            min_id = 0 if full else int(meta.get("last_message_id") or 0)
            scanned = 0 if full else int(meta.get("scanned_count") or 0)
            media_count = 0 if full else await db.count_media_index(chat_id)
            max_seen_id = int(meta.get("last_message_id") or 0)
            album_captions: dict[Any, str] = {}
            pending_album: dict[Any, list[dict]] = {}
            # 两个文案之间的无文案视频 → 归「下一个」文案
            prev_caption_id = (
                0
                if full
                else await db.get_last_caption_message_id(chat_id)
            )

            await db.upsert_index_meta(
                chat_id,
                chat_title=chat_title,
                status="scanning",
                last_error=None,
            )

            async def _progress(**fields: Any) -> None:
                if not on_progress:
                    return
                meta = {
                    "chat_id": chat_id,
                    "chat_title": chat_title,
                    "status": "scanning",
                    "scanned_count": fields.get("scanned_count", scanned),
                    "media_count": fields.get("media_count", media_count),
                    "last_message_id": fields.get("last_message_id", max_seen_id),
                    "last_error": fields.get("last_error"),
                }
                try:
                    result = on_progress(meta)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    logger.debug("index progress callback failed", exc_info=True)

            while True:
                if stop_event.is_set():
                    await db.upsert_index_meta(
                        chat_id,
                        status="idle",
                        last_error="已停止",
                        scanned_count=scanned,
                        media_count=media_count,
                        last_message_id=max_seen_id,
                        chat_title=chat_title,
                    )
                    return

                iter_kwargs: dict[str, Any] = {"reverse": True}
                if min_id > 0:
                    iter_kwargs["min_id"] = min_id

                try:
                    async for message in client.iter_messages(int(chat_id), **iter_kwargs):
                        if stop_event.is_set():
                            break

                        scanned += 1
                        if message.id > max_seen_id:
                            max_seen_id = message.id

                        if not has_media(message):
                            # 纯文字也视为「文案分界」：其前方无文案视频归到此文案
                            text_only = message_text(message)
                            if text_only:
                                prev_caption_id = await self._on_caption_seen(
                                    chat_id,
                                    caption=text_only,
                                    caption_message_id=message.id,
                                    prev_caption_id=prev_caption_id,
                                )
                            if scanned % 80 == 0:
                                await db.upsert_index_meta(
                                    chat_id,
                                    scanned_count=scanned,
                                    media_count=media_count,
                                    last_message_id=max_seen_id,
                                    status="scanning",
                                    chat_title=chat_title,
                                )
                                await _progress(
                                    scanned_count=scanned,
                                    media_count=media_count,
                                    last_message_id=max_seen_id,
                                )
                            continue

                        media_type = detect_media_type(message)
                        if media_type == "sticker":
                            media_type = "document"

                        text = message_text(message)
                        grouped_id = getattr(message, "grouped_id", None)
                        if grouped_id and text:
                            album_captions[grouped_id] = text

                        caption = text
                        if not caption and grouped_id:
                            caption = album_captions.get(grouped_id, "") or ""

                        # Album members may arrive before the caption-bearing msg;
                        # buffer until we know the caption, then flush.
                        item = {
                            "message_id": message.id,
                            "grouped_id": grouped_id,
                            "media_type": media_type,
                            "caption": caption,
                            "msg_date": _msg_date_iso(message),
                        }

                        if grouped_id and not caption:
                            pending_album.setdefault(grouped_id, []).append(item)
                            # try pull caption from nearby siblings once
                            if len(pending_album[grouped_id]) == 1:
                                cap = await self._lookup_album_caption(
                                    client, int(chat_id), message, grouped_id
                                )
                                if cap:
                                    album_captions[grouped_id] = cap
                                    caption = cap
                                    item["caption"] = cap
                                    for p in pending_album.pop(grouped_id, []):
                                        p["caption"] = cap
                                        await self._write_item(chat_id, p)
                                        media_count += 1
                                    prev_caption_id = await self._on_caption_seen(
                                        chat_id,
                                        caption=cap,
                                        caption_message_id=message.id,
                                        prev_caption_id=prev_caption_id,
                                    )
                                    continue
                            continue

                        if grouped_id and caption:
                            album_captions[grouped_id] = caption
                            for p in pending_album.pop(grouped_id, []):
                                p["caption"] = caption
                                await self._write_item(chat_id, p)
                                media_count += 1

                        await self._write_item(chat_id, item)
                        media_count += 1
                        if caption:
                            prev_caption_id = await self._on_caption_seen(
                                chat_id,
                                caption=caption,
                                caption_message_id=message.id,
                                prev_caption_id=prev_caption_id,
                            )

                        if scanned % 40 == 0:
                            await db.commit()
                            await db.upsert_index_meta(
                                chat_id,
                                scanned_count=scanned,
                                media_count=media_count,
                                last_message_id=max_seen_id,
                                status="scanning",
                                chat_title=chat_title,
                            )
                            await _progress(
                                scanned_count=scanned,
                                media_count=media_count,
                                last_message_id=max_seen_id,
                            )

                    # flush remaining pending albums (no caption found)
                    for gid, items in list(pending_album.items()):
                        cap = album_captions.get(gid, "") or ""
                        for p in items:
                            p["caption"] = cap
                            await self._write_item(chat_id, p)
                            media_count += 1
                        pending_album.pop(gid, None)

                    await db.commit()
                    break

                except FloodWaitError as e:
                    wait_s = min(int(e.seconds) + 1, 300)
                    logger.warning("index FloodWait %ss chat=%s", wait_s, chat_id)
                    if max_seen_id > min_id:
                        min_id = max_seen_id
                    await db.upsert_index_meta(
                        chat_id,
                        status="scanning",
                        last_error=f"FloodWait {wait_s}s，等待中",
                        scanned_count=scanned,
                        media_count=media_count,
                        last_message_id=max_seen_id,
                    )
                    await _progress(
                        scanned_count=scanned,
                        media_count=media_count,
                        last_message_id=max_seen_id,
                        last_error=f"FloodWait {wait_s}s，等待中",
                    )
                    end = asyncio.get_event_loop().time() + wait_s
                    while asyncio.get_event_loop().time() < end:
                        if stop_event.is_set():
                            await db.upsert_index_meta(
                                chat_id,
                                status="idle",
                                last_error="已停止",
                                scanned_count=scanned,
                                media_count=media_count,
                                last_message_id=max_seen_id,
                            )
                            return
                        await asyncio.sleep(0.5)
                    continue

            media_count = await db.count_media_index(chat_id)
            await db.upsert_index_meta(
                chat_id,
                chat_title=chat_title,
                status="idle",
                last_error=None,
                scanned_count=scanned,
                media_count=media_count,
                last_message_id=max_seen_id,
                last_scan_at=_utcnow(),
            )
            await _progress(
                scanned_count=scanned,
                media_count=media_count,
                last_message_id=max_seen_id,
            )
            logger.info(
                "index done chat=%s media=%s scanned=%s",
                chat_id,
                media_count,
                scanned,
            )
            # Wake local monitor tasks for this chat to download index gaps
            try:
                from app.downloader import scheduler

                scheduler.notify_index_updated(chat_id)
            except Exception:
                logger.debug("notify_index_updated failed", exc_info=True)
            # Drop UI caches so 命中/索引 counts refresh on next poll
            try:
                from app.api.routes_tasks import invalidate_index_count_cache

                invalidate_index_count_cache(chat_id)
            except Exception:
                logger.debug("invalidate_index_count_cache failed", exc_info=True)
        except Exception as e:
            logger.exception("index scan failed chat=%s", chat_id)
            await db.upsert_index_meta(
                chat_id,
                status="error",
                last_error=str(e)[:300],
            )

    async def _write_item(self, chat_id: str, item: dict) -> None:
        caption = item.get("caption") or ""
        tags = extract_tags(caption)
        await db.upsert_media_index_item(
            chat_id,
            int(item["message_id"]),
            caption=caption,
            tags=tags,
            grouped_id=item.get("grouped_id"),
            media_type=item.get("media_type"),
            msg_date=item.get("msg_date"),
        )

    async def _on_caption_seen(
        self,
        chat_id: str,
        *,
        caption: str,
        caption_message_id: int,
        prev_caption_id: int,
    ) -> int:
        """
        When a captioned media is seen (oldest→newest scan):
        empty-caption videos between prev_caption and this one get THIS caption.
        """
        caption = (caption or "").strip()
        if not caption:
            return prev_caption_id
        updated = await db.backfill_empty_video_captions(
            chat_id,
            caption,
            after_message_id=prev_caption_id,
            before_message_id=int(caption_message_id),
        )
        if updated:
            logger.info(
                "next-caption rule: chat=%s caption_msg=%s filled %s videos",
                chat_id,
                caption_message_id,
                len(updated),
            )
        return int(caption_message_id)

    async def _lookup_album_caption(
        self, client, chat_id: int, message, grouped_id
    ) -> str:
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
                    return t
        except Exception:
            logger.debug("album caption lookup failed", exc_info=True)
        return ""

    def start_auto_scheduler(self) -> None:
        """Start background loop that runs enabled incremental scans on a timer."""
        if self._auto_task and not self._auto_task.done():
            return
        self._auto_stop.clear()
        self._auto_task = asyncio.create_task(self._auto_loop(), name="index-auto-scan")

    async def stop_auto_scheduler(self) -> None:
        self._auto_stop.set()
        t = self._auto_task
        self._auto_task = None
        if t and not t.done():
            t.cancel()
            try:
                await t
            except Exception:
                pass

    async def _auto_loop(self) -> None:
        # First pass soon after boot (wait for Telegram reconnect)
        await asyncio.sleep(20)
        while not self._auto_stop.is_set():
            try:
                await self._tick_auto_scans()
            except Exception:
                logger.exception("Auto incremental index tick failed")
            try:
                await asyncio.wait_for(self._auto_stop.wait(), timeout=45)
            except asyncio.TimeoutError:
                pass

    async def _tick_auto_scans(self) -> None:
        rows = await db.list_auto_index_chats()
        if not rows:
            return
        try:
            client = await tg_manager.ensure_client()
            if not await client.is_user_authorized():
                return
        except Exception:
            return

        now = datetime.now(timezone.utc)
        for row in rows:
            chat_id = row["chat_id"]
            if self.is_scanning(chat_id):
                continue
            interval = max(5, min(24 * 60, int(row.get("auto_interval_min") or 60)))
            last_raw = row.get("last_scan_at")
            if last_raw:
                try:
                    last = datetime.fromisoformat(str(last_raw).replace("Z", "+00:00"))
                    if last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    age_sec = (now - last.astimezone(timezone.utc)).total_seconds()
                    if age_sec < interval * 60:
                        continue
                except Exception:
                    pass
            title = row.get("chat_title") or ""
            try:
                logger.info(
                    "Auto incremental index: chat=%s interval=%sm",
                    chat_id,
                    interval,
                )
                await self.start_scan(chat_id, chat_title=title, full=False)
            except Exception:
                logger.exception("Auto scan failed for chat %s", chat_id)


indexer = ChatIndexer()
