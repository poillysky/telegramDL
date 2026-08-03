"""Telethon client — aligned with Dineshkarthik/telegram_media_downloader connection style."""

from __future__ import annotations

import asyncio
import logging
import platform
from pathlib import Path
from typing import Any, Optional

from telethon import TelegramClient
from telethon.errors import (
    LimitInvalidError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)
from telethon.tl.types import Channel, Chat, User

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)

# Same meta style as dineshkarthik_ref/utils/meta.py
APP_VERSION = "Telegram Media Downloader 1.0.10"
DEVICE_MODEL = f"{platform.python_implementation()} {platform.python_version()}"
SYSTEM_VERSION = f"{platform.system()} {platform.release()}"
LANG_CODE = "en"

# Telegram file offsets must be multiples of 4 KiB; limit ≤1MiB and 1MiB%limit==0.
_TG_OFFSET_ALIGN = 4096
_TG_MAX_REQUEST_SIZE = 1024 * 1024  # official upload.getFile max
_TG_SAFE_REQUEST_SIZE = 512 * 1024  # fallback if a DC returns LIMIT_INVALID
# Without precise, limit must divide 1MiB (API rule). 192KiB etc. → LIMIT_INVALID.
_TG_VALID_LIMITS = (
    1024 * 1024,
    512 * 1024,
    256 * 1024,
    128 * 1024,
    64 * 1024,
    32 * 1024,
    16 * 1024,
    8 * 1024,
    4 * 1024,
)
# Prefer 1MiB parts over many connections; keep caps modest for account safety.
_TG_SAFE_POOL_SIZE = 3
_TG_SAFE_PIPELINE = 4
_TG_MAX_POOL_SIZE = 8
_TG_MAX_PIPELINE = 8

# GetConfig often omits media_only for DC5; these are known media endpoints
# that accept parallel file-transfer sessions with the same auth_key.
_KNOWN_MEDIA_IPS: dict[int, list[str]] = {
    2: ["149.154.167.222", "149.154.167.151"],
    4: ["149.154.166.120", "149.154.164.250"],
    5: ["91.108.56.151", "91.108.56.128", "91.108.56.102"],
}


class _MediaDc:
    __slots__ = ("id", "ip_address", "port", "media_only")

    def __init__(self, dc_id: int, ip: str, port: int = 443, media_only: bool = True):
        self.id = int(dc_id)
        self.ip_address = ip
        self.port = int(port)
        self.media_only = bool(media_only)


class DownloadPaused(Exception):
    """Raised when a resumable download stops early; .part file is kept."""


class TelegramAuthError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


async def _list_media_dcs(client: TelegramClient, dc_id: int) -> list[_MediaDc]:
    """Resolve media-only endpoints for parallel download sessions."""
    from telethon.tl import functions

    cls = client.__class__
    if not cls._config:
        cls._config = await client(functions.help.GetConfigRequest())

    found: list[_MediaDc] = []
    seen: set[str] = set()
    for dc in cls._config.dc_options:
        if int(dc.id) != int(dc_id) or bool(dc.cdn) or bool(dc.ipv6):
            continue
        if not bool(dc.media_only):
            continue
        ip = str(dc.ip_address)
        if ip in seen:
            continue
        seen.add(ip)
        found.append(_MediaDc(dc_id, ip, int(dc.port or 443), True))

    for ip in _KNOWN_MEDIA_IPS.get(int(dc_id), []):
        if ip in seen:
            continue
        seen.add(ip)
        found.append(_MediaDc(dc_id, ip, 443, True))
    return found


def _patch_telethon_download_iters_for_pool() -> None:
    """Route home-DC downloads through the media connection pool."""
    from telethon.client.downloads import _DirectDownloadIter

    if getattr(_DirectDownloadIter, "_media_pool_home_patched", False):
        return
    orig = _DirectDownloadIter._init

    async def _init(self, file, dc_id, *args, **kwargs):  # type: ignore[no-untyped-def]
        await orig(self, file, dc_id, *args, **kwargs)
        client = self.client
        if not getattr(client, "_media_pool_cfg", None):
            return
        if getattr(self, "_exported", False):
            return
        home = int(getattr(client.session, "dc_id", 0) or 0)
        use_dc = int(dc_id or home or 0)
        if use_dc <= 0:
            return
        try:
            self._sender = await client._borrow_exported_sender(use_dc)
            self._exported = True
        except Exception:
            logger.debug("media pool home-DC redirect failed", exc_info=True)

    _DirectDownloadIter._init = _init  # type: ignore[method-assign]
    _DirectDownloadIter._media_pool_home_patched = True


def install_media_connection_pool(
    client: TelegramClient, pool_size: int = 8
) -> None:
    """
    Warm pool of media TCP sessions for concurrent downloads.

    On real media endpoints (config media_only or known DC media IPs), Telegram
    allows multiple sessions with the same auth_key — that is what unlocks Nx
    throughput. Without media endpoints, home DC is capped at 1 extra session.
    """
    # Cap pool: official large-file queue is 2; keep media TCP modest.
    pool_size = max(1, min(_TG_MAX_POOL_SIZE, int(pool_size or _TG_SAFE_POOL_SIZE)))
    _patch_telethon_download_iters_for_pool()

    cfg = getattr(client, "_media_pool_cfg", None)
    if isinstance(cfg, dict) and cfg.get("pools") is not None:
        cfg["size"] = max(int(cfg.get("size") or 1), pool_size)
        return

    from telethon.crypto import AuthKey
    from telethon.network import MTProtoSender
    from telethon.tl import functions
    from telethon.tl.alltlobjects import LAYER
    from telethon.tl.functions import InvokeWithLayerRequest

    # dc_id -> [{"sender", "borrows", "dc"}, ...]
    pools: dict[int, list[dict[str, Any]]] = {}
    media_targets: dict[int, list[_MediaDc]] = {}
    rr_index: dict[int, int] = {}
    lock = asyncio.Lock()
    cfg = {"size": pool_size, "pools": pools}

    def _is_home(dc_id: int) -> bool:
        home = int(getattr(client.session, "dc_id", 0) or 0)
        return bool(home) and int(dc_id) == home

    async def _targets(dc_id: int) -> list[_MediaDc]:
        cached = media_targets.get(int(dc_id))
        if cached is not None:
            return cached
        found = await _list_media_dcs(client, int(dc_id))
        media_targets[int(dc_id)] = found
        return found

    async def _pick_media_dc(dc_id: int) -> _MediaDc:
        found = await _targets(dc_id)
        if found:
            i = int(rr_index.get(int(dc_id), 0) or 0)
            rr_index[int(dc_id)] = i + 1
            return found[i % len(found)]
        # Last resort: main DC IP (NOT safe for multi-conn on home)
        raw = await client._get_dc(int(dc_id))
        return _MediaDc(
            int(dc_id),
            str(raw.ip_address),
            int(raw.port or 443),
            media_only=False,
        )

    async def _dc_pool_cap(dc_id: int) -> int:
        found = await _targets(dc_id)
        if found:
            # Parallel media sessions are allowed on media endpoints.
            return int(cfg["size"] or 1)
        if _is_home(dc_id):
            return 1
        return int(cfg["size"] or 1)

    def _make_init(query: Any):
        return functions.InitConnectionRequest(
            api_id=client.api_id,
            device_model=client._init_request.device_model,
            system_version=client._init_request.system_version,
            app_version=client._init_request.app_version,
            lang_code=client._init_request.lang_code,
            system_lang_code=client._init_request.system_lang_code,
            lang_pack="",
            query=query,
            proxy=client._init_request.proxy,
        )

    async def _connect_sender(sender, dc: _MediaDc) -> None:
        await sender.connect(
            client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=client._log,
                proxy=client._proxy,
                local_addr=client._local_addr,
            )
        )

    async def _create_home_media_sender(dc_id: int):
        key = getattr(client.session, "auth_key", None)
        if key is None or not getattr(key, "key", None):
            raise RuntimeError("session has no auth_key for home-DC media sender")
        dc = await _pick_media_dc(dc_id)
        sender = MTProtoSender(AuthKey(data=key.key), loggers=client._log)
        await _connect_sender(sender, dc)
        await sender.send(
            InvokeWithLayerRequest(
                LAYER, _make_init(functions.help.GetConfigRequest())
            )
        )
        sender.dc_id = int(dc_id)
        sender._media_dc = dc  # type: ignore[attr-defined]
        logger.info(
            "Home media sender ready dc=%s via %s:%s media_only=%s",
            dc_id,
            dc.ip_address,
            dc.port,
            dc.media_only,
        )
        return sender

    async def _create_exported_media_sender(dc_id: int):
        dc = await _pick_media_dc(dc_id)
        sender = MTProtoSender(None, loggers=client._log)
        await _connect_sender(sender, dc)
        auth = await client(functions.auth.ExportAuthorizationRequest(dc_id))
        await sender.send(
            InvokeWithLayerRequest(
                LAYER,
                _make_init(
                    functions.auth.ImportAuthorizationRequest(
                        id=auth.id, bytes=auth.bytes
                    )
                ),
            )
        )
        sender.dc_id = int(dc_id)
        sender._media_dc = dc  # type: ignore[attr-defined]
        return sender

    async def _create_pool_sender(dc_id: int):
        if _is_home(dc_id):
            return await _create_home_media_sender(int(dc_id))
        return await _create_exported_media_sender(int(dc_id))

    async def _ensure_connected(slot: dict[str, Any], dc_id: int) -> None:
        sender = slot.get("sender")
        if sender is None:
            return
        try:
            connected = bool(sender.is_connected())
        except Exception:
            connected = False
        if connected:
            return
        dc = slot.get("dc") or getattr(sender, "_media_dc", None)
        if dc is None:
            dc = await _pick_media_dc(dc_id)
            slot["dc"] = dc
        await _connect_sender(sender, dc)

    async def borrow(dc_id: int):
        """Borrow a media sender. Never await network I/O while holding the pool lock."""
        dc_id = int(dc_id)
        # Resolve cap outside lock (may hit GetConfig once; then cached)
        cap = await _dc_pool_cap(dc_id)
        create_slot: dict[str, Any] | None = None
        use_slot: dict[str, Any] | None = None

        async with lock:
            bucket = pools.setdefault(dc_id, [])
            free = [
                s
                for s in bucket
                if int(s["borrows"]) <= 0 and s.get("sender") is not None
            ]
            if free:
                use_slot = free[0]
                use_slot["borrows"] = 1
            elif len(bucket) < cap:
                create_slot = {"sender": None, "borrows": 1, "dc": None}
                bucket.append(create_slot)
            else:
                # Prefer already-ready senders; avoid slots still being created
                ready_slots = [s for s in bucket if s.get("sender") is not None]
                use_slot = min(
                    ready_slots or bucket, key=lambda s: int(s["borrows"])
                )
                use_slot["borrows"] = int(use_slot["borrows"]) + 1

        if create_slot is not None:
            try:
                sender = await _create_pool_sender(dc_id)
                dc = getattr(sender, "_media_dc", None)
                create_slot["sender"] = sender
                create_slot["dc"] = dc
                logger.info(
                    "Media connection pool: dc=%s conns=%s/%s ip=%s",
                    dc_id,
                    len(pools.get(dc_id, [])),
                    cap,
                    getattr(dc, "ip_address", "?"),
                )
                return sender
            except Exception:
                async with lock:
                    bucket = pools.get(dc_id, [])
                    if create_slot in bucket:
                        bucket.remove(create_slot)
                raise

        assert use_slot is not None
        # If we raced a still-creating slot, wait briefly for sender
        for _ in range(100):
            if use_slot.get("sender") is not None:
                break
            await asyncio.sleep(0.05)
        if use_slot.get("sender") is None:
            async with lock:
                use_slot["borrows"] = max(0, int(use_slot["borrows"]) - 1)
            raise RuntimeError("media pool sender not ready")
        await _ensure_connected(use_slot, dc_id)
        return use_slot["sender"]

    async def ret(sender) -> None:
        async with lock:
            for bucket in pools.values():
                for slot in bucket:
                    if slot.get("sender") is sender:
                        slot["borrows"] = max(0, int(slot["borrows"]) - 1)
                        return

    client._borrow_exported_sender = borrow  # type: ignore[method-assign]
    client._return_exported_sender = ret  # type: ignore[method-assign]
    client._media_pool_cfg = cfg
    from app import runtime_tune

    chunk_kib = int(runtime_tune.download_part_size() or _TG_MAX_REQUEST_SIZE) // 1024
    logger.info(
        "Installed media connection pool size≤%s chunk=%sKiB",
        pool_size,
        chunk_kib,
    )


class TelegramManager:
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self.client: Optional[TelegramClient] = None
        self._phone_code_hash: Optional[str] = None
        self._pending_phone: Optional[str] = None
        self._lock = asyncio.Lock()

    def _build_client(self, api_id: int, api_hash: str) -> TelegramClient:
        """
        Mirror Dineshkarthik media_downloader.begin_import client construction:

            TelegramClient(session, api_id=..., api_hash=..., proxy=proxy_dict,
                           device_model=..., system_version=..., app_version=..., lang_code=...)
        """
        self.settings.ensure_dirs()
        proxy = self.settings.parse_proxy()
        session = str(self.settings.session_path.resolve())
        logger.info(
            "Build TelegramClient session=%s proxy=%s",
            session,
            proxy or "None(direct)",
        )
        client = TelegramClient(
            session,
            api_id=api_id,
            api_hash=api_hash,
            proxy=proxy,
            device_model=DEVICE_MODEL,
            system_version=SYSTEM_VERSION,
            app_version=APP_VERSION,
            lang_code=LANG_CODE,
        )
        # Telethon's SQLiteSession defaults to a short busy wait; concurrent
        # connects (or a leftover probe) otherwise surface as "database is locked"
        # and the UI wrongly looks like a proxy failure.
        try:
            sess = client.session
            if getattr(sess, "_conn", None) is None and hasattr(sess, "_cursor"):
                cur = sess._cursor()
                try:
                    cur.close()
                except Exception:
                    pass
            conn = getattr(sess, "_conn", None)
            if conn is not None:
                conn.execute("PRAGMA busy_timeout=60000")
        except Exception:
            logger.debug("session busy_timeout pragma failed", exc_info=True)
        return client

    async def _reset_client(self) -> None:
        if self.client is not None:
            cfg = getattr(self.client, "_media_pool_cfg", None)
            if isinstance(cfg, dict):
                pools = cfg.get("pools") or {}
                for bucket in list(pools.values()):
                    for slot in list(bucket or []):
                        sender = slot.get("sender")
                        if sender is None:
                            continue
                        try:
                            await sender.disconnect()
                        except Exception:
                            pass
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None

    def set_proxy(self, proxy_url: str) -> None:
        """Update runtime proxy (also used before rebuild)."""
        self.settings.proxy = (proxy_url or "").strip()

    async def load_runtime_config(self) -> None:
        """Restore API / proxy saved from Web UI (overlays .env defaults)."""
        from app.db import db

        api_id = await db.get_meta("tg_api_id")
        api_hash = await db.get_meta("tg_api_hash")
        proxy = await db.get_meta("tg_proxy")
        if api_id:
            try:
                self.settings.api_id = int(api_id)
            except ValueError:
                pass
        if api_hash:
            self.settings.api_hash = str(api_hash)
        if proxy is not None:
            self.settings.proxy = str(proxy)

    async def save_runtime_config(self) -> None:
        """Persist API / proxy so restart can auto-reconnect."""
        from app.db import db

        if self.settings.api_id:
            await db.set_meta("tg_api_id", str(int(self.settings.api_id)))
        if self.settings.api_hash:
            await db.set_meta("tg_api_hash", str(self.settings.api_hash))
        await db.set_meta("tg_proxy", self.settings.proxy or "")

    async def try_auto_reconnect(self) -> dict[str, Any]:
        """
        On server start: load saved config and reconnect using existing session.
        Non-fatal — failures are logged and returned.
        """
        await self.load_runtime_config()
        if not self._session_has_auth():
            logger.info("Telegram auto-reconnect skipped: no saved session")
            return {"ok": False, "reason": "no_session"}
        if not self.settings.api_id or not self.settings.api_hash:
            logger.info("Telegram auto-reconnect skipped: missing API credentials")
            return {"ok": False, "reason": "missing_api"}

        try:
            client = await asyncio.wait_for(self.ensure_client(), timeout=45)
            authorized = await asyncio.wait_for(
                client.is_user_authorized(), timeout=15
            )
            if not authorized:
                logger.info("Telegram session exists but not authorized")
                return {"ok": False, "reason": "unauthorized"}
            me = await asyncio.wait_for(client.get_me(), timeout=15)
            name = getattr(me, "first_name", None) or getattr(me, "username", None) or me.id
            logger.info(
                "Telegram auto-reconnect OK: %s (proxy=%s)",
                name,
                self.settings.proxy or "direct",
            )
            return {
                "ok": True,
                "authorized": True,
                "user": {
                    "id": me.id,
                    "username": me.username,
                    "first_name": me.first_name,
                },
            }
        except Exception as e:
            logger.warning("Telegram auto-reconnect failed: %s", e)
            return {"ok": False, "reason": str(e)}

    async def ensure_client(
        self,
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None,
        *,
        force_rebuild: bool = False,
        persist: bool = True,
    ) -> TelegramClient:
        async with self._lock:
            changed = False
            if api_id:
                self.settings.api_id = int(api_id)
                changed = True
            if api_hash:
                self.settings.api_hash = api_hash
                changed = True

            aid = int(self.settings.api_id or 0)
            ahash = self.settings.api_hash or ""
            if not aid or not ahash:
                raise TelegramAuthError("missing_api", "请配置 API_ID 与 API_HASH")

            if force_rebuild:
                if self.client is not None:
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass
                    self.client = None

            if self.client is None:
                self.client = self._build_client(aid, ahash)

            if not self.client.is_connected():
                last_err: Exception | None = None
                for attempt in range(1, 4):
                    try:
                        # Same as reference: await client.connect() — no wait_for wrapper
                        await self.client.connect()
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        msg = str(e).lower()
                        locked = "database is locked" in msg or "database locked" in msg
                        logger.exception(
                            "Telegram connect failed (attempt %s/3)%s",
                            attempt,
                            " — session sqlite locked" if locked else "",
                        )
                        try:
                            await self.client.disconnect()
                        except Exception:
                            pass
                        self.client = None
                        if locked and attempt < 3:
                            await asyncio.sleep(1.2 * attempt)
                            self.client = self._build_client(aid, ahash)
                            continue
                        break
                if last_err is not None:
                    proxy = self.settings.proxy or ""
                    err_s = str(last_err)
                    if "database is locked" in err_s.lower():
                        tip = (
                            "Session 文件被占用（常见于重复进程/探测脚本），"
                            "不是代理挂了。请关闭多余 python 进程后重试"
                        )
                    elif not proxy:
                        tip = (
                            "当前为直连。本机实测 Telethon/MTProto 直连会超时，"
                            "官方 App 能开不等于库能直连。请填写 PROXY，例如 socks5://127.0.0.1:7897"
                        )
                    else:
                        tip = f"当前 PROXY={proxy}，请确认代理软件已开启"
                    raise TelegramAuthError(
                        "connect_error",
                        f"连接 Telegram 失败: {last_err}。{tip}",
                    ) from last_err

            # Media connection pool (sized to official-safe defaults)
            if self.client is not None and self.client.is_connected():
                from app import runtime_tune

                install_media_connection_pool(
                    self.client,
                    pool_size=runtime_tune.media_connections(),
                )

        if persist and (changed or aid):
            try:
                await self.save_runtime_config()
            except Exception:
                logger.debug("save_runtime_config failed", exc_info=True)

        return self.client

    async def test_connection(self) -> dict[str, Any]:
        """Diagnostic: try connect and report mode."""
        proxy = self.settings.parse_proxy()
        try:
            await self._reset_client()
            client = await self.ensure_client(force_rebuild=True)
            ok = client.is_connected()
            return {
                "ok": ok,
                "mode": "proxy" if proxy else "direct",
                "proxy": self.settings.proxy or None,
                "message": "连接成功" if ok else "未连接",
            }
        except TelegramAuthError as e:
            return {
                "ok": False,
                "mode": "proxy" if proxy else "direct",
                "proxy": self.settings.proxy or None,
                "message": str(e),
            }

    def _session_has_auth(self) -> bool:
        if self.client is not None:
            try:
                return bool(self.client.session and self.client.session.auth_key)
            except Exception:
                pass
        session_path = str(self.settings.session_path.resolve())
        session_file = Path(session_path + ".session")
        if not session_file.exists():
            return False
        try:
            from telethon.sessions import SQLiteSession

            session = SQLiteSession(session_path)
            has_auth = session.auth_key is not None
            session.close()
            return has_auth
        except Exception:
            return False

    async def status(self) -> dict[str, Any]:
        """
        Cheap status for the Web UI.

        Important: do NOT call ensure_client()/connect() here. Connecting can stall
        the event loop and freeze /api/tasks (download cards stuck on skeleton).
        Background try_auto_reconnect() owns connecting.
        """
        api_configured = bool(self.settings.api_id and self.settings.api_hash)
        has_session = self._session_has_auth()
        client = self.client

        if client is None or not client.is_connected():
            if not has_session:
                return {
                    "authorized": False,
                    "connected": False,
                    "need_api": not api_configured,
                    "message": "需要登录" if api_configured else "请配置 API_ID 与 API_HASH",
                    "user": None,
                    "api_configured": api_configured,
                    "proxy": self.settings.proxy or None,
                }
            return {
                # Session file exists — likely logged in; UI can show soft "connecting"
                "authorized": True,
                "connected": False,
                "need_api": False,
                "message": "Telegram 连接中…",
                "user": None,
                "api_configured": api_configured,
                "proxy": self.settings.proxy or None,
                "connecting": True,
            }

        try:
            authorized = await asyncio.wait_for(client.is_user_authorized(), timeout=1.5)
            user_info = None
            if authorized:
                me = await asyncio.wait_for(client.get_me(), timeout=1.5)
                user_info = {
                    "id": me.id,
                    "username": me.username,
                    "first_name": me.first_name,
                    "phone": me.phone,
                }
            return {
                "authorized": authorized,
                "connected": True,
                "need_api": False,
                "message": "ok" if authorized else "需要登录",
                "user": user_info,
                "api_configured": api_configured,
                "proxy": self.settings.proxy or None,
            }
        except asyncio.TimeoutError:
            return {
                "authorized": has_session,
                "connected": True,
                "need_api": not api_configured,
                "message": "Telegram 连接中…",
                "user": None,
                "api_configured": api_configured,
                "proxy": self.settings.proxy or None,
                "connecting": True,
            }
        except TelegramAuthError as e:
            return {
                "authorized": False,
                "connected": False,
                "need_api": e.code == "missing_api",
                "message": str(e),
                "user": None,
                "api_configured": api_configured,
                "proxy": self.settings.proxy or None,
            }
        except Exception as e:
            return {
                "authorized": False,
                "connected": False,
                "need_api": not api_configured,
                "message": f"会话异常: {e}",
                "user": None,
                "api_configured": api_configured,
                "proxy": self.settings.proxy or None,
            }

    async def send_code(
        self,
        phone: str,
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None,
        proxy: Optional[str] = None,
    ) -> dict[str, Any]:
        phone = (phone or "").strip()
        if not phone:
            raise TelegramAuthError("bad_phone", "请填写手机号")

        if proxy is not None:
            self.set_proxy(proxy)
            await self.save_runtime_config()

        await self._reset_client()
        client = await self.ensure_client(
            api_id=api_id, api_hash=api_hash, force_rebuild=True, persist=True
        )

        if await client.is_user_authorized():
            return {"status": "already_authorized"}

        result = await client.send_code_request(phone)
        self._phone_code_hash = result.phone_code_hash
        self._pending_phone = phone
        return {
            "status": "code_sent",
            "phone": phone,
            "proxy": self.settings.proxy or None,
        }

    async def sign_in(self, code: str, password: Optional[str] = None) -> dict[str, Any]:
        client = await self.ensure_client()
        if await client.is_user_authorized():
            return await self.status()

        if not self._pending_phone or not self._phone_code_hash:
            raise TelegramAuthError("no_pending", "请先发送验证码")

        try:
            await client.sign_in(
                phone=self._pending_phone,
                code=code.strip(),
                phone_code_hash=self._phone_code_hash,
            )
        except SessionPasswordNeededError:
            if not password:
                raise TelegramAuthError("need_2fa", "该账号启用了两步验证，请提供密码")
            try:
                await client.sign_in(password=password)
            except PasswordHashInvalidError as e:
                raise TelegramAuthError("bad_2fa", "两步验证密码错误") from e
        except PhoneCodeInvalidError as e:
            raise TelegramAuthError("bad_code", "验证码错误") from e
        except PhoneCodeExpiredError as e:
            raise TelegramAuthError("code_expired", "验证码已过期，请重新发送") from e

        self._phone_code_hash = None
        self._pending_phone = None
        await self.save_runtime_config()
        return await self.status()

    async def sign_in_2fa(self, password: str) -> dict[str, Any]:
        client = await self.ensure_client()
        try:
            await client.sign_in(password=password)
        except PasswordHashInvalidError as e:
            raise TelegramAuthError("bad_2fa", "两步验证密码错误") from e
        await self.save_runtime_config()
        return await self.status()

    async def logout(self) -> None:
        if self.client:
            try:
                if await self.client.is_user_authorized():
                    await self.client.log_out()
            finally:
                await self._reset_client()
        base = Path(str(self.settings.session_path.resolve()))
        for p in base.parent.glob(base.name + "*"):
            try:
                p.unlink()
            except OSError:
                pass

    async def list_dialogs(self, query: str = "") -> list[dict[str, Any]]:
        client = await self.ensure_client()
        if not await client.is_user_authorized():
            raise TelegramAuthError("unauthorized", "请先登录 Telegram")

        q = (query or "").strip().lower()
        results: list[dict[str, Any]] = []
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            is_group = dialog.is_group or dialog.is_channel
            if not is_group:
                continue
            if isinstance(entity, User):
                continue

            title = dialog.name or ""
            username = getattr(entity, "username", None) or ""
            if q and q not in title.lower() and q not in username.lower():
                continue

            kind = "channel"
            if dialog.is_group and not dialog.is_channel:
                kind = "group"
            elif isinstance(entity, Channel) and entity.megagroup:
                kind = "supergroup"
            elif isinstance(entity, Chat):
                kind = "group"

            results.append(
                {
                    "id": dialog.id,
                    "title": title,
                    "username": username,
                    "kind": kind,
                    "unread": dialog.unread_count,
                }
            )
        results.sort(key=lambda x: x["title"].lower())
        return results

    async def get_chat_title(self, chat_id: int | str) -> str:
        client = await self.ensure_client()
        entity = await client.get_entity(
            int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id
        )
        return (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or str(chat_id)
        )

    async def _pipelined_download(
        self,
        client: TelegramClient,
        media,
        path: Path,
        *,
        offset: int,
        expected: int,
        mode: str,
        progress_callback=None,
        stop_event: Optional[asyncio.Event] = None,
        pipeline: int = 6,
    ) -> int:
        """
        Multi-request pipelined getFile across the media connection pool.

        Sequential iter_download waits one RTT per chunk; through SOCKS to DC5
        that often lands around one part every few seconds. Keeping several
        parts in flight hides latency and raises sustained throughput.
        Prefers 1MiB parts (same as Pyrogram); falls back to 512KiB on LIMIT_INVALID.
        """
        from telethon import utils
        from telethon.errors import FileMigrateError
        from telethon.tl import functions, types

        dc_id, location = utils.get_input_location(media)
        home = int(getattr(client.session, "dc_id", 0) or 0)
        dc_id = int(dc_id or home or 0)
        if dc_id <= 0:
            raise RuntimeError("unknown media dc_id")

        from app import runtime_tune

        part = int(runtime_tune.download_part_size() or _TG_MAX_REQUEST_SIZE)
        # Align and clamp; Telegram allows up to 1MiB when 1MiB % limit == 0
        part = max(_TG_OFFSET_ALIGN, min(_TG_MAX_REQUEST_SIZE, part))
        part -= part % _TG_OFFSET_ALIGN
        if _TG_MAX_REQUEST_SIZE % part != 0:
            # keep only divisors of 1MiB (API rule without precise flag)
            for candidate in (1024 * 1024, 512 * 1024, 256 * 1024, 128 * 1024):
                if candidate <= part and _TG_MAX_REQUEST_SIZE % candidate == 0:
                    part = candidate
                    break
        pipeline = max(1, min(_TG_MAX_PIPELINE, int(pipeline or _TG_SAFE_PIPELINE)))
        next_fetch = int(offset)
        next_write = int(offset)
        pending: dict[int, asyncio.Task] = {}
        ready: dict[int, bytes] = {}
        asked: dict[int, int] = {}
        invalid_restarts = 0

        fetch_log_left = 6  # log first few getFile shapes for diagnosis

        async def _fetch(off: int, limit: int, *, _migrated: bool = False) -> bytes:
            nonlocal dc_id, fetch_log_left
            sender = await client._borrow_exported_sender(dc_id)
            retry_dc: int | None = None
            # Tail pieces (<4KiB or not 4KiB-aligned) need precise=True
            use_precise = (limit % _TG_OFFSET_ALIGN) != 0 or limit < _TG_OFFSET_ALIGN
            try:
                result = await asyncio.wait_for(
                    client._call(
                        sender,
                        functions.upload.GetFileRequest(
                            location,
                            off,
                            limit,
                            precise=True if use_precise else None,
                            cdn_supported=True,
                        ),
                    ),
                    timeout=60,
                )
                if isinstance(result, types.upload.FileCdnRedirect):
                    raise RuntimeError("cdn_redirect")
                data = result.bytes or b""
                if fetch_log_left > 0:
                    fetch_log_left -= 1
                    logger.debug(
                        "getFile OK off=%s lim=%sKiB got=%sKiB aligned=%s",
                        off,
                        limit // 1024,
                        len(data) // 1024,
                        (off % _TG_MAX_REQUEST_SIZE) == 0,
                    )
                return data
            except FileMigrateError as e:
                if _migrated:
                    raise
                retry_dc = int(e.new_dc)
            except LimitInvalidError:
                logger.warning(
                    "getFile LIMIT_INVALID off=%s lim=%sKiB aligned=%s "
                    "(Pyrogram only uses 1MiB-aligned offsets)",
                    off,
                    limit // 1024,
                    (off % _TG_MAX_REQUEST_SIZE) == 0,
                )
                raise
            finally:
                try:
                    await client._return_exported_sender(sender)
                except Exception:
                    pass
            if retry_dc is not None:
                dc_id = retry_dc
                return await _fetch(off, limit, _migrated=True)

        def _cancel_pending() -> None:
            for task in pending.values():
                task.cancel()
            pending.clear()

        def _limit_at(off: int) -> int:
            """
            Stay inside one 1MiB window; never request past EOF.
            Prefer limits that divide 1MiB (required when precise is off/ignored).
            """
            if expected > 0 and off >= expected:
                return 0
            remain = (expected - off) if expected > 0 else part
            if remain <= 0:
                return 0
            window_end = ((off // _TG_MAX_REQUEST_SIZE) + 1) * _TG_MAX_REQUEST_SIZE
            max_ok = min(part, window_end - off, remain)
            for candidate in _TG_VALID_LIMITS:
                if candidate <= max_ok:
                    return int(candidate)
            # remain < 4KiB — precise mode, 1KiB steps
            if remain >= 1024:
                return int(remain - (remain % 1024))
            return 1024

        # Warm media pool so the first pipeline wave is truly parallel
        from app import runtime_tune

        warm_n = max(1, min(pipeline, runtime_tune.media_connections()))
        warm_senders = []
        try:
            for _ in range(warm_n):
                warm_senders.append(await client._borrow_exported_sender(dc_id))
        finally:
            for s in warm_senders:
                try:
                    await client._return_exported_sender(s)
                except Exception:
                    pass

        logger.debug(
            "Pipelined download %s offset=%s pipeline=%s part=%sKiB dc=%s "
            "(1MiB-window aligned, pyrogram-style)",
            path.name,
            offset,
            pipeline,
            part // 1024,
            dc_id,
        )

        with path.open(mode) as f:
            idle_spins = 0
            while True:
                if stop_event is not None and stop_event.is_set():
                    _cancel_pending()
                    f.flush()
                    raise DownloadPaused()

                while len(pending) + len(ready) < pipeline and (
                    expected <= 0 or next_fetch < expected
                ):
                    off = next_fetch
                    lim = _limit_at(off)
                    if lim <= 0:
                        break
                    next_fetch += lim
                    asked[off] = lim
                    pending[off] = asyncio.create_task(_fetch(off, lim))

                if not pending and not ready:
                    break

                if pending:
                    idle_spins = 0
                    done, _ = await asyncio.wait(
                        pending.values(), return_when=asyncio.FIRST_COMPLETED
                    )
                    shrink_part = False
                    for task in done:
                        off = next(o for o, t in pending.items() if t is task)
                        pending.pop(off, None)
                        try:
                            ready[off] = task.result()
                            # Count buffered out-of-order bytes so UI speed ≠ 0
                            # while waiting on the head-of-line chunk.
                            if progress_callback:
                                buffered = sum(len(b) for b in ready.values())
                                progress_callback(
                                    next_write + buffered,
                                    expected or (next_write + buffered),
                                )
                        except asyncio.CancelledError:
                            raise
                        except LimitInvalidError:
                            failed_lim = int(asked.get(off, part))
                            remain = (
                                (expected - next_write) if expected > 0 else 0
                            )
                            # Near EOF: drain with valid 1MiB divisors (never use
                            # remain as limit — e.g. 197KiB → LIMIT_INVALID).
                            if (
                                expected > 0
                                and remain > 0
                                and remain <= _TG_MAX_REQUEST_SIZE
                            ):
                                logger.warning(
                                    "EOF tail LIMIT_INVALID off=%s lim=%sKiB "
                                    "remain=%s; draining with valid limits "
                                    "at %s/%s",
                                    off,
                                    failed_lim // 1024,
                                    remain,
                                    next_write,
                                    expected,
                                )
                                _cancel_pending()
                                ready.clear()
                                asked.clear()
                                try:
                                    while expected > 0 and next_write < expected:
                                        left = expected - next_write
                                        lim = 0
                                        for candidate in _TG_VALID_LIMITS:
                                            if candidate <= left:
                                                lim = candidate
                                                break
                                        if lim <= 0:
                                            lim = (
                                                left - (left % 1024)
                                                if left >= 1024
                                                else 1024
                                            )
                                        tail = await _fetch(next_write, lim)
                                        if not tail:
                                            break
                                        chunk = tail[:left]
                                        f.write(chunk)
                                        next_write += len(chunk)
                                        if progress_callback:
                                            progress_callback(
                                                next_write, expected
                                            )
                                        if len(tail) < lim:
                                            break
                                except Exception as tail_exc:
                                    logger.warning(
                                        "EOF precise tail failed: %s", tail_exc
                                    )
                                return next_write
                            if (
                                part > _TG_SAFE_REQUEST_SIZE
                                and failed_lim >= _TG_MAX_REQUEST_SIZE
                                and (off % _TG_MAX_REQUEST_SIZE) == 0
                            ):
                                shrink_part = True
                                break
                            invalid_restarts += 1
                            if invalid_restarts > 8:
                                _cancel_pending()
                                raise
                            _cancel_pending()
                            ready.clear()
                            asked.clear()
                            next_fetch = next_write
                            logger.warning(
                                "Restarting pipeline after LIMIT_INVALID "
                                "at off=%s lim=%sKiB (try %s)",
                                off,
                                failed_lim // 1024,
                                invalid_restarts,
                            )
                            break
                        except Exception:
                            _cancel_pending()
                            raise
                    else:
                        # no LimitInvalid restart break
                        pass
                    if shrink_part:
                        logger.warning(
                            "Aligned 1MiB getFile rejected on this DC; "
                            "falling back to %sKiB for %s",
                            _TG_SAFE_REQUEST_SIZE // 1024,
                            path.name,
                        )
                        _cancel_pending()
                        ready.clear()
                        asked.clear()
                        part = _TG_SAFE_REQUEST_SIZE
                        next_fetch = next_write
                        continue
                    if not pending and not ready and next_fetch == next_write:
                        # just restarted after mis-aligned LIMIT_INVALID
                        continue

                wrote = False
                while next_write in ready:
                    if stop_event is not None and stop_event.is_set():
                        _cancel_pending()
                        f.flush()
                        raise DownloadPaused()
                    wrote = True
                    idle_spins = 0
                    data = ready.pop(next_write)
                    want = asked.pop(next_write, part)
                    if not data:
                        _cancel_pending()
                        ready.clear()
                        asked.clear()
                        return next_write
                    f.write(data)
                    next_write += len(data)
                    if progress_callback:
                        progress_callback(next_write, expected or next_write)
                    if len(data) < want:
                        _cancel_pending()
                        ready.clear()
                        asked.clear()
                        return next_write
                    if expected > 0 and next_write >= expected:
                        _cancel_pending()
                        ready.clear()
                        asked.clear()
                        return next_write

                # Head-of-line hole / stall: always yield so HTTP stays responsive.
                if not pending and not wrote:
                    idle_spins += 1
                    if ready and next_write not in ready:
                        logger.warning(
                            "pipeline hole at off=%s (ready=%s); resync from head",
                            next_write,
                            sorted(ready)[:8],
                        )
                        ready.clear()
                        asked.clear()
                        next_fetch = next_write
                    elif expected > 0 and next_write >= expected:
                        break
                    elif expected > 0 and next_fetch >= expected and not ready:
                        break
                    elif idle_spins >= 32:
                        logger.warning(
                            "pipeline stall at off=%s/%s — abort to sequential",
                            next_write,
                            expected or "?",
                        )
                        _cancel_pending()
                        ready.clear()
                        asked.clear()
                        raise RuntimeError("pipeline_stall")
                    await asyncio.sleep(0.05 if idle_spins > 4 else 0)
                else:
                    # Yield between waves so pause/API are not starved
                    await asyncio.sleep(0)

        return next_write

    async def download_media_to(
        self,
        message,
        path: Path,
        progress_callback=None,
        *,
        resume: bool = True,
        stop_event: Optional[asyncio.Event] = None,
    ) -> Optional[Path]:
        """
        Download message media to path.

        Prefers pipelined multi-connection getFile (resume-safe .part). Falls
        back to Telethon iter_download on CDN / unsupported media.
        """
        client = await self.ensure_client()
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        expected = 0
        try:
            if message.file and getattr(message.file, "size", None):
                expected = int(message.file.size or 0)
        except (TypeError, ValueError):
            expected = 0

        offset = 0
        if resume and path.exists():
            try:
                offset = int(path.stat().st_size)
            except OSError:
                offset = 0

        if expected and offset >= expected > 0:
            if progress_callback:
                progress_callback(expected, expected)
            return path

        # Align offset down to 4KB (Telegram API requirement)
        if offset > 0:
            aligned = (offset // _TG_OFFSET_ALIGN) * _TG_OFFSET_ALIGN
            if aligned != offset:
                try:
                    with path.open("rb+") as f:
                        f.truncate(aligned)
                    offset = aligned
                    logger.info(
                        "Resume align truncate %s → %s bytes", path.name, offset
                    )
                except OSError:
                    path.unlink(missing_ok=True)
                    offset = 0

        if offset > 0:
            logger.info(
                "Resuming download %s from %s / %s",
                path.name,
                offset,
                expected or "?",
            )
            mode = "ab"
        else:
            mode = "wb"
            if path.exists():
                path.unlink(missing_ok=True)

        from app import runtime_tune

        install_media_connection_pool(
            client,
            pool_size=runtime_tune.media_connections(),
        )

        media = getattr(message, "media", None) or message
        downloaded = offset
        if progress_callback:
            progress_callback(downloaded, expected or 0)

        pipeline = runtime_tune.download_pipeline()
        try:
            try:
                downloaded = await self._pipelined_download(
                    client,
                    media,
                    path,
                    offset=offset,
                    expected=expected,
                    mode=mode,
                    progress_callback=progress_callback,
                    stop_event=stop_event,
                    pipeline=pipeline,
                )
            except DownloadPaused:
                raise
            except Exception as exc:
                logger.warning(
                    "pipelined download fallback for %s: %s: %r",
                    path.name,
                    type(exc).__name__,
                    exc,
                )
                downloaded = -1  # force sequential path below

            # Incomplete after pipeline (common on odd EOF tails) → Telethon iter_download
            need_seq = False
            if downloaded < 0:
                need_seq = True
            elif expected > 0:
                try:
                    have = int(path.stat().st_size) if path.exists() else int(downloaded)
                except OSError:
                    have = int(downloaded or 0)
                if have < expected:
                    need_seq = True
                    downloaded = have
                    logger.warning(
                        "incomplete after pipeline %s: %s/%s → sequential tail",
                        path.name,
                        have,
                        expected,
                    )

            if need_seq:
                if path.exists():
                    try:
                        offset = int(path.stat().st_size)
                    except OSError:
                        offset = max(0, int(downloaded or 0))
                else:
                    offset = max(0, int(downloaded or 0))
                mode = "ab" if offset > 0 else "wb"
                downloaded = offset
                with path.open(mode) as f:
                    # Telethon clamps request_size to 512KiB (valid 1MiB divisor)
                    async for chunk in client.iter_download(
                        media,
                        offset=offset,
                        request_size=_TG_SAFE_REQUEST_SIZE,
                        chunk_size=_TG_SAFE_REQUEST_SIZE,
                    ):
                        if stop_event is not None and stop_event.is_set():
                            f.flush()
                            raise DownloadPaused()
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback:
                            progress_callback(downloaded, expected or downloaded)
                        if expected > 0 and downloaded >= expected:
                            break
        except DownloadPaused:
            raise
        except Exception:
            logger.exception(
                "download failed at %s/%s for %s", downloaded, expected, path
            )
            raise

        if not path.exists() or path.stat().st_size <= 0:
            return None
        # Do not pretend success when bytes are still missing
        if expected > 0 and path.stat().st_size < expected:
            raise IOError(
                f"incomplete download: {path.stat().st_size}/{expected}"
            )
        return path

    async def disconnect(self) -> None:
        await self._reset_client()


tg_manager = TelegramManager()
