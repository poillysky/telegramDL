import json
import re
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiosqlite

from app.config import get_settings


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_download_mode(value: Any) -> str:
    """Map legacy modes → sequential | monitor."""
    m = (str(value or "sequential")).strip().lower()
    if m in ("sequential", "all", "date"):
        return "sequential"
    if m in ("monitor", "tags"):
        return "monitor"
    return "sequential"


# Factory defaults for relation / folder hub tags (editable via settings UI)
DEFAULT_TAG_RELATION_BLACKLIST = frozenset(
    {
        # hub / promo
        "半糖",
        "推荐",
        "热门",
        "精选",
        "合集",
        "资源",
        # platform / broadcast hubs
        "主播",
        "直播",
        "快手",
        "抖音",
        "色播",
        "推特",
        "易直播",
        "喵播",
        "双人",
        "新人播",
        "黄播",
        "花椒",
        "斗鱼",
        "直播录像",
        "直播间",
        "随缘直播间",
        # geo / content type
        "网红",
        "越南",
        "中国",
        "国产",
        "国产精品",
        "福利",
        "福利姬",
        "录屏",
        "定制",
        "大尺度",
        "偷拍宿舍",
        # recurring nick / persona hubs (pollute 关联)
        "小妲己",
        "小奶猫",
        "小狐狸",
        # body / look / role attrs (not person handles)
        "虎牙",
        "巨乳",
        "极品",
        "番茄",
        "翘臀",
        "啪啪",
        "清纯",
        "少妇",
        "萝莉",
        "学生",
        "学妹",
        "学姐",
        "御姐",
        "人妻",
        "极品人妻",
        "姐姐",
        "小姐姐",
        "高颜值",
        "反差",
        "反差尤物",
        "尤物",
        "白虎",
        "大奶",
        "性感",
        "美女",
        "美人儿",
        "混血",
        "可爱",
        "粉粉嫩嫩",
        "童颜巨乳",
        "一线馒头",
        "一线天",
        "白丝",
        "黑丝",
        "丝袜",
        "黑丝袜",
        "口交",
        "喷水",
        "调教",
        "自拍",
        "高潮",
        "自慰",
        "跳蛋",
        "裸舞",
        "女神",
        "大长腿",
        "潮喷",
        "露脸",
        "屁眼",
        "脱衣舞",
        "玩具",
        "小妹妹",
        "销售客服",
        "肛塞",
        "全裸",
        "骚货",
        "母狗",
        "情趣",
        "双飞",
        "约炮",
        "宿舍",
        "声音超甜",
    }
)

# Backward-compatible alias (prefer Database.get_tag_relation_blacklist)
TAG_RELATION_BLACKLIST = DEFAULT_TAG_RELATION_BLACKLIST

_TAG_BL_META_KEY = "tag_relation_blacklist"
_META_MAX_PARALLEL = "max_parallel_chats"
_META_FAILED_RETRY = "failed_retry_interval_sec"
_META_MONITOR_HEARTBEAT = "monitor_heartbeat_sec"
_META_MAX_FLOOD_WAIT = "max_flood_wait"
_META_MEDIA_CONNECTIONS = "media_connections"
_META_DOWNLOAD_PIPELINE = "download_pipeline"
_META_DOWNLOAD_PART_SIZE = "download_part_size"


def normalize_file_formats(raw: Any) -> list[str] | dict[str, list[str]]:
    """Global list, or per-type dict like {video:['mp4'], photo:['jpg']}."""
    if raw is None or raw == "" or raw == [] or raw == {}:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        if s.startswith("{") or s.startswith("["):
            try:
                raw = json.loads(s)
            except Exception:
                parts = [x.strip().lstrip(".").lower() for x in re.split(r"[,，\s]+", s) if x.strip()]
                return list(dict.fromkeys(parts))
        else:
            parts = [x.strip().lstrip(".").lower() for x in re.split(r"[,，\s]+", s) if x.strip()]
            return list(dict.fromkeys(parts))
    if isinstance(raw, dict):
        out: dict[str, list[str]] = {}
        for k, v in raw.items():
            key = str(k or "").strip().lower()
            if not key:
                continue
            if isinstance(v, str):
                items = [
                    x.strip().lstrip(".").lower()
                    for x in re.split(r"[,，\s]+", v)
                    if x.strip()
                ]
            elif isinstance(v, (list, tuple, set)):
                items = [
                    str(x).strip().lstrip(".").lower() for x in v if str(x).strip()
                ]
            else:
                items = []
            cleaned = list(dict.fromkeys(items))
            if cleaned:
                out[key] = cleaned
        return out
    if isinstance(raw, (list, tuple, set)):
        parts = [str(x).strip().lstrip(".").lower() for x in raw if str(x).strip()]
        return list(dict.fromkeys(parts))
    return []


def normalize_blacklist_tag(tag: Any) -> str:
    t = str(tag or "").strip().lstrip("#").strip()
    return t


def normalize_blacklist_tags(raw: Any) -> list[str]:
    """Deduped tag list preserving first-seen casing."""
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = re.split(r"[,，\s]+", raw)
    if not isinstance(raw, (list, tuple, set, frozenset)):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        t = normalize_blacklist_tag(item)
        if not t:
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    out.sort(key=lambda x: x.lower())
    return out


SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT NOT NULL,
    chat_title TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    media_types TEXT NOT NULL DEFAULT '["photo","video","document","audio","voice","video_note"]',
    use_text_as_folder INTEGER NOT NULL DEFAULT 1,
    min_folder_title_len INTEGER NOT NULL DEFAULT 2,
    start_message_id INTEGER NOT NULL DEFAULT 0,
    end_message_id INTEGER,
    start_date TEXT,
    end_date TEXT,
    last_message_id INTEGER NOT NULL DEFAULT 0,
    current_folder TEXT,
    processed_count INTEGER NOT NULL DEFAULT 0,
    downloaded_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_log TEXT,
    download_order TEXT NOT NULL DEFAULT 'added_first',
    test_mode INTEGER NOT NULL DEFAULT 0,
    concurrency INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS downloaded (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    chat_id TEXT,
    file_path TEXT,
    status TEXT NOT NULL DEFAULT 'done',
    error TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(task_id, message_id)
);

CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS web_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_downloaded_task ON downloaded(task_id);
CREATE INDEX IF NOT EXISTS idx_downloaded_status_id ON downloaded(status, id DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

CREATE TABLE IF NOT EXISTS chat_completed (
    chat_id TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    file_path TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_chat_completed_chat ON chat_completed(chat_id);

CREATE TABLE IF NOT EXISTS chat_index_meta (
    chat_id TEXT PRIMARY KEY,
    chat_title TEXT NOT NULL DEFAULT '',
    last_scan_at TEXT,
    last_message_id INTEGER NOT NULL DEFAULT 0,
    media_count INTEGER NOT NULL DEFAULT 0,
    scanned_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'idle',
    last_error TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS chat_media_index (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    grouped_id TEXT,
    media_type TEXT,
    caption TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]',
    msg_date TEXT,
    updated_at TEXT,
    UNIQUE(chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS chat_tag_map (
    chat_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY (chat_id, tag, message_id)
);

CREATE INDEX IF NOT EXISTS idx_media_index_chat ON chat_media_index(chat_id);
CREATE INDEX IF NOT EXISTS idx_tag_map_chat_tag ON chat_tag_map(chat_id, tag);
"""


class Database:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path or get_settings().db_path)
        self._conn: Optional[aiosqlite.Connection] = None
        self._tag_bl_cache: Optional[frozenset[str]] = None
        # Debounced log lines: task_id -> [line, ...]
        self._log_pending: dict[int, list[str]] = {}
        self._log_flush_task: Optional[asyncio.Task] = None
        self._write_lock = asyncio.Lock()

    async def connect(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        # Cut writer lock storms under multi-task downloads / index scans
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA busy_timeout=30000")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA temp_store=MEMORY")
        await self._conn.executescript(SCHEMA)
        await self._migrate()
        await self._conn.commit()

    async def _migrate(self) -> None:
        async with self.conn.execute("PRAGMA table_info(tasks)") as cur:
            cols = {row["name"] for row in await cur.fetchall()}
        if "download_order" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN download_order TEXT NOT NULL DEFAULT 'added_first'"
            )
        if "test_mode" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN test_mode INTEGER NOT NULL DEFAULT 0"
            )
        if "concurrency" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN concurrency INTEGER NOT NULL DEFAULT 1"
            )
        if "file_formats" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN file_formats TEXT NOT NULL DEFAULT '[]'"
            )
        if "max_messages" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN max_messages INTEGER"
            )
        if "delay_min" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN delay_min REAL NOT NULL DEFAULT 0.5"
            )
        if "delay_max" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN delay_max REAL NOT NULL DEFAULT 0.5"
            )
        if "folder_mode" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN folder_mode TEXT NOT NULL DEFAULT 'caption'"
            )
        if "include_tags" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN include_tags TEXT NOT NULL DEFAULT '[]'"
            )
        if "caption_keywords" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN caption_keywords TEXT NOT NULL DEFAULT '[]'"
            )
        if "tag_match_mode" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN tag_match_mode TEXT NOT NULL DEFAULT 'any'"
            )
        if "download_mode" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN download_mode TEXT NOT NULL DEFAULT 'sequential'"
            )
        if "use_index" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN use_index INTEGER NOT NULL DEFAULT 0"
            )
        if "min_file_bytes" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN min_file_bytes INTEGER NOT NULL DEFAULT 0"
            )
        if "max_file_bytes" not in cols:
            await self.conn.execute(
                "ALTER TABLE tasks ADD COLUMN max_file_bytes INTEGER NOT NULL DEFAULT 0"
            )
        async with self.conn.execute("PRAGMA table_info(chat_index_meta)") as cur:
            idx_cols = {row["name"] for row in await cur.fetchall()}
        if "auto_incremental" not in idx_cols:
            await self.conn.execute(
                "ALTER TABLE chat_index_meta ADD COLUMN auto_incremental INTEGER NOT NULL DEFAULT 0"
            )
        if "auto_interval_min" not in idx_cols:
            await self.conn.execute(
                "ALTER TABLE chat_index_meta ADD COLUMN auto_interval_min INTEGER NOT NULL DEFAULT 60"
            )

        # Chat-level completed media (survives task delete; queue uses this + local files)
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_completed (
                chat_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                file_path TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chat_completed_chat ON chat_completed(chat_id)"
        )
        # One-time backfill from historical downloaded rows
        if not await self.get_meta("chat_completed_backfilled"):
            await self.conn.execute(
                """
                INSERT OR IGNORE INTO chat_completed(chat_id, message_id, file_path, updated_at)
                SELECT t.chat_id, d.message_id, d.file_path, COALESCE(d.created_at, ?)
                FROM downloaded d
                JOIN tasks t ON t.id = d.task_id
                WHERE d.status = 'done'
                """
                ,
                (_utcnow(),),
            )
            await self.set_meta("chat_completed_backfilled", "1")

        # Denormalize chat_id onto downloaded for fast history queries
        async with self.conn.execute("PRAGMA table_info(downloaded)") as cur:
            dl_cols = {row["name"] for row in await cur.fetchall()}
        if "chat_id" not in dl_cols:
            await self.conn.execute(
                "ALTER TABLE downloaded ADD COLUMN chat_id TEXT"
            )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_downloaded_status_id ON downloaded(status, id DESC)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_downloaded_chat_status_id "
            "ON downloaded(chat_id, status, id DESC)"
        )
        if not await self.get_meta("downloaded_chat_id_backfilled"):
            await self.conn.execute(
                """
                UPDATE downloaded
                SET chat_id = (
                    SELECT t.chat_id FROM tasks t WHERE t.id = downloaded.task_id
                )
                WHERE chat_id IS NULL OR chat_id = ''
                """
            )
            await self.set_meta("downloaded_chat_id_backfilled", "1")

        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_tag_graph_cache (
                chat_id TEXT PRIMARY KEY,
                groups_json TEXT NOT NULL DEFAULT '[]',
                related_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            )
            """
        )

        # One-time: strip 「标签】日期索引」junk from stored index tags
        if not await self.get_meta("tag_bracket_cleaned_v2"):
            await self._cleanup_bracket_junk_tags()
            await self.set_meta("tag_bracket_cleaned_v2", "1")
            # keep v1 marker too so older checks stay quiet
            await self.set_meta("tag_bracket_cleaned_v1", "1")

    async def _cleanup_bracket_junk_tags(self) -> None:
        """Normalize dirty tags in chat_tag_map / chat_media_index / tasks."""
        from app.organizer import normalize_tag_list, normalize_tag_name

        # --- chat_tag_map ---
        async with self.conn.execute(
            "SELECT chat_id, tag, message_id FROM chat_tag_map"
        ) as cur:
            rows = await cur.fetchall()
        dirty_map: list[tuple[str, str, int]] = []
        clean_inserts: list[tuple[str, str, int]] = []
        for r in rows:
            raw = str(r["tag"] or "")
            clean = normalize_tag_name(raw)
            if clean == raw:
                continue
            dirty_map.append((r["chat_id"], raw, int(r["message_id"])))
            if clean:
                clean_inserts.append((r["chat_id"], clean, int(r["message_id"])))
        if clean_inserts:
            await self.conn.executemany(
                """
                INSERT OR IGNORE INTO chat_tag_map(chat_id, tag, message_id)
                VALUES (?, ?, ?)
                """,
                clean_inserts,
            )
        if dirty_map:
            await self.conn.executemany(
                "DELETE FROM chat_tag_map WHERE chat_id = ? AND tag = ? AND message_id = ?",
                dirty_map,
            )

        # --- chat_media_index.tags JSON ---
        async with self.conn.execute(
            "SELECT chat_id, message_id, tags FROM chat_media_index"
        ) as cur:
            media_rows = await cur.fetchall()
        media_updates: list[tuple[str, str, int]] = []
        for r in media_rows:
            raw_tags = r["tags"] or "[]"
            try:
                parsed = (
                    json.loads(raw_tags) if isinstance(raw_tags, str) else (raw_tags or [])
                )
            except Exception:
                parsed = []
            if not isinstance(parsed, list):
                parsed = []
            cleaned = normalize_tag_list(parsed)
            stripped = [str(x).strip().lstrip("#") for x in parsed if str(x).strip()]
            dirty = cleaned != stripped or any(
                normalize_tag_name(x) != x for x in stripped
            )
            if dirty:
                media_updates.append(
                    (
                        json.dumps(cleaned, ensure_ascii=False),
                        r["chat_id"],
                        int(r["message_id"]),
                    )
                )
        if media_updates:
            await self.conn.executemany(
                """
                UPDATE chat_media_index SET tags = ?
                WHERE chat_id = ? AND message_id = ?
                """,
                media_updates,
            )

        # --- tasks.include_tags ---
        async with self.conn.execute("SELECT id, include_tags FROM tasks") as cur:
            task_rows = await cur.fetchall()
        for r in task_rows:
            raw = r["include_tags"] or "[]"
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else (raw or [])
            except Exception:
                parsed = []
            cleaned = normalize_tag_list(parsed)
            new_json = json.dumps(cleaned, ensure_ascii=False)
            old_json = (
                raw if isinstance(raw, str) else json.dumps(parsed, ensure_ascii=False)
            )
            if new_json != old_json:
                await self.conn.execute(
                    "UPDATE tasks SET include_tags = ? WHERE id = ?",
                    (new_json, int(r["id"])),
                )

        # Drop related-tag graph so it rebuilds from cleaned tags
        await self.conn.execute("DELETE FROM chat_tag_graph_cache")
        self.invalidate_tag_groups_cache()
        await self.conn.commit()


    async def close(self) -> None:
        try:
            await self.flush_pending_logs()
        except Exception:
            pass
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if not self._conn:
            raise RuntimeError("Database not connected")
        return self._conn

    async def get_meta(self, key: str, default: Optional[str] = None) -> Optional[str]:
        async with self.conn.execute(
            "SELECT value FROM app_meta WHERE key = ?", (key,)
        ) as cur:
            row = await cur.fetchone()
            return row["value"] if row else default

    async def set_meta(self, key: str, value: str) -> None:
        await self.conn.execute(
            """
            INSERT INTO app_meta(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        await self.conn.commit()

    async def get_tag_relation_blacklist(self) -> frozenset[str]:
        """Runtime blacklist for 关联 / folder hubs (app_meta or factory defaults)."""
        if self._tag_bl_cache is not None:
            return self._tag_bl_cache
        raw = await self.get_meta(_TAG_BL_META_KEY)
        if raw is None:
            self._tag_bl_cache = frozenset(DEFAULT_TAG_RELATION_BLACKLIST)
            return self._tag_bl_cache
        tags = normalize_blacklist_tags(raw)
        self._tag_bl_cache = frozenset(tags)
        return self._tag_bl_cache

    async def set_tag_relation_blacklist(self, tags: Any) -> list[str]:
        """Replace blacklist; returns sorted display list."""
        cleaned = normalize_blacklist_tags(tags)
        await self.set_meta(_TAG_BL_META_KEY, json.dumps(cleaned, ensure_ascii=False))
        self._tag_bl_cache = frozenset(cleaned)
        # Related map depends on blacklist — drop durable graph for all chats
        try:
            await self.invalidate_tag_graph_cache()
        except Exception:
            self.invalidate_tag_groups_cache()
        return cleaned

    async def add_tag_relation_blacklist(self, tag: str) -> list[str]:
        name = normalize_blacklist_tag(tag)
        if not name:
            raise ValueError("标签名不能为空")
        current = list(await self.get_tag_relation_blacklist())
        key = name.lower()
        if key not in {t.lower() for t in current}:
            current.append(name)
        return await self.set_tag_relation_blacklist(current)

    async def remove_tag_relation_blacklist(self, tag: str) -> list[str]:
        name = normalize_blacklist_tag(tag)
        if not name:
            raise ValueError("标签名不能为空")
        key = name.lower()
        current = [
            t for t in await self.get_tag_relation_blacklist() if t.lower() != key
        ]
        return await self.set_tag_relation_blacklist(current)

    async def reset_tag_relation_blacklist(self) -> list[str]:
        return await self.set_tag_relation_blacklist(
            sorted(DEFAULT_TAG_RELATION_BLACKLIST, key=lambda x: x.lower())
        )

    @staticmethod
    def _manual_tag_links_meta_key(chat_id: str | int) -> str:
        return f"tag_manual_links:{chat_id}"

    @staticmethod
    def _normalize_manual_link_tags(tags: Any) -> list[str]:
        from app.organizer import normalize_tag_list

        cleaned = normalize_tag_list(tags or [])
        # stable unique by lower key
        seen: set[str] = set()
        out: list[str] = []
        for t in cleaned:
            key = t.lower().lstrip("#")
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(t)
        return out

    @staticmethod
    def _manual_link_set_key(tags: list[str]) -> frozenset[str]:
        return frozenset(t.lower().lstrip("#") for t in tags if t)

    async def list_manual_tag_links(self, chat_id: str | int) -> list[list[str]]:
        """User-defined co-occurrence groups for this chat (manual associations)."""
        raw = await self.get_meta(self._manual_tag_links_meta_key(chat_id))
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except Exception:
            return []
        if not isinstance(data, list):
            return []
        out: list[list[str]] = []
        for item in data:
            tags = self._normalize_manual_link_tags(item)
            if len(tags) >= 2:
                out.append(tags)
        return out

    async def _save_manual_tag_links(
        self, chat_id: str | int, links: list[list[str]]
    ) -> list[list[str]]:
        cleaned: list[list[str]] = []
        seen_keys: set[frozenset[str]] = set()
        for item in links:
            tags = self._normalize_manual_link_tags(item)
            if len(tags) < 2:
                continue
            key = self._manual_link_set_key(tags)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            cleaned.append(tags)
        await self.set_meta(
            self._manual_tag_links_meta_key(chat_id),
            json.dumps(cleaned, ensure_ascii=False),
        )
        try:
            await self.invalidate_tag_graph_cache(chat_id)
        except Exception:
            self.invalidate_tag_groups_cache(chat_id)
        return cleaned

    async def add_manual_tag_link(
        self, chat_id: str | int, tags: Any
    ) -> list[list[str]]:
        """
        Persist a manual association group (union of two picker rows).
        Overlapping existing links are merged into one group.
        """
        new_tags = self._normalize_manual_link_tags(tags)
        if len(new_tags) < 2:
            raise ValueError("至少需要两个标签才能关联")
        new_key = self._manual_link_set_key(new_tags)
        current = await self.list_manual_tag_links(chat_id)
        merged_tags = list(new_tags)
        kept: list[list[str]] = []
        for link in current:
            link_key = self._manual_link_set_key(link)
            if link_key & new_key:
                # merge overlapping manual links
                for t in link:
                    lk = t.lower().lstrip("#")
                    if lk and lk not in {x.lower().lstrip("#") for x in merged_tags}:
                        merged_tags.append(t)
            else:
                kept.append(link)
        merged_tags = self._normalize_manual_link_tags(merged_tags)
        kept.append(merged_tags)
        return await self._save_manual_tag_links(chat_id, kept)

    async def remove_manual_tag_link(
        self, chat_id: str | int, tags: Any
    ) -> list[list[str]]:
        """Remove the manual link whose tag set equals the given tags."""
        target = self._normalize_manual_link_tags(tags)
        if len(target) < 2:
            raise ValueError("标签无效")
        target_key = self._manual_link_set_key(target)
        current = await self.list_manual_tag_links(chat_id)
        kept = [
            link
            for link in current
            if self._manual_link_set_key(link) != target_key
        ]
        return await self._save_manual_tag_links(chat_id, kept)

    async def _relation_tag_groups(self, chat_id: str | int) -> list[list[str]]:
        """Caption co-occurrence groups plus user manual links."""
        groups = list(await self.list_index_tag_groups(chat_id))
        groups.extend(await self.list_manual_tag_links(chat_id))
        return groups

    def _row_to_task(self, row: aiosqlite.Row) -> dict[str, Any]:
        d = dict(row)
        d["media_types"] = json.loads(d["media_types"])
        raw_fmts = d.get("file_formats") or "[]"
        try:
            d["file_formats"] = json.loads(raw_fmts) if isinstance(raw_fmts, str) else (raw_fmts or [])
        except Exception:
            d["file_formats"] = []
        d["use_text_as_folder"] = bool(d["use_text_as_folder"])
        d["test_mode"] = bool(d.get("test_mode", 0))
        d["concurrency"] = max(1, min(5, int(d.get("concurrency") or 1)))
        d["max_messages"] = d.get("max_messages")
        d["min_file_bytes"] = max(0, int(d.get("min_file_bytes") or 0))
        d["max_file_bytes"] = max(0, int(d.get("max_file_bytes") or 0))
        d["delay_min"] = float(d.get("delay_min") if d.get("delay_min") is not None else 0.5)
        d["delay_max"] = float(d.get("delay_max") if d.get("delay_max") is not None else d["delay_min"])
        d["folder_mode"] = d.get("folder_mode") or "caption"
        for key in ("include_tags", "caption_keywords"):
            raw = d.get(key) or "[]"
            try:
                d[key] = json.loads(raw) if isinstance(raw, str) else (raw or [])
            except Exception:
                d[key] = []
        mode = (d.get("tag_match_mode") or "any").strip().lower()
        d["tag_match_mode"] = mode if mode in ("any", "all") else "any"
        d["download_mode"] = normalize_download_mode(d.get("download_mode"))
        d["use_index"] = bool(
            d.get("use_index", 0) or d["download_mode"] == "monitor"
        )
        return d

    async def create_task(self, data: dict[str, Any]) -> dict[str, Any]:
        now = _utcnow()
        settings = get_settings()
        media_types = json.dumps(
            data.get(
                "media_types",
                ["photo", "video", "document", "audio", "voice", "video_note"],
            )
        )
        formats = normalize_file_formats(data.get("file_formats"))
        min_file_bytes = max(0, int(data.get("min_file_bytes") or 0))
        max_file_bytes = max(0, int(data.get("max_file_bytes") or 0))
        concurrency = max(1, min(5, int(data.get("concurrency") or 2)))
        delay_min = float(data.get("delay_min", settings.download_delay_min or settings.download_delay))
        delay_max = float(data.get("delay_max", settings.download_delay_max or delay_min))
        if delay_max < delay_min:
            delay_max = delay_min
        max_messages = data.get("max_messages")
        if max_messages is not None and max_messages != "":
            max_messages = int(max_messages)
            if max_messages <= 0:
                max_messages = None
        else:
            max_messages = None
        folder_mode = data.get("folder_mode") or "caption"
        if folder_mode not in ("caption", "media_type", "flat"):
            folder_mode = "caption"
        use_text = bool(data.get("use_text_as_folder", True))
        if folder_mode != "caption":
            use_text = False
        include_tags = data.get("include_tags") or []
        from app.organizer import normalize_tag_list

        include_tags = normalize_tag_list(include_tags)
        caption_keywords = data.get("caption_keywords") or []
        if isinstance(caption_keywords, str):
            caption_keywords = [x.strip() for x in re.split(r"[,，]+", caption_keywords) if x.strip()]
        else:
            caption_keywords = [str(x).strip() for x in caption_keywords if str(x).strip()]
        tag_match_mode = (data.get("tag_match_mode") or "any").strip().lower()
        if tag_match_mode not in ("any", "all"):
            tag_match_mode = "any"
        download_mode = normalize_download_mode(data.get("download_mode"))
        use_index = bool(data.get("use_index", download_mode == "monitor"))

        cur = await self.conn.execute(
            """
            INSERT INTO tasks (
                chat_id, chat_title, status, media_types, use_text_as_folder,
                min_folder_title_len, start_message_id, end_message_id,
                start_date, end_date, download_order, test_mode, concurrency,
                file_formats, max_messages, delay_min, delay_max, folder_mode,
                include_tags, caption_keywords, tag_match_mode,
                download_mode, use_index, min_file_bytes, max_file_bytes,
                created_at, updated_at
            ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(data["chat_id"]),
                data.get("chat_title", ""),
                media_types,
                1 if use_text else 0,
                int(data.get("min_folder_title_len", settings.min_folder_title_len)),
                int(data.get("start_message_id") or 0),
                data.get("end_message_id"),
                data.get("start_date"),
                data.get("end_date"),
                data.get("download_order") or "added_first",
                1 if data.get("test_mode", False) else 0,
                concurrency,
                json.dumps(formats, ensure_ascii=False),
                max_messages,
                delay_min,
                delay_max,
                folder_mode,
                json.dumps(include_tags, ensure_ascii=False),
                json.dumps(caption_keywords, ensure_ascii=False),
                tag_match_mode,
                download_mode,
                1 if use_index else 0,
                min_file_bytes,
                max_file_bytes,
                now,
                now,
            ),
        )
        await self.conn.commit()
        return await self.get_task(cur.lastrowid)

    async def get_task(self, task_id: int) -> Optional[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ) as cur:
            row = await cur.fetchone()
            return self._row_to_task(row) if row else None

    async def list_tasks(self) -> list[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT * FROM tasks ORDER BY id DESC"
        ) as cur:
            rows = await cur.fetchall()
            return [self._row_to_task(r) for r in rows]

    async def update_task(
        self, task_id: int, *, commit: bool = True, **fields: Any
    ) -> Optional[dict[str, Any]]:
        if not fields:
            return await self.get_task(task_id)
        if "media_types" in fields and not isinstance(fields["media_types"], str):
            fields["media_types"] = json.dumps(fields["media_types"])
        if "file_formats" in fields and not isinstance(fields["file_formats"], str):
            fields["file_formats"] = json.dumps(fields["file_formats"] or [])
        for key in ("include_tags", "caption_keywords"):
            if key in fields and not isinstance(fields[key], str):
                if key == "include_tags":
                    from app.organizer import normalize_tag_list

                    fields[key] = json.dumps(
                        normalize_tag_list(fields[key] or []), ensure_ascii=False
                    )
                else:
                    fields[key] = json.dumps(fields[key] or [], ensure_ascii=False)
        if "use_text_as_folder" in fields:
            fields["use_text_as_folder"] = 1 if fields["use_text_as_folder"] else 0
        if "test_mode" in fields:
            fields["test_mode"] = 1 if fields["test_mode"] else 0
        if "use_index" in fields:
            fields["use_index"] = 1 if fields["use_index"] else 0
        fields["updated_at"] = _utcnow()
        cols = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [task_id]
        await self.conn.execute(
            f"UPDATE tasks SET {cols} WHERE id = ?", values
        )
        if commit:
            await self.conn.commit()
        return await self.get_task(task_id)

    async def delete_task(self, task_id: int) -> None:
        # Preserve chat-level completion so recreate can skip local/already-done media
        task = await self.get_task(task_id)
        if task:
            chat_id = str(task.get("chat_id") or "")
            if chat_id:
                await self.conn.execute(
                    """
                    INSERT INTO chat_completed(chat_id, message_id, file_path, updated_at)
                    SELECT ?, d.message_id, d.file_path, COALESCE(d.created_at, ?)
                    FROM downloaded d
                    WHERE d.task_id = ? AND d.status = 'done'
                    ON CONFLICT(chat_id, message_id) DO UPDATE SET
                        file_path = COALESCE(excluded.file_path, chat_completed.file_path),
                        updated_at = excluded.updated_at
                    """,
                    (chat_id, _utcnow(), int(task_id)),
                )
        await self.conn.execute("DELETE FROM downloaded WHERE task_id = ?", (task_id,))
        await self.conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        await self.conn.commit()

    async def mark_chat_completed(
        self,
        chat_id: str | int,
        message_id: int,
        file_path: Optional[str] = None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO chat_completed(chat_id, message_id, file_path, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET
                file_path = COALESCE(excluded.file_path, chat_completed.file_path),
                updated_at = excluded.updated_at
            """,
            (str(chat_id), int(message_id), file_path, _utcnow()),
        )

    async def is_chat_message_completed(
        self, chat_id: str | int, message_id: int
    ) -> bool:
        async with self.conn.execute(
            """
            SELECT 1 FROM chat_completed
            WHERE chat_id = ? AND message_id = ?
            """,
            (str(chat_id), int(message_id)),
        ) as cur:
            return await cur.fetchone() is not None

    async def is_message_done(
        self, task_id: int, message_id: int, *, chat_id: str | int | None = None
    ) -> bool:
        async with self.conn.execute(
            "SELECT 1 FROM downloaded WHERE task_id = ? AND message_id = ? AND status = 'done'",
            (task_id, message_id),
        ) as cur:
            if await cur.fetchone() is not None:
                return True
        cid = chat_id
        if cid is None:
            task = await self.get_task(task_id)
            cid = task.get("chat_id") if task else None
        if cid is None:
            return False
        return await self.is_chat_message_completed(cid, message_id)

    async def list_done_message_ids(self, task_id: int) -> set[int]:
        async with self.conn.execute(
            """
            SELECT message_id FROM downloaded
            WHERE task_id = ? AND status = 'done'
            """,
            (int(task_id),),
        ) as cur:
            rows = await cur.fetchall()
            ids = {int(r["message_id"]) for r in rows}
        task = await self.get_task(task_id)
        if task and task.get("chat_id") is not None:
            async with self.conn.execute(
                """
                SELECT message_id FROM chat_completed WHERE chat_id = ?
                """,
                (str(task["chat_id"]),),
            ) as cur:
                rows = await cur.fetchall()
                ids.update(int(r["message_id"]) for r in rows)
        return ids

    async def count_index_pending(
        self,
        task_id: int,
        chat_id: str | int,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        keywords: list[str] | None = None,
        media_types: list[str] | None = None,
    ) -> int:
        """How many indexed matches are not yet done (task + chat-level / local)."""
        _, total = await self.list_task_queue_items(
            task_id,
            chat_id,
            tags=tags,
            tag_match_mode=tag_match_mode,
            keywords=keywords,
            media_types=media_types,
            limit=1,
            offset=0,
        )
        return int(total)

    async def find_chat_completed_file(
        self, chat_id: str | int, message_id: int
    ) -> Optional[str]:
        """Return file_path if this chat already finished the message (any task / local)."""
        chat_id = str(chat_id)
        mid = int(message_id)
        async with self.conn.execute(
            """
            SELECT file_path FROM chat_completed
            WHERE chat_id = ? AND message_id = ?
              AND file_path IS NOT NULL AND file_path != ''
            LIMIT 1
            """,
            (chat_id, mid),
        ) as cur:
            row = await cur.fetchone()
            if row and row["file_path"]:
                return str(row["file_path"])
        async with self.conn.execute(
            """
            SELECT d.file_path
            FROM downloaded d
            JOIN tasks t ON t.id = d.task_id
            WHERE t.chat_id = ? AND d.message_id = ? AND d.status = 'done'
              AND d.file_path IS NOT NULL AND d.file_path != ''
            ORDER BY d.id DESC
            LIMIT 1
            """,
            (chat_id, mid),
        ) as cur:
            row = await cur.fetchone()
            return str(row["file_path"]) if row and row["file_path"] else None

    async def list_failed_message_ids(self, task_id: int) -> list[int]:
        """Unfinished downloads: partial (.part resume) first, then hard fails."""
        async with self.conn.execute(
            """
            SELECT message_id FROM downloaded
            WHERE task_id = ? AND status IN ('failed', 'partial')
            ORDER BY CASE status WHEN 'partial' THEN 0 ELSE 1 END,
                     message_id ASC
            """,
            (task_id,),
        ) as cur:
            rows = await cur.fetchall()
            return [int(r["message_id"]) for r in rows]

    async def count_failed(self, task_id: int) -> int:
        async with self.conn.execute(
            """
            SELECT COUNT(*) AS c FROM downloaded
            WHERE task_id = ? AND status IN ('failed', 'partial')
            """,
            (task_id,),
        ) as cur:
            row = await cur.fetchone()
            return int(row["c"] if row else 0)

    async def map_basenames_to_undone_message_ids(
        self, task_id: int, basenames: list[str] | set[str]
    ) -> dict[str, int]:
        """Map final filename → message_id for non-done rows with matching file_path."""
        from pathlib import Path

        want = {str(b).strip().lower() for b in (basenames or []) if str(b).strip()}
        if not want:
            return {}
        async with self.conn.execute(
            """
            SELECT message_id, file_path FROM downloaded
            WHERE task_id = ?
              AND status != 'done'
              AND file_path IS NOT NULL AND file_path != ''
            """,
            (task_id,),
        ) as cur:
            rows = await cur.fetchall()
        out: dict[str, int] = {}
        for r in rows:
            name = Path(str(r["file_path"] or "")).name.lower()
            if name in want and name not in out:
                out[name] = int(r["message_id"])
        return out

    async def list_resume_basenames(self, task_id: int) -> set[str]:
        """Final filenames of partial/failed rows (for temp keep-list)."""
        from pathlib import Path

        names: set[str] = set()
        async with self.conn.execute(
            """
            SELECT file_path FROM downloaded
            WHERE task_id = ?
              AND status IN ('failed', 'partial')
              AND file_path IS NOT NULL AND file_path != ''
            """,
            (task_id,),
        ) as cur:
            for r in await cur.fetchall():
                p = str(r["file_path"] or "").strip()
                if p:
                    names.add(Path(p).name)
        return names

    async def list_done_message_ids_for_basenames(
        self, task_id: int, basenames: list[str] | set[str]
    ) -> set[str]:
        """Basenames that already have a done row (should not keep .part)."""
        from pathlib import Path

        want = {str(b).strip().lower() for b in (basenames or []) if str(b).strip()}
        if not want:
            return set()
        hit: set[str] = set()
        async with self.conn.execute(
            """
            SELECT file_path FROM downloaded
            WHERE task_id = ?
              AND status = 'done'
              AND file_path IS NOT NULL AND file_path != ''
            """,
            (task_id,),
        ) as cur:
            for r in await cur.fetchall():
                name = Path(str(r["file_path"] or "")).name.lower()
                if name in want:
                    hit.add(name)
        return hit

    async def mark_message(
        self,
        task_id: int,
        message_id: int,
        status: str = "done",
        file_path: Optional[str] = None,
        error: Optional[str] = None,
        *,
        chat_id: str | int | None = None,
        commit: bool = True,
    ) -> None:
        cid = chat_id
        if cid is None:
            task = await self.get_task(task_id)
            cid = task.get("chat_id") if task else None
        cid_s = str(cid) if cid is not None else None
        await self.conn.execute(
            """
            INSERT INTO downloaded(task_id, message_id, chat_id, file_path, status, error, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id, message_id) DO UPDATE SET
                chat_id = COALESCE(excluded.chat_id, downloaded.chat_id),
                file_path = excluded.file_path,
                status = excluded.status,
                error = excluded.error
            """,
            (task_id, message_id, cid_s, file_path, status, error, _utcnow()),
        )
        if status == "done" and cid_s is not None:
            await self.mark_chat_completed(cid_s, message_id, file_path=file_path)
        if commit:
            await self.conn.commit()

    async def list_download_history(
        self,
        *,
        q: str = "",
        chat_id: str = "",
        status: str = "done",
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """
        History page query.

        Avoids correlated ORDER BY subqueries (O(n²) on large tables). Uses
        denormalized downloaded.chat_id + a one-shot chat_last CTE when listing
        across all groups.
        """
        limit = max(1, min(200, int(limit)))
        offset = max(0, int(offset))
        status = (status or "").strip()
        chat_id = str(chat_id or "").strip()
        q = (q or "").strip()

        where: list[str] = ["1=1"]
        params: list[Any] = []
        if status:
            where.append("d.status = ?")
            params.append(status)
        if chat_id:
            where.append("d.chat_id = ?")
            params.append(chat_id)
        if q:
            where.append(
                "(d.file_path LIKE ? OR IFNULL(t.chat_title,'') LIKE ? "
                "OR CAST(d.message_id AS TEXT) LIKE ?)"
            )
            like = f"%{q}%"
            params.extend([like, like, like])
        clause = " AND ".join(where)

        # COUNT: prefer downloaded-only when search does not touch chat_title
        if q:
            count_sql = f"""
                SELECT COUNT(*) AS c
                FROM downloaded d
                LEFT JOIN tasks t ON t.id = d.task_id
                WHERE {clause}
            """
            count_params = list(params)
        elif chat_id and status:
            count_sql = (
                "SELECT COUNT(*) AS c FROM downloaded d "
                "WHERE d.status = ? AND d.chat_id = ?"
            )
            count_params = [status, chat_id]
        elif status:
            count_sql = "SELECT COUNT(*) AS c FROM downloaded d WHERE d.status = ?"
            count_params = [status]
        else:
            count_sql = f"""
                SELECT COUNT(*) AS c
                FROM downloaded d
                LEFT JOIN tasks t ON t.id = d.task_id
                WHERE {clause}
            """
            count_params = list(params)

        async with self.conn.execute(count_sql, count_params) as cur:
            total = int((await cur.fetchone())["c"])

        select_cols = """
            d.id, d.task_id, d.message_id, d.file_path, d.status, d.error, d.created_at,
            COALESCE(d.chat_id, t.chat_id) AS chat_id,
            IFNULL(t.chat_title, '') AS chat_title
        """
        if chat_id:
            # Single group: cheap index scan on (chat_id, status, id)
            list_sql = f"""
                SELECT {select_cols}
                FROM downloaded d
                LEFT JOIN tasks t ON t.id = d.task_id
                WHERE {clause}
                ORDER BY d.id DESC
                LIMIT ? OFFSET ?
            """
            list_params = params + [limit, offset]
        else:
            # All groups: cluster by latest activity without per-row subquery
            status_for_cte = status or "done"
            list_sql = f"""
                WITH chat_last AS (
                    SELECT chat_id AS cid, MAX(id) AS last_id
                    FROM downloaded
                    WHERE status = ?
                      AND chat_id IS NOT NULL
                      AND chat_id != ''
                    GROUP BY chat_id
                )
                SELECT {select_cols}
                FROM downloaded d
                LEFT JOIN tasks t ON t.id = d.task_id
                LEFT JOIN chat_last cl ON cl.cid = d.chat_id
                WHERE {clause}
                ORDER BY COALESCE(cl.last_id, d.id) DESC, d.chat_id, d.id DESC
                LIMIT ? OFFSET ?
            """
            list_params = [status_for_cte] + params + [limit, offset]

        async with self.conn.execute(list_sql, list_params) as cur:
            rows = await cur.fetchall()
        items = []
        for r in rows:
            d = dict(r)
            path = d.get("file_path") or ""
            d["file_name"] = Path(path).name if path else ""
            items.append(d)
        return items, total

    async def list_download_history_groups(
        self,
        *,
        q: str = "",
        status: str = "done",
    ) -> list[dict[str, Any]]:
        status = (status or "").strip()
        q = (q or "").strip()
        where = ["1=1"]
        params: list[Any] = []
        if status:
            where.append("d.status = ?")
            params.append(status)
        if q:
            where.append(
                "(d.file_path LIKE ? OR IFNULL(t.chat_title,'') LIKE ? "
                "OR CAST(d.message_id AS TEXT) LIKE ?)"
            )
            like = f"%{q}%"
            params.extend([like, like, like])
        clause = " AND ".join(where)
        # Prefer denormalized chat_id; fall back to tasks join for title / legacy rows
        async with self.conn.execute(
            f"""
            SELECT COALESCE(d.chat_id, t.chat_id) AS chat_id,
                   MAX(IFNULL(t.chat_title, '')) AS chat_title,
                   COUNT(*) AS count,
                   MAX(d.created_at) AS latest_at,
                   MAX(d.id) AS latest_id
            FROM downloaded d
            LEFT JOIN tasks t ON t.id = d.task_id
            WHERE {clause}
            GROUP BY COALESCE(d.chat_id, t.chat_id)
            ORDER BY latest_id DESC
            """,
            params,
        ) as cur:
            rows = await cur.fetchall()
        return [
            {
                "chat_id": str(r["chat_id"] or ""),
                "chat_title": r["chat_title"] or str(r["chat_id"] or "未知群组"),
                "count": int(r["count"] or 0),
                "latest_at": r["latest_at"] or "",
            }
            for r in rows
        ]

    async def get_download_by_id(self, item_id: int) -> Optional[dict[str, Any]]:
        async with self.conn.execute(
            """
            SELECT d.id, d.task_id, d.message_id, d.file_path, d.status, d.error, d.created_at,
                   t.chat_id, t.chat_title
            FROM downloaded d
            JOIN tasks t ON t.id = d.task_id
            WHERE d.id = ?
            """,
            (int(item_id),),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        d = dict(row)
        path = d.get("file_path") or ""
        d["file_name"] = Path(path).name if path else ""
        return d

    async def get_max_parallel_chats(self) -> int:
        raw = await self.get_meta(_META_MAX_PARALLEL)
        if raw is None or str(raw).strip() == "":
            return max(1, int(get_settings().max_parallel_chats or 1))
        try:
            return max(1, min(10, int(raw)))
        except (TypeError, ValueError):
            return max(1, int(get_settings().max_parallel_chats or 1))

    async def set_max_parallel_chats(self, n: int) -> int:
        val = max(1, min(10, int(n)))
        await self.set_meta(_META_MAX_PARALLEL, str(val))
        return val

    async def _get_meta_int(
        self,
        key: str,
        *,
        default: int,
        min_v: int,
        max_v: int,
    ) -> int:
        raw = await self.get_meta(key)
        if raw is None or str(raw).strip() == "":
            return max(min_v, min(max_v, int(default)))
        try:
            return max(min_v, min(max_v, int(raw)))
        except (TypeError, ValueError):
            return max(min_v, min(max_v, int(default)))

    async def get_failed_retry_interval_sec(self) -> int:
        env = int(get_settings().failed_retry_interval_sec or 900)
        return await self._get_meta_int(
            _META_FAILED_RETRY, default=env, min_v=120, max_v=86400
        )

    async def set_failed_retry_interval_sec(self, n: int) -> int:
        val = max(120, min(86400, int(n)))
        await self.set_meta(_META_FAILED_RETRY, str(val))
        return val

    async def get_monitor_heartbeat_sec(self) -> int:
        env = int(get_settings().monitor_heartbeat_sec or 600)
        return await self._get_meta_int(
            _META_MONITOR_HEARTBEAT, default=env, min_v=60, max_v=86400
        )

    async def set_monitor_heartbeat_sec(self, n: int) -> int:
        val = max(60, min(86400, int(n)))
        await self.set_meta(_META_MONITOR_HEARTBEAT, str(val))
        return val

    async def get_max_flood_wait(self) -> int:
        env = int(get_settings().max_flood_wait or 1800)
        return await self._get_meta_int(
            _META_MAX_FLOOD_WAIT, default=env, min_v=60, max_v=86400
        )

    async def set_max_flood_wait(self, n: int) -> int:
        val = max(60, min(86400, int(n)))
        await self.set_meta(_META_MAX_FLOOD_WAIT, str(val))
        return val

    async def get_media_connections(self) -> int:
        env = int(get_settings().media_connections or 3)
        return await self._get_meta_int(
            _META_MEDIA_CONNECTIONS, default=env, min_v=1, max_v=8
        )

    async def set_media_connections(self, n: int) -> int:
        val = max(1, min(8, int(n)))
        await self.set_meta(_META_MEDIA_CONNECTIONS, str(val))
        return val

    async def get_download_pipeline(self) -> int:
        env = int(get_settings().download_pipeline or 4)
        return await self._get_meta_int(
            _META_DOWNLOAD_PIPELINE, default=env, min_v=1, max_v=8
        )

    async def set_download_pipeline(self, n: int) -> int:
        val = max(1, min(8, int(n)))
        await self.set_meta(_META_DOWNLOAD_PIPELINE, str(val))
        return val

    async def get_download_part_size(self) -> int:
        env = int(get_settings().download_part_size or (1024 * 1024))
        raw = await self.get_meta(_META_DOWNLOAD_PART_SIZE)
        if raw is None or str(raw).strip() == "":
            part = env
        else:
            try:
                part = int(raw)
            except (TypeError, ValueError):
                part = env
        if part >= 1024 * 1024:
            return 1024 * 1024
        return 512 * 1024

    async def set_download_part_size(self, n: int) -> int:
        val = 1024 * 1024 if int(n) >= 768 * 1024 else 512 * 1024
        await self.set_meta(_META_DOWNLOAD_PART_SIZE, str(val))
        return val

    async def append_log(self, task_id: int, message: str, keep: int = 80) -> None:
        """Buffer log lines and flush in batches (cuts SQLite commits under load)."""
        stamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{stamp}] {message}"
        async with self._write_lock:
            self._log_pending.setdefault(int(task_id), []).append(line)
            # Bound memory if flush is delayed
            if len(self._log_pending[int(task_id)]) > keep:
                self._log_pending[int(task_id)] = self._log_pending[int(task_id)][-keep:]
        self._arm_log_flush(keep=keep)

    def _arm_log_flush(self, *, keep: int = 80) -> None:
        if self._log_flush_task and not self._log_flush_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _delayed() -> None:
            try:
                await asyncio.sleep(0.4)
                await self.flush_pending_logs(keep=keep)
            except Exception:
                pass

        self._log_flush_task = loop.create_task(_delayed())

    async def flush_pending_logs(self, *, keep: int = 80) -> None:
        async with self._write_lock:
            pending = self._log_pending
            self._log_pending = {}
        if not pending:
            return
        for task_id, new_lines in pending.items():
            task = await self.get_task(task_id)
            if not task:
                continue
            lines = [x for x in (task.get("last_log") or "").split("\n") if x.strip()]
            times = []
            for line in lines:
                m = re.match(r"^\[(\d{2}:\d{2}:\d{2})\]", line.strip())
                if m:
                    times.append(m.group(1))
            if len(times) >= 2 and times == sorted(times):
                lines = list(reversed(lines))
            # Newest first: later buffered lines end up on top
            for line in new_lines:
                lines.insert(0, line)
            lines = lines[:keep]
            await self.update_task(task_id, commit=False, last_log="\n".join(lines))
        await self.conn.commit()

    async def clear_log(self, task_id: int) -> bool:
        async with self._write_lock:
            self._log_pending.pop(int(task_id), None)
        task = await self.get_task(task_id)
        if not task:
            return False
        await self.update_task(task_id, last_log="")
        return True

    # —— Web users ——
    async def count_web_users(self) -> int:
        async with self.conn.execute("SELECT COUNT(*) AS c FROM web_users") as cur:
            row = await cur.fetchone()
            return int(row["c"] if row else 0)

    async def web_auth_required(self) -> bool:
        if await self.count_web_users() > 0:
            return True
        return bool(get_settings().web_password)

    async def ensure_web_users_seeded(self) -> None:
        """Seed first Web user from WEB_USERNAME / WEB_PASSWORD if table empty."""
        if await self.count_web_users() > 0:
            return
        settings = get_settings()
        if not settings.web_password:
            return
        from app.auth_token import hash_password

        username = (settings.web_username or "admin").strip() or "admin"
        await self.create_web_user(username, settings.web_password)

    async def list_web_users(self) -> list[dict[str, Any]]:
        async with self.conn.execute(
            """
            SELECT id, username, created_at, updated_at
            FROM web_users
            ORDER BY id ASC
            """
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_web_user(self, username: str) -> Optional[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT * FROM web_users WHERE username = ?",
            (username.strip(),),
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

    async def create_web_user(self, username: str, password: str) -> dict[str, Any]:
        from app.auth_token import hash_password

        username = username.strip()
        if not username:
            raise ValueError("用户名不能为空")
        if len(username) > 64:
            raise ValueError("用户名过长")
        if any(ch.isspace() or ch in ":/\\" for ch in username):
            raise ValueError("用户名不能包含空格或 : / \\")
        if not password or len(password) < 4:
            raise ValueError("密码至少 4 位")
        if await self.get_web_user(username):
            raise ValueError("用户名已存在")
        now = _utcnow()
        await self.conn.execute(
            """
            INSERT INTO web_users(username, password_hash, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (username, hash_password(password), now, now),
        )
        await self.conn.commit()
        user = await self.get_web_user(username)
        assert user
        return {"id": user["id"], "username": user["username"], "created_at": user["created_at"]}

    async def set_web_user_password(self, username: str, password: str) -> None:
        from app.auth_token import hash_password

        if not password or len(password) < 4:
            raise ValueError("密码至少 4 位")
        user = await self.get_web_user(username)
        if not user:
            raise ValueError("用户不存在")
        await self.conn.execute(
            """
            UPDATE web_users
            SET password_hash = ?, updated_at = ?
            WHERE username = ?
            """,
            (hash_password(password), _utcnow(), username.strip()),
        )
        await self.conn.commit()

    async def delete_web_user(self, username: str) -> None:
        username = username.strip()
        if await self.count_web_users() <= 1:
            raise ValueError("至少保留一个 Web 账号")
        user = await self.get_web_user(username)
        if not user:
            raise ValueError("用户不存在")
        await self.conn.execute(
            "DELETE FROM web_users WHERE username = ?", (username,)
        )
        await self.conn.commit()

    # ── chat media caption index ──────────────────────────────────────

    async def get_index_meta(self, chat_id: str | int) -> Optional[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT * FROM chat_index_meta WHERE chat_id = ?",
            (str(chat_id),),
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

    async def upsert_index_meta(self, chat_id: str | int, **fields: Any) -> dict[str, Any]:
        chat_id = str(chat_id)
        fields = {k: v for k, v in fields.items() if k == "last_error" or v is not None}
        fields["updated_at"] = _utcnow()
        if "auto_incremental" in fields:
            fields["auto_incremental"] = 1 if fields["auto_incremental"] else 0
        if "auto_interval_min" in fields:
            fields["auto_interval_min"] = max(
                5, min(24 * 60, int(fields["auto_interval_min"] or 60))
            )
        existing = await self.get_index_meta(chat_id)
        if not existing:
            await self.conn.execute(
                """
                INSERT INTO chat_index_meta (
                    chat_id, chat_title, last_scan_at, last_message_id,
                    media_count, scanned_count, status, last_error, updated_at,
                    auto_incremental, auto_interval_min
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    fields.get("chat_title", ""),
                    fields.get("last_scan_at"),
                    int(fields.get("last_message_id") or 0),
                    int(fields.get("media_count") or 0),
                    int(fields.get("scanned_count") or 0),
                    fields.get("status", "idle"),
                    fields.get("last_error"),
                    fields["updated_at"],
                    int(fields.get("auto_incremental") or 0),
                    int(fields.get("auto_interval_min") or 60),
                ),
            )
        else:
            cols = ", ".join(f"{k} = ?" for k in fields)
            values = list(fields.values()) + [chat_id]
            await self.conn.execute(
                f"UPDATE chat_index_meta SET {cols} WHERE chat_id = ?",
                values,
            )
        await self.conn.commit()
        meta = await self.get_index_meta(chat_id)
        assert meta
        return meta

    async def list_auto_index_chats(self) -> list[dict[str, Any]]:
        """Chats with automatic incremental index enabled."""
        async with self.conn.execute(
            """
            SELECT chat_id, chat_title, last_scan_at, auto_interval_min, status
            FROM chat_index_meta
            WHERE auto_incremental = 1
            ORDER BY chat_id
            """
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def set_auto_index(
        self,
        chat_id: str | int,
        *,
        enabled: bool,
        interval_min: int = 60,
        chat_title: str = "",
    ) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "auto_incremental": bool(enabled),
            "auto_interval_min": interval_min,
        }
        if chat_title:
            fields["chat_title"] = chat_title
        return await self.upsert_index_meta(chat_id, **fields)

    async def clear_chat_index(self, chat_id: str | int) -> None:
        chat_id = str(chat_id)
        await self.conn.execute(
            "DELETE FROM chat_tag_map WHERE chat_id = ?", (chat_id,)
        )
        await self.conn.execute(
            "DELETE FROM chat_media_index WHERE chat_id = ?", (chat_id,)
        )
        await self.conn.execute(
            """
            UPDATE chat_index_meta
            SET media_count = 0, scanned_count = 0, last_message_id = 0,
                last_error = NULL, updated_at = ?
            WHERE chat_id = ?
            """,
            (_utcnow(), chat_id),
        )
        await self.conn.commit()

    async def upsert_media_index_item(
        self,
        chat_id: str | int,
        message_id: int,
        *,
        caption: str = "",
        tags: list[str] | None = None,
        grouped_id: Any = None,
        media_type: Optional[str] = None,
        msg_date: Optional[str] = None,
    ) -> None:
        from app.organizer import normalize_tag_list

        chat_id = str(chat_id)
        tags = normalize_tag_list(tags or [])
        now = _utcnow()
        gid = str(grouped_id) if grouped_id is not None else None
        await self.conn.execute(
            """
            INSERT INTO chat_media_index (
                chat_id, message_id, grouped_id, media_type, caption, tags, msg_date, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET
                grouped_id = excluded.grouped_id,
                media_type = excluded.media_type,
                caption = excluded.caption,
                tags = excluded.tags,
                msg_date = excluded.msg_date,
                updated_at = excluded.updated_at
            """,
            (
                chat_id,
                int(message_id),
                gid,
                media_type,
                caption or "",
                json.dumps(tags, ensure_ascii=False),
                msg_date,
                now,
            ),
        )
        await self.conn.execute(
            "DELETE FROM chat_tag_map WHERE chat_id = ? AND message_id = ?",
            (chat_id, int(message_id)),
        )
        tag_rows = []
        for tag in tags:
            if tag:
                tag_rows.append((chat_id, tag, int(message_id)))
        if tag_rows:
            await self.conn.executemany(
                """
                INSERT OR IGNORE INTO chat_tag_map(chat_id, tag, message_id)
                VALUES (?, ?, ?)
                """,
                tag_rows,
            )

    async def resolve_stored_tag_aliases(
        self, chat_id: str | int, tags: list[str] | None
    ) -> list[str]:
        """Map clean tag names to all stored variants (incl. dirty bracket junk)."""
        from app.organizer import normalize_tag_name, normalize_tag_list

        clean = normalize_tag_list(tags or [])
        wanted = {t.lower() for t in clean}
        if not wanted:
            return []
        chat_id = str(chat_id)
        async with self.conn.execute(
            "SELECT DISTINCT tag FROM chat_tag_map WHERE chat_id = ?",
            (chat_id,),
        ) as cur:
            rows = await cur.fetchall()
        out: list[str] = []
        seen: set[str] = set()
        for r in rows:
            raw = str(r["tag"] or "")
            if normalize_tag_name(raw).lower() in wanted and raw not in seen:
                seen.add(raw)
                out.append(raw)
        for t in clean:
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out

    async def _tag_match_sql(
        self,
        chat_id: str,
        tags: list[str] | None,
        *,
        tag_match_mode: str = "any",
    ) -> tuple[str, list[Any]]:
        """Build SQL fragment for tag filtering; expands dirty stored aliases."""
        from app.organizer import normalize_tag_list

        clean = normalize_tag_list(tags or [])
        if not clean:
            return "", []
        mode = (tag_match_mode or "any").strip().lower()
        if mode not in ("any", "all"):
            mode = "any"

        if mode == "all":
            parts: list[str] = []
            params: list[Any] = []
            for t in clean:
                aliases = await self.resolve_stored_tag_aliases(chat_id, [t])
                ph = ",".join("?" for _ in aliases)
                parts.append(
                    f"""m.message_id IN (
                        SELECT DISTINCT message_id FROM chat_tag_map
                        WHERE chat_id = ? AND tag IN ({ph})
                    )"""
                )
                params.extend([chat_id, *aliases])
            return " AND ".join(parts), params

        aliases = await self.resolve_stored_tag_aliases(chat_id, clean)
        ph = ",".join("?" for _ in aliases)
        return (
            f"""m.message_id IN (
                SELECT DISTINCT message_id FROM chat_tag_map
                WHERE chat_id = ? AND tag IN ({ph})
            )""",
            [chat_id, *aliases],
        )

    async def commit(self) -> None:
        await self.conn.commit()

    async def get_last_caption_message_id(
        self, chat_id: str | int, *, before_id: Optional[int] = None
    ) -> int:
        """Latest indexed message that already has a non-empty caption."""
        chat_id = str(chat_id)
        if before_id is not None and int(before_id) > 0:
            sql = """
                SELECT message_id FROM chat_media_index
                WHERE chat_id = ? AND caption != '' AND message_id < ?
                ORDER BY message_id DESC LIMIT 1
            """
            params: tuple[Any, ...] = (chat_id, int(before_id))
        else:
            sql = """
                SELECT message_id FROM chat_media_index
                WHERE chat_id = ? AND caption != ''
                ORDER BY message_id DESC LIMIT 1
            """
            params = (chat_id,)
        async with self.conn.execute(sql, params) as cur:
            row = await cur.fetchone()
            return int(row["message_id"]) if row else 0

    async def backfill_empty_video_captions(
        self,
        chat_id: str | int,
        caption: str,
        *,
        after_message_id: int,
        before_message_id: int,
        media_types: tuple[str, ...] = ("video", "video_note"),
    ) -> list[int]:
        """
        Assign caption to empty-caption videos strictly between two message ids.
        Rule: uncaptioned videos between two captions belong to the *next* caption.
        Returns updated message_ids.
        """
        caption = (caption or "").strip()
        if not caption:
            return []
        after_id = int(after_message_id or 0)
        before_id = int(before_message_id or 0)
        if before_id <= after_id + 1:
            return []
        chat_id = str(chat_id)
        placeholders = ",".join("?" for _ in media_types)
        async with self.conn.execute(
            f"""
            SELECT message_id, grouped_id, media_type, msg_date
            FROM chat_media_index
            WHERE chat_id = ?
              AND message_id > ? AND message_id < ?
              AND (caption = '' OR caption IS NULL)
              AND media_type IN ({placeholders})
            ORDER BY message_id ASC
            """,
            (chat_id, after_id, before_id, *media_types),
        ) as cur:
            rows = await cur.fetchall()
        if not rows:
            return []

        from app.organizer import extract_tags

        tags = extract_tags(caption)
        updated: list[int] = []
        for row in rows:
            mid = int(row["message_id"])
            await self.upsert_media_index_item(
                chat_id,
                mid,
                caption=caption,
                tags=tags,
                grouped_id=row["grouped_id"],
                media_type=row["media_type"],
                msg_date=row["msg_date"],
            )
            updated.append(mid)
        return updated

    async def count_media_index(self, chat_id: str | int) -> int:
        async with self.conn.execute(
            "SELECT COUNT(*) AS c FROM chat_media_index WHERE chat_id = ?",
            (str(chat_id),),
        ) as cur:
            row = await cur.fetchone()
            return int(row["c"] if row else 0)

    async def list_index_tags(self, chat_id: str | int) -> list[dict[str, Any]]:
        from app.organizer import normalize_tag_name

        async with self.conn.execute(
            """
            SELECT tag, COUNT(*) AS count
            FROM chat_tag_map
            WHERE chat_id = ?
            GROUP BY tag
            ORDER BY count DESC, tag ASC
            """,
            (str(chat_id),),
        ) as cur:
            rows = await cur.fetchall()
        # 合并「泡泡咕」与「泡泡咕】11-26…」等脏标签
        by_lower: dict[str, tuple[str, int]] = {}
        for r in rows:
            name = normalize_tag_name(r["tag"])
            if not name:
                continue
            key = name.lower()
            count = int(r["count"])
            if key in by_lower:
                disp, prev = by_lower[key]
                by_lower[key] = (disp, prev + count)
            else:
                by_lower[key] = (name, count)
        return [
            {"tag": name, "count": count}
            for name, count in sorted(
                by_lower.values(), key=lambda x: (-x[1], x[0])
            )
        ]

    # chat_id -> (monotonic_ts, groups) — expand_related scans whole index otherwise
    _tag_groups_cache: dict[str, tuple[float, list[list[str]]]] = {}
    _TAG_GROUPS_TTL = 45.0

    def invalidate_tag_groups_cache(self, chat_id: str | int | None = None) -> None:
        if chat_id is None:
            self._tag_groups_cache.clear()
            return
        self._tag_groups_cache.pop(str(chat_id), None)

    async def invalidate_tag_graph_cache(self, chat_id: str | int | None = None) -> None:
        """Drop durable + memory tag graph (call after index scan / before rebuild)."""
        self.invalidate_tag_groups_cache(chat_id)
        if chat_id is None:
            await self.conn.execute("DELETE FROM chat_tag_graph_cache")
        else:
            await self.conn.execute(
                "DELETE FROM chat_tag_graph_cache WHERE chat_id = ?",
                (str(chat_id),),
            )
        await self.conn.commit()

    async def _load_tag_groups_raw(self, chat_id: str | int) -> list[list[str]]:
        """Scan chat_media_index for caption tag groups (no cache)."""
        key = str(chat_id)
        async with self.conn.execute(
            """
            SELECT tags FROM chat_media_index
            WHERE chat_id = ? AND tags != '[]' AND tags IS NOT NULL
            """,
            (key,),
        ) as cur:
            rows = await cur.fetchall()
        from app.organizer import normalize_tag_list

        groups: list[list[str]] = []
        for r in rows:
            raw = r["tags"] or "[]"
            try:
                tags = json.loads(raw) if isinstance(raw, str) else (raw or [])
            except Exception:
                tags = []
            cleaned = normalize_tag_list(tags)
            if cleaned:
                groups.append(cleaned)
        return groups

    async def rebuild_tag_graph_cache(self, chat_id: str | int) -> None:
        """Precompute groups + related map after index scan (avoids cold full-table expand)."""
        import time
        from collections import defaultdict

        from app.organizer import strip_relation_blacklist

        key = str(chat_id)
        groups = await self._load_tag_groups_raw(key)
        self._tag_groups_cache[key] = (time.monotonic(), groups)

        blacklist = await self.get_tag_relation_blacklist()
        weight: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        casing: dict[str, str] = {}
        for group in groups:
            tags = strip_relation_blacklist(group, blacklist)
            if len(tags) < 2:
                continue
            for t in tags:
                casing.setdefault(t.lower(), t)
            for i, a in enumerate(tags):
                for b in tags[i + 1 :]:
                    ka, kb = a.lower(), b.lower()
                    if ka == kb:
                        continue
                    weight[ka][kb] += 1
                    weight[kb][ka] += 1
        related: dict[str, list[str]] = {}
        bl = {t.lower() for t in blacklist}
        for k, partners in weight.items():
            if k in bl:
                continue
            name = casing.get(k, k)
            ranked = sorted(partners.items(), key=lambda kv: (-kv[1], kv[0]))
            related[name] = [
                casing.get(p, p) for p, c in ranked if c >= 2 and p not in bl
            ]

        await self.conn.execute(
            """
            INSERT INTO chat_tag_graph_cache(chat_id, groups_json, related_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                groups_json = excluded.groups_json,
                related_json = excluded.related_json,
                updated_at = excluded.updated_at
            """,
            (
                key,
                json.dumps(groups, ensure_ascii=False),
                json.dumps(related, ensure_ascii=False),
                _utcnow(),
            ),
        )
        await self.conn.commit()

    async def list_index_tag_groups(self, chat_id: str | int) -> list[list[str]]:
        """Tag lists from each indexed caption (for co-occurrence / relatedness)."""
        import time

        key = str(chat_id)
        now = time.monotonic()
        cached = self._tag_groups_cache.get(key)
        if cached and (now - cached[0]) < self._TAG_GROUPS_TTL:
            return cached[1]

        # Durable cache (rebuilt after index scan) — skip full table parse on cold start
        async with self.conn.execute(
            "SELECT groups_json FROM chat_tag_graph_cache WHERE chat_id = ?",
            (key,),
        ) as cur:
            row = await cur.fetchone()
        if row and row["groups_json"]:
            try:
                groups = json.loads(row["groups_json"])
                if isinstance(groups, list):
                    self._tag_groups_cache[key] = (now, groups)
                    return groups
            except Exception:
                pass

        groups = await self._load_tag_groups_raw(key)
        self._tag_groups_cache[key] = (now, groups)
        return groups

    async def list_tag_cooccur_bundles(
        self, chat_id: str | int, *, min_count: int = 1, limit: int = 2000
    ) -> list[dict[str, Any]]:
        """
        Multi-tag caption patterns: tags that appear together in one caption.
        [{tags: [...], count: n}, ...] sorted by frequency.
        """
        from collections import Counter

        from app.organizer import normalize_tag_list

        min_count = max(1, int(min_count or 1))
        limit = max(1, min(5000, int(limit or 2000)))
        ctr: Counter[tuple[str, ...]] = Counter()
        display: dict[tuple[str, ...], list[str]] = {}
        # Hub blacklist (settings) — strip before co-occur keys
        blacklist = {t.lower() for t in await self.get_tag_relation_blacklist()}
        for group in await self.list_index_tag_groups(chat_id):
            tags = [
                t
                for t in normalize_tag_list(group)
                if t and t.lower().lstrip("#") not in blacklist
            ]
            if len(tags) < 2:
                continue
            key = tuple(sorted(t.lower() for t in tags))
            ctr[key] += 1
            if key not in display:
                # stable display order: original caption order after normalize
                display[key] = tags
        # Manual associations count as co-occurrence bridges for the picker
        manual_keys: set[tuple[str, ...]] = set()
        for group in await self.list_manual_tag_links(chat_id):
            tags = [
                t
                for t in normalize_tag_list(group)
                if t and t.lower().lstrip("#") not in blacklist
            ]
            if len(tags) < 2:
                continue
            key = tuple(sorted(t.lower() for t in tags))
            manual_keys.add(key)
            ctr[key] += max(1, ctr.get(key, 0))  # at least appear once
            if ctr[key] < 2:
                ctr[key] = 2  # keep above min_count=1 solos visually as a bundle
            if key not in display:
                display[key] = tags
        out: list[dict[str, Any]] = []
        for key, count in ctr.most_common():
            if count < min_count:
                break
            tags = display.get(key) or list(key)
            item: dict[str, Any] = {"tags": tags, "count": int(count)}
            if key in manual_keys:
                item["manual"] = True
            out.append(item)
            if len(out) >= limit:
                break
        return out

    async def get_tag_relations(self, chat_id: str | int) -> dict[str, list[str]]:
        """
        Direct co-occurrence partners (same caption, count ≥ 2).
        Prefer durable cache rebuilt after index scan.
        """
        key = str(chat_id)
        async with self.conn.execute(
            "SELECT related_json FROM chat_tag_graph_cache WHERE chat_id = ?",
            (key,),
        ) as cur:
            row = await cur.fetchone()
        if row and row["related_json"]:
            try:
                data = json.loads(row["related_json"])
                if isinstance(data, dict):
                    return {str(k): list(v or []) for k, v in data.items()}
            except Exception:
                pass

        # Cold path: build once and persist
        await self.rebuild_tag_graph_cache(key)
        async with self.conn.execute(
            "SELECT related_json FROM chat_tag_graph_cache WHERE chat_id = ?",
            (key,),
        ) as cur:
            row = await cur.fetchone()
        if row and row["related_json"]:
            try:
                data = json.loads(row["related_json"])
                if isinstance(data, dict):
                    return {str(k): list(v or []) for k, v in data.items()}
            except Exception:
                pass
        return {}

    async def expand_related_tags(
        self, chat_id: str | int, tags: list[str]
    ) -> list[str]:
        """
        Expand seeds with all tags in the same co-occurrence UF component
        (identical rules to folder naming / merge).
        """
        from app.organizer import expand_tags_via_cooccurrence, normalize_tag_list

        seeds = normalize_tag_list(tags)
        if not seeds:
            return []
        groups = await self._relation_tag_groups(chat_id)
        return expand_tags_via_cooccurrence(
            seeds,
            groups,
            blacklist=await self.get_tag_relation_blacklist(),
        )

    async def get_tag_folder_map(
        self,
        chat_id: str | int,
        group_dir: Path | str | None = None,
    ) -> dict[str, str]:
        """tag → full related multi-tag folder name (blacklist hubs excluded)."""
        from app.organizer import build_tag_folder_map_from_groups

        groups = await self._relation_tag_groups(chat_id)
        path = Path(group_dir) if group_dir else None
        return build_tag_folder_map_from_groups(
            groups,
            group_dir=path,
            blacklist=await self.get_tag_relation_blacklist(),
        )

    async def list_index_tag_groups_for_merge(
        self, chat_id: str | int
    ) -> list[list[str]]:
        """Caption tag groups with relation-blacklist tags stripped (for folder merge)."""
        from app.organizer import normalize_tag_list

        bl = {x.lower() for x in await self.get_tag_relation_blacklist()}
        out: list[list[str]] = []
        for group in await self._relation_tag_groups(chat_id):
            tags = [
                t
                for t in normalize_tag_list(group)
                if t and t.lower().lstrip("#") not in bl
            ]
            if tags:
                out.append(tags)
        return out

    async def suggest_index_tags(
        self, chat_id: str | int, q: str = "", *, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Autocomplete: tags matching prefix/substring, with related counts."""
        q = (q or "").strip().lstrip("#").lower()
        limit = max(1, min(50, int(limit or 20)))
        tags = await self.list_index_tags(chat_id)
        related_map = await self.get_tag_relations(chat_id)
        if q:
            tags = [
                t
                for t in tags
                if q in str(t["tag"]).lower() or str(t["tag"]).lower().startswith(q)
            ]
            # prefer prefix matches
            tags.sort(
                key=lambda t: (
                    0 if str(t["tag"]).lower().startswith(q) else 1,
                    -int(t["count"]),
                    str(t["tag"]).lower(),
                )
            )
        out = []
        for t in tags[:limit]:
            rel = related_map.get(t["tag"]) or []
            if not rel:
                for k, v in related_map.items():
                    if k.lower() == str(t["tag"]).lower():
                        rel = v
                        break
            out.append(
                {
                    "tag": t["tag"],
                    "count": int(t["count"]),
                    "related": rel,
                    "related_count": len(rel),
                }
            )
        return out

    async def list_index_items(
        self,
        chat_id: str | int,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        q: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        chat_id = str(chat_id)
        mode = (tag_match_mode or "any").strip().lower()
        if mode not in ("any", "all"):
            mode = "any"
        limit = max(1, min(200, int(limit or 50)))
        offset = max(0, int(offset or 0))
        q = (q or "").strip()

        where = ["m.chat_id = ?"]
        params: list[Any] = [chat_id]

        tag_sql, tag_params = await self._tag_match_sql(
            chat_id, tags, tag_match_mode=mode
        )
        if tag_sql:
            where.append(tag_sql)
            params.extend(tag_params)
        if q:
            where.append("m.caption LIKE ?")
            params.append(f"%{q}%")

        where_sql = " AND ".join(where)
        async with self.conn.execute(
            f"SELECT COUNT(*) AS c FROM chat_media_index m WHERE {where_sql}",
            params,
        ) as cur:
            total = int((await cur.fetchone())["c"])

        async with self.conn.execute(
            f"""
            SELECT m.message_id, m.grouped_id, m.media_type, m.caption, m.tags, m.msg_date
            FROM chat_media_index m
            WHERE {where_sql}
            ORDER BY m.message_id DESC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ) as cur:
            rows = await cur.fetchall()

        items = []
        from app.organizer import normalize_tag_list

        for r in rows:
            d = dict(r)
            raw_tags = d.get("tags") or "[]"
            try:
                parsed = json.loads(raw_tags) if isinstance(raw_tags, str) else (raw_tags or [])
            except Exception:
                parsed = []
            d["tags"] = normalize_tag_list(parsed)
            items.append(d)
        return items, total

    async def list_index_message_ids(
        self,
        chat_id: str | int,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        keywords: list[str] | None = None,
        newest_first: bool = True,
        order_by: str = "message_id",
    ) -> list[int]:
        """
        order_by:
          - id / added: FIFO by index insertion (先入库先下载)
          - message_id: by Telegram message id (newest_first controls ASC/DESC)
        """
        chat_id = str(chat_id)
        keywords = [str(k).strip() for k in (keywords or []) if str(k).strip()]
        mode = (tag_match_mode or "any").strip().lower()
        if mode not in ("any", "all"):
            mode = "any"

        where = ["m.chat_id = ?"]
        params: list[Any] = [chat_id]

        tag_sql, tag_params = await self._tag_match_sql(
            chat_id, tags, tag_match_mode=mode
        )
        if tag_sql:
            where.append(tag_sql)
            params.extend(tag_params)

        if keywords:
            # any keyword match (same as matches_caption_keywords)
            kw_parts = " OR ".join("m.caption LIKE ?" for _ in keywords)
            where.append(f"({kw_parts})")
            params.extend([f"%{k}%" for k in keywords])

        ob = (order_by or "message_id").strip().lower()
        if ob in ("id", "added", "added_first", "fifo"):
            order_sql = "m.id ASC"
        else:
            order_sql = f"m.message_id {'DESC' if newest_first else 'ASC'}"
        async with self.conn.execute(
            f"""
            SELECT m.message_id FROM chat_media_index m
            WHERE {' AND '.join(where)}
            ORDER BY {order_sql}
            """,
            params,
        ) as cur:
            rows = await cur.fetchall()
            return [int(r["message_id"]) for r in rows]

    async def get_index_captions(
        self, chat_id: str | int, message_ids: list[int]
    ) -> dict[int, str]:
        if not message_ids:
            return {}
        chat_id = str(chat_id)
        out: dict[int, str] = {}
        for i in range(0, len(message_ids), 200):
            chunk = message_ids[i : i + 200]
            placeholders = ",".join("?" for _ in chunk)
            async with self.conn.execute(
                f"""
                SELECT message_id, caption FROM chat_media_index
                WHERE chat_id = ? AND message_id IN ({placeholders})
                """,
                [chat_id, *chunk],
            ) as cur:
                for row in await cur.fetchall():
                    out[int(row["message_id"])] = row["caption"] or ""
        return out

    async def list_index_captions(self, chat_id: str | int) -> list[str]:
        """Captions that may contain date tokens (for folder repair)."""
        chat_id = str(chat_id)
        async with self.conn.execute(
            """
            SELECT caption FROM chat_media_index
            WHERE chat_id = ?
              AND caption IS NOT NULL AND caption != ''
              AND (caption LIKE '%.%' OR caption LIKE '%月%')
            """,
            (chat_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [str(r["caption"] or "") for r in rows if r["caption"]]

    async def infer_nearby_year(
        self,
        chat_id: str | int,
        message_id: int,
        *,
        window: int = 120,
        limit: int = 40,
    ) -> tuple[Optional[int], list[str]]:
        """
        Infer calendar year from nearby index rows when filenames have no year.

        Returns (year, nearby_caption_texts) — year from msg_date / caption text
        of the closest messages by message_id.
        """
        from app.organizer import extract_years_from_text

        chat_id = str(chat_id)
        mid = int(message_id)
        lo = max(0, mid - int(window))
        hi = mid + int(window)
        async with self.conn.execute(
            """
            SELECT message_id, caption, msg_date
            FROM chat_media_index
            WHERE chat_id = ? AND message_id BETWEEN ? AND ?
            ORDER BY ABS(message_id - ?) ASC
            LIMIT ?
            """,
            (chat_id, lo, hi, mid, max(1, int(limit))),
        ) as cur:
            rows = await cur.fetchall()

        nearby_texts: list[str] = []
        best_year: Optional[int] = None
        for r in rows:
            cap = str(r["caption"] or "")
            if cap.strip():
                nearby_texts.append(cap)
            # Prefer explicit year in caption
            ys = extract_years_from_text(cap)
            if ys and best_year is None:
                best_year = ys[0]
                continue
            raw = r["msg_date"]
            if best_year is None and raw:
                try:
                    s = str(raw).replace("Z", "+00:00")
                    # 2026-04-11T... or date-only
                    y = int(s[:4])
                    if 2000 <= y <= 2100:
                        best_year = y
                except (TypeError, ValueError):
                    pass
        return best_year, nearby_texts

    async def rewrite_file_paths_dir_move(
        self, src_dir: Path | str, dst_dir: Path | str
    ) -> int:
        """Rewrite stored file_path prefixes after a local directory rename/merge."""
        src = str(Path(src_dir))
        dst = str(Path(dst_dir))
        if not src or src == dst:
            return 0
        variants = [
            (src, dst),
            (src.replace("\\", "/"), dst.replace("\\", "/")),
            (src.replace("/", "\\"), dst.replace("/", "\\")),
        ]
        # Dedupe while preserving order
        seen: set[tuple[str, str]] = set()
        pairs: list[tuple[str, str]] = []
        for a, b in variants:
            key = (a, b)
            if a and a != b and key not in seen:
                seen.add(key)
                pairs.append(key)

        changed = 0
        for old, new in pairs:
            like = old.replace("\\", "/") + "%"
            like_win = old.replace("/", "\\") + "%"
            for table in ("chat_completed", "downloaded"):
                cur = await self.conn.execute(
                    f"""
                    UPDATE {table}
                    SET file_path = REPLACE(file_path, ?, ?)
                    WHERE file_path IS NOT NULL
                      AND (file_path LIKE ? OR file_path LIKE ?)
                    """,
                    (old, new, like, like_win),
                )
                changed += int(cur.rowcount or 0)
        if changed:
            await self.conn.commit()
        return changed

    async def _index_filter_sql(
        self,
        chat_id: str,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        media_types: list[str] | None = None,
        keywords: list[str] | None = None,
    ) -> tuple[str, list[Any]]:
        keywords = [str(k).strip() for k in (keywords or []) if str(k).strip()]
        media_types = [str(m).strip() for m in (media_types or []) if str(m).strip()]
        mode = (tag_match_mode or "any").strip().lower()
        if mode not in ("any", "all"):
            mode = "any"

        where = ["m.chat_id = ?"]
        params: list[Any] = [chat_id]

        tag_sql, tag_params = await self._tag_match_sql(
            chat_id, tags, tag_match_mode=mode
        )
        if tag_sql:
            where.append(tag_sql)
            params.extend(tag_params)

        if media_types:
            # sticker is treated as document in the downloader
            expanded = list(media_types)
            if "document" in expanded and "sticker" not in expanded:
                expanded.append("sticker")
            ph = ",".join("?" for _ in expanded)
            where.append(f"m.media_type IN ({ph})")
            params.extend(expanded)

        if keywords:
            kw_parts = " OR ".join("m.caption LIKE ?" for _ in keywords)
            where.append(f"({kw_parts})")
            params.extend([f"%{k}%" for k in keywords])

        return " AND ".join(where), params

    async def count_media_index_filtered(
        self,
        chat_id: str | int,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        media_types: list[str] | None = None,
        keywords: list[str] | None = None,
    ) -> int:
        """How many indexed media rows match tags / types / keywords."""
        chat_id = str(chat_id)
        where_sql, params = await self._index_filter_sql(
            chat_id,
            tags=tags,
            tag_match_mode=tag_match_mode,
            media_types=media_types,
            keywords=keywords,
        )
        async with self.conn.execute(
            f"SELECT COUNT(*) AS c FROM chat_media_index m WHERE {where_sql}",
            params,
        ) as cur:
            row = await cur.fetchone()
            return int(row["c"] if row else 0)

    async def count_task_done_matching_index(
        self,
        task_id: int,
        chat_id: str | int,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        media_types: list[str] | None = None,
        keywords: list[str] | None = None,
    ) -> int:
        """How many done downloads for this task fall inside the filtered index set."""
        chat_id = str(chat_id)
        where_sql, params = await self._index_filter_sql(
            chat_id,
            tags=tags,
            tag_match_mode=tag_match_mode,
            media_types=media_types,
            keywords=keywords,
        )
        async with self.conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM downloaded d
            WHERE d.task_id = ?
              AND d.status = 'done'
              AND d.message_id IN (
                SELECT m.message_id FROM chat_media_index m WHERE {where_sql}
              )
            """,
            [int(task_id), *params],
        ) as cur:
            row = await cur.fetchone()
            return int(row["c"] if row else 0)

    def _parse_index_row(self, r: aiosqlite.Row | dict) -> dict[str, Any]:
        d = dict(r)
        raw_tags = d.get("tags") or "[]"
        try:
            d["tags"] = (
                json.loads(raw_tags) if isinstance(raw_tags, str) else (raw_tags or [])
            )
        except Exception:
            d["tags"] = []
        return d

    async def list_task_match_items(
        self,
        chat_id: str | int,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        media_types: list[str] | None = None,
        keywords: list[str] | None = None,
        limit: int = 50,
        offset: int = 0,
        newest_first: bool = False,
    ) -> tuple[list[dict[str, Any]], int]:
        """All indexed media matching the task filters (标签命中)."""
        chat_id = str(chat_id)
        limit = max(1, min(200, int(limit or 50)))
        offset = max(0, int(offset or 0))
        where_sql, params = await self._index_filter_sql(
            chat_id,
            tags=tags,
            tag_match_mode=tag_match_mode,
            media_types=media_types,
            keywords=keywords,
        )
        async with self.conn.execute(
            f"SELECT COUNT(*) AS c FROM chat_media_index m WHERE {where_sql}",
            params,
        ) as cur:
            total = int((await cur.fetchone())["c"])
        order = "m.message_id DESC" if newest_first else "m.id ASC"
        async with self.conn.execute(
            f"""
            SELECT m.message_id, m.grouped_id, m.media_type, m.caption, m.tags, m.msg_date
            FROM chat_media_index m
            WHERE {where_sql}
            ORDER BY {order}
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ) as cur:
            rows = await cur.fetchall()
        return [self._parse_index_row(r) for r in rows], total

    async def list_task_queue_items(
        self,
        task_id: int,
        chat_id: str | int,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        media_types: list[str] | None = None,
        keywords: list[str] | None = None,
        limit: int = 50,
        offset: int = 0,
        newest_first: bool = False,
    ) -> tuple[list[dict[str, Any]], int]:
        """Indexed matches not yet done for this chat (local/chat_completed, not only task queue)."""
        chat_id = str(chat_id)
        limit = max(1, min(200, int(limit or 50)))
        offset = max(0, int(offset or 0))
        where_sql, params = await self._index_filter_sql(
            chat_id,
            tags=tags,
            tag_match_mode=tag_match_mode,
            media_types=media_types,
            keywords=keywords,
        )
        # Done = this task downloaded OR chat-level completed (survives task delete / local sync)
        not_done = (
            "m.message_id NOT IN ("
            "SELECT message_id FROM downloaded "
            "WHERE task_id = ? AND status = 'done'"
            ") AND m.message_id NOT IN ("
            "SELECT message_id FROM chat_completed WHERE chat_id = ?"
            ")"
        )
        full_where = f"{where_sql} AND {not_done}"
        count_params = [*params, int(task_id), chat_id]
        async with self.conn.execute(
            f"SELECT COUNT(*) AS c FROM chat_media_index m WHERE {full_where}",
            count_params,
        ) as cur:
            total = int((await cur.fetchone())["c"])

        order = "m.message_id DESC" if newest_first else "m.id ASC"
        async with self.conn.execute(
            f"""
            SELECT m.message_id, m.grouped_id, m.media_type, m.caption, m.tags, m.msg_date
            FROM chat_media_index m
            WHERE {full_where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
            """,
            [*count_params, limit, offset],
        ) as cur:
            rows = await cur.fetchall()
        return [self._parse_index_row(r) for r in rows], total

    async def count_index_done(
        self,
        task_id: int,
        chat_id: str | int,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        media_types: list[str] | None = None,
        keywords: list[str] | None = None,
    ) -> int:
        """Indexed matches already done (task record or chat/local completed)."""
        chat_id = str(chat_id)
        where_sql, params = await self._index_filter_sql(
            chat_id,
            tags=tags,
            tag_match_mode=tag_match_mode,
            media_types=media_types,
            keywords=keywords,
        )
        done = (
            "("
            "m.message_id IN ("
            "SELECT message_id FROM downloaded "
            "WHERE task_id = ? AND status = 'done'"
            ") OR m.message_id IN ("
            "SELECT message_id FROM chat_completed WHERE chat_id = ?"
            ")"
            ")"
        )
        async with self.conn.execute(
            f"SELECT COUNT(*) AS c FROM chat_media_index m WHERE {where_sql} AND {done}",
            [*params, int(task_id), chat_id],
        ) as cur:
            return int((await cur.fetchone())["c"])

    async def list_task_done_items(
        self,
        task_id: int,
        chat_id: str | int,
        *,
        tags: list[str] | None = None,
        tag_match_mode: str = "any",
        media_types: list[str] | None = None,
        keywords: list[str] | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Completed items for this chat (chat_completed preferred; local-aware)."""
        chat_id = str(chat_id)
        limit = max(1, min(200, int(limit or 50)))
        offset = max(0, int(offset or 0))
        where_sql, params = await self._index_filter_sql(
            chat_id,
            tags=tags,
            tag_match_mode=tag_match_mode,
            media_types=media_types,
            keywords=keywords,
        )
        # Prefer chat_completed path; fall back to this task's downloaded row
        done_where = (
            f"{where_sql} AND ("
            "m.message_id IN (SELECT message_id FROM chat_completed WHERE chat_id = ?) "
            "OR m.message_id IN ("
            "SELECT message_id FROM downloaded WHERE task_id = ? AND status = 'done')"
            ")"
        )
        count_params = [*params, chat_id, int(task_id)]
        async with self.conn.execute(
            f"SELECT COUNT(*) AS c FROM chat_media_index m WHERE {done_where}",
            count_params,
        ) as cur:
            total = int((await cur.fetchone())["c"])
        async with self.conn.execute(
            f"""
            SELECT m.message_id, m.grouped_id, m.media_type, m.caption, m.tags, m.msg_date,
                   COALESCE(c.file_path, d.file_path) AS file_path,
                   COALESCE(c.updated_at, d.created_at) AS created_at
            FROM chat_media_index m
            LEFT JOIN chat_completed c
              ON c.chat_id = ? AND c.message_id = m.message_id
            LEFT JOIN downloaded d
              ON d.task_id = ? AND d.message_id = m.message_id AND d.status = 'done'
            WHERE {where_sql}
              AND (c.message_id IS NOT NULL OR d.message_id IS NOT NULL)
            ORDER BY COALESCE(c.updated_at, d.created_at) DESC
            LIMIT ? OFFSET ?
            """,
            [chat_id, int(task_id), *params, limit, offset],
        ) as cur:
            rows = await cur.fetchall()
        items: list[dict[str, Any]] = []
        for r in rows:
            d = self._parse_index_row(r)
            path = d.get("file_path") or ""
            d["file_name"] = Path(path).name if path else ""
            items.append(d)
        return items, total

    async def list_completed_basenames(self, chat_id: str | int) -> set[str]:
        """Filenames of finished downloads for this chat (for temp cleanup)."""
        from pathlib import Path

        chat_id = str(chat_id)
        names: set[str] = set()
        async with self.conn.execute(
            """
            SELECT file_path FROM chat_completed
            WHERE chat_id = ?
              AND file_path IS NOT NULL AND file_path != ''
            """,
            (chat_id,),
        ) as cur:
            for r in await cur.fetchall():
                p = str(r["file_path"] or "").strip()
                if p:
                    names.add(Path(p).name)
        async with self.conn.execute(
            """
            SELECT d.file_path FROM downloaded d
            JOIN tasks t ON t.id = d.task_id
            WHERE t.chat_id = ?
              AND d.status = 'done'
              AND d.file_path IS NOT NULL AND d.file_path != ''
            """,
            (chat_id,),
        ) as cur:
            for r in await cur.fetchall():
                p = str(r["file_path"] or "").strip()
                if p:
                    names.add(Path(p).name)
        return names

    async def sync_local_completed_from_dir(
        self,
        chat_id: str | int,
        group_dir: Path,
        *,
        known_message_ids: Optional[set[int]] = None,
    ) -> int:
        """Mark index message_ids found as local files under group_dir as chat_completed.

        Filenames like ``photo_123.jpg`` / ``name_123.mp4`` / ``name_123_1.mp4``.
        Returns how many new completions were written.
        """
        from app.organizer import collect_local_message_ids

        chat_id = str(chat_id)
        if known_message_ids is None:
            async with self.conn.execute(
                "SELECT message_id FROM chat_media_index WHERE chat_id = ?",
                (chat_id,),
            ) as cur:
                rows = await cur.fetchall()
                known_message_ids = {int(r["message_id"]) for r in rows}
        if not known_message_ids:
            return 0
        found = await asyncio.to_thread(
            collect_local_message_ids, Path(group_dir), known_message_ids
        )
        if not found:
            return 0
        # Only insert missing ones
        async with self.conn.execute(
            "SELECT message_id FROM chat_completed WHERE chat_id = ?",
            (chat_id,),
        ) as cur:
            already = {int(r["message_id"]) for r in await cur.fetchall()}
        new_ids = found - already
        if not new_ids:
            return 0
        now = _utcnow()
        await self.conn.executemany(
            """
            INSERT OR IGNORE INTO chat_completed(chat_id, message_id, file_path, updated_at)
            VALUES (?, ?, NULL, ?)
            """,
            [(chat_id, int(mid), now) for mid in new_ids],
        )
        await self.conn.commit()
        return len(new_ids)

db = Database()
