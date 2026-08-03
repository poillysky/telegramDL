"""SQLite WAL, batch tag writes, durable tag-graph cache."""

from __future__ import annotations

import asyncio
from pathlib import Path

from app.db import Database


def _run(coro):
    return asyncio.run(coro)


def test_wal_mode_enabled(tmp_path: Path):
    async def _t():
        db = Database(tmp_path / "t.db")
        await db.connect()
        try:
            async with db.conn.execute("PRAGMA journal_mode") as cur:
                row = await cur.fetchone()
            mode = str(row[0] if row else "").lower()
            assert mode == "wal"
        finally:
            await db.close()

    _run(_t())


def test_upsert_tags_batch_and_graph_cache(tmp_path: Path):
    async def _t():
        db = Database(tmp_path / "t.db")
        await db.connect()
        try:
            await db.upsert_media_index_item(
                "c1", 10, caption="#a #b", tags=["a", "b"], media_type="photo"
            )
            await db.upsert_media_index_item(
                "c1", 11, caption="#b #c", tags=["b", "c"], media_type="video"
            )
            await db.commit()
            await db.rebuild_tag_graph_cache("c1")

            groups = await db.list_index_tag_groups("c1")
            assert any(set(g) == {"a", "b"} for g in groups)

            related = await db.get_tag_relations("c1")
            keys = {str(k).lower() for k in related}
            assert "a" in keys or "b" in keys

            async with db.conn.execute(
                "SELECT groups_json, related_json FROM chat_tag_graph_cache WHERE chat_id=?",
                ("c1",),
            ) as cur:
                row = await cur.fetchone()
            assert row is not None
            assert row["groups_json"]
            assert row["related_json"]
        finally:
            await db.close()

    _run(_t())


def test_append_log_buffered_flush(tmp_path: Path):
    async def _t():
        db = Database(tmp_path / "t.db")
        await db.connect()
        try:
            task = await db.create_task(
                {"chat_id": "1", "chat_title": "t", "media_types": ["photo"], "concurrency": 1}
            )
            tid = int(task["id"])
            await db.append_log(tid, "hello")
            await db.append_log(tid, "world")
            await db.flush_pending_logs()
            task = await db.get_task(tid)
            log = task.get("last_log") or ""
            assert "hello" in log and "world" in log
        finally:
            await db.close()

    _run(_t())


def test_mark_message_commit_false(tmp_path: Path):
    async def _t():
        db = Database(tmp_path / "t.db")
        await db.connect()
        try:
            task = await db.create_task(
                {"chat_id": "9", "chat_title": "t", "media_types": ["photo"], "concurrency": 1}
            )
            tid = int(task["id"])
            await db.mark_message(
                tid, 42, status="done", file_path="/x", commit=False
            )
            await db.update_task(tid, commit=False, downloaded_count=1)
            await db.commit()
            async with db.conn.execute(
                "SELECT status FROM downloaded WHERE task_id=? AND message_id=?",
                (tid, 42),
            ) as cur:
                row = await cur.fetchone()
            assert row and row["status"] == "done"
            task = await db.get_task(tid)
            assert int(task.get("downloaded_count") or 0) == 1
        finally:
            await db.close()

    _run(_t())
