"""History list query performance / correctness."""

from __future__ import annotations

import asyncio
from pathlib import Path

from app.db import Database


def _run(coro):
    return asyncio.run(coro)


def test_history_list_clustered_and_filtered(tmp_path: Path):
    async def _t():
        db = Database(tmp_path / "hist.db")
        await db.connect()
        try:
            t1 = await db.create_task(
                {"chat_id": "100", "chat_title": "群A", "media_types": ["photo"]}
            )
            t2 = await db.create_task(
                {"chat_id": "200", "chat_title": "群B", "media_types": ["photo"]}
            )
            # Older activity in A, newer in B, then more in A
            await db.mark_message(int(t1["id"]), 1, status="done", file_path="a/1.jpg", chat_id="100")
            await db.mark_message(int(t2["id"]), 1, status="done", file_path="b/1.jpg", chat_id="200")
            await db.mark_message(int(t1["id"]), 2, status="done", file_path="a/2.jpg", chat_id="100")

            items, total = await db.list_download_history(status="done", limit=50, offset=0)
            assert total == 3
            # Latest overall activity is chat 100 (id max), so A cluster first
            assert str(items[0]["chat_id"]) == "100"
            assert items[0]["file_name"] == "2.jpg"

            # Filter by chat
            items_b, total_b = await db.list_download_history(
                status="done", chat_id="200", limit=50, offset=0
            )
            assert total_b == 1
            assert str(items_b[0]["chat_id"]) == "200"

            groups = await db.list_download_history_groups(status="done")
            assert len(groups) == 2
            assert {g["chat_id"] for g in groups} == {"100", "200"}
        finally:
            await db.close()

    _run(_t())


def test_downloaded_chat_id_denormalized(tmp_path: Path):
    async def _t():
        db = Database(tmp_path / "hist2.db")
        await db.connect()
        try:
            task = await db.create_task(
                {"chat_id": "77", "chat_title": "x", "media_types": ["photo"]}
            )
            await db.mark_message(
                int(task["id"]), 9, status="done", file_path="x.jpg", chat_id="77"
            )
            async with db.conn.execute(
                "SELECT chat_id FROM downloaded WHERE message_id=9"
            ) as cur:
                row = await cur.fetchone()
            assert row and str(row["chat_id"]) == "77"
        finally:
            await db.close()

    _run(_t())
