"""Runtime-tunable download params (DB-backed, sync-readable cache)."""

from __future__ import annotations

from typing import Any

from app.config import get_settings

_PART_ALLOWED = (512 * 1024, 1024 * 1024)

_cache: dict[str, int] = {}


def _env_defaults() -> dict[str, int]:
    s = get_settings()
    part = int(getattr(s, "download_part_size", 1024 * 1024) or (1024 * 1024))
    if part not in _PART_ALLOWED:
        part = 1024 * 1024 if part >= 1024 * 1024 else 512 * 1024
    return {
        "media_connections": max(1, min(8, int(getattr(s, "media_connections", 3) or 3))),
        "download_pipeline": max(1, min(8, int(getattr(s, "download_pipeline", 4) or 4))),
        "download_part_size": part,
    }


def snapshot() -> dict[str, int]:
    base = _env_defaults()
    out = dict(base)
    out.update(_cache)
    # Re-clamp after merge
    out["media_connections"] = max(1, min(8, int(out["media_connections"])))
    out["download_pipeline"] = max(1, min(8, int(out["download_pipeline"])))
    part = int(out["download_part_size"])
    out["download_part_size"] = part if part in _PART_ALLOWED else base["download_part_size"]
    return out


def media_connections() -> int:
    return int(snapshot()["media_connections"])


def download_pipeline() -> int:
    return int(snapshot()["download_pipeline"])


def download_part_size() -> int:
    return int(snapshot()["download_part_size"])


def apply_overrides(
    *,
    media_connections: int | None = None,
    download_pipeline: int | None = None,
    download_part_size: int | None = None,
) -> dict[str, int]:
    if media_connections is not None:
        _cache["media_connections"] = max(1, min(8, int(media_connections)))
    if download_pipeline is not None:
        _cache["download_pipeline"] = max(1, min(8, int(download_pipeline)))
    if download_part_size is not None:
        part = int(download_part_size)
        if part not in _PART_ALLOWED:
            part = 1024 * 1024 if part >= 768 * 1024 else 512 * 1024
        _cache["download_part_size"] = part
    return snapshot()


async def load_from_db(database: Any) -> dict[str, int]:
    """Refresh in-memory cache from DB meta (falls back to .env)."""
    conn = await database.get_media_connections()
    pipe = await database.get_download_pipeline()
    part = await database.get_download_part_size()
    return apply_overrides(
        media_connections=conn,
        download_pipeline=pipe,
        download_part_size=part,
    )
