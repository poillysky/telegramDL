from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse

from app.api.deps import require_web_auth
from app.config import get_settings
from app.db import db

router = APIRouter(prefix="/api/history", tags=["history"])

_PREVIEW_EXT = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".bmp",
    ".mp4",
    ".webm",
    ".mkv",
    ".mov",
    ".m4v",
    ".mp3",
    ".m4a",
    ".ogg",
    ".wav",
    ".flac",
    ".aac",
    ".opus",
}


def _safe_download_path(file_path: str) -> Path:
    """Resolve path and ensure it stays under download_dir."""
    settings = get_settings()
    root = Path(settings.download_dir).resolve()
    raw = Path(file_path)
    candidate = raw.resolve() if raw.is_absolute() else (root / raw).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as e:
        raise HTTPException(status_code=403, detail="路径不允许") from e
    if not candidate.is_file():
        raise HTTPException(status_code=404, detail="文件不存在")
    return candidate


def _guess_media_kind(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}:
        return "image"
    if ext in {".mp4", ".webm", ".mkv", ".mov", ".m4v"}:
        return "video"
    if ext in {".mp3", ".m4a", ".ogg", ".wav", ".flac", ".aac", ".opus"}:
        return "audio"
    return "file"


@router.get("")
async def list_history(
    q: str = Query(default=""),
    chat_id: str = Query(default=""),
    status: str = Query(default="done"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _: Optional[str] = Depends(require_web_auth),
):
    items, total = await db.list_download_history(
        q=q.strip(),
        chat_id=chat_id.strip(),
        status=status.strip() or "done",
        limit=limit,
        offset=offset,
    )
    root = Path(get_settings().download_dir).resolve()
    for it in items:
        path_str = it.get("file_path") or ""
        kind = "file"
        previewable = False
        if path_str:
            try:
                p = Path(path_str)
                candidate = p.resolve() if p.is_absolute() else (root / p).resolve()
                candidate.relative_to(root)
                if candidate.is_file():
                    kind = _guess_media_kind(candidate)
                    previewable = candidate.suffix.lower() in _PREVIEW_EXT
            except Exception:
                pass
        it["media_kind"] = kind
        it["previewable"] = previewable
    return {
        "ok": True,
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/{item_id}/file")
async def history_file(
    item_id: int,
    _: Optional[str] = Depends(require_web_auth),
):
    row = await db.get_download_by_id(item_id)
    if not row or not row.get("file_path"):
        raise HTTPException(status_code=404, detail="记录不存在")
    path = _safe_download_path(row["file_path"])
    return FileResponse(
        path,
        filename=path.name,
        content_disposition_type="inline",
    )
