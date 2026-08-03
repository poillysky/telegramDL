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


def _resolve_download_path(file_path: str) -> Optional[Path]:
    """Resolve a stored path to a real file under download_dir.

    Paths may be absolute, relative to download_dir, or accidentally prefixed
    with the download_dir name itself (e.g. ``downloads\\chat\\a.mp4`` while
    download_dir is already ``downloads``).
    """
    if not file_path or not str(file_path).strip():
        return None
    settings = get_settings()
    root = Path(settings.download_dir).resolve()
    raw = Path(str(file_path).strip())

    candidates: list[Path] = []
    if raw.is_absolute():
        candidates.append(raw)
    else:
        candidates.append(root / raw)
        # cwd-relative as often produced by str(Path("downloads/..."))
        candidates.append(Path.cwd() / raw)
        if raw.parts and raw.parts[0].casefold() == root.name.casefold():
            rest = Path(*raw.parts[1:]) if len(raw.parts) > 1 else Path(".")
            candidates.append(root / rest)

    seen: set[Path] = set()
    for cand in candidates:
        try:
            resolved = cand.resolve()
        except OSError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        try:
            resolved.relative_to(root)
        except ValueError:
            continue
        if resolved.is_file():
            return resolved
    return None


def _safe_download_path(file_path: str) -> Path:
    """Resolve path and ensure it stays under download_dir."""
    candidate = _resolve_download_path(file_path)
    if candidate is None:
        settings = get_settings()
        root = Path(settings.download_dir).resolve()
        raw = Path(str(file_path).strip()) if file_path else None
        if raw is not None:
            try:
                probe = raw.resolve() if raw.is_absolute() else (root / raw).resolve()
                probe.relative_to(root)
            except ValueError as e:
                raise HTTPException(status_code=403, detail="路径不允许") from e
            except OSError:
                pass
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
    for it in items:
        path_str = it.get("file_path") or ""
        # List view: no per-row disk stat (slow on large NAS / 10万+ rows pages).
        # Existence is checked when opening / previewing the file.
        kind = _guess_media_kind(Path(path_str)) if path_str else "file"
        previewable = bool(path_str) and Path(path_str).suffix.lower() in _PREVIEW_EXT
        it["media_kind"] = kind
        it["previewable"] = previewable
        it["file_missing"] = False
    return {
        "ok": True,
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "chat_id": chat_id.strip(),
    }


@router.get("/groups")
async def list_history_groups(
    q: str = Query(default=""),
    status: str = Query(default="done"),
    _: Optional[str] = Depends(require_web_auth),
):
    groups = await db.list_download_history_groups(
        q=q.strip(),
        status=status.strip() or "done",
    )
    return {
        "ok": True,
        "groups": groups,
        "total_groups": len(groups),
        "total_items": sum(int(g.get("count") or 0) for g in groups),
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
