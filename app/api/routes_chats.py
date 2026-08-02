from fastapi import APIRouter, Depends, Query

from app.api.deps import require_web_auth
from app.telegram_client import TelegramAuthError, tg_manager

router = APIRouter(prefix="/api/chats", tags=["chats"])


@router.get("")
async def list_chats(q: str = Query(default=""), _: None = Depends(require_web_auth)):
    try:
        chats = await tg_manager.list_dialogs(q)
        return {"ok": True, "chats": chats}
    except TelegramAuthError as e:
        return {"ok": False, "code": e.code, "message": str(e), "chats": []}
    except Exception as e:
        return {"ok": False, "code": "error", "message": str(e), "chats": []}
