from typing import Optional

from fastapi import Header, HTTPException, Request

from app.auth_token import unpack_web_cookie, verify_web_token
from app.db import db


async def require_web_auth(
    request: Request, x_web_password: str | None = Header(default=None)
) -> Optional[str]:
    """
    Require Web login when users exist (or legacy WEB_PASSWORD seed pending).
    Returns the authenticated username, or None when auth is disabled.
    """
    if not await db.web_auth_required():
        return None

    raw = x_web_password or request.cookies.get("web_auth")
    username, token = unpack_web_cookie(raw)
    if not username or not token:
        raise HTTPException(status_code=401, detail="需要 Web 登录")

    user = await db.get_web_user(username)
    if not user or not verify_web_token(username, user["password_hash"], token):
        raise HTTPException(status_code=401, detail="需要 Web 登录")
    return username
