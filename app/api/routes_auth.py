import asyncio
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, Field

from app.api.deps import require_web_auth
from app.auth_token import pack_web_cookie, verify_password
from app.config import get_settings
from app.db import db
from app.telegram_client import TelegramAuthError, tg_manager

router = APIRouter(prefix="/api/auth", tags=["auth"])


class WebLoginBody(BaseModel):
    username: str = ""
    password: str = ""


class WebUserCreateBody(BaseModel):
    username: str
    password: str


class WebPasswordBody(BaseModel):
    username: str = ""
    old_password: str = ""
    new_password: str
    confirm_password: str = ""


class SendCodeBody(BaseModel):
    phone: str
    api_id: Optional[int] = None
    api_hash: Optional[str] = None
    proxy: Optional[str] = None


class ProxyBody(BaseModel):
    proxy: str = ""


class SignInBody(BaseModel):
    code: str
    password: Optional[str] = Field(default=None, description="两步验证密码")


class TwoFABody(BaseModel):
    password: str


def _set_web_cookie(response: Response, username: str, password_hash: str) -> None:
    response.set_cookie(
        "web_auth",
        pack_web_cookie(username, password_hash),
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
    )


@router.post("/web-login")
async def web_login(body: WebLoginBody, response: Response):
    await db.ensure_web_users_seeded()
    username = body.username.strip()
    password = body.password

    if not await db.web_auth_required():
        return {"ok": True, "need_password": False, "username": None}

    user = await db.get_web_user(username) if username else None
    if not user or not verify_password(password, user["password_hash"]):
        return {"ok": False, "message": "账号或密码错误"}

    _set_web_cookie(response, user["username"], user["password_hash"])
    return {
        "ok": True,
        "need_password": True,
        "username": user["username"],
    }


@router.get("/web-status")
async def web_status():
    await db.ensure_web_users_seeded()
    return {"need_password": await db.web_auth_required()}


@router.get("/web-session")
async def web_session(request: Request):
    """Lightweight session check — never touches Telegram."""
    from app.auth_token import unpack_web_cookie, verify_web_token

    await db.ensure_web_users_seeded()
    need = await db.web_auth_required()
    if not need:
        return {"ok": True, "need_password": False, "authenticated": True, "username": None}

    raw = request.cookies.get("web_auth")
    username, token = unpack_web_cookie(raw)
    if not username or not token:
        return {"ok": True, "need_password": True, "authenticated": False, "username": None}
    user = await db.get_web_user(username)
    if not user or not verify_web_token(username, user["password_hash"], token):
        return {"ok": True, "need_password": True, "authenticated": False, "username": None}
    return {
        "ok": True,
        "need_password": True,
        "authenticated": True,
        "username": username,
    }


@router.get("/web-users")
async def list_web_users(current: Optional[str] = Depends(require_web_auth)):
    users = await db.list_web_users()
    return {"ok": True, "users": users, "current": current}


@router.post("/web-users")
async def create_web_user(
    body: WebUserCreateBody, _: Optional[str] = Depends(require_web_auth)
):
    try:
        user = await db.create_web_user(body.username, body.password)
        return {"ok": True, "user": user}
    except ValueError as e:
        return {"ok": False, "message": str(e)}


@router.post("/web-users/change-password")
async def change_web_password(
    body: WebPasswordBody,
    response: Response,
    current: Optional[str] = Depends(require_web_auth),
):
    target = (body.username or current or "").strip()
    if not target:
        return {"ok": False, "message": "未指定账号"}
    if body.confirm_password and body.confirm_password != body.new_password:
        return {"ok": False, "message": "两次输入的新密码不一致"}

    user = await db.get_web_user(target)
    if not user:
        return {"ok": False, "message": "用户不存在"}

    # Changing own password requires old password
    if current and target == current:
        if not verify_password(body.old_password, user["password_hash"]):
            return {"ok": False, "message": "原密码错误"}
    else:
        # Resetting another account: verify current user's password as confirmation
        if not current:
            return {"ok": False, "message": "需要登录"}
        me = await db.get_web_user(current)
        if not me or not verify_password(body.old_password, me["password_hash"]):
            return {"ok": False, "message": "请输入当前登录账号的密码以确认"}

    try:
        await db.set_web_user_password(target, body.new_password)
    except ValueError as e:
        return {"ok": False, "message": str(e)}

    # Refresh cookie if current user changed their own password
    if current and target == current:
        updated = await db.get_web_user(target)
        if updated:
            _set_web_cookie(response, updated["username"], updated["password_hash"])

    return {"ok": True, "username": target}


@router.delete("/web-users/{username}")
async def delete_web_user(
    username: str, current: Optional[str] = Depends(require_web_auth)
):
    try:
        if current and username.strip() == current and await db.count_web_users() <= 1:
            return {"ok": False, "message": "不能删除唯一账号"}
        await db.delete_web_user(username)
        return {"ok": True}
    except ValueError as e:
        return {"ok": False, "message": str(e)}


@router.get("/status")
async def tg_status(_: Optional[str] = Depends(require_web_auth)):
    """Never block Web login / boot for more than a few seconds on Telegram I/O."""
    settings = get_settings()
    try:
        return await asyncio.wait_for(tg_manager.status(), timeout=3.5)
    except asyncio.TimeoutError:
        return {
            "authorized": False,
            "connected": False,
            "need_api": not bool(settings.api_id and settings.api_hash),
            "message": "Telegram 连接中…",
            "user": None,
            "api_configured": bool(settings.api_id and settings.api_hash),
            "proxy": settings.proxy or None,
            "connecting": True,
        }
    except Exception as e:
        return {
            "authorized": False,
            "connected": False,
            "need_api": not bool(settings.api_id and settings.api_hash),
            "message": str(e),
            "user": None,
            "api_configured": bool(settings.api_id and settings.api_hash),
            "proxy": settings.proxy or None,
        }


@router.post("/send-code")
async def send_code(
    body: SendCodeBody, _: Optional[str] = Depends(require_web_auth)
):
    try:
        return await tg_manager.send_code(
            body.phone, body.api_id, body.api_hash, proxy=body.proxy
        )
    except TelegramAuthError as e:
        return {"ok": False, "code": e.code, "message": str(e)}
    except Exception as e:
        return {"ok": False, "code": "error", "message": str(e)}


@router.post("/set-proxy")
async def set_proxy(body: ProxyBody, _: Optional[str] = Depends(require_web_auth)):
    tg_manager.set_proxy(body.proxy)
    await tg_manager.save_runtime_config()
    return {"ok": True, "proxy": tg_manager.settings.proxy or None}


@router.post("/test-connection")
async def test_connection(_: Optional[str] = Depends(require_web_auth)):
    return await tg_manager.test_connection()


@router.post("/reconnect")
async def reconnect_telegram(_: Optional[str] = Depends(require_web_auth)):
    """Reload saved config and reconnect using existing session."""
    try:
        result = await tg_manager.try_auto_reconnect()
        st = await asyncio.wait_for(tg_manager.status(), timeout=8)
        return {
            "ok": bool(result.get("ok")),
            "reason": result.get("reason"),
            "message": result.get("reason") if not result.get("ok") else "ok",
            "status": st,
        }
    except Exception as e:
        return {"ok": False, "reason": str(e), "message": str(e)}


@router.post("/sign-in")
async def sign_in(body: SignInBody, _: Optional[str] = Depends(require_web_auth)):
    try:
        result = await tg_manager.sign_in(body.code, body.password)
        return {"ok": True, **result}
    except TelegramAuthError as e:
        return {"ok": False, "code": e.code, "message": str(e)}
    except Exception as e:
        return {"ok": False, "code": "error", "message": str(e)}


@router.post("/2fa")
async def two_fa(body: TwoFABody, _: Optional[str] = Depends(require_web_auth)):
    try:
        result = await tg_manager.sign_in_2fa(body.password)
        return {"ok": True, **result}
    except TelegramAuthError as e:
        return {"ok": False, "code": e.code, "message": str(e)}
    except Exception as e:
        return {"ok": False, "code": "error", "message": str(e)}


@router.post("/logout")
async def logout(_: Optional[str] = Depends(require_web_auth)):
    await tg_manager.logout()
    return {"ok": True}


@router.post("/web-logout")
async def web_logout(response: Response):
    """Clear Web console session cookie (does not log out Telegram)."""
    response.delete_cookie("web_auth")
    return {"ok": True}


@router.get("/account")
async def account(current: Optional[str] = Depends(require_web_auth)):
    """Account overview for the management panel."""
    settings = get_settings()
    try:
        st = await asyncio.wait_for(tg_manager.status(), timeout=8)
    except Exception:
        st = {
            "authorized": False,
            "connected": False,
            "user": None,
            "proxy": settings.proxy or None,
            "message": "Telegram 状态获取超时或失败",
        }
    users = await db.list_web_users()
    session_file = Path(str(settings.session_path.resolve()) + ".session")
    return {
        "ok": True,
        "web_protected": await db.web_auth_required(),
        "web_username": current,
        "web_users": users,
        "telegram": st,
        "proxy": st.get("proxy") or settings.proxy or None,
        "api_configured": bool(settings.api_id and settings.api_hash),
        "api_id": int(settings.api_id) if settings.api_id else None,
        "api_hash_set": bool(settings.api_hash),
        "session_exists": session_file.exists(),
        "session_file": str(session_file),
        "download_dir": str(settings.download_dir),
    }
