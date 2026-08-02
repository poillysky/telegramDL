"""Web auth helpers — password hashing and session tokens."""

from __future__ import annotations

import hashlib
import hmac
import secrets


def hash_password(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        120_000,
    )
    return f"{salt}${digest.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    if not password_hash or "$" not in password_hash:
        return False
    salt, _ = password_hash.split("$", 1)
    return hmac.compare_digest(hash_password(password, salt), password_hash)


def make_web_token(username: str, password_hash: str) -> str:
    material = f"{username}\0{password_hash}".encode("utf-8")
    return hmac.new(
        material,
        b"telegram-group-downloader-web-auth",
        hashlib.sha256,
    ).hexdigest()


def pack_web_cookie(username: str, password_hash: str) -> str:
    return f"{username}:{make_web_token(username, password_hash)}"


def unpack_web_cookie(value: str | None) -> tuple[str | None, str | None]:
    if not value or ":" not in value:
        return None, None
    username, token = value.split(":", 1)
    username = username.strip()
    token = token.strip()
    if not username or not token:
        return None, None
    return username, token


def verify_web_token(username: str, password_hash: str, token: str | None) -> bool:
    if not username or not password_hash or not token:
        return False
    expected = make_web_token(username, password_hash)
    return hmac.compare_digest(token, expected)
