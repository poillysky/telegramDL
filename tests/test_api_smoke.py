"""API route registration / auth gate smoke (no Telegram)."""

from __future__ import annotations

from app.main import app


def test_health_route_registered():
    paths = {getattr(r, "path", None) for r in app.routes}
    assert "/api/health" in paths
    assert "/api/tasks" in paths
    assert "/api/settings/runtime" in paths or any(
        (getattr(r, "path", "") or "").startswith("/api/settings") for r in app.routes
    )


def test_gzip_middleware_installed():
    names = [type(m).__name__ for m in app.user_middleware]
    # Starlette stores middleware factory classes
    assert any("GZip" in n for n in names) or any(
        "gzip" in str(m).lower() for m in app.user_middleware
    )
