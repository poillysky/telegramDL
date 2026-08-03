import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api.routes_auth import router as auth_router
from app.api.routes_chats import router as chats_router
from app.api.routes_history import router as history_router
from app.api.routes_index import router as index_router
from app.api.routes_settings import router as settings_router
from app.api.routes_tasks import router as tasks_router
from app.config import get_settings
from app.db import db
from app.downloader import scheduler
from app.indexer import indexer
from app.logging_setup import setup_logging
from app.telegram_client import tg_manager

_settings_boot = get_settings()
setup_logging(data_dir=_settings_boot.data_dir, level="INFO")
logger = logging.getLogger("app")

WEB_DIR = Path(__file__).parent / "web"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    settings.ensure_dirs()
    await db.connect()
    await db.ensure_web_users_seeded()
    try:
        from app import runtime_tune

        await runtime_tune.load_from_db(db)
    except Exception:
        logger.debug("runtime_tune load failed", exc_info=True)
    await scheduler.resume_running_on_startup()

    # Restore Web-saved API/proxy and reconnect Telegram in background
    # so startup is not blocked by network/proxy.
    async def _auto_tg():
        try:
            result = await tg_manager.try_auto_reconnect()
            if result.get("ok"):
                logger.info("Telegram session restored after restart")
                n = await scheduler.auto_resume_after_telegram()
                if n:
                    logger.info("Auto-resumed %s task(s) after restart", n)
            else:
                reason = str(result.get("reason") or "unknown")
                logger.info(
                    "Telegram not auto-connected (%s) — login in Settings if needed",
                    reason,
                )
                scheduler.abandon_startup_resume(reason)
        except Exception:
            logger.exception("Telegram auto-reconnect background task failed")
            scheduler.abandon_startup_resume("reconnect error")

    asyncio.create_task(_auto_tg())
    indexer.start_auto_scheduler()
    logger.info("Telegram Group Downloader started")
    yield
    # Bounded shutdown — never wedge uvicorn reload on a stuck download/Telethon
    async def _shutdown_step(name: str, coro, timeout: float = 2.5) -> None:
        try:
            await asyncio.wait_for(coro, timeout=timeout)
        except (asyncio.TimeoutError, Exception):
            logger.warning("shutdown %s did not finish within %.1fs", name, timeout)

    await _shutdown_step("indexer", indexer.stop_auto_scheduler(), 2.0)
    await _shutdown_step("scheduler", scheduler.stop_all(), 3.0)
    await _shutdown_step("telegram", tg_manager.disconnect(), 2.0)
    await _shutdown_step("db", db.close(), 2.0)
    logger.info("Shutdown complete")


app = FastAPI(title="Telegram Group Downloader", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=400)
app.include_router(auth_router)
app.include_router(chats_router)
app.include_router(tasks_router)
app.include_router(history_router)
app.include_router(index_router)
app.include_router(settings_router)


@app.get("/api/health")
async def health():
    return {"ok": True}


if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")


@app.get("/")
async def index():
    index_file = WEB_DIR / "index.html"
    # iOS Safari aggressively caches HTML; without this phones keep old JS forever
    return FileResponse(
        index_file,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
