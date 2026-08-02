import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
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
    await scheduler.resume_running_on_startup()

    # Restore Web-saved API/proxy and reconnect Telegram in background
    # so startup is not blocked by network/proxy.
    async def _auto_tg():
        try:
            result = await tg_manager.try_auto_reconnect()
            if result.get("ok"):
                logger.info("Telegram session restored after restart")
            else:
                logger.info(
                    "Telegram not auto-connected (%s) — login in Settings if needed",
                    result.get("reason") or "unknown",
                )
        except Exception:
            logger.exception("Telegram auto-reconnect background task failed")

    asyncio.create_task(_auto_tg())
    indexer.start_auto_scheduler()
    logger.info("Telegram Group Downloader started")
    yield
    await indexer.stop_auto_scheduler()
    await scheduler.stop_all()
    await tg_manager.disconnect()
    await db.close()
    logger.info("Shutdown complete")


app = FastAPI(title="Telegram Group Downloader", lifespan=lifespan)
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
    return FileResponse(index_file)
