from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_id: int = 0
    api_hash: str = ""
    web_username: str = "admin"
    web_password: str = ""
    proxy: str = ""

    host: str = "0.0.0.0"
    port: int = 9345

    download_dir: Path = Path("downloads")
    data_dir: Path = Path("data")
    session_dir: Path = Path("sessions")
    session_name: str = "telegram_downloader"

    max_parallel_chats: int = 1
    download_delay: float = 0.5
    download_delay_min: float = 0.5
    download_delay_max: float = 0.5
    max_retries: int = 3
    min_folder_title_len: int = 2
    # FloodWait 超过该秒数则暂停任务，避免长时间无响应；以内则自动等待续下
    max_flood_wait: int = 1800
    # Official help.getAppConfig (this account):
    #   large_queue_max_active_operations_count = 2  (>20MB files)
    #   small_queue_max_active_operations_count = 5
    # Keep defaults within those caps to avoid FloodWait / connection resets.
    large_file_concurrency: int = 2
    small_file_concurrency: int = 5
    # Media TCP sessions on media DCs (parallel file sessions are allowed there).
    # Higher hides SOCKS/代理 RTT; keep ≤8 to avoid FloodWait.
    # Defaults; raise via .env for proxy RTT (hard cap 8).
    media_connections: int = 3
    download_pipeline: int = 4
    # upload.getFile part size (bytes). Official max 1MiB.
    download_part_size: int = 1024 * 1024

    # 测试模式：不拉完整媒体，只按文案建目录/占位文件；到期自动停
    test_mode: bool = False
    test_duration_sec: float = 10

    @property
    def db_path(self) -> Path:
        return self.data_dir / "downloader.db"

    @property
    def session_path(self) -> Path:
        return self.session_dir / self.session_name

    def ensure_dirs(self) -> None:
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session_dir.mkdir(parents=True, exist_ok=True)

    def parse_proxy(self) -> Optional[dict]:
        """
        Return Telethon proxy dict (same style as Dineshkarthik media_downloader).

        Accepts: socks5://127.0.0.1:7897  or  http://user:pass@host:port
        """
        raw = (self.proxy or "").strip()
        if not raw:
            return None
        from urllib.parse import urlparse

        parsed = urlparse(raw)
        scheme = (parsed.scheme or "socks5").lower()
        if scheme in ("socks5", "socks5h"):
            proxy_type = "socks5"
        elif scheme in ("socks4", "socks4a"):
            proxy_type = "socks4"
        elif scheme in ("http", "https"):
            proxy_type = "http"
        else:
            return None

        return {
            "proxy_type": proxy_type,
            "addr": parsed.hostname or "127.0.0.1",
            "port": int(parsed.port or 1080),
            "username": parsed.username,
            "password": parsed.password,
            "rdns": True,
        }


@lru_cache
def get_settings() -> Settings:
    return Settings()
