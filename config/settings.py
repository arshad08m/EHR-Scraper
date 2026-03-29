"""
config/settings.py — single source of truth for all configuration.

All values loaded from config/.env — nothing hardcoded.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the config/ directory
_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_env_path)


def _env_bool(name: str, default: str = "false") -> bool:
    """Parse common truthy env values (1/true/yes/on)."""
    raw = os.getenv(name, default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: str) -> int:
    raw = os.getenv(name, default)
    try:
        return int(raw)
    except Exception:
        return int(default)


class Settings:
    # ── Credentials ───────────────────────────────────────────────────────────
    USERNAME: str = os.getenv("KANTIME_USERNAME", "")
    PASSWORD: str = os.getenv("KANTIME_PASSWORD", "")

    # ── URLs ──────────────────────────────────────────────────────────────────
    BASE_URL:      str = os.getenv("KANTIME_BASE_URL",      "https://www.kantimehealth.net/HH/Z1")
    LOGIN_URL:     str = os.getenv("KANTIME_LOGIN_URL",     "https://www.kantimehealth.net/identity/v2/Accounts/Authorize?product=hh")
    WORLDVIEW_URL: str = os.getenv("KANTIME_WORLDVIEW_URL",
                         "https://www.kantimehealth.net/HH/Z1/UI/Orders/WorldView_ReceivedDocuments.aspx")

    # ── MongoDB ───────────────────────────────────────────────────────────────
    MONGO_URI:        str = os.getenv("MONGO_CONNECTION_STRING", "")
    MONGO_DB:         str = os.getenv("MONGO_DB_NAME",           "kantime_ehr")
    MONGO_COLLECTION: str = os.getenv("MONGO_COLLECTION",        "orders")

    # ── Paths ─────────────────────────────────────────────────────────────────
    DATA_DIR:        Path = Path("data")
    LOGS_DIR:        Path = Path("logs")
    PDF_DIR:         Path = Path("data/pdfs")
    CHECKPOINT_FILE: Path = Path("data/checkpoint.json")
    JSONL_OUTPUT:    Path = Path("data/orders_output.jsonl")

    # ── Scraper behaviour ─────────────────────────────────────────────────────
    REQUEST_DELAY: float = float(os.getenv("REQUEST_DELAY_SECONDS", "1.5"))
    MAX_RETRIES:   int   = int(os.getenv("MAX_RETRIES",             "3"))
    MAX_DOCS:      int   = int(os.getenv("MAX_DOCS",                "0"))  # 0 = unlimited, >0 = test mode
    ENABLE_DATA_EXTRACTION: bool = _env_bool("ENABLE_DATA_EXTRACTION", "false")

    # ── Ollama Vision extraction ─────────────────────────────────────────────
    OLLAMA_ENABLED:         bool = _env_bool("OLLAMA_ENABLED", "false")
    OLLAMA_MODEL:           str  = os.getenv("OLLAMA_MODEL", "qwen2.5vl:7b")
    OLLAMA_URL:             str  = os.getenv("OLLAMA_URL", "http://localhost:11434")
    OLLAMA_TIMEOUT_SECONDS: int  = _env_int("OLLAMA_TIMEOUT_SECONDS", "90")
    OLLAMA_MAX_PAGES:       int  = _env_int("OLLAMA_MAX_PAGES", "2")

    def __post_init__(self):
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        self.PDF_DIR.mkdir(parents=True, exist_ok=True)

    def validate(self):
        missing = [k for k, v in {
            "KANTIME_USERNAME": self.USERNAME,
            "KANTIME_PASSWORD": self.PASSWORD,
        }.items() if not v]
        if missing:
            raise EnvironmentError(
                f"Missing required env vars: {missing}\n"
                f"Edit config/.env (copy from config/.env.template first)."
            )


# Singleton
settings = Settings()
settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
settings.LOGS_DIR.mkdir(parents=True, exist_ok=True)
settings.PDF_DIR.mkdir(parents=True, exist_ok=True)
