import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent

EXPORT_BASE = BASE_DIR / "exports"
EXPORT_BASE.mkdir(parents=True, exist_ok=True)

CORS_ALLOW_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*").split(",")

# Path to .env at repo root (used to read API keys into os.environ)
DOTENV_PATH = ROOT_DIR / ".env"
