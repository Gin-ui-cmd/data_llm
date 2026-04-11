from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
SRC_DIR = PACKAGE_DIR.parent
PROJECT_ROOT = SRC_DIR.parent

DATA_DIR = PROJECT_ROOT / "data"
DATABASE_DIR = DATA_DIR / "databases"
SAMPLE_CSV_DIR = DATA_DIR / "sample_csv"
LOG_DIR = PROJECT_ROOT / "logs"

DEFAULT_DB_PATH = DATABASE_DIR / "app.db"
ERROR_LOG_PATH = LOG_DIR / "error_log.txt"


def ensure_project_dirs() -> None:
    """Create required runtime directories if they do not exist."""
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)
    SAMPLE_CSV_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)