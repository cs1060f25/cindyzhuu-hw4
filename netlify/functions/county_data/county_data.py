import os, sys
from pathlib import Path

# Add repo root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Point FastAPI at DB copy inside function folder
os.environ.setdefault("DATA_DB_PATH", str(Path(__file__).resolve().parent / "data.db"))

from app import app
from mangum import Mangum

handler = Mangum(app, lifespan="off")
