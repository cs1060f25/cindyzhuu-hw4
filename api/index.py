# api/index.py (or app.py if that's what you're using)
import os
import re
import sqlite3
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Body, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# -------------------------
# Config & Helpers
# -------------------------

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(THIS_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Try likely locations for data.db (local & Vercel bundle)
_DB_CANDIDATES = [
    os.path.normpath(os.path.join(THIS_DIR, "..", "data.db")),  # repo root
    os.path.normpath(os.path.join(THIS_DIR, "data.db")),        # bundled next to function
    os.path.normpath(os.path.join(os.getcwd(), "data.db")),     # working dir
]
DB_PATH = next((p for p in _DB_CANDIDATES if os.path.exists(p)), None)

ZIP_RE = re.compile(r"^\d{5}$")

ALLOWED_MEASURES = {
    "Violent crime rate",
    "Unemployment",
    "Children in poverty",
    "Diabetic screening",
    "Mammography screening",
    "Preventable hospital stays",
    "Uninsured",
    "Sexually transmitted infections",
    "Physical inactivity",
    "Adult obesity",
    "Premature Death",
    "Daily fine particulate matter",
}

# Exact DB column names (from county_health_rankings.csv) -> desired JSON keys
PROJECTION_MAP = {
    "Confidence_Interval_Lower_Bound": "confidence_interval_lower_bound",
    "Confidence_Interval_Upper_Bound": "confidence_interval_upper_bound",
    "County": "county",
    "County_code": "county_code",
    "Data_Release_Year": "data_release_year",
    "Denominator": "denominator",
    "fipscode": "fipscode",
    "Measure_id": "measure_id",
    "Measure_name": "measure_name",
    "Numerator": "numerator",
    "Raw_value": "raw_value",
    "State": "state",
    "State_code": "state_code",
    "Year_span": "year_span",
}
PROJECTION_SQL = ", ".join([f'chr."{dbcol}" AS "{alias}"' for dbcol, alias in PROJECTION_MAP.items()])

def is_valid_zip(z: str) -> bool:
    return bool(ZIP_RE.fullmatch(z or ""))

def get_conn() -> sqlite3.Connection:
    if not DB_PATH or not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail="Database file data.db not found in function bundle.")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# -------------------------
# FastAPI App
# -------------------------

app = FastAPI(
    title="County Data API Prototype",
    description="POST /county_data with JSON {zip, measure_name} returns rows from county_health_rankings joined via zip_county.",
    version="1.1.0",
)

# CORS (leave permissive for testing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------- Browser UI ----------
@app.get("/", response_class=HTMLResponse, tags=["ui"])
def ui(request: Request):
    """
    Simple browser form that POSTs to /county_data and shows pretty JSON.
    """
    return templates.TemplateResponse(
        "county_playground.html",
        {
            "request": request,
            "allowed_measures": sorted(ALLOWED_MEASURES),
            "db_found": bool(DB_PATH),
            "db_path": DB_PATH,
        },
    )

# ---------- Meta ----------
@app.get("/health", tags=["meta"])
def health():
    return {
        "ok": True,
        "message": "Use POST /county_data with JSON.",
        "allowed_measures": sorted(ALLOWED_MEASURES),
        "database_found": bool(DB_PATH),
        "database_path": DB_PATH,
    }

# ---------- Core logic ----------
def _county_data_logic(payload: Dict[str, Any]):
    # 418 “teapot” supersedes everything
    if payload.get("coffee") == "teapot":
        raise HTTPException(status_code=418, detail="I'm a teapot.")

    # Validate required fields
    zip_code = payload.get("zip")
    measure_name = payload.get("measure_name")

    if zip_code is None or measure_name is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Both 'zip' and 'measure_name' are required.",
        )

    if not isinstance(zip_code, str) or not is_valid_zip(zip_code):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="zip must be a 5-digit string.",
        )

    if not isinstance(measure_name, str) or measure_name not in ALLOWED_MEASURES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="measure_name must be one of the allowed strings.",
        )

    # Correct join per your schema dump:
    #   zip_county."county_code"  = county_health_rankings."fipscode"
    sql = f"""
        SELECT {PROJECTION_SQL}
        FROM county_health_rankings AS chr
        JOIN zip_county AS zc
          ON zc."county_code" = chr."fipscode"
        WHERE zc."zip" = ? AND chr."Measure_name" = ?
        ORDER BY CAST(chr."Data_Release_Year" AS INTEGER) ASC, chr."Year_span" ASC
    """
    params = (zip_code, measure_name)

    with get_conn() as conn:
        try:
            cur = conn.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.Error as e:
            raise HTTPException(status_code=500, detail=f"Database error: {e}")

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No data found for zip={zip_code} & measure_name='{measure_name}'.",
        )

    return rows

# ---------- API ----------
@app.post("/county_data", tags=["county"])
def county_data(payload: Dict[str, Any] = Body(..., media_type="application/json")):
    return _county_data_logic(payload)
