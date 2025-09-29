# api/index.py
import os
import re
import sqlite3
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Body, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(THIS_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

_DB_CANDIDATES = [
    os.path.normpath(os.path.join(THIS_DIR, "..", "data.db")),
    os.path.normpath(os.path.join(THIS_DIR, "data.db")),
    os.path.normpath(os.path.join(os.getcwd(), "data.db")),
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

app = FastAPI(
    title="County Data API",
    description="POST /county_data with JSON {zip, measure_name} returns rows from county_health_rankings joined via zip_county.",
    version="1.3.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------- Browser UI (uses api/templates/index.html) ----------
@app.get("/", response_class=HTMLResponse, tags=["ui"])
def ui(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "allowed_measures": sorted(ALLOWED_MEASURES),
            "allow_none_measure": True,   # <-- tell the template to render a “no measure” option
            "db_found": bool(DB_PATH),
            "db_path": DB_PATH,
        },
    )

@app.get("/health", tags=["meta"])
def health():
    return {
        "ok": True,
        "message": "Use POST /county_data with JSON.",
        "allowed_measures": sorted(ALLOWED_MEASURES),
        "database_found": bool(DB_PATH),
        "database_path": DB_PATH,
    }

def _county_data_logic(payload: Dict[str, Any]):
    if payload.get("coffee") == "teapot":
        raise HTTPException(status_code=418, detail="I'm a teapot.")

    zip_code = payload.get("zip")
    measure_name = payload.get("measure_name")

    if zip_code is None or measure_name is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Both 'zip' and 'measure_name' are required.",
        )

    if not isinstance(zip_code, str) or not is_valid_zip(zip_code):
        raise HTTPException(status_code=400, detail="zip must be a 5-digit string.")

    if not isinstance(measure_name, str) or measure_name not in ALLOWED_MEASURES:
        raise HTTPException(status_code=400, detail="measure_name must be one of the allowed strings.")

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

@app.post("/county_data", tags=["county"])
def county_data(payload: Dict[str, Any] = Body(..., media_type="application/json")):
    return _county_data_logic(payload)
