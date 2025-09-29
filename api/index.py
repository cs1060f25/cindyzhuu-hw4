# api/index.py
import os
import re
import sqlite3
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Body, status
from fastapi.middleware.cors import CORSMiddleware

# --- Config ---
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.normpath(os.path.join(THIS_DIR, "..", "data.db"))

# Allowed measures
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

# Columns to project (as in county_health_rankings)
PROJECTION_COLS = [
    "confidence_interval_lower_bound",
    "confidence_interval_upper_bound",
    "county",
    "county_code",
    "data_release_year",
    "denominator",
    "fipscode",
    "measure_id",
    "measure_name",
    "numerator",
    "raw_value",
    "state",
    "state_code",
    "year_span",
]

# --- App ---
app = FastAPI(
    title="County Data API Prototype",
    description="POST /county_data with { zip, measure_name } → JSON rows from county_health_rankings joined by ZIP.",
    version="1.0.0",
)

# CORS (adjust if you have a specific frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["*"],
)

# --- DB helpers ---
def get_conn() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail=f"Database not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

ZIP_RE = re.compile(r"^\d{5}$")

def is_valid_zip(z: str) -> bool:
    return bool(ZIP_RE.fullmatch(z or ""))

# --- Routes ---

@app.get("/", tags=["meta"])
def root():
    """Tiny health/info endpoint."""
    return {
        "ok": True,
        "message": "County Data API is live. Use POST /county_data with JSON.",
        "requires": {"zip": "5-digit string", "measure_name": "one of allowed set"},
        "allowed_measures": sorted(ALLOWED_MEASURES),
        "database_path": DB_PATH,
    }

@app.post("/county_data", tags=["county"])
def county_data(payload: Dict[str, Any] = Body(..., media_type="application/json")):
    """
    Accepts JSON: { "zip": "02138", "measure_name": "Adult obesity", ... }
    - If {"coffee":"teapot"} present (value exactly "teapot"), returns HTTP 418.
    - zip and measure_name are required; else 400.
    - measure_name must be in ALLOWED_MEASURES; else 400.
    - If no rows found for (zip, measure_name), return 404.
    - Returns list of dicts with the county_health_rankings schema (projected columns).
    """
    # 418 Easter egg (supersedes everything)
    if payload.get("coffee") == "teapot":
        # RFC 7168 nod :) — “I’m a teapot”
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

    # Build query safely (parameterized)
    # Assumes:
    #   - zip_county table has at least columns: zip, fipscode
    #   - county_health_rankings table has columns in PROJECTION_COLS (plus measure_name, fipscode)
    proj_sql = ", ".join([f'chr."{c}"' for c in PROJECTION_COLS])

    sql = f"""
        SELECT {proj_sql}
        FROM county_health_rankings AS chr
        JOIN zip_county AS zc
          ON zc.fipscode = chr.fipscode
        WHERE zc.zip = ? AND chr.measure_name = ?
        ORDER BY chr.data_release_year ASC, chr.year_span ASC
    """

    params = (zip_code, measure_name)

    with get_conn() as conn:
        try:
            cur = conn.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.Error as e:
            # Surface a 500 if schema/query errors happen
            raise HTTPException(status_code=500, detail=f"Database error: {e}")

    if not rows:
        # No matching pair → 404 as requested
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No data found for zip={zip_code} & measure_name='{measure_name}'.",
        )

    # Return all matching rows
    return rows
