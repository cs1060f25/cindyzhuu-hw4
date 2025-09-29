from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from typing import List
import sqlite3
import os
import re

DB_PATH = os.getenv("DATA_DB_PATH", "data.db")

app = FastAPI(title="County Data API", version="0.2.1")

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

def get_conn_ro() -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)
    except sqlite3.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Failed to open database at '{DB_PATH}': {e}")
    conn.row_factory = sqlite3.Row
    return conn

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,))
    return cur.fetchone() is not None

@app.get("/health")
def health():
    exists = os.path.exists(DB_PATH)
    size = os.path.getsize(DB_PATH) if exists else 0
    return {"ok": True, "db_path": DB_PATH, "db_exists": exists, "db_size_bytes": size}

@app.post("/county_data")
async def county_data(request: Request):
    # Enforce JSON
    ctype = request.headers.get("content-type", "")
    if ctype.split(";")[0].strip().lower() != "application/json":
        raise HTTPException(status_code=400, detail="Content-Type must be application/json")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Teapot (supersedes all)
    if isinstance(body, dict) and body.get("coffee") == "teapot":
        raise HTTPException(status_code=418, detail="I'm a teapot")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")

    zip_code = body.get("zip")
    measure_name = body.get("measure_name")

    # Requireds
    if zip_code is None or measure_name is None:
        raise HTTPException(status_code=400, detail="Missing required keys: 'zip' and 'measure_name' are required")

    # zip: 5 digits
    if not isinstance(zip_code, str) or not re.fullmatch(r"\d{5}", zip_code):
        raise HTTPException(status_code=400, detail="'zip' must be a 5-digit string")

    # measure_name in allowed list
    if not isinstance(measure_name, str) or measure_name not in ALLOWED_MEASURES:
        raise HTTPException(status_code=400, detail="Invalid 'measure_name'. Must be one of the allowed measures.")

    def get_colmap(conn, table):
        # returns {lowercased_name: actual_name}
        cur = conn.execute(f'PRAGMA table_info("{table}")')
        rows = cur.fetchall()
        return {r[1].lower(): r[1] for r in rows}  # r[1] is 'name'

    conn = get_conn_ro()
    try:
        if not table_exists(conn, "county_health_rankings") or not table_exists(conn, "zip_county"):
            raise HTTPException(status_code=500, detail="Required tables not found in database")

        zc = get_colmap(conn, "zip_county")
        chr_ = get_colmap(conn, "county_health_rankings")

        # Expected columns (logical names -> must exist in chr)
        needed_chr = [
            "state","county","state_code","county_code","year_span","measure_name","measure_id",
            "numerator","denominator","raw_value",
            "confidence_interval_lower_bound","confidence_interval_upper_bound",
            "data_release_year","fipscode",
        ]
        # Map to actual names (handling case like State vs state, etc.)
        try:
            chr_names = [chr_[name] for name in needed_chr]
        except KeyError as e:
            missing = [n for n in needed_chr if n not in chr_]
            raise HTTPException(status_code=500, detail=f"Missing columns in county_health_rankings: {missing}")

        # zip_county columns needed
        # (your file uses lowercase, but we still resolve dynamically)
        try:
            zc_zip = zc["zip"]
            zc_county_code = zc["county_code"]
        except KeyError:
            raise HTTPException(status_code=500, detail="zip_county must have 'zip' and 'county_code' columns")

        # Also need chr 'state_code' actual name for FIPS concat
        chr_state_code = chr_["state_code"]  # actual (could be 'State_code')
        chr_fipscode   = chr_["fipscode"]    # actual (already lowercase in your CSV)

        # Build SELECT list (return keys match DB schema exactly)
        select_sql = ", ".join([f'chr."{c}"' for c in chr_names])

        # Robust FIPS join:
        # chr.fipscode == 2-digit state_code + 3-digit county_code (zero-padded)
        sql = (
            "SELECT " + select_sql + " "
            "FROM county_health_rankings AS chr "
            "INNER JOIN zip_county AS zc "
            f'  ON chr."{chr_fipscode}" = printf(\'%s%03d\', chr."{chr_state_code}", CAST(zc."{zc_county_code}" AS INTEGER)) '
            f'WHERE zc."{zc_zip}" = ? '
            "  AND chr.\"{measure}\" = ? "
            "ORDER BY chr.\"{drel}\", chr.\"{yspan}\""
        ).format(
            measure=chr_["measure_name"],
            drel=chr_["data_release_year"],
            yspan=chr_["year_span"],
        )

        params = [zip_code, measure_name]
        cur = conn.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        if not rows:
            raise HTTPException(status_code=404, detail="No data found for given zip and measure_name")
        return JSONResponse(rows)
    finally:
        conn.close()
