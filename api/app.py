#!/usr/bin/env python3
# api/app.py
import os
import sqlite3
from pathlib import Path
from flask import Flask, jsonify, request, g

app = Flask(__name__)

# Expect data.db at repo root (one level up from /api)
DB_PATH = (Path(__file__).resolve().parent.parent / "data.db")

def get_db():
    if "db" not in g:
        if not DB_PATH.exists():
            # Fail clearly if DB not bundled/deployed
            raise FileNotFoundError(f"data.db not found at {DB_PATH}")
        # Read-only connection is safest on serverless
        g.db = sqlite3.connect(f"file:{DB_PATH.as_posix()}?mode=ro", uri=True)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()

@app.get("/")
def health():
    # tiny health check + where DB is expected
    return jsonify({
        "ok": True,
        "db_exists": DB_PATH.exists(),
        "db_path": str(DB_PATH),
        "endpoints": ["/county_data"]
    })

@app.get("/county_data")
def county_data():
    """
    Examples:
      /county_data?zip=02138
      /county_data?county=Middlesex&state_abbreviation=MA
      /county_data?fips=25017
      /county_data?limit=5&offset=0
    """
    args = request.args
    zip_code = args.get("zip")
    county = args.get("county")
    state = args.get("state")  # maps to 'default_state' in zip_county
    state_abbrev = args.get("state_abbreviation")
    county_state = args.get("county_state")
    fips = args.get("fips")

    try:
        limit = int(args.get("limit", 50))
        offset = int(args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit/offset must be integers"}), 400

    db = get_db()

    if fips:
        # If your CHR table encodes FIPS as state_code||county_code, we can query it directly.
        # We use a LEFT JOIN from zip_county to CHR to attach a couple of metrics.
        sql = """
            SELECT
              zc.*,
              chr.measure_name,
              chr.raw_value,
              chr.data_release_year
            FROM zip_county AS zc
            LEFT JOIN county_health_rankings AS chr
              ON chr.fipscode = (chr.state_code || chr.county_code)
            WHERE chr.fipscode = ?
            LIMIT ? OFFSET ?
        """
        rows = db.execute(sql, (fips, limit, offset)).fetchall()
        return jsonify({"count": len(rows), "results": [dict(r) for r in rows]})

    # Otherwise, respond from zip_county with optional filters
    where, params = [], []
    if zip_code:
        where.append("zip = ?"); params.append(zip_code)
    if county:
        where.append("county = ?"); params.append(county)
    if state:
        where.append("default_state = ?"); params.append(state)
    if state_abbrev:
        where.append("state_abbreviation = ?"); params.append(state_abbrev)
    if county_state:
        where.append("county_state = ?"); params.append(county_state)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"SELECT * FROM zip_county {where_sql} LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = db.execute(sql, params).fetchall()
    return jsonify({"count": len(rows), "results": [dict(r) for r in rows]})
