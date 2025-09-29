#!/usr/bin/env python3
# api/app.py
import os
import sqlite3
from pathlib import Path
from flask import Flask, jsonify, request, g, render_template

app = Flask(__name__)

# Resolve path to data.db (bundled with your deployment)
DB_PATH = (Path(__file__).resolve().parent.parent / "data.db").as_posix()

def get_db():
    if "db" not in g:
        # One connection per request; safe for serverless concurrency
        conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db

@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()

@app.get("/")
def root():
    return render_template("index.html")

@app.get("/county_data")
def county_data():
    """
    Query examples:
      /county_data?zip=02138
      /county_data?county=Middlesex&state_abbreviation=MA
      /county_data?fips=25017
      /county_data?limit=5&offset=10
    """
    args = request.args

    # Optional filters
    zip_code = args.get("zip")
    county = args.get("county")
    state = args.get("state")  # matches 'default_state' in zip_county
    state_abbrev = args.get("state_abbreviation")
    county_state = args.get("county_state")
    fips = args.get("fips")  # matches 'fipscode' in county_health_rankings

    # Pagination
    try:
        limit = int(args.get("limit", 50))
        offset = int(args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit/offset must be integers"}), 400

    # Build a simple join when FIPS is provided; otherwise respond from zip_county only
    db = get_db()
    rows = []
    if fips:
        # Join example: left join rankings on FIPS
        sql = """
            SELECT zc.*, chr.measure_name, chr.raw_value, chr.data_release_year
            FROM zip_county AS zc
            LEFT JOIN county_health_rankings AS chr
              ON chr.fipscode = (chr.state_code || chr.county_code)
            WHERE (chr.fipscode = ?)
            LIMIT ? OFFSET ?
        """
        rows = db.execute(sql, (fips, limit, offset)).fetchall()
    else:
        # Filter zip_county table
        where = []
        params = []
        if zip_code:
            where.append("zip = ?")
            params.append(zip_code)
        if county:
            where.append("county = ?")
            params.append(county)
        if state:
            where.append("default_state = ?")
            params.append(state)
        if state_abbrev:
            where.append("state_abbreviation = ?")
            params.append(state_abbrev)
        if county_state:
            where.append("county_state = ?")
            params.append(county_state)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
            SELECT *
            FROM zip_county
            {where_sql}
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        rows = db.execute(sql, params).fetchall()

    data = [dict(r) for r in rows]
    return jsonify({"count": len(data), "results": data})
