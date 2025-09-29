# County Data API (CSV → SQLite → FastAPI)

This project converts CSV files (`zip_county.csv`, `county_health_rankings.csv`) into a SQLite database, then exposes them via a JSON API and a browser-friendly UI deployed on Vercel.

## Overview

- **CSV Loader:** `csv_to_sqlite.py` turns valid CSV files into a SQLite database (`data.db`), using the CSV header row verbatim as column names.
- **API:** `api/index.py` (FastAPI) provides:
  - `POST /county_data` — query county health data by ZIP and measure.
  - `POST /country-data` — same as above (alias for convenience).
  - `GET /` — simple HTML UI to interact with the API.
  - `GET /health` — JSON health check and schema info.

## Data Schema

Tables created in `data.db`:
- **zip_county**
  - `zip`
  - `county_code`
  - other columns (`default_state`, `county`, `state_abbreviation`, etc.)
- **county_health_rankings**
  - `State`, `County`, `State_code`, `County_code`, `Year_span`, `Measure_name`, `Measure_id`, `Numerator`, `Denominator`, `Raw_value`, `Confidence_Interval_Lower_Bound`, `Confidence_Interval_Upper_Bound`, `Data_Release_Year`, `fipscode`

Join key: `zip_county.county_code = county_health_rankings.fipscode`.

## Setup (Local)

```bash
# 1. Create data.db
python3 csv_to_sqlite.py data.db zip_county.csv
python3 csv_to_sqlite.py data.db county_health_rankings.csv

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run locally
uvicorn api.index:app --reload --port 8000
