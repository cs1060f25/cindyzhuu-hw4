# County Data API (FastAPI + SQLite)

This is a minimal API exposing your `data.db` via a `/county_data` endpoint.

## Local setup

```bash
# In this folder, ensure data.db is present (copy from Part 1)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload
# open http://127.0.0.1:8000/docs
```

## Query examples

- All counties (first 100 rows):
  `GET /county_data`

- Filter by state:
  `GET /county_data?state=Massachusetts`

- Search county name substring:
  `GET /county_data?q=Los`

- Choose table:
  `GET /county_data?table=zip_county&select=zip,county,state_abbreviation&limit=20`

- Order results:
  `GET /county_data?order_by=state&order_dir=desc`

## Deploy to Render (recommended quick start)

1. Push this folder (including `data.db`) to a new GitHub repo.
2. Create a new **Web Service** on Render, connect your repo.
3. Environment: **Python**.
4. Build command: `pip install -r requirements.txt`
5. Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
6. Once deployed, visit `/docs` on your Render URL to try it.

> Tip: You may set `DATA_DB_PATH` as an environment variable if your DB lives at a non-default path.

## Deploy to Vercel (optional, serverless)

Vercel's Python support can run ASGI apps. One simple approach is using a [server](https://vercel.com/docs) adapter; however, for SQLite-heavy workloads a small Render dyno is simpler. If you still want Vercel, create `vercel.json` with a Python build and route everything to `app.py`. Ensure `data.db` is committed so the function can read it (read-only).

## Security note

This prototype is read-only (SQLite opened in `mode=ro`) and accepts only whitelisted columns.
