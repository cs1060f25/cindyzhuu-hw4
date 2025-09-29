#!/usr/bin/env python3
"""
csv_to_sqlite.py

Usage:
    python3 csv_to_sqlite.py <database.db> <input.csv>

- Expects a valid CSV with a header row of SQL-safe column names (no spaces/escaping).
- Creates (or overwrites) a SQLite database at the given path.
- Creates one table named after the CSV filename (sanitized), with TEXT columns.
- Inserts all rows from the CSV.

Example:
    python3 csv_to_sqlite.py data.db people.csv
    # -> creates table "people" inside data.db
"""

import argparse
import csv
import os
import re
import sqlite3
from pathlib import Path
from typing import List, Optional


def sanitize_table_name(name: str) -> str:
    """
    Make a safe SQL identifier for the table name:
    - replace non-alphanumeric chars with underscores
    - ensure it doesn't start with a digit
    - collapse multiple underscores
    - strip leading/trailing underscores
    """
    base = re.sub(r"\W+", "_", name)  # non-alphanumeric -> _
    base = re.sub(r"_+", "_", base).strip("_") or "data"
    if re.match(r"^\d", base):
        base = f"t_{base}"
    return base


def create_table(conn: sqlite3.Connection, table: str, columns: List[str]) -> None:
    # Columns are assumed already valid SQL identifiers per the prompt.
    cols_sql = ", ".join(f"{col} TEXT" for col in columns)
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute(f"CREATE TABLE {table} ({cols_sql})")


def insert_rows(conn: sqlite3.Connection, table: str, columns: List[str], rows: List[List[Optional[str]]]) -> int:
    placeholders = ", ".join(["?"] * len(columns))
    cols_list = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols_list}) VALUES ({placeholders})"
    with conn:  # ensures a transaction
        conn.executemany(sql, rows)
    return len(rows)


def read_csv_rows(csv_path: Path) -> (List[str], List[List[Optional[str]]]):
    # Use utf-8-sig to gracefully handle BOM if present
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV file must have a header row.")
        columns = [h.strip() for h in reader.fieldnames]
        data: List[List[Optional[str]]] = []
        for row in reader:
            # Preserve column order; missing keys become None
            data.append([row.get(col, None) for col in columns])
    return columns, data


def main():
    parser = argparse.ArgumentParser(description="Import a CSV into a SQLite database.")
    parser.add_argument("database", help="Output SQLite database filename (e.g., data.db)")
    parser.add_argument("csvfile", help="Input CSV filename (with header row)")
    args = parser.parse_args()

    db_path = Path(args.database).resolve()
    csv_path = Path(args.csvfile).resolve()

    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    table_name = sanitize_table_name(csv_path.stem)

    columns, rows = read_csv_rows(csv_path)

    # Connect and import
    # Ensure directory exists for the DB path (if a nested path was given)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        create_table(conn, table_name, columns)
        count = insert_rows(conn, table_name, columns, rows)
    finally:
        conn.close()

    print(f"Created database: {db_path}")
    print(f"Created table: {table_name}")
    print(f"Columns: {', '.join(columns)}")
    print(f"Inserted rows: {count}")


if __name__ == "__main__":
    main()
