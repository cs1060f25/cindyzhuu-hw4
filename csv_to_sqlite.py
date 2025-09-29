#!/usr/bin/env python3
"""
csv_to_sqlite.py

Usage:
    python csv_to_sqlite.py <database_name.sqlite3> <input.csv>

Description:
    - Reads a CSV (with a header row) and writes it into a SQLite database.
    - Table name is derived from the CSV filename (stem), sanitized to be SQL-friendly.
    - Column names are taken directly from the CSV header (assumed valid SQL identifiers).
    - Simple type inference per column: INTEGER, REAL, or TEXT (default). Empty strings -> NULL.
    - If a table with the same name exists, it will be dropped and recreated.
"""
import csv
import os
import re
import sqlite3
import sys
from typing import List, Tuple

def sanitize_table_name(path: str) -> str:
    """Derive a SQL-friendly table name from a filename (without extension)."""
    stem = os.path.splitext(os.path.basename(path))[0]
    # Lowercase, replace non-alphanumeric with underscores, collapse repeats, strip edges.
    name = re.sub(r'[^0-9a-zA-Z]+', '_', stem).strip('_').lower()
    # Ensure it doesn't start with a digit
    if re.match(r'^\d', name):
        name = f"t_{name}"
    # Fallback
    return name or "imported_table"

def infer_type(value: str) -> str:
    """Return the inferred SQLite affinity for a single value: INTEGER, REAL, or TEXT."""
    if value is None:
        return "TEXT"
    s = value.strip()
    if s == "":
        return "TEXT"
    # Integers
    try:
        # Ensure no decimals in integer check
        if re.match(r'^[+-]?\d+$', s):
            int(s)
            return "INTEGER"
    except Exception:
        pass
    # Reals
    try:
        # Allow decimals and scientific notation
        if re.match(r'^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$', s):
            float(s)
            return "REAL"
    except Exception:
        pass
    return "TEXT"

def reconcile_types(types: List[str]) -> str:
    """
    Given a list of observed atomic types for a column, decide the column type.
    Priority: if any TEXT -> TEXT; elif any REAL -> REAL; else INTEGER.
    """
    if "TEXT" in types:
        return "TEXT"
    if "REAL" in types:
        return "REAL"
    return "INTEGER"

def read_csv_header_and_rows(csv_path: str) -> Tuple[List[str], List[List[str]]]:
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # may raise StopIteration on empty file (undefined behavior per spec)
        rows = [row for row in reader]
    return header, rows

def infer_schema(header: List[str], rows: List[List[str]]) -> List[str]:
    """Infer SQLite types for each column based on the observed row values."""
    n_cols = len(header)
    observed: List[List[str]] = [[] for _ in range(n_cols)]
    for row in rows:
        # Pad/truncate to header length defensively
        row = (row + [""] * n_cols)[:n_cols]
        for i, val in enumerate(row):
            t = infer_type(val)
            # Only record stronger signals (INTEGER/REAL/TEXT), but keep TEXT if seen
            observed[i].append(t)
    col_types = [reconcile_types(list(set(col_obs))) for col_obs in observed]
    return col_types

def make_create_table_sql(table: str, columns: List[Tuple[str, str]]) -> str:
    cols_sql = ", ".join([f'"{name}" {ctype}' for name, ctype in columns])
    return f'CREATE TABLE "{table}" ({cols_sql});'

def main():
    if len(sys.argv) != 3:
        print("Usage: python csv_to_sqlite.py <database_name.sqlite3> <input.csv>")
        sys.exit(1)

    db_path = sys.argv[1]
    csv_path = sys.argv[2]

    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    table_name = sanitize_table_name(csv_path)
    header, rows = read_csv_header_and_rows(csv_path)
    col_types = infer_schema(header, rows)
    columns = list(zip(header, col_types))

    # Connect to SQLite and (re)create table
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        conn.execute(make_create_table_sql(table_name, columns))

        # Prepare insert
        placeholders = ", ".join(["?"] * len(header))
        quoted_cols = ", ".join(f'"{h}"' for h in header)
        insert_sql = f'INSERT INTO "{table_name}" ({quoted_cols}) VALUES ({placeholders});'

        # Normalize rows: empty string -> None
        def normalize_row(row):
            row = (row + [""] * len(header))[:len(header)]
            norm = []
            for v, ctype in zip(row, col_types):
                if v is None:
                    norm.append(None)
                    continue
                s = v.strip()
                if s == "":
                    norm.append(None)
                else:
                    if ctype == "INTEGER":
                        try:
                            norm.append(int(s))
                            continue
                        except Exception:
                            pass
                    if ctype == "REAL":
                        try:
                            norm.append(float(s))
                            continue
                        except Exception:
                            pass
                    norm.append(s)
            return norm

        with conn:
            conn.executemany(insert_sql, (normalize_row(r) for r in rows))

        # Basic summary
        cur = conn.execute(f'SELECT COUNT(*) FROM "{table_name}";')
        count = cur.fetchone()[0]
        print(f"Imported {count} rows into table '{table_name}' in '{db_path}'.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
