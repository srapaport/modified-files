"""Build license-case-study pickles from the 2026-03-02 graph tables.

Prerequisites:
  - DATABASE_URL in ../.env
  - ../results_2026_03_02/grades.csv (run modified-files `all_grade` on the new graph)
"""

from __future__ import annotations

import io
import os
import pickle
import subprocess
import time
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

from graph_config_2026_03_02 import MODIFIED_FILES_TABLE, RESULTS_DIR

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

results_path = Path(__file__).resolve().parent / RESULTS_DIR
results_path.mkdir(parents=True, exist_ok=True)

database_url = os.environ["DATABASE_URL"]


def query_postgres(sql: str) -> pd.DataFrame:
    """Run a read-only query via psql (no SQLAlchemy dependency)."""
    copy_sql = f"COPY ({sql.strip()}) TO STDOUT WITH (FORMAT csv, HEADER true)"
    proc = subprocess.run(
        ["psql", database_url, "-v", "ON_ERROR_STOP=1", "-c", copy_sql],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return pd.read_csv(io.StringIO(proc.stdout))


script_start = time.perf_counter()
print("Starting working_db_2026_03_02.py")

license_query = f"""
    SELECT origin, revision, branch, snapshot_without, path, status
    FROM {MODIFIED_FILES_TABLE}
    WHERE branch = 'refs/heads/main'
      AND (
          lower(path) LIKE '%license%'
          OR lower(path) LIKE '%licence%'
      )
"""

print("Step 1/4: Running license query against database...")
step_start = time.perf_counter()
result = query_postgres(license_query)
print(f"Step 1/4: License query finished in {time.perf_counter() - step_start:.1f}s ({len(result):,} rows)")

print(f"Step 2/4: Writing result.pkl to {results_path / 'result.pkl'}...")
step_start = time.perf_counter()
with open(results_path / "result.pkl", "wb") as f:
    pickle.dump(result, f)
print(f"Step 2/4: Wrote result.pkl in {time.perf_counter() - step_start:.1f}s")

grades_csv = results_path / "grades.csv"
if not grades_csv.exists():
    raise FileNotFoundError(
        f"Missing {grades_csv}. Generate it with modified-files `all_grade` on the "
        f"2026-03-02 graph, then re-run this script."
    )

print(f"Step 3/4: Running grades query from {grades_csv}...")
step_start = time.perf_counter()
result_grade = duckdb.sql(
    f"""
    SELECT *
    FROM read_csv_auto('{grades_csv}', strict_mode=false, max_line_size=10000000, ignore_errors=true)
    WHERE amount_snap > 1
      AND amount_rev > 1
"""
).fetchdf()
print(
    f"Step 3/4: Grades query finished in {time.perf_counter() - step_start:.1f}s "
    f"({len(result_grade):,} rows)"
)

print(f"Step 4/4: Writing grades.pkl to {results_path / 'grades.pkl'}...")
step_start = time.perf_counter()
with open(results_path / "grades.pkl", "wb") as f:
    pickle.dump(result_grade, f)
print(f"Step 4/4: Wrote grades.pkl in {time.perf_counter() - step_start:.1f}s")

print(f"Done. Wrote {len(result):,} license rows to {results_path / 'result.pkl'}")
print(f"Done. Wrote {len(result_grade):,} grade rows to {results_path / 'grades.pkl'}")
print(f"Total script time: {time.perf_counter() - script_start:.1f}s")
