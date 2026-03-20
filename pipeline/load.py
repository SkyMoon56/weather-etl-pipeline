"""
pipeline/load.py
Loads a transformed pandas DataFrame into the SQLite database.
Uses INSERT OR IGNORE to safely handle duplicate (city, observed_at) pairs.
"""

import sqlite3
import pandas as pd
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "weather.db")


def load_observations(df: pd.DataFrame, db_path: str = DB_PATH) -> int:
    """
    Insert rows from df into weather_observations.
    Duplicate (city, observed_at) pairs are silently skipped (INSERT OR IGNORE).
    Returns the number of new rows inserted.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to load_observations — nothing to load.")
        return 0

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get row count before to calculate how many were actually new
    cursor.execute("SELECT COUNT(*) FROM weather_observations")
    count_before = cursor.fetchone()[0]

    rows = df.to_dict(orient="records")

    sql = """
        INSERT OR IGNORE INTO weather_observations (
            city, latitude, longitude, observed_at,
            temp_c, temp_f, feels_like_c, humidity_pct,
            wind_speed_kmh, wind_direction_deg, precipitation_mm,
            weather_code, weather_category, heat_index_c, wind_chill_c,
            ingested_at
        ) VALUES (
            :city, :latitude, :longitude, :observed_at,
            :temp_c, :temp_f, :feels_like_c, :humidity_pct,
            :wind_speed_kmh, :wind_direction_deg, :precipitation_mm,
            :weather_code, :weather_category, :heat_index_c, :wind_chill_c,
            :ingested_at
        )
    """

    cursor.executemany(sql, rows)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM weather_observations")
    count_after = cursor.fetchone()[0]
    conn.close()

    new_rows = count_after - count_before
    logger.info("Loaded %d new rows (%d duplicates skipped)", new_rows, len(rows) - new_rows)
    return new_rows


def log_pipeline_run(
    db_path: str,
    status: str,
    rows_extracted: int,
    rows_loaded: int,
    run_at: str,
    duration_seconds: float,
    error_message: Optional[str] = None,
) -> None:
    """Record a pipeline run in the pipeline_runs table."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO pipeline_runs
            (run_at, status, rows_extracted, rows_loaded, error_message, duration_seconds)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (run_at, status, rows_extracted, rows_loaded, error_message, duration_seconds),
    )
    conn.commit()
    conn.close()
    logger.info("Pipeline run logged: status=%s, loaded=%d rows in %.1fs", status, rows_loaded, duration_seconds)
