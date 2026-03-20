"""
schema/init_db.py
Creates the SQLite database and tables for the weather ETL pipeline.
Run once: python schema/init_db.py
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "weather.db")


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS weather_observations (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            city                TEXT    NOT NULL,
            latitude            REAL    NOT NULL,
            longitude           REAL    NOT NULL,
            observed_at         TEXT    NOT NULL,
            temp_c              REAL,
            temp_f              REAL,
            feels_like_c        REAL,
            humidity_pct        REAL,
            wind_speed_kmh      REAL,
            wind_direction_deg  REAL,
            precipitation_mm    REAL,
            weather_code        INTEGER,
            weather_category    TEXT,
            heat_index_c        REAL,
            wind_chill_c        REAL,
            ingested_at         TEXT    NOT NULL,
            UNIQUE(city, observed_at)
        );

        CREATE INDEX IF NOT EXISTS idx_obs_city_time
            ON weather_observations (city, observed_at);

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at           TEXT    NOT NULL,
            status           TEXT    NOT NULL CHECK(status IN ('success', 'failure')),
            rows_extracted   INTEGER DEFAULT 0,
            rows_loaded      INTEGER DEFAULT 0,
            error_message    TEXT,
            duration_seconds REAL
        );
    """)

    conn.commit()
    conn.close()
    print(f"Database initialised at: {os.path.abspath(db_path)}")


if __name__ == "__main__":
    init_db()
