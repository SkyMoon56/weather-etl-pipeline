"""
pipeline/analyze.py
Queries the SQLite database and generates a Markdown analytics report.
Output: reports/daily_summary.md
"""

import sqlite3
import os
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

DB_PATH   = os.path.join(os.path.dirname(__file__), "..", "weather.db")
REPORT_PATH = os.path.join(os.path.dirname(__file__), "..", "reports", "daily_summary.md")


def query(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def generate_report(db_path: str = DB_PATH, report_path: str = REPORT_PATH) -> str:
    """Run analytics queries and write a Markdown report. Returns the report text."""
    conn = sqlite3.connect(db_path)
    now_utc = datetime.now(timezone.utc)
    cutoff  = (now_utc - timedelta(days=7)).isoformat()

    # --- Queries ---
    total_rows = query(conn, "SELECT COUNT(*) AS n FROM weather_observations")[0]["n"]
    cities     = query(conn, "SELECT DISTINCT city FROM weather_observations ORDER BY city")

    hottest = query(conn, """
        SELECT city, MAX(temp_c) AS max_temp_c, observed_at
        FROM weather_observations
        WHERE observed_at >= ?
        ORDER BY max_temp_c DESC LIMIT 1
    """, (cutoff,))

    coldest = query(conn, """
        SELECT city, MIN(temp_c) AS min_temp_c, observed_at
        FROM weather_observations
        WHERE observed_at >= ?
        ORDER BY min_temp_c ASC LIMIT 1
    """, (cutoff,))

    city_avg = query(conn, """
        SELECT
            city,
            ROUND(AVG(temp_c), 1)          AS avg_temp_c,
            ROUND(AVG(humidity_pct), 1)    AS avg_humidity,
            ROUND(SUM(precipitation_mm), 1) AS total_precip_mm,
            COUNT(*)                        AS obs_count
        FROM weather_observations
        WHERE observed_at >= ?
        GROUP BY city
        ORDER BY city
    """, (cutoff,))

    pipeline_runs = query(conn, """
        SELECT run_at, status, rows_loaded, ROUND(duration_seconds,1) AS duration_s
        FROM pipeline_runs
        ORDER BY run_at DESC LIMIT 10
    """)

    conn.close()

    # --- Build Markdown ---
    lines = [
        "# 📊 Weather Pipeline — Daily Summary",
        f"",
        f"_Generated: {now_utc.strftime('%Y-%m-%d %H:%M UTC')}_",
        f"",
        f"**Total observations in database:** {total_rows:,}",
        f"**Cities tracked:** {', '.join(c['city'] for c in cities)}",
        f"",
        "---",
        "",
        "## 🌡️ Last 7 Days Highlights",
        "",
    ]

    if hottest:
        h = hottest[0]
        lines.append(f"- **Hottest:** {h['city']} — {h['max_temp_c']}°C at {h['observed_at'][:16]} UTC")
    if coldest:
        c = coldest[0]
        lines.append(f"- **Coldest:** {c['city']} — {c['min_temp_c']}°C at {c['observed_at'][:16]} UTC")

    lines += [
        "",
        "---",
        "",
        "## 🏙️ City Averages (Last 7 Days)",
        "",
        "| City | Avg Temp (°C) | Avg Humidity (%) | Total Precip (mm) | Observations |",
        "|------|:---:|:---:|:---:|:---:|",
    ]
    for row in city_avg:
        lines.append(
            f"| {row['city']} | {row['avg_temp_c']} | {row['avg_humidity']} "
            f"| {row['total_precip_mm']} | {row['obs_count']} |"
        )

    lines += [
        "",
        "---",
        "",
        "## ⚙️ Recent Pipeline Runs",
        "",
        "| Run At (UTC) | Status | Rows Loaded | Duration (s) |",
        "|---|:---:|:---:|:---:|",
    ]
    for run in pipeline_runs:
        status_icon = "✅" if run["status"] == "success" else "❌"
        lines.append(
            f"| {run['run_at'][:19]} | {status_icon} {run['status']} "
            f"| {run['rows_loaded']} | {run['duration_s']} |"
        )

    lines.append("")
    report = "\n".join(lines)

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)

    logger.info("Report written to %s", report_path)
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(generate_report())
