"""
pipeline/run_pipeline.py
Orchestrates the full ETL pipeline: Extract -> Transform -> Load -> Analyze.
Usage:  python pipeline/run_pipeline.py
        python pipeline/run_pipeline.py --days 14   (override lookback window)
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

# Allow running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pipeline.extract   import extract_all
from pipeline.transform import transform_all
from pipeline.load      import load_observations, log_pipeline_run
from pipeline.analyze   import generate_report
from schema.init_db     import init_db, DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("run_pipeline")


def run(days_back: int = 7) -> bool:
    """
    Execute the full ETL pipeline.
    Returns True on success, False on failure.
    """
    run_at = datetime.now(timezone.utc).isoformat()
    start  = time.monotonic()
    rows_extracted = 0
    rows_loaded    = 0

    logger.info("=" * 60)
    logger.info("Pipeline starting | run_at=%s | days_back=%d", run_at, days_back)
    logger.info("=" * 60)

    try:
        # 0. Ensure DB exists
        init_db(DB_PATH)

        # 1. Extract
        logger.info("--- EXTRACT ---")
        raw_results    = extract_all(days_back=days_back)
        rows_extracted = sum(
            len(r["data"]["hourly"]["time"])
            for r in raw_results
            if r.get("data") and r["data"].get("hourly", {}).get("time")
        )
        logger.info("Extracted %d total hourly records", rows_extracted)

        # 2. Transform
        logger.info("--- TRANSFORM ---")
        df = transform_all(raw_results)
        if df.empty:
            raise ValueError("Transform produced empty DataFrame — aborting load.")
        logger.info("Transformed DataFrame: %d rows x %d cols", *df.shape)

        # 3. Load
        logger.info("--- LOAD ---")
        rows_loaded = load_observations(df, DB_PATH)

        # 4. Analyze
        logger.info("--- ANALYZE ---")
        generate_report()

        duration = round(time.monotonic() - start, 2)
        log_pipeline_run(DB_PATH, "success", rows_extracted, rows_loaded, run_at, duration)

        logger.info("=" * 60)
        logger.info("Pipeline SUCCESS | loaded=%d rows | %.1fs", rows_loaded, duration)
        logger.info("=" * 60)
        return True

    except Exception as exc:
        duration = round(time.monotonic() - start, 2)
        logger.exception("Pipeline FAILED: %s", exc)
        log_pipeline_run(DB_PATH, "failure", rows_extracted, rows_loaded, run_at, duration, str(exc))
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the weather ETL pipeline")
    parser.add_argument("--days", type=int, default=7, help="Days of history to fetch (default: 7)")
    args = parser.parse_args()

    success = run(days_back=args.days)
    sys.exit(0 if success else 1)
