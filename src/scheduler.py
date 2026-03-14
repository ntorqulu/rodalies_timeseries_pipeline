"""
scheduler.py — Master entry point.

  Static collection:  once per day at midnight (+ immediately on start)
  Dynamic collection: every 60 seconds

Usage:
    python scheduler.py
    python scheduler.py --interval 120   # slower polling
"""

import argparse
import logging
import time
import schedule
from storage import StorageManager
from collect_static import run as run_static
from collect_dynamic import run_once as run_dynamic_once

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def main(interval: int = 60) -> None:
    store = StorageManager()

    # ── Static: once at startup, then daily at midnight ──────────────────────
    log.info("Running initial static collection …")
    run_static()

    schedule.every().day.at("00:00").do(run_static)

    # ── Dynamic: every N seconds ──────────────────────────────────────────────
    schedule.every(interval).seconds.do(run_dynamic_once, store=store)

    log.info(f"Scheduler running — dynamic every {interval}s, static daily at 00:00")

    run_dynamic_once(store)  # immediate first dynamic pass

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()
    main(args.interval)