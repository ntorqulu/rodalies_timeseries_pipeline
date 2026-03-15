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
import signal
import time
import schedule
from storage import StorageManager
from collect_static import run as run_static
from collect_dynamic import run_once as run_dynamic_once
from upload_daily import upload_midnight

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def main(interval: int = 60) -> None:
    store = StorageManager()
    shutdown = False

    def _handle_signal(sig, frame):
        nonlocal shutdown
        log.info("Shutdown requested — finishing current pass...")
        shutdown = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    def run_daily_tasks() -> None:
        """Refresh static data then upload everything — runs once at midnight."""
        log.info("── Daily tasks ──")
        try:
            run_static()
        except Exception as e:
            log.error(f"Static collection failed: {e}")
        try:
            upload_midnight()
        except Exception as e:
            log.error(f"Midnight upload failed: {e}")
        log.info("── Daily tasks complete ──")

    # ── Static: once at startup, then daily at midnight ──────────────────────
    log.info("Running initial static collection …")
    run_static()

    schedule.every().day.at("00:00").do(run_daily_tasks)

    # ── Dynamic: every N seconds ──────────────────────────────────────────────
    schedule.every(interval).seconds.do(run_dynamic_once, store=store)

    log.info(f"Scheduler running — dynamic every {interval}s, daily tasks at 00:00")

    run_dynamic_once(store)  # immediate first dynamic pass

    while not shutdown:
        schedule.run_pending()
        time.sleep(1)

    log.info("Scheduler stopped cleanly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()
    main(args.interval)