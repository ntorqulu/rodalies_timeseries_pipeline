"""
collect_static.py — Fetch stations + lines once per day and persist to parquet.

Usage:
    python collect_static.py
"""

import logging
import pandas as pd
from api_client import get_stations, get_lines
from schemas import build_station_rows, build_line_rows
from storage import StorageManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def collect_stations(store: StorageManager) -> None:
    log.info("Fetching stations …")
    data = get_stations(limit=500)
    rows = build_station_rows(data)
    df = pd.DataFrame(rows).astype({
        "station_id":  "string",
        "name":        "string",
        "accessible":  "boolean",
        "latitude":    "float64",
        "longitude":   "float64",
        "connections": "int32",
        "services":    "int32",
        "last_updated":"string",
    })
    store.write_static("stations", df)
    log.info(f"Stations saved: {len(df)} rows")


def collect_lines(store: StorageManager) -> None:
    log.info("Fetching lines …")
    data = get_lines(limit=500)
    rows = build_line_rows(data)
    df = pd.DataFrame(rows).astype({
        "line_id":        "string",
        "name":           "string",
        "origin":         "string",
        "destination":    "string",
        "stations_count": "int32",
        "last_updated":   "string",
    })
    store.write_static("lines", df)
    log.info(f"Lines saved: {len(df)} rows")


def run():
    store = StorageManager()
    collect_stations(store)
    collect_lines(store)


if __name__ == "__main__":
    run()