"""
collect_dynamic.py — Poll live train data every 60 s and append to daily parquets.

Usage:
    python collect_dynamic.py          # runs forever
    python collect_dynamic.py --once   # single pass
"""

import argparse
import logging
import pandas as pd
from itertools import permutations
from api_client import get_timetable
from schemas import build_train_rows, build_timetable_rows, build_journey_rows
from storage import StorageManager
import asyncio
import aiohttp
from config import BASE_URL, LANG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── Dedup keys: what makes a row "the same observation" ──────────────────────
TRAIN_DEDUP     = ["train_id", "current_station_id"]
TIMETABLE_DEDUP = ["train_id", "station_id", "planned_departure"]
JOURNEY_DEDUP   = ["journey_id"]

# Cache of station IDs that consistently return 500 — skip them
_DEAD_STATIONS: set[str] = set()


async def fetch_departures_async(
    session: aiohttp.ClientSession,
    station_id: str,
) -> tuple[str, dict | None]:
    if station_id in _DEAD_STATIONS:
        return station_id, None

    url = f"{BASE_URL}/departures"
    params = {"stationId": station_id, "minute": 120, "fullResponse": "true", "lang": LANG}

    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 500:
                _DEAD_STATIONS.add(station_id)
                return station_id, None
            resp.raise_for_status()
            return station_id, await resp.json()
    except Exception as e:
        log.warning(f"Departures failed for station {station_id}: {e}")
        return station_id, None


async def fetch_all_departures(station_ids: list[str]) -> dict[str, dict]:
    """Fetch all stations concurrently, max 20 at a time."""
    results = {}
    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_departures_async(session, sid) for sid in station_ids]
        for coro in asyncio.as_completed(tasks):
            station_id, data = await coro
            if data is not None:
                results[station_id] = data
    return results


def collect_trains_and_timetables(
    store: StorageManager,
    station_ids: list[str],
) -> None:
    all_data = asyncio.run(fetch_all_departures(station_ids))

    train_rows: list[dict] = []
    timetable_rows: list[dict] = []

    for station_id, data in all_data.items():
        train_rows.extend(build_train_rows(data, station_id))
        timetable_rows.extend(build_timetable_rows(data, station_id))

    if train_rows:
        df = pd.DataFrame(train_rows).astype({
            "train_id": "string", "line_id": "string",
            "current_station_id": "string", "next_station_id": "string",
            "status": "string", "delay_minutes": "int32", "timestamp": "string",
        })
        store.append_dynamic("trains", df, dedup_keys=TRAIN_DEDUP)

    if timetable_rows:
        df = pd.DataFrame(timetable_rows).astype({
            "train_id": "string", "station_id": "string", "timestamp": "string",
        })
        store.append_dynamic("timetables", df, dedup_keys=TIMETABLE_DEDUP)

_VALID_PAIRS: list[tuple] | None = None

def get_valid_station_pairs(store: StorageManager) -> list[tuple]:
    global _VALID_PAIRS
    if _VALID_PAIRS is not None:
        return _VALID_PAIRS
    from api_client import get_lines as _get_lines
    try:
        data = _get_lines(limit=500)
    except Exception as e:
        log.warning(f"Failed to fetch lines for valid station pairs: {e}")
        return []
    pairs = set()
    for line in data["included"]:
        station_ids = [s["id"] for s in line.get("stations", [])]
        for a, b in permutations(station_ids, 2):
            pairs.add((str(a), str(b)))
    _VALID_PAIRS = list(pairs)
    return _VALID_PAIRS

def collect_journeys(
    store: StorageManager,
    max_pairs: int = 100,
) -> None:
    """
    Sample O/D pairs and hit /timetables to record journey-level data.
    max_pairs limits API calls per run; increase for broader coverage.
    """
    import random
    try:
        pairs = get_valid_station_pairs(store)
    except Exception as e:
        log.warning(f"Failed to fetch valid station pairs: {e}")
        return
    sample = random.sample(pairs, min(max_pairs, len(pairs)))

    journey_rows: list[dict] = []

    for origin, dest in sample:
        try:
            data = get_timetable(origin, dest)
            journey_rows.extend(build_journey_rows(data, origin, dest))
        except Exception as e:
            log.warning(f"Timetable failed {origin}→{dest}: {e}")

    if journey_rows:
        df = pd.DataFrame(journey_rows).astype({
            "journey_id":             "string",
            "train_id":               "string",
            "origin_station_id":      "string",
            "destination_station_id": "string",
            "departure_time":         "string",
            "arrival_time":           "string",
            "duration":               "string",
            "accessible":             "boolean",
            "steps":                  "int32",
            "timestamp":              "string",
        })
        store.append_dynamic("journeys", df, dedup_keys=JOURNEY_DEDUP)


def run_once(store: StorageManager) -> None:
    log.info("── Dynamic collection pass ──")

    # Load station IDs from static cache (avoids an API call every 60 s)
    stations_df = store.read_static("stations")
    if stations_df is None:
        log.warning("No static stations file found — fetching from API")
        from api_client import get_stations
        data = get_stations(limit=500)
        station_ids = [str(s["id"]) for s in data["included"]]
    else:
        station_ids = stations_df["station_id"].tolist()

    collect_trains_and_timetables(store, station_ids)
    collect_journeys(store, max_pairs=50)
    store.enrich_actuals()
    store.enrich_positions()
    log.info("── Pass complete ──")


def run_forever(interval_seconds: int = 60) -> None:
    import schedule
    import time

    store = StorageManager()

    schedule.every(interval_seconds).seconds.do(run_once, store=store)

    log.info(f"Scheduler started — polling every {interval_seconds}s")
    run_once(store)  # immediate first run
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single collection pass then exit",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Poll interval in seconds (default: 60)",
    )
    args = parser.parse_args()

    if args.once:
        run_once(StorageManager())
    else:
        run_forever(args.interval)