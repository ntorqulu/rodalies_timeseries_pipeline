import logging
from pathlib import Path
from datetime import datetime, timedelta

from driver_uploader import get_service, upload_file
from config import DRIVE_FOLDER

log = logging.getLogger(__name__)

# Base data dir — same anchor as StorageManager
DATA_DIR = Path(__file__).parent.parent / "data"


def upload_yesterday_dynamic() -> None:
    service = get_service()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y_%m_%d")

    dynamic_files = {
        "trains":     DATA_DIR / f"dynamic/trains/trains_{yesterday}.parquet",
        "timetables": DATA_DIR / f"dynamic/timetables/timetables_{yesterday}.parquet",
        "journeys":   DATA_DIR / f"dynamic/journeys/journeys_{yesterday}.parquet",
        "weather":    DATA_DIR / f"dynamic/weather/weather_{yesterday}.parquet",
    }

    for table, path in dynamic_files.items():
        folder_id = DRIVE_FOLDER.get(table)
        if not folder_id:
            log.warning(f"[drive] no folder configured for {table}, skipping")
            continue
        upload_file(service, path, folder_id)


def upload_static_once() -> None:
    """
    Upload static files. Since static data is refreshed daily,
    always update (upload_file handles create-or-update).
    """
    service = get_service()

    static_files = {
        "stations": DATA_DIR / "static/stations.parquet",
        "lines":    DATA_DIR / "static/lines.parquet",
    }

    for table, path in static_files.items():
        folder_id = DRIVE_FOLDER.get(table)
        if not folder_id:
            log.warning(f"[drive] no folder configured for {table}, skipping")
            continue
        upload_file(service, path, folder_id)


def upload_midnight() -> None:
    log.info("[drive] uploading yesterday's dynamic files …")
    try:
        upload_yesterday_dynamic()
    except Exception as e:
        log.error(f"[drive] dynamic upload failed: {e}")

    log.info("[drive] uploading static files …")
    try:
        upload_static_once()
    except Exception as e:
        log.error(f"[drive] static upload failed: {e}")

    log.info("[drive] upload complete")