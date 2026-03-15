import logging
from pathlib import Path
from datetime import datetime, timedelta

from drive_uploader import get_service, upload_file
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
    }

    for table, path in dynamic_files.items():
        upload_file(service, path, DRIVE_FOLDER[table])


def file_exists(service, name: str, folder_id: str) -> bool:
    query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id,name)").execute()
    return len(results.get("files", [])) > 0


def upload_static_once() -> None:
    """Upload static files only if they haven't been uploaded yet."""
    service = get_service()

    static_files = {
        "stations": DATA_DIR / "static/stations.parquet",
        "lines":    DATA_DIR / "static/lines.parquet",
    }

    for table, path in static_files.items():
        folder_id = DRIVE_FOLDER[table]
        if file_exists(service, path.name, folder_id):
            log.info(f"{path.name} already uploaded, skipping")
            continue
        upload_file(service, path, folder_id)


def upload_midnight() -> None:
    log.info("Uploading yesterday's dynamic files …")
    try:
        upload_yesterday_dynamic()
    except Exception as e:
        log.error(f"Dynamic upload failed: {e}")

    log.info("Checking static files …")
    try:
        upload_static_once()
    except Exception as e:
        log.error(f"Static upload failed: {e}")

    log.info("Upload complete")