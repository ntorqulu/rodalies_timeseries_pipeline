import logging
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

log = logging.getLogger(__name__)

def get_service():
    creds = Credentials.from_authorized_user_file("credentials.json", SCOPES)
    return build("drive", "v3", credentials=creds)

def upload_file(service, filepath: Path, folder_id: str):
    if not filepath.exists():
        log.warning(f"{filepath} does not exist")
        return

    file_metadata = {
        "name": filepath.name,
        "parents": [folder_id],
    }

    media = MediaFileUpload(str(filepath), resumable=True)

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id",
    ).execute()

    log.info(f"Uploaded {filepath} → id={file.get('id')}")