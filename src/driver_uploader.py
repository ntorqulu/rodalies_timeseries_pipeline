import logging
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

log = logging.getLogger(__name__)

_ROOT            = Path(__file__).parent.parent
CREDENTIALS_PATH = _ROOT / "credentials.json"
TOKEN_PATH       = _ROOT / "token.json"


def get_service():
    creds = None

    # Load existing token
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    # Refresh if expired, or run OAuth flow if no token exists
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log.info("[drive] refreshing expired token …")
            creds.refresh(Request())
        else:
            if not CREDENTIALS_PATH.exists():
                raise FileNotFoundError(f"credentials.json not found at {CREDENTIALS_PATH}")
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)

        # Save updated token for next run
        TOKEN_PATH.write_text(creds.to_json())
        log.info(f"[drive] token saved to {TOKEN_PATH}")

    return build("drive", "v3", credentials=creds)


def file_exists(service, name: str, folder_id: str) -> str | None:
    """Return file ID if file exists in folder, else None."""
    query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id,name)").execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


def upload_file(service, filepath: Path, folder_id: str) -> None:
    """Upload file to Drive folder. Updates existing file if already present."""
    if not filepath.exists():
        log.warning(f"[drive] {filepath} does not exist, skipping")
        return

    try:
        media = MediaFileUpload(
            str(filepath),
            mimetype="application/octet-stream",
            resumable=True,
        )

        existing_id = file_exists(service, filepath.name, folder_id)

        if existing_id:
            file = service.files().update(
                fileId=existing_id,
                media_body=media,
            ).execute()
            log.info(f"[drive] updated {filepath.name} → id={file.get('id')}")
        else:
            file_metadata = {
                "name": filepath.name,
                "parents": [folder_id],
            }
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id",
            ).execute()
            log.info(f"[drive] uploaded {filepath.name} → id={file.get('id')}")

    except HttpError as e:
        log.error(f"[drive] HTTP error uploading {filepath.name}: {e}")
    except Exception as e:
        log.error(f"[drive] failed to upload {filepath.name}: {e}")


if __name__ == "__main__":
    from upload_daily import upload_midnight
    upload_midnight()