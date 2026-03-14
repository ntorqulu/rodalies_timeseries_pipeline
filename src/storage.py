import os
import logging
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def save_parquet(df, category, data_dir):
    os.makedirs(os.path.join(data_dir, category), exist_ok=True)
    today = datetime.today().strftime("%Y-%m-%d")
    now = datetime.now().strftime("%H-%M-%S")
    filename = os.path.join(data_dir, category, f"{category}_{today}_{now}.parquet")
    df.to_parquet(filename, index=False)
    logging.info(f"Saved {category} parquet: {filename}")
    return filename

def google_drive_service():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    service = build("drive", "v3", credentials=creds)
    return service

def upload_to_drive(local_file, folder_id):
    service = google_drive_service()
    file_metadata = {"name": os.path.basename(local_file), "parents": [folder_id]}
    media = MediaFileUpload(local_file, mimetype="application/octet-stream")
    file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    logging.info(f"Uploaded {local_file} to Drive (file ID: {file['id']})")
