import time
import schedule
import logging
from pathlib import Path
import yaml
from src import pipeline

# Load config
config_path = Path(__file__).parent.parent / "config/config.yaml"
with open(config_path) as f:
    config = yaml.safe_load(f)

DATA_DIR = config["data_dir"]
LOG_DIR = config["log_dir"]
GDRIVE_FOLDERS = config["gdrive_folder_ids"]

logging.basicConfig(filename=f"{LOG_DIR}/pipeline.log",
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# --- Schedule snapshots every 30s for frequent updates ---
schedule.every(30).seconds.do(pipeline.run_snapshot, DATA_DIR, GDRIVE_FOLDERS)

# --- Schedule nightly upload at 23:59 ---
schedule.every().day.at("23:59").do(pipeline.run_drive_upload, DATA_DIR, GDRIVE_FOLDERS)

logging.info("Scheduler started")

while True:
    schedule.run_pending()
    time.sleep(1)