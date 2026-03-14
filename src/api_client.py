import requests
import logging
import yaml
from pathlib import Path

# Load config
config_path = Path(__file__).parent.parent / "config/config.yaml"
with open(config_path) as f:
    config = yaml.safe_load(f)

API_BASE = config["api_base"]


def fetch_api(endpoint, params=None):
    url = f"{API_BASE}/{endpoint}"
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching {endpoint}: {e}")
        return None
