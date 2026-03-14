import pandas as pd

# ----- Normalize API JSON to DataFrame -----
def normalize_stations(json_data):
    if not json_data or "included" not in json_data:
        return pd.DataFrame()
    return pd.json_normalize(json_data["included"])

def normalize_lines(json_data):
    if not json_data or "included" not in json_data:
        return pd.DataFrame()
    return pd.json_normalize(json_data["included"], sep="_")

def normalize_trains(json_data):
    if not json_data:
        return pd.DataFrame()
    if "train" in json_data:
        return pd.json_normalize(json_data["train"])
    if "included" in json_data:
        return pd.json_normalize(json_data["included"])
    return pd.DataFrame()

def normalize_timetables(json_data):
    if not json_data:
        return pd.DataFrame()
    if "result" in json_data and "items" in json_data["result"]:
        return pd.json_normalize(json_data["result"]["items"], sep="_")
    if "included" in json_data:
        return pd.json_normalize(json_data["included"])
    return pd.DataFrame()

# ----- Extract map / geo data -----
def extract_station_map(df_stations):
    if "attributes.latitude" in df_stations and "attributes.longitude" in df_stations:
        return df_stations[["id", "attributes.latitude", "attributes.longitude"]]
    return pd.DataFrame()

def extract_line_map(df_lines):
    if "attributes.stations" in df_lines:
        df_map = df_lines[["id", "attributes.stations"]].explode("attributes.stations")
        df_map = df_map.rename(columns={"id": "line_id", "attributes.stations": "station_id"})
        return df_map
    return pd.DataFrame()