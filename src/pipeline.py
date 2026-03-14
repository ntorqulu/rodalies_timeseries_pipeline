import logging
from src import api_client, processing, storage
import pandas as pd

def run_snapshot(DATA_DIR, GDRIVE_FOLDERS):
    logging.info("Pipeline snapshot started")

    # ----- Immutable data (once a day) -----
    stations_json = api_client.fetch_api("stations", params={"limit":500,"page":0})
    df_stations = processing.normalize_stations(stations_json)
    if not df_stations.empty:
        storage.save_parquet(df_stations, "stations", DATA_DIR)
        station_map = processing.extract_station_map(df_stations)
        if not station_map.empty:
            storage.save_parquet(station_map, "maps_stations", DATA_DIR)

    lines_json = api_client.fetch_api("lines", params={"type":"RODALIES","limit":500,"page":0})
    df_lines = processing.normalize_lines(lines_json)
    if not df_lines.empty:
        storage.save_parquet(df_lines, "lines", DATA_DIR)
        line_map = processing.extract_line_map(df_lines)
        if not line_map.empty:
            storage.save_parquet(line_map, "maps_lines", DATA_DIR)

    # ----- Frequent updates (every 30s) -----
    if not df_lines.empty:
        trains_list = []
        timetables_list = []

        for line_id in df_lines["id"]:
            # Fetch trains for this line
            trains_json = api_client.fetch_api("trains", params={"line": line_id})
            df_trains = processing.normalize_trains(trains_json)
            if not df_trains.empty:
                trains_list.append(df_trains)

            # Fetch timetables for this line
            timetables_json = api_client.fetch_api("timetables", params={"line": line_id})
            df_tt = processing.normalize_timetables(timetables_json)
            if not df_tt.empty:
                timetables_list.append(df_tt)

        # Combine all lines
        if trains_list:
            df_all_trains = pd.concat(trains_list, ignore_index=True)
            storage.save_parquet(df_all_trains, "trains", DATA_DIR)

        if timetables_list:
            df_all_tt = pd.concat(timetables_list, ignore_index=True)
            storage.save_parquet(df_all_tt, "timetables", DATA_DIR)

    logging.info("Pipeline snapshot finished")