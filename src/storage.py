"""
storage.py: Parquet read/write layer.
- static data: single file per table, overwritten daily
- dynamic data: one file per table per day, new rows are appended, never overwritten
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional
import logging

log = logging.getLogger(__name__)

### DIRECTORY LAYOUT ####
#
# data/
#   static/
#     stations.parquet
#     lines.parquet
#   dynamic/
#     trains/
#       trains_2026_03_14.parquet
#       trains_2026_03_15.parquet
#     timetables/
#       timetables_2026_03_14.parquet
#       timetables_2026_03_15.parquet
#     jouneys/
#       journeys_2026_03_14.parquet
#       journeys_2026_03_15.parquet
#


class StorageManager:
    def __init__(self, base_dir: str = None):
        if base_dir is None:
            # always resolves to project/data regardless of where you run from
            base_dir = Path(__file__).parent.parent / "data"
        self.base = Path(base_dir)
        self.static_dir = self.base / "static"
        self.dynamic_dir = self.base / "dynamic"

        self.static_dir.mkdir(parents=True, exist_ok=True)
        for table in ("trains", "timetables", "journeys"):
            (self.dynamic_dir / table).mkdir(parents=True, exist_ok=True)

    # HELPERS

    def _today(self) -> str:
        return datetime.now().strftime("%Y_%m_%d")

    def _dynamic_path(self, table: str, date: Optional[str] = None) -> Path:
        date = date or self._today()
        return self.dynamic_dir / table / f"{table}_{date}.parquet"

    # STATIC

    def write_static(self, table: str, df: pd.DataFrame) -> None:
        """Overwrite the static table (called once per day by the scheduler)"""
        path = self.static_dir / f"{table}.parquet"
        df.to_parquet(path, index=False)
        log.info(f"[static] wrote {len(df)} rows to {path}")

    def read_static(self, table: str) -> Optional[pd.DataFrame]:
        """Read the static table (called by the API)"""
        path = self.static_dir / f"{table}.parquet"
        if not path.exists():
            return None
        return pd.read_parquet(path)

    # DYNAMIC

    def append_dynamic(
        self,
        table: str,
        df: pd.DataFrame,
        dedup_keys: Optional[list[str]] = None,
        date: Optional[str] = None,
    ) -> None:
        """
        Append rows to today's parquet file for `table`.
        If the file doesn't exist, it will be created.
        Optional dedup_keys: drop exact duplicates on those columns
        before writing (keeps last occurrence).
        """
        if df.empty:
            log.warning(f"[dynamic/{table}] no data to append")
            return
        path = self._dynamic_path(table, date)

        if path.exists():
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, df], ignore_index=True)
        else:
            combined = df.copy()

        if dedup_keys:
            before = len(combined)
            combined = combined.drop_duplicates(subset=dedup_keys, keep="last")
            dropped = before - len(combined)
            if dropped:
                log.debug(
                    f"[dynamic/{table}] dropped {dropped} duplicates based on keys {dedup_keys}"
                )

        combined.to_parquet(path, index=False)
        log.info(
            f"[dynamic/{table}] wrote {len(df)} rows to {path} (total {len(combined)})"
        )

    def read_dynamic(
        self, table: str, date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        path = self._dynamic_path(table, date)
        if not path.exists():
            log.warning(f"[dynamic/{table}] file {path} does not exist")
            return None
        return pd.read_parquet(path)

    def read_dynamic_range(
        self, table: str, start_date: str, end_date: str
        ) -> pd.DataFrame:
        """Read and concatenate daily files across a date range (inclusive)."""
        dates = pd.date_range(start_date, end_date, freq="D").strftime("%Y_%m_%d")
        frames = []
        for d in dates:
            df = self.read_dynamic(table, date=d)
            if df is not None:
                frames.append(df)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    
    def enrich_actuals(self, date: str = None) -> None:
        trains_df = self.read_dynamic("trains", date=date)
        timetables_df = self.read_dynamic("timetables", date=date)

        if trains_df is None or timetables_df is None:
            return

        # Latest observed delay per train_id — propagate to ALL its stops
        latest_delay = (
            trains_df
            .sort_values("timestamp")
            .groupby("train_id")["delay_minutes"]
            .last()
            .reset_index()
        )

        timetables_df = timetables_df.drop(columns=["delay_minutes"], errors="ignore")
        merged = timetables_df.merge(latest_delay, on="train_id", how="left")

        # Normalize planned times to datetime — handles both "2026-03-14 13:50:00"
        # and "2026-03-14T13:50:00" and "13:50:00" (time-only from timetable API)
        today = datetime.now().strftime("%Y-%m-%d")

        def normalize_dt(series: pd.Series) -> pd.Series:
            # treat "None", "nan", "NaT" as missing
            series = series.replace({"None": pd.NA, "nan": pd.NA, "NaT": pd.NA})
            time_only = series.str.match(r"^\d{2}:\d{2}", na=False)
            series = series.copy()
            series[time_only] = today + " " + series[time_only]
            return pd.to_datetime(series, errors="coerce")

        planned_dep = normalize_dt(merged["planned_departure"].astype(str))
        planned_arr = normalize_dt(merged["planned_arrival"].astype(str))

        fmt = "%Y-%m-%d %H:%M:%S"  # single output format for all datetime columns

        # actual = planned + delay for all trains with a known delay
        # covers on-time (0), early (-N), and delayed (+N) in one pass
        has_delay = merged["delay_minutes"].notna()
        delay_td = pd.to_timedelta(merged.loc[has_delay, "delay_minutes"], unit="m")

        dep_mask = has_delay & planned_dep.notna()
        merged.loc[dep_mask, "actual_departure"] = (
            (planned_dep[dep_mask] + delay_td[dep_mask])
            .dt.strftime(fmt)
        )

        arr_mask = has_delay & planned_arr.notna()
        merged.loc[arr_mask, "actual_arrival"] = (
            (planned_arr[arr_mask] + delay_td[arr_mask])
            .dt.strftime(fmt)
        )

        # Normalise planned columns to consistent format
        merged["planned_departure"] = planned_dep.dt.strftime(fmt).where(planned_dep.notna())
        merged["planned_arrival"] = planned_arr.dt.strftime(fmt).where(planned_arr.notna())

        merged = merged.drop(columns=["delay_minutes"])
        path = self._dynamic_path("timetables", date)
        merged.to_parquet(path, index=False)
        log.info(
            f"[enrich_actuals] filled actual_departure for {dep_mask.sum()} rows, "
            f"actual_arrival for {arr_mask.sum()} rows"
        )
    
    def enrich_positions(self, date: str = None) -> None:
        trains_df = self.read_dynamic("trains", date=date)
        trains_df = trains_df.drop(columns=["position_lat", "position_lon"], errors="ignore")
        stations_df = self.read_static("stations")

        if trains_df is None or stations_df is None:
            return

        coords = stations_df[["station_id", "latitude", "longitude"]]

        # Join current station coords
        trains_df = trains_df.merge(
            coords.rename(columns={"station_id": "current_station_id",
                                "latitude": "current_lat",
                                "longitude": "current_lon"}),
            on="current_station_id", how="left"
        )

        # Join next station coords
        trains_df = trains_df.merge(
            coords.rename(columns={"station_id": "next_station_id",
                                "latitude": "next_lat",
                                "longitude": "next_lon"}),
            on="next_station_id", how="left"
        )

        # Midpoint between current and next station as position estimate
        trains_df["position_lat"] = (
            (trains_df["current_lat"] + trains_df["next_lat"]) / 2
        )
        trains_df["position_lon"] = (
            (trains_df["current_lon"] + trains_df["next_lon"]) / 2
        )

        trains_df = trains_df.drop(
            columns=["current_lat", "current_lon", "next_lat", "next_lon"]
        )

        path = self._dynamic_path("trains", date)
        trains_df.to_parquet(path, index=False)
        log.info("[enrich_positions] estimated positions for trains")