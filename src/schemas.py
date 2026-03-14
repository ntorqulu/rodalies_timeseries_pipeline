"""
schemas.py — Canonical column definitions and row-builder functions.

Each `build_*` function accepts raw API dicts and returns a list of
clean dicts that match the agreed schema exactly.
"""

from datetime import datetime

def _now() -> str:
    """ISO datetime with space separator — consistent with API datetime format."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _normalize_dt(value: str | None) -> str | None:
    """
    Normalize any datetime string to 'YYYY-MM-DD HH:MM:SS'.
    Handles:
      - None / empty          → None
      - 'HH:MM:SS'            → prepend today's date
      - '2026-03-14T19:26:38' → replace T with space
      - '2026-03-14 19:26:38' → unchanged
    """
    if not value:
        return None
    value = str(value).replace("T", " ")
    if len(value) <= 8:  # bare time like "19:26:38"
        value = datetime.now().strftime("%Y-%m-%d") + " " + value
    return value


# ── Static ────────────────────────────────────────────────────────────────────


def build_station_rows(api_response: dict) -> list[dict]:
    """
    Source: GET /stations  →  response["included"]

    Columns
    -------
    station_id   str
    name         str
    accessible   bool
    latitude     float
    longitude    float
    connections  int    number of connecting lines/services
    services     int    number of on-site services (toilets, etc.)
    last_updated str    ISO datetime of fetch
    """
    rows = []
    for s in api_response.get("included", []):
        rows.append(
            {
                "station_id": str(s["id"]),
                "name": s.get("name", ""),
                "accessible": bool(s.get("accessible", False)),
                "latitude": float(s.get("latitude") or 0),
                "longitude": float(s.get("longitude") or 0),
                "connections": len(s.get("connections", [])),
                "services": len(s.get("services", [])),
                "last_updated": _now(),
            }
        )
    return rows


def build_line_rows(api_response: dict) -> list[dict]:
    """
    Source: GET /lines  →  response["included"]

    Columns
    -------
    line_id         str
    name            str
    origin          str   origin station name
    destination     str   destination station name
    stations_count  int
    last_updated    str
    """
    rows = []
    for l in api_response.get("included", []):
        rows.append(
            {
                "line_id": str(l["id"]),
                "name": l.get("name", ""),
                "origin": l.get("originStation", {}).get("name", ""),
                "destination": l.get("destinationStation", {}).get("name", ""),
                "stations_count": len(l.get("stations", [])),
                "last_updated": _now(),
            }
        )
    return rows


# ── Dynamic ───────────────────────────────────────────────────────────────────


def build_train_rows(departures_response: dict, station_id: str) -> list[dict]:
    """
    Source: GET /departures  →  response["trains"]

    One row = one real-time train observation at a station.

    Columns
    -------
    train_id            str
    line_id             str
    current_station_id  str   station where this observation was recorded
    next_station_id     str
    status              str   on_time | delayed | cancelled
    delay_minutes       int
    timestamp           str   ISO datetime of this observation
    """
    rows = []
    ts = _now()
    for t in departures_response.get("trains", []):
        delay = int(t.get("delay") or 0)

        if t.get("trainCancelled"):
            status = "cancelled"
        elif delay > 0:
            status = "delayed"
        elif delay < 0:
            status = "early"
        else:
            status = "on_time"

        rows.append(
            {
                "train_id": str(t.get("technicalNumber", "")),
                "line_id": t.get("line", {}).get("id", ""),
                "current_station_id": str(station_id),
                "next_station_id": str(t.get("nextStation", {}).get("id", "")),
                "status": status,
                "delay_minutes": delay,
                "timestamp": ts,
            }
        )
    return rows


def build_timetable_rows(
    departures_response: dict,
    station_id: str,
) -> list[dict]:
    """
    Source: GET /departures  →  response["trains"][*].stations

    One row = one scheduled stop (planned + actual times when available).

    Columns
    -------
    train_id            str
    station_id          str
    planned_arrival     str | None
    planned_departure   str | None
    actual_arrival      str | None
    actual_departure    str | None
    timestamp           str          record creation time
    """
    rows = []
    ts = _now()
    for t in departures_response.get("trains", []):
        train_id = str(t.get("technicalNumber", ""))

        # The departure at the queried station is the most reliable data point
        sched_dep = _normalize_dt(t.get("departureDateHourSelectedStation"))

        rows.append(
            {
                "train_id": train_id,
                "station_id": str(station_id),
                "planned_arrival": None,  # /departures does not give arrival
                "planned_departure": sched_dep,
                "actual_arrival": None,
                "actual_departure": None,
                "timestamp": ts,
            }
        )

        # Full stop list (all stations the train passes through)
        for stop in t.get("stations", []):
            rows.append(
                {
                    "train_id": train_id,
                    "station_id": str(stop.get("id", "")),
                    "planned_arrival": _normalize_dt(stop.get("arrivalDateHour")),
                    "planned_departure": _normalize_dt(stop.get("departureDateHour")),
                    "actual_arrival": None,
                    "actual_departure": None,
                    "timestamp": ts,
                }
            )

    return rows


def build_journey_rows(
    timetable_response: dict,
    origin_id: str,
    destination_id: str,
) -> list[dict]:
    """
    Source: GET /timetables  →  response["result"]["items"]

    One row = one schedulable journey between an O/D pair.

    Columns
    -------
    journey_id            str   "{train_id}_{origin}_{dest}_{departure}"
    train_id              str   first train in the journey
    origin_station_id     str
    destination_station_id str
    departure_time        str   ISO / HH:MM:SS from API
    arrival_time          str
    duration              str   HH:MM:SS
    accessible            bool
    steps                 int   number of stops/transfers
    timestamp             str
    """
    rows = []
    ts = _now()
    items = timetable_response.get("result", {}).get("items", [])

    for item in items:
        first_step = item.get("steps", [{}])[0]
        train_id = str(first_step.get("train", {}).get("id", ""))
        departure = item.get("departsAtOrigin", "")

        journey_id = f"{train_id}_{origin_id}_{destination_id}_{departure}"

        rows.append(
            {
                "journey_id": journey_id,
                "train_id": train_id,
                "origin_station_id": str(origin_id),
                "destination_station_id": str(destination_id),
                "departure_time": departure,
                "arrival_time": item.get("arrivesAtDestination"),
                "duration": item.get("duration"),
                "accessible": bool(item.get("globalAccessibility", False)),
                "steps": len(item.get("steps", [])),
                "timestamp": ts,
            }
        )

    return rows