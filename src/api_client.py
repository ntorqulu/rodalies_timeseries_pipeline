import requests
from config import BASE_URL, LANG, TIMEOUT

_session = requests.Session()
_session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
})

def get_stations(limit=500, page=0):
    url = f"{BASE_URL}/stations"
    params = {
        "limit": limit,
        "page": page,
        "lang": LANG
    }
    r = _session.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def get_lines(limit=500, page=0):
    url = f"{BASE_URL}/lines"
    params = {
        "type": "RODALIES",
        "limit": limit,
        "page": page,
        "lang": LANG
    }
    r = _session.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def get_timetable(origin, destination):
    url = f"{BASE_URL}/timetables"
    params = {
        "originStationId": origin,
        "destinationStationId": destination,
        "fullResponse": "true",
        "lang": LANG
    }
    r = _session.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def get_train(train_id):
    url = f"{BASE_URL}/trains/{train_id}"
    params = {
        "fullResponse": "true",
        "lang": LANG,
    }
    r = _session.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_departures(station_id, hour=None, minute=120):
    url = f"{BASE_URL}/departures"
    params = {
        "stationId": station_id,
        "minute": minute,
        "fullResponse": "true",
        "lang": LANG
    }
    if hour:
        params["hour"] = hour
    r = _session.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()