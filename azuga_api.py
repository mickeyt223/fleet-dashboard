"""Azuga Fleet API v3 client.

Endpoints discovered from https://developer.azuga.com/reference/:
- Vehicles/locations: https://services.azuga.com/azuga-ws-oauth/v3/
- Reports (trips, breadcrumb): https://services.azuga.com/reports/v3/reports/
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

AZUGA_AUTH_URL = os.getenv("AZUGA_AUTH_URL")
AZUGA_USERNAME = os.getenv("AZUGA_USERNAME")
AZUGA_PASSWORD = os.getenv("AZUGA_PASSWORD")
AZUGA_CLIENT_ID = os.getenv("AZUGA_CLIENT_ID")

# Two different base URLs for different API groups
VEHICLES_BASE = "https://services.azuga.com/azuga-ws-oauth/v3"
REPORTS_BASE = "https://services.azuga.com/reports/v3/reports"

_token_cache = {"token": None, "expires_at": 0}


def authenticate():
    """Get a Bearer token from Azuga. Caches until expiry."""
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]

    print(f"[Azuga] Authenticating to {AZUGA_AUTH_URL} ...")
    resp = requests.post(
        AZUGA_AUTH_URL,
        json={
            "userName": AZUGA_USERNAME,
            "password": AZUGA_PASSWORD,
            "clientId": AZUGA_CLIENT_ID,
        },
        timeout=15,
    )
    print(f"[Azuga] Auth response: {resp.status_code}")
    resp.raise_for_status()
    data = resp.json()
    print(f"[Azuga] Auth response type: {type(data).__name__}")

    # Azuga nests the token under "data" — handle both dict and list
    if isinstance(data, list):
        # Some Azuga endpoints return a list; try first element
        if data and isinstance(data[0], dict):
            inner = data[0]
        else:
            raise ValueError(f"Auth returned unexpected list: {str(data)[:200]}")
    elif isinstance(data, dict):
        inner = data.get("data", data)
        # If inner is also a dict, use it; otherwise fall back to data
        if not isinstance(inner, dict):
            inner = data
    else:
        raise ValueError(f"Auth returned unexpected type {type(data)}: {str(data)[:200]}")

    token = (
        inner.get("access_token")
        or inner.get("accessToken")
        or inner.get("token")
        or inner.get("Token")
    )
    if not token:
        raise ValueError(f"Could not extract token from auth response: {str(data)[:300]}")

    # Token expires_in is ~180 days, but cache for 23 hours to be safe
    _token_cache["token"] = token
    _token_cache["expires_at"] = now + 23 * 60 * 60
    return token


def _headers():
    return {"Authorization": f"Bearer {authenticate()}", "Content-Type": "application/json"}


def _retry_on_401(make_request):
    """Execute a request, retry on 401 (re-auth) and 429 (rate limit with backoff)."""
    MAX_RETRIES = 4
    for attempt in range(MAX_RETRIES):
        resp = make_request()
        if resp.status_code == 401 and attempt == 0:
            _token_cache["token"] = None
            _token_cache["expires_at"] = 0
            continue
        if resp.status_code == 429:
            wait = min(2 ** attempt * 3, 30)  # 3s, 6s, 12s, 24s
            print(f"Azuga 429 rate limit — waiting {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return resp.json()


# ── Response Cache ────────────────────────────────────────────────
_response_cache = {}

def _cached(key, ttl_seconds, fetch_fn):
    """Return cached result if fresh, otherwise call fetch_fn and cache it."""
    now = time.time()
    if key in _response_cache:
        cached_at, data = _response_cache[key]
        if now - cached_at < ttl_seconds:
            return data
    data = fetch_fn()
    _response_cache[key] = (now, data)
    return data


# ── Vehicle endpoints ──────────────────────────────────────────────

def get_latest_locations():
    """Get latest GPS location for all vehicles.
    POST https://services.azuga.com/azuga-ws-oauth/v3/vehicles/latestlocation
    Cached for 30 seconds — shared across all users.
    """
    def _fetch():
        url = f"{VEHICLES_BASE}/vehicles/latestlocation"
        print(f"[Azuga] Fetching latest locations ...")
        result = _retry_on_401(
            lambda: requests.post(url, headers=_headers(), json={}, timeout=30)
        )
        if isinstance(result, dict):
            inner = result.get("data", {})
            if isinstance(inner, dict):
                vcount = len(inner.get("result", []))
            elif isinstance(inner, list):
                vcount = len(inner)
            else:
                vcount = "?"
        elif isinstance(result, list):
            vcount = len(result)
        else:
            vcount = "?"
        print(f"[Azuga] Got {vcount} vehicles")
        return result
    return _cached("latest_locations", 60, _fetch)  # 60s cache


def get_vehicles():
    """Get all vehicles (alias for latest locations since that includes vehicle info)."""
    return get_latest_locations()


# ── Reports ────────────────────────────────────────────────────────

def get_breadcrumb(vehicle_id, start_date, end_date):
    """Get breadcrumb trail (historical GPS points) for a vehicle.
    POST https://services.azuga.com/reports/v3/reports/breadcrumb
    Cached for 2 minutes per vehicle+date combo.
    """
    cache_key = f"bc:{vehicle_id}:{start_date}:{end_date}"

    def _fetch():
        url = f"{REPORTS_BASE}/breadcrumb"
        payload = {
            "startDate": _to_iso(start_date, start_of_day=True),
            "endDate": _to_iso(end_date, start_of_day=False),
            "browserTimezone": "US/Eastern",
            "filter": {
                "orFilter": {
                    "vehicleId": [vehicle_id]
                }
            },
            "index": 0,
            "size": 5000,
            "desc": False,
        }
        return _retry_on_401(
            lambda: requests.post(url, headers=_headers(), json=payload, timeout=60)
        )
    return _cached(cache_key, 300, _fetch)  # 5 min cache per vehicle+date


def get_trips(vehicle_id, start_date, end_date):
    """Get trip report for a vehicle.
    POST https://services.azuga.com/reports/v3/reports/trip
    """
    url = f"{REPORTS_BASE}/trip"
    payload = {
        "startDate": _to_iso(start_date, start_of_day=True),
        "endDate": _to_iso(end_date, start_of_day=False),
        "browserTimezone": "US/Eastern",
        "filter": {
            "orFilter": {
                "vehicleId": [vehicle_id]
            }
        },
        "index": 0,
        "size": 500,
        "desc": False,
    }
    return _retry_on_401(
        lambda: requests.post(url, headers=_headers(), json=payload, timeout=60)
    )


def get_alerts_report(vehicle_ids=None, start_date=None, end_date=None):
    """Get alerts report.
    POST https://services.azuga.com/reports/v3/reports/allalertreport
    """
    url = f"{REPORTS_BASE}/allalertreport"
    payload = {
        "startDate": _to_iso(start_date, start_of_day=True),
        "endDate": _to_iso(end_date, start_of_day=False),
        "browserTimezone": "US/Eastern",
        "index": 0,
        "size": 500,
    }
    if vehicle_ids:
        payload["filter"] = {"orFilter": {"vehicleId": vehicle_ids}}
    return _retry_on_401(
        lambda: requests.post(url, headers=_headers(), json=payload, timeout=60)
    )


# ── Helpers ────────────────────────────────────────────────────────

def _to_iso(date_str, start_of_day=True):
    """Convert YYYY-MM-DD to ISO 8601 timestamp."""
    if "T" in str(date_str):
        return date_str  # Already ISO
    if start_of_day:
        return f"{date_str}T00:00:00.000Z"
    else:
        return f"{date_str}T23:59:59.999Z"
