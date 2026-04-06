"""MTScapes Fleet Dashboard — Flask backend."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from functools import wraps
import time as _time
import requests as http_requests
from dateutil import parser as dtparser
from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
import azuga_api
import models

# ── Report cache (shared across users) ────────────────────────────
_report_cache = {}

def _get_cached_report(key, ttl_seconds):
    """Return cached report data if still fresh, else None."""
    if key in _report_cache:
        cached_at, data = _report_cache[key]
        if _time.time() - cached_at < ttl_seconds:
            return data
    return None

def _set_cached_report(key, data):
    _report_cache[key] = (_time.time(), data)

app = Flask(__name__)
app.secret_key = "mtscapes-fleet-dashboard-2026"

models.init_db()

login_manager = LoginManager(app)
login_manager.login_view = "login"


@login_manager.user_loader
def load_user(user_id):
    return models.User.get_by_id(int(user_id))


@login_manager.unauthorized_handler
def unauthorized():
    if request.path.startswith("/api/") or request.path.startswith("/admin/api/"):
        return jsonify({"error": "Not authenticated"}), 401
    return redirect(url_for("login"))


def admin_required(f):
    """Decorator: must be logged in AND admin."""
    @wraps(f)
    @login_required
    def decorated(*args, **kwargs):
        if not current_user.is_admin:
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated


def _user_can_access_vehicle(vehicle_id):
    """Check if current user can access a specific vehicle."""
    if current_user.is_admin:
        return True
    allowed = models.get_allowed_vehicle_ids(current_user.id)
    return vehicle_id in (allowed or [])


def _extract_vehicle_list(data):
    """Extract vehicle list from Azuga response (handles both dict and list shapes)."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        vdata = data.get("data", {})
        if isinstance(vdata, dict):
            return vdata.get("result", [])
        if isinstance(vdata, list):
            return vdata
    return []


def _filter_vehicles_for_user(vehicle_list, id_key="trackeeId"):
    """Filter a vehicle list to only those the current user can access."""
    if current_user.is_admin:
        return vehicle_list
    allowed = set(models.get_allowed_vehicle_ids(current_user.id) or [])
    return [v for v in vehicle_list if v.get(id_key) in allowed]


# ── Auth Routes ───────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect("/")
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = models.User.get_by_username(username)
        if user and user.check_password(password):
            login_user(user, remember=True)
            return redirect("/")
        error = "Invalid username or password"
    return render_template("login.html", error=error)


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/login")


# ── Pages ──────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return render_template("dashboard.html",
                           user=current_user,
                           is_admin=current_user.is_admin)


@app.route("/admin")
@admin_required
def admin_page():
    return render_template("admin.html")


# ── Admin API ─────────────────────────────────────────────────────

@app.route("/admin/api/users")
@admin_required
def admin_get_users():
    users = models.get_all_users()
    for u in users:
        if u["is_admin"]:
            u["vehicle_count"] = "all"
        else:
            u["vehicle_count"] = len(models.get_user_vehicles(u["id"]))
    return jsonify(users)


@app.route("/admin/api/users", methods=["POST"])
@admin_required
def admin_create_user():
    data = request.get_json()
    try:
        uid = models.create_user(
            data["username"], data["password"],
            data.get("display_name", ""), data.get("is_admin", False)
        )
        return jsonify({"id": uid})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/admin/api/users/<int:user_id>", methods=["PUT"])
@admin_required
def admin_update_user(user_id):
    data = request.get_json()
    try:
        models.update_user(
            user_id,
            username=data.get("username"),
            display_name=data.get("display_name"),
            is_admin=data.get("is_admin"),
            password=data.get("password") or None,
        )
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/admin/api/users/<int:user_id>", methods=["DELETE"])
@admin_required
def admin_delete_user(user_id):
    models.delete_user(user_id)
    return jsonify({"ok": True})


@app.route("/admin/api/users/<int:user_id>/vehicles")
@admin_required
def admin_get_user_vehicles(user_id):
    return jsonify(models.get_user_vehicles(user_id))


@app.route("/admin/api/users/<int:user_id>/vehicles", methods=["PUT"])
@admin_required
def admin_set_user_vehicles(user_id):
    data = request.get_json()
    models.set_user_vehicles(user_id, data.get("vehicle_ids", []))
    return jsonify({"ok": True})


@app.route("/api/all-vehicles")
@admin_required
def api_all_vehicles():
    """Full vehicle list for admin vehicle assignment."""
    data = azuga_api.get_latest_locations()
    vlist = _extract_vehicle_list(data)
    result = []
    for v in vlist:
        result.append({
            "trackeeId": v.get("trackeeId", ""),
            "trackeeName": v.get("trackeeName", ""),
            "groupName": v.get("groupName", ""),
        })
    result.sort(key=lambda x: x.get("trackeeName", ""))
    return jsonify(result)


# ── API proxy routes ───────────────────────────────────────────────

@app.route("/api/vehicles")
@login_required
def api_vehicles():
    """Return all vehicles."""
    try:
        data = azuga_api.get_vehicles()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/healthz")
def healthz():
    """Health check / keep-alive endpoint (no auth required)."""
    return jsonify({"status": "ok"})


@app.route("/api/debug-locations")
def debug_locations():
    """Debug endpoint — test Azuga API connectivity (no auth)."""
    import traceback
    try:
        data = azuga_api.get_latest_locations()
        vlist = _extract_vehicle_list(data)
        return jsonify({"status": "ok", "vehicle_count": len(vlist),
                        "response_type": type(data).__name__})
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[DEBUG] locations error:\n{tb}")
        return jsonify({"status": "error", "error": str(e),
                        "traceback": tb.split("\n")[-4:]}), 500


@app.route("/api/locations")
@login_required
def api_locations():
    """Return latest GPS positions for all vehicles."""
    try:
        data = azuga_api.get_latest_locations()
        # Handle both dict and list responses from Azuga
        vlist = _extract_vehicle_list(data)
        filtered = _filter_vehicles_for_user(vlist)
        return jsonify({"data": {"result": filtered}})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/trips/<vehicle_id>")
@login_required
def api_trips(vehicle_id):
    """Derive trips from breadcrumb data (Azuga trip endpoint is broken)."""
    if not _user_can_access_vehicle(vehicle_id):
        return jsonify({"error": "Access denied"}), 403
    start = request.args.get("start", str(date.today()))
    end = request.args.get("end", str(date.today()))
    try:
        raw = azuga_api.get_breadcrumb(vehicle_id, start, end)
        points = _get_breadcrumb_points(raw)
        trips = _derive_trips(points)
        return jsonify(trips)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/breadcrumb/<vehicle_id>")
@login_required
def api_breadcrumb(vehicle_id):
    """Return breadcrumb trail for a vehicle."""
    if not _user_can_access_vehicle(vehicle_id):
        return jsonify({"error": "Access denied"}), 403
    start = request.args.get("start", str(date.today()))
    end = request.args.get("end", str(date.today()))
    try:
        data = azuga_api.get_breadcrumb(vehicle_id, start, end)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/geofence-report")
@login_required
def api_geofence_report():
    """Geofence dwell time report. Query: vehicle_id, start, end."""
    vehicle_id = request.args.get("vehicle_id")
    start = request.args.get("start", str(date.today()))
    end = request.args.get("end", str(date.today()))
    if not vehicle_id:
        return jsonify({"error": "vehicle_id required"}), 400
    if not _user_can_access_vehicle(vehicle_id):
        return jsonify({"error": "Access denied"}), 403
    try:
        data = azuga_api.get_geofence_report([vehicle_id], start, end)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stops/<vehicle_id>")
@login_required
def api_stops(vehicle_id):
    """Calculate stops with dwell time > 5 minutes from breadcrumb data."""
    if not _user_can_access_vehicle(vehicle_id):
        return jsonify({"error": "Access denied"}), 403
    start = request.args.get("start", str(date.today()))
    end = request.args.get("end", str(date.today()))
    try:
        raw = azuga_api.get_breadcrumb(vehicle_id, start, end)
        points = _get_breadcrumb_points(raw)
        stops = _extract_stops_from_points(points, min_dwell_minutes=5)
        return jsonify(stops)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/road-match", methods=["POST"])
@login_required
def api_road_match():
    """Snap GPS breadcrumb points to actual roads using OSRM map-matching.
    Accepts JSON body: { "points": [{"lat": ..., "lng": ...}, ...] }
    Returns a smooth road-following polyline.
    """
    data = request.get_json()
    points = data.get("points", [])
    if len(points) < 2:
        return jsonify({"coordinates": []})

    try:
        coords = _osrm_match(points)
        return jsonify({"coordinates": coords})
    except Exception as e:
        # Fall back to raw points if OSRM fails
        return jsonify({
            "coordinates": [[p["lat"], p["lng"]] for p in points],
            "fallback": True,
            "error": str(e),
        })


@app.route("/api/yard-departure")
@login_required
def api_yard_departure():
    """Yard departure analysis — how long each truck sits at the yard before leaving.
    Parallelized: fetches all truck/day combos concurrently (15 threads).
    Filters out named/manager trucks (only returns 'Truck XX' vehicles).
    """
    num_days = int(request.args.get("days", 5))

    # Check report cache first (5 minutes for admin, keyed by user for non-admin)
    cache_key = f"yard:{current_user.id}:{num_days}"
    cached = _get_cached_report(cache_key, 300)
    if cached:
        return jsonify(cached)

    try:
        # Get all vehicle IDs
        vraw = azuga_api.get_latest_locations()
        vlist = _extract_vehicle_list(vraw)
        vlist = _filter_vehicles_for_user(vlist)

        MAINT_GROUPS = {
            'Maintenance', "Pablo's Crews", "Elias' Crews", "Omar's Crews",
            "Carlos' Crews", "Bianca's Crews", "Jeremy's Crews", "Flower Crews",
        }

        def _get_division(group):
            if group == 'Install':
                return 'Install'
            if group in MAINT_GROUPS:
                return 'Maintenance'
            if group == 'Chemical':
                return 'Chemical'
            if group == 'Shop':
                return 'Shop'
            return 'Other'

        trucks = []
        for v in vlist:
            tid = v.get("trackeeId")
            name = v.get("trackeeName", "")
            if tid and name:
                # Pre-filter: only numbered trucks, skip managers and shop
                nl = name.lower()
                div = _get_division(v.get("groupName", ""))
                if div == "Shop":
                    continue
                if nl.startswith("truck ") or nl.startswith("ll truck"):
                    trucks.append({
                        "id": tid,
                        "name": name,
                        "division": div,
                    })

        # Collect weekdays
        days = []
        d = date.today() - timedelta(days=1)
        while len(days) < num_days:
            if d.weekday() < 5:
                days.append(d)
            d -= timedelta(days=1)

        # Build all (truck, day) fetch jobs
        jobs = []
        for day in days:
            for truck in trucks:
                jobs.append((truck, day))

        # Fetch breadcrumbs in parallel
        raw_data = {}  # (truck_name, day) -> points
        truck_divisions = {t["name"]: t["division"] for t in trucks}

        def _fetch(truck, day):
            day_str = str(day)
            try:
                raw = azuga_api.get_breadcrumb(truck["id"], day_str, day_str)
                return (truck["name"], day, _get_breadcrumb_points(raw))
            except Exception:
                return (truck["name"], day, [])

        with ThreadPoolExecutor(max_workers=15) as pool:
            futures = [pool.submit(_fetch, t, d) for t, d in jobs]
            for f in as_completed(futures):
                name, day, points = f.result()
                if points:
                    raw_data[(name, day)] = points

        # Dispatch locations — keyword matching + auto-detected coords
        YARDS = [
            {"name": "Cumming Shop", "keywords": ["6720", "matt hwy", "matt highway"],
             "lat": None, "lng": None},
            {"name": "Tyrone Shop", "keywords": ["dublin ct", "dublin court"],
             "lat": None, "lng": None},
            {"name": "Hoschton Shop", "keywords": ["amy industrial", "33 amy"],
             "lat": None, "lng": None},
        ]

        def _match_yard(address, plat, plng):
            """Return yard name if point is at a dispatch location, else None."""
            addr = (address or "").lower()
            for yard in YARDS:
                if any(kw in addr for kw in yard["keywords"]):
                    # Auto-detect coords on first address match
                    if yard["lat"] is None and plat and plng:
                        try:
                            yard["lat"], yard["lng"] = float(plat), float(plng)
                        except (TypeError, ValueError):
                            pass
                    return yard["name"]
            # Fall back to coord proximity for known yards
            if plat and plng:
                try:
                    flat, flng = float(plat), float(plng)
                    for yard in YARDS:
                        if yard["lat"] is not None:
                            if (abs(flat - yard["lat"]) < 0.003
                                    and abs(flng - yard["lng"]) < 0.003):
                                return yard["name"]
                except (TypeError, ValueError):
                    pass
            return None

        # Analyze mornings
        results = {}

        for (truck_name, day), points in raw_data.items():
            morning = []
            for pt in points:
                ts = pt.get("locationTimeInDTZ") or pt.get("locationTime")
                t = _parse_yard_time(ts)
                if t and 5 <= t.hour < 10:
                    # Skip heartbeat pings where engine is off
                    event = (pt.get("eventName") or "").lower()
                    if "ignition off" in event:
                        continue
                    morning.append((t, pt))
            morning.sort(key=lambda x: x[0])
            if not morning:
                continue

            # Find yard points and which location
            yard_times = []
            matched_yard = None
            for t, pt in morning:
                yname = _match_yard(
                    pt.get("address"), pt.get("latitude"), pt.get("longitude"))
                if yname:
                    yard_times.append(t)
                    if matched_yard is None:
                        matched_yard = yname
            if not yard_times:
                continue

            first = min(yard_times)
            last = max(yard_times)

            departure = None
            for t, pt in morning:
                if t <= last:
                    continue
                speed = float(pt.get("sog", 0) or 0)
                if speed >= 5:
                    departure = t
                    break
            if departure is None:
                departure = last

            dwell = (departure - first).total_seconds() / 60
            if dwell < 2:
                continue

            if truck_name not in results:
                results[truck_name] = []
            results[truck_name].append({
                "day": day.strftime("%a %m/%d"),
                "first_on": first.strftime("%I:%M %p"),
                "departed": departure.strftime("%I:%M %p"),
                "dwell_min": round(dwell, 1),
                "yard": matched_yard,
            })

        # Build response
        from collections import Counter
        report = []
        yard_totals = Counter()  # yard_name -> total avg minutes across trucks

        for name, days_data in results.items():
            avg = sum(d["dwell_min"] for d in days_data) / len(days_data)
            # Primary yard = most frequent
            yard_counts = Counter(d.get("yard", "Unknown") for d in days_data)
            primary_yard = yard_counts.most_common(1)[0][0]
            yard_totals[primary_yard] += avg

            report.append({
                "truck": name,
                "division": truck_divisions.get(name, "Other"),
                "yard": primary_yard,
                "avg_minutes": round(avg, 0),
                "days_seen": len(days_data),
                "daily": days_data,
            })

        report.sort(key=lambda x: -x["avg_minutes"])

        all_avgs = [r["avg_minutes"] for r in report]
        fleet_avg = round(sum(all_avgs) / len(all_avgs), 0) if all_avgs else 0
        total_wasted = round(sum(all_avgs), 0)

        # Per-yard breakdown
        yard_summary = []
        for yname, total_min in yard_totals.most_common():
            count = sum(1 for r in report if r["yard"] == yname)
            yard_summary.append({
                "name": yname,
                "truck_count": count,
                "total_avg_minutes": round(total_min, 0),
                "total_avg_hours": round(total_min / 60, 1),
            })

        result = {
            "trucks": report,
            "yards": yard_summary,
            "fleet_avg_minutes": fleet_avg,
            "total_crew_minutes": total_wasted,
            "total_crew_hours": round(total_wasted / 60, 1),
            "truck_count": len(report),
            "days_analyzed": [d.strftime("%a %m/%d") for d in days],
        }
        _set_cached_report(cache_key, result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/gas-parking-stops")
@login_required
def api_gas_parking_stops():
    """Gas station & parking lot stop report.
    Finds all stops at gas stations or parking lots across the fleet.
    """
    num_days = int(request.args.get("days", 5))

    # Check report cache first (5 minutes)
    cache_key = f"gas:{current_user.id}:{num_days}"
    cached = _get_cached_report(cache_key, 300)
    if cached:
        return jsonify(cached)

    try:
        vraw = azuga_api.get_latest_locations()
        vlist = _extract_vehicle_list(vraw)
        vlist = _filter_vehicles_for_user(vlist)

        MAINT_GROUPS = {
            'Maintenance', "Pablo's Crews", "Elias' Crews", "Omar's Crews",
            "Carlos' Crews", "Bianca's Crews", "Jeremy's Crews", "Flower Crews",
        }

        def _div(group):
            if group == 'Install': return 'Install'
            if group in MAINT_GROUPS: return 'Maintenance'
            if group == 'Chemical': return 'Chemical'
            if group == 'Shop': return 'Shop'
            return 'Other'

        trucks = []
        for v in vlist:
            tid = v.get("trackeeId")
            name = v.get("trackeeName", "")
            if tid and name:
                trucks.append({
                    "id": tid, "name": name,
                    "division": _div(v.get("groupName", "")),
                })

        # Collect weekdays
        days = []
        d = date.today() - timedelta(days=1)
        while len(days) < num_days:
            if d.weekday() < 5:
                days.append(d)
            d -= timedelta(days=1)

        # Parallel breadcrumb fetch
        raw_data = {}
        truck_meta = {t["name"]: t for t in trucks}

        def _fetch(truck, day):
            day_str = str(day)
            try:
                raw = azuga_api.get_breadcrumb(truck["id"], day_str, day_str)
                return (truck["name"], day, _get_breadcrumb_points(raw))
            except Exception:
                return (truck["name"], day, [])

        jobs = [(t, d) for d in days for t in trucks]
        with ThreadPoolExecutor(max_workers=15) as pool:
            futures = [pool.submit(_fetch, t, d) for t, d in jobs]
            for f in as_completed(futures):
                name, day, points = f.result()
                if points:
                    raw_data[(name, day)] = points

        # Extract ALL stops from all truck/day combos first
        all_raw_stops = []
        for (truck_name, day), points in raw_data.items():
            stops = _extract_stops_from_points(points, min_dwell_minutes=3)
            meta = truck_meta.get(truck_name, {})
            for s in stops:
                all_raw_stops.append({
                    "truck": truck_name,
                    "division": meta.get("division", "Other"),
                    "day": day.strftime("%a %m/%d"),
                    "day_sort": str(day),
                    "address": s.get("address", ""),
                    "arrival": s.get("arrival", ""),
                    "departure": s.get("departure", ""),
                    "dwell_minutes": s.get("dwell_minutes", 0),
                    "lat": s.get("lat"),
                    "lng": s.get("lng"),
                })

        # Classify stops using cached POI data from OpenStreetMap
        poi_list = _load_poi_cache()

        unique_locs = {}
        for s in all_raw_stops:
            if s["lat"] and s["lng"]:
                key = (round(float(s["lat"]), 4), round(float(s["lng"]), 4))
                if key not in unique_locs:
                    unique_locs[key] = None

        THRESHOLD = 0.001  # ~110m — no commercial properties in service area

        for loc_key in unique_locs:
            best = None
            best_dist = THRESHOLD
            for plat, plng, pcat, pname in poi_list:
                dist = max(abs(loc_key[0] - plat), abs(loc_key[1] - plng))
                if dist < best_dist:
                    best_dist = dist
                    best = (pcat, pname, round(dist * 111000))
            if best:
                unique_locs[loc_key] = best

        # Filter stops to only those near gas stations / parking lots
        matched_stops = []
        for s in all_raw_stops:
            if s["lat"] and s["lng"]:
                key = (round(float(s["lat"]), 4), round(float(s["lng"]), 4))
                match = unique_locs.get(key)
                if match:
                    cat, poi_name, dist_m = match
                    s["category"] = cat
                    s["poi_name"] = poi_name
                    s["distance_m"] = dist_m
                    matched_stops.append(s)

        # Deduplicate overlapping stops for the same truck+day
        # (truck near 2 POI nodes at same intersection → 2 stops with overlapping times)
        matched_stops.sort(key=lambda x: (x["truck"], x["day_sort"], x["arrival"]))
        all_stops = []
        for s in matched_stops:
            if all_stops and all_stops[-1]["truck"] == s["truck"] and all_stops[-1]["day_sort"] == s["day_sort"]:
                prev = all_stops[-1]
                # Check time overlap: if arrival of new is before departure of prev
                try:
                    prev_dep = _parse_ts_for_stops(prev["departure"])
                    this_arr = _parse_ts_for_stops(s["arrival"])
                    if prev_dep and this_arr and this_arr <= prev_dep:
                        # Merge: extend departure, keep longer dwell, prefer named POI
                        s_dep = _parse_ts_for_stops(s["departure"])
                        if s_dep and s_dep > prev_dep:
                            prev["departure"] = s["departure"]
                        prev["dwell_minutes"] = max(prev["dwell_minutes"], s["dwell_minutes"])
                        if not prev.get("poi_name") and s.get("poi_name"):
                            prev["poi_name"] = s["poi_name"]
                        continue
                except Exception:
                    pass
            all_stops.append(s)

        all_stops.sort(key=lambda x: (-x["dwell_minutes"]))

        # Summary stats
        gas_stops = [s for s in all_stops if s["category"] == "gas"]
        parking_stops = [s for s in all_stops if s["category"] == "parking"]
        total_gas_min = sum(s["dwell_minutes"] for s in gas_stops)
        total_parking_min = sum(s["dwell_minutes"] for s in parking_stops)

        result = {
            "stops": all_stops,
            "summary": {
                "gas_count": len(gas_stops),
                "parking_count": len(parking_stops),
                "total_gas_minutes": round(total_gas_min, 1),
                "total_parking_minutes": round(total_parking_min, 1),
                "total_gas_hours": round(total_gas_min / 60, 1),
                "total_parking_hours": round(total_parking_min / 60, 1),
            },
            "days_analyzed": [d.strftime("%a %m/%d") for d in days],
            "truck_count": len(set(s["truck"] for s in all_stops)),
        }
        _set_cached_report(cache_key, result)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


import json as _json
import os as _os

_POI_CACHE_FILE = _os.path.join(_os.path.dirname(__file__), "poi_cache.json")

# North Georgia bounding box covering MTScapes fleet area
_FLEET_BBOX = "33.70,-84.60,34.50,-83.50"


def _load_poi_cache():
    """Load POI cache from disk. Never auto-refreshes — use /api/refresh-poi-cache manually."""
    if _os.path.exists(_POI_CACHE_FILE):
        try:
            with open(_POI_CACHE_FILE, "r") as f:
                data = _json.load(f)
            age_hrs = (datetime.now().timestamp() - _os.path.getmtime(_POI_CACHE_FILE)) / 3600
            print(f"POI cache loaded: {len(data)} locations ({age_hrs:.0f}h old)")
            return [(d[0], d[1], d[2], d[3]) for d in data]
        except Exception as e:
            print(f"POI cache read error: {e}")

    # No cache exists at all — fetch fresh data
    return _refresh_poi_cache()


def _refresh_poi_cache():
    """Fetch gas stations and parking lots from Overpass and cache to disk."""
    poi_list = []
    bbox = _FLEET_BBOX

    OVERPASS_URLS = [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    ]

    def _query_overpass(query_str):
        """Try multiple Overpass mirrors."""
        for url in OVERPASS_URLS:
            try:
                resp = http_requests.post(url, data={"data": query_str}, timeout=60)
                if resp.status_code == 200 and resp.text.strip().startswith("{"):
                    return resp.json()
            except Exception:
                continue
        return None

    # Gas stations
    gas_q = f'[out:json][timeout:60];node["amenity"="fuel"]({bbox});out;'
    gas_data = _query_overpass(gas_q)
    if gas_data:
        for el in gas_data.get("elements", []):
            lat, lng = el.get("lat"), el.get("lon")
            name = el.get("tags", {}).get("name", "")
            if lat and lng:
                poi_list.append((float(lat), float(lng), "gas", name))
    print(f"Overpass: {len([p for p in poi_list if p[2]=='gas'])} gas stations")

    # Parking lots — only commercial (named, multi-storey, or fee-based)
    # Excludes residential parking which floods the results
    park_q = (
        f'[out:json][timeout:60];'
        f'(node["amenity"="parking"]["name"]({bbox});'
        f'way["amenity"="parking"]["name"]({bbox});'
        f'node["amenity"="parking"]["parking"="multi-storey"]({bbox});'
        f'way["amenity"="parking"]["parking"="multi-storey"]({bbox});'
        f'node["amenity"="parking"]["fee"="yes"]({bbox});'
        f'way["amenity"="parking"]["fee"="yes"]({bbox});'
        f');out center;'
    )
    park_data = _query_overpass(park_q)
    if park_data:
        for el in park_data.get("elements", []):
            lat = el.get("lat") or el.get("center", {}).get("lat")
            lng = el.get("lon") or el.get("center", {}).get("lon")
            name = el.get("tags", {}).get("name", "")
            if lat and lng:
                poi_list.append((float(lat), float(lng), "parking", name))
    print(f"Overpass: {len([p for p in poi_list if p[2]=='parking'])} parking lots")

    # Only save if we got BOTH gas and parking — don't wipe good data on API failure
    new_gas = len([p for p in poi_list if p[2] == "gas"])
    new_park = len([p for p in poi_list if p[2] == "parking"])

    if new_gas > 0 and new_park > 0:
        try:
            with open(_POI_CACHE_FILE, "w") as f:
                _json.dump(poi_list, f)
            print(f"POI cache saved: {len(poi_list)} locations ({new_gas} gas, {new_park} parking)")
        except Exception as e:
            print(f"POI cache write error: {e}")
    elif new_gas > 0 or new_park > 0:
        # Partial success — merge new data with existing cache
        print(f"Partial Overpass result ({new_gas} gas, {new_park} parking) — merging with existing cache")
        try:
            existing = []
            if os.path.exists(_POI_CACHE_FILE):
                with open(_POI_CACHE_FILE) as f:
                    existing = _json.load(f)
            # Keep categories we didn't get fresh data for
            if new_gas == 0:
                poi_list.extend([p for p in existing if p[2] == "gas"])
            if new_park == 0:
                poi_list.extend([p for p in existing if p[2] == "parking"])
            with open(_POI_CACHE_FILE, "w") as f:
                _json.dump(poi_list, f)
            print(f"POI cache merged: {len(poi_list)} locations")
        except Exception as e:
            print(f"POI cache merge error: {e}")
    else:
        print("Overpass returned nothing — keeping existing cache")

    return poi_list


@app.route("/api/refresh-poi-cache")
@admin_required
def api_refresh_poi_cache():
    """Force refresh the POI cache from Overpass."""
    try:
        pois = _refresh_poi_cache()
        gas = len([p for p in pois if p[2] == "gas"])
        park = len([p for p in pois if p[2] == "parking"])
        return jsonify({"status": "ok", "gas_stations": gas, "parking_lots": park})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _parse_yard_time(ts):
    """Parse Azuga DTZ timestamp as local time (already EDT, ignore offset)."""
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts)
        s = str(ts)
        if '+' in s:
            s = s[:s.rfind('+')]
        elif s.endswith('Z'):
            s = s[:-1]
        return dtparser.parse(s)
    except Exception:
        return None


def _osrm_match(points):
    """Send GPS points to OSRM match API and return road-snapped coordinates.
    OSRM has a ~100 coordinate limit, so we sample if needed.
    """
    # Sample down to ~100 points if there are too many
    max_pts = 100
    if len(points) > max_pts:
        step = len(points) / max_pts
        sampled = [points[int(i * step)] for i in range(max_pts)]
        # Always include last point
        sampled[-1] = points[-1]
        points = sampled

    # Build OSRM coordinates string: lng,lat;lng,lat;...
    coords_str = ";".join(f"{p['lng']},{p['lat']}" for p in points)

    # Radiuses: allow some GPS error tolerance (25m per point)
    radiuses = ";".join(["25"] * len(points))

    url = (
        f"https://router.project-osrm.org/match/v1/driving/{coords_str}"
        f"?overview=full&geometries=geojson&radiuses={radiuses}"
    )

    resp = http_requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != "Ok" or not data.get("matchings"):
        raise ValueError(f"OSRM match failed: {data.get('code', 'unknown')}")

    # Combine all matching segments into one polyline
    all_coords = []
    for matching in data["matchings"]:
        geom = matching.get("geometry", {})
        # GeoJSON coordinates are [lng, lat] — flip to [lat, lng] for Leaflet
        for coord in geom.get("coordinates", []):
            all_coords.append([coord[1], coord[0]])

    return all_coords


def _get_breadcrumb_points(raw):
    """Extract the points array from Azuga's nested breadcrumb response."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        data = raw.get("data", {})
        if isinstance(data, dict) and "result" in data:
            return data["result"]
        if isinstance(data, list):
            return data
    return []


def _derive_trips(points):
    """Group breadcrumb points by tripNumber to build trip summaries."""
    from collections import OrderedDict

    trips_map = OrderedDict()
    for pt in points:
        tn = pt.get("tripNumber", 0)
        if tn not in trips_map:
            trips_map[tn] = []
        trips_map[tn].append(pt)

    trips = []
    for tn, pts in trips_map.items():
        if len(pts) < 2:
            continue
        first = pts[0]
        last = pts[-1]

        # tripDistance is cumulative per trip — max value is total miles
        total_miles = max(
            (float(p.get("tripDistance") or 0) for p in pts),
            default=0,
        )

        # Get max speed
        max_speed = max((float(p.get("sog", 0) or 0) for p in pts), default=0)

        trips.append({
            "tripNumber": tn,
            "startAddress": first.get("address", ""),
            "endAddress": last.get("address", ""),
            "startTime": first.get("locationTimeInDTZ") or first.get("locationTime", ""),
            "endTime": last.get("locationTimeInDTZ") or last.get("locationTime", ""),
            "startLat": first.get("latitude"),
            "startLng": first.get("longitude"),
            "endLat": last.get("latitude"),
            "endLng": last.get("longitude"),
            "points": len(pts),
            "distance": round(total_miles, 1),
            "maxSpeed": round(max_speed),
            "breadcrumbs": [
                {"lat": p.get("latitude"), "lng": p.get("longitude")}
                for p in pts if p.get("latitude") and p.get("longitude")
            ],
        })

    return trips


def _extract_stops_from_points(points, min_dwell_minutes=5):
    """Find stops using LOCATION STICKINESS — if a truck stays within ~150m
    of the same spot for multiple pings, it's stopped, regardless of reported speed.

    Azuga GPS reports 5-10 mph "drift" while parked/creeping through a parking lot,
    so speed alone misses many real stops. Instead we track whether the truck is
    staying put geographically.

    Rules:
    - A stop starts when 2+ consecutive points are within MAX_DRIFT of each other
    - A stop continues as long as points stay within MAX_DRIFT of the stop's anchor
    - A stop breaks when a point moves beyond MAX_DRIFT OR there's a >30 min gap
    - Only high-speed points (>25 mph) immediately break a stop — low speed near
      the same spot is likely GPS noise or creeping in a lot
    """
    if not points:
        return []

    MAX_GAP_MINUTES = 30    # max time gap between pings to stay in same stop
    MAX_DRIFT = 0.0015      # ~165m — "same location" tolerance
    BREAK_SPEED = 25        # speed that definitely means driving away

    # Sort by timestamp — Azuga sometimes returns points out of order
    points = sorted(points, key=lambda p: str(
        p.get("locationTimeInDTZ") or p.get("locationTime", "")))

    stops = []
    current_stop = None      # confirmed stop (2+ pings at same spot)
    pending_stop = None      # candidate stop (1 ping, not yet confirmed)
    last_ts_parsed = None

    for pt in points:
        lat = pt.get("latitude")
        lng = pt.get("longitude")
        speed = float(pt.get("sog", 0) or 0)
        ts = pt.get("locationTimeInDTZ") or pt.get("locationTime", "")
        address = pt.get("address", "")

        if lat is None or lng is None or not ts:
            continue

        flat, flng = float(lat), float(lng)
        this_ts = _parse_ts_for_stops(ts)

        # ── Currently in a confirmed stop ──
        if current_stop is not None:
            drift = max(abs(flat - current_stop["lat"]),
                        abs(flng - current_stop["lng"]))
            gap_min = 999
            if last_ts_parsed and this_ts:
                gap_min = (this_ts - last_ts_parsed).total_seconds() / 60

            if drift < MAX_DRIFT and gap_min < MAX_GAP_MINUTES and speed < BREAK_SPEED:
                current_stop["departure"] = ts
                last_ts_parsed = this_ts
                if address and not current_stop["address"]:
                    current_stop["address"] = address
                continue

            # Break the stop
            dwell = _calc_dwell_minutes(current_stop["arrival"],
                                        current_stop["departure"])
            if dwell >= min_dwell_minutes:
                current_stop["dwell_minutes"] = round(dwell, 1)
                stops.append(current_stop)
            current_stop = None
            pending_stop = None
            last_ts_parsed = None

        # ── Have a pending candidate — check if this point confirms it ──
        if pending_stop is not None:
            drift = max(abs(flat - pending_stop["lat"]),
                        abs(flng - pending_stop["lng"]))
            gap_min = 999
            pend_ts = _parse_ts_for_stops(pending_stop["arrival"])
            if pend_ts and this_ts:
                gap_min = (this_ts - pend_ts).total_seconds() / 60

            if drift < MAX_DRIFT and gap_min < MAX_GAP_MINUTES and speed < BREAK_SPEED:
                # Confirmed — promote to real stop
                current_stop = pending_stop
                current_stop["departure"] = ts
                last_ts_parsed = this_ts
                if address and not current_stop["address"]:
                    current_stop["address"] = address
                pending_stop = None
                continue
            else:
                # Not confirmed — discard and maybe start new candidate
                pending_stop = None

        # ── Start a new candidate (requires confirmation from next ping) ──
        if current_stop is None and speed < BREAK_SPEED:
            pending_stop = {
                "lat": flat, "lng": flng,
                "address": address,
                "arrival": ts, "departure": ts,
            }

    # Close any remaining stop
    if current_stop is not None:
        dwell = _calc_dwell_minutes(current_stop["arrival"],
                                    current_stop["departure"])
        if dwell >= min_dwell_minutes:
            current_stop["dwell_minutes"] = round(dwell, 1)
            stops.append(current_stop)

    return stops


def _parse_ts_for_stops(ts):
    """Parse a timestamp for stop gap calculations."""
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts)
        s = str(ts)
        if '+' in s:
            s = s[:s.rfind('+')]
        elif s.endswith('Z'):
            s = s[:-1]
        return dtparser.parse(s)
    except Exception:
        return None


def _calc_dwell_minutes(arrival, departure):
    """Calculate minutes between two timestamps (epoch millis or ISO strings)."""
    from dateutil import parser as dtparser
    try:
        # Handle epoch milliseconds
        if isinstance(arrival, (int, float)) and arrival > 1_000_000_000_000:
            return (departure - arrival) / 60_000  # millis to minutes
        if isinstance(arrival, (int, float)) and arrival > 1_000_000_000:
            return (departure - arrival) / 60  # seconds to minutes
        # Handle ISO strings
        a = dtparser.parse(str(arrival))
        d = dtparser.parse(str(departure))
        return (d - a).total_seconds() / 60
    except Exception:
        return 0


if __name__ == "__main__":
    models.init_db()
    app.run(debug=True, port=5555)
