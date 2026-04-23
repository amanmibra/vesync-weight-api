import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

# --- Config ---

CLIENT_ID = os.environ.get("WITHINGS_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("WITHINGS_CLIENT_SECRET", "")
REDIRECT_URI = os.environ.get("WITHINGS_REDIRECT_URI", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
TZ = ZoneInfo("America/Los_Angeles")

WITHINGS_AUTH_URL = "https://account.withings.com/oauth2_user/authorize2"
WITHINGS_TOKEN_URL = "https://wbsapi.withings.net/v2/oauth2"
WITHINGS_MEASURE_URL = "https://wbsapi.withings.net/measure"

MEAS_TYPES = {
    1: "weight_kg",
    5: "fat_free_mass_kg",
    6: "fat_ratio_pct",
    8: "fat_mass_kg",
    76: "muscle_mass_kg",
    77: "hydration_kg",
    88: "bone_mass_kg",
}

_tokens = {
    "access_token": "",
    "refresh_token": os.environ.get("WITHINGS_REFRESH_TOKEN", ""),
    "expires_at": 0,
}

# --- Database ---


def get_db():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS measurements (
                    id SERIAL PRIMARY KEY,
                    measured_at TIMESTAMPTZ NOT NULL,
                    weight_kg REAL,
                    weight_lb REAL,
                    fat_ratio_pct REAL,
                    fat_mass_kg REAL,
                    fat_free_mass_kg REAL,
                    muscle_mass_kg REAL,
                    bone_mass_kg REAL,
                    hydration_kg REAL,
                    device_id TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(measured_at)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_measurements_measured_at
                ON measurements(measured_at DESC)
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS withings_auth (
                    id INT PRIMARY KEY DEFAULT 1,
                    refresh_token TEXT NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    CHECK (id = 1)
                )
            """)
        conn.commit()


def _load_refresh_token() -> str:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_token FROM withings_auth WHERE id = 1")
            row = cur.fetchone()
    return row[0] if row else ""


def _save_refresh_token(token: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO withings_auth (id, refresh_token, updated_at)
                VALUES (1, %s, NOW())
                ON CONFLICT (id) DO UPDATE
                SET refresh_token = EXCLUDED.refresh_token,
                    updated_at = NOW()
            """, (token,))
        conn.commit()


def store_measurement(data: dict):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO measurements
                    (measured_at, weight_kg, weight_lb, fat_ratio_pct, fat_mass_kg,
                     fat_free_mass_kg, muscle_mass_kg, bone_mass_kg, hydration_kg, device_id)
                VALUES (%(measured_at)s, %(weight_kg)s, %(weight_lb)s, %(fat_ratio_pct)s,
                        %(fat_mass_kg)s, %(fat_free_mass_kg)s, %(muscle_mass_kg)s,
                        %(bone_mass_kg)s, %(hydration_kg)s, %(device_id)s)
                ON CONFLICT (measured_at) DO NOTHING
            """, data)
        conn.commit()


# --- Withings API ---


class WithingsAuthError(Exception):
    """Raised when Withings authentication fails and re-auth is required."""


def _refresh_access_token():
    if not _tokens["refresh_token"]:
        print("[auth] No refresh token available")
        return False
    resp = requests.post(WITHINGS_TOKEN_URL, data={
        "action": "requesttoken",
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": _tokens["refresh_token"],
    }, timeout=15)
    data = resp.json()
    if data.get("status") != 0:
        print(f"[auth] Token refresh failed: {data}")
        return False
    body = data["body"]
    _tokens["access_token"] = body["access_token"]
    _tokens["refresh_token"] = body["refresh_token"]
    _tokens["expires_at"] = time.time() + body.get("expires_in", 10800)
    try:
        _save_refresh_token(body["refresh_token"])
    except Exception as e:
        print(f"[auth] Failed to persist refresh token: {e}")
    return True


def _get_access_token() -> str:
    if _tokens["access_token"] and time.time() < _tokens["expires_at"] - 60:
        return _tokens["access_token"]
    if _refresh_access_token():
        return _tokens["access_token"]
    raise WithingsAuthError("Withings authentication expired — visit /auth to re-authenticate")


def _parse_measure(value: int, unit: int) -> float:
    return round(value * (10 ** unit), 2)


def fetch_withings_measurements(startdate: int = 0, enddate: int = 0) -> list[dict]:
    """Fetch measurements from Withings API and return parsed list.
    Raises WithingsAuthError if the refresh token is invalid."""
    access_token = _get_access_token()

    meastypes = ",".join(str(t) for t in MEAS_TYPES.keys())
    params = {"action": "getmeas", "meastypes": meastypes, "category": 1}
    if startdate:
        params["startdate"] = startdate
    if enddate:
        params["enddate"] = enddate

    resp = requests.post(WITHINGS_MEASURE_URL, headers={
        "Authorization": f"Bearer {access_token}",
    }, data=params, timeout=15)

    data = resp.json()
    if data.get("status") in (401, 403):
        _tokens["access_token"] = ""
        raise WithingsAuthError(f"Withings API auth error: {data}")
    if data.get("status") != 0:
        return []

    results = []
    for grp in data.get("body", {}).get("measuregrps", []):
        row = {
            "measured_at": datetime.fromtimestamp(grp["date"], tz=timezone.utc),
            "device_id": grp.get("deviceid"),
            "weight_kg": None, "weight_lb": None, "fat_ratio_pct": None,
            "fat_mass_kg": None, "fat_free_mass_kg": None, "muscle_mass_kg": None,
            "bone_mass_kg": None, "hydration_kg": None,
        }
        for m in grp.get("measures", []):
            mtype = m.get("type")
            if mtype in MEAS_TYPES:
                row[MEAS_TYPES[mtype]] = _parse_measure(m["value"], m["unit"])
        if row["weight_kg"]:
            row["weight_lb"] = round(row["weight_kg"] * 2.20462, 2)
        results.append(row)
    return results


def _latest_measured_at_ts() -> int | None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(measured_at) FROM measurements")
            row = cur.fetchone()
    if not row or not row[0]:
        return None
    return int(row[0].timestamp())


def _dedup_latest_per_day(measurements: list[dict]) -> list[dict]:
    """Keep only the most recent measurement per local-TZ calendar day."""
    by_date: dict = {}
    for m in measurements:
        day = m["measured_at"].astimezone(TZ).date()
        if day not in by_date or m["measured_at"] > by_date[day]["measured_at"]:
            by_date[day] = m
    return list(by_date.values())


def sync_measurements() -> int:
    """Incrementally fetch measurements since the latest stored one, keeping
    the most recent measurement per day, and write them to the DB."""
    now = int(time.time())
    last_ts = _latest_measured_at_ts()
    # Start just after the latest stored measurement; fall back to 30 days.
    start = (last_ts + 1) if last_ts else (now - 30 * 86400)
    measurements = fetch_withings_measurements(startdate=start, enddate=now)
    measurements = _dedup_latest_per_day(measurements)
    for m in measurements:
        store_measurement(m)
    return len(measurements)


def daily_sync():
    """Cron job wrapper around sync_measurements with logging."""
    try:
        count = sync_measurements()
        print(f"[cron] Synced {count} measurements at {datetime.now(TZ)}")
    except WithingsAuthError as e:
        print(f"[cron] Auth expired ({e}) — user must visit /auth to re-authenticate")


# --- App lifecycle ---

scheduler = BackgroundScheduler(timezone="America/Los_Angeles")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if DATABASE_URL:
        init_db()
        try:
            db_token = _load_refresh_token()
            if db_token:
                _tokens["refresh_token"] = db_token
                print("[auth] Loaded refresh token from DB")
        except Exception as e:
            print(f"[auth] Failed to load refresh token from DB: {e}")
        try:
            daily_sync()
        except Exception as e:
            print(f"[startup] Initial sync failed: {e}")
    # Run daily at 11 PM PT
    scheduler.add_job(daily_sync, "cron", hour=23, minute=0)
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="Withings Weight API", lifespan=lifespan)


# --- Auth endpoints ---

@app.get("/auth")
def auth():
    """Redirect to Withings OAuth2 authorization page."""
    if not CLIENT_ID or not REDIRECT_URI:
        raise HTTPException(status_code=500, detail="WITHINGS_CLIENT_ID and WITHINGS_REDIRECT_URI must be set")
    url = (
        f"{WITHINGS_AUTH_URL}?response_type=code"
        f"&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
        f"&scope=user.metrics&state=withings-weight-api"
    )
    return RedirectResponse(url=url)


@app.get("/callback")
def callback(code: str = "", state: str = "", error: str = ""):
    """OAuth2 callback — exchanges authorization code for tokens."""
    if error:
        raise HTTPException(status_code=400, detail=f"OAuth error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="No authorization code received")

    resp = requests.post(WITHINGS_TOKEN_URL, data={
        "action": "requesttoken",
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI,
    }, timeout=15)
    data = resp.json()
    if data.get("status") != 0:
        raise HTTPException(status_code=401, detail=f"Token exchange failed: {data}")

    body = data["body"]
    _tokens["access_token"] = body["access_token"]
    _tokens["refresh_token"] = body["refresh_token"]
    _tokens["expires_at"] = time.time() + body.get("expires_in", 10800)
    try:
        _save_refresh_token(body["refresh_token"])
    except Exception as e:
        print(f"[auth] Failed to persist refresh token: {e}")
    return {
        "status": "authenticated",
        "message": "Save this refresh token as WITHINGS_REFRESH_TOKEN env var for persistence",
        "refresh_token": body["refresh_token"],
    }


# --- API endpoints ---

@app.get("/api/latest")
def api_latest():
    """Get the most recent measurement from the database."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM measurements ORDER BY measured_at DESC LIMIT 1")
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="No measurements found")
    row["measured_at"] = row["measured_at"].isoformat()
    row["created_at"] = row["created_at"].isoformat()
    return dict(row)


@app.get("/api/history")
def api_history(days: int = 90):
    """Get the latest measurement per day for the last N days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (measured_at::date) *
                FROM measurements
                WHERE measured_at >= %s
                ORDER BY measured_at::date, measured_at DESC
            """, (cutoff,))
            rows = cur.fetchall()
    for r in rows:
        r["measured_at"] = r["measured_at"].isoformat()
        r["created_at"] = r["created_at"].isoformat()
    return rows


@app.get("/api/weekly")
def api_weekly(weeks: int = 12):
    """Get weekly averages for the last N weeks."""
    cutoff = datetime.now(timezone.utc) - timedelta(weeks=weeks)
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH daily AS (
                    SELECT DISTINCT ON (measured_at::date) *
                    FROM measurements
                    WHERE measured_at >= %s
                    ORDER BY measured_at::date, measured_at DESC
                )
                SELECT
                    date_trunc('week', measured_at) AS week_start,
                    COUNT(*) AS measurement_count,
                    ROUND(AVG(weight_kg)::numeric, 2) AS avg_weight_kg,
                    ROUND(AVG(weight_lb)::numeric, 2) AS avg_weight_lb,
                    ROUND(AVG(fat_ratio_pct)::numeric, 1) AS avg_fat_ratio_pct,
                    ROUND(AVG(fat_mass_kg)::numeric, 2) AS avg_fat_mass_kg,
                    ROUND(AVG(muscle_mass_kg)::numeric, 2) AS avg_muscle_mass_kg,
                    ROUND(AVG(bone_mass_kg)::numeric, 2) AS avg_bone_mass_kg,
                    ROUND(AVG(hydration_kg)::numeric, 2) AS avg_hydration_kg,
                    ROUND(MIN(weight_kg)::numeric, 2) AS min_weight_kg,
                    ROUND(MAX(weight_kg)::numeric, 2) AS max_weight_kg
                FROM daily
                GROUP BY week_start
                ORDER BY week_start ASC
            """, (cutoff,))
            rows = cur.fetchall()
    for r in rows:
        r["week_start"] = r["week_start"].isoformat()
    return rows


@app.post("/api/sync")
def api_sync():
    """Manually trigger an incremental sync from Withings API to database."""
    try:
        return {"synced": sync_measurements()}
    except WithingsAuthError:
        raise HTTPException(status_code=401, detail={
            "auth_required": True,
            "auth_url": "/auth",
            "message": "Withings authentication expired. Re-authenticate to continue syncing.",
        })


@app.get("/latest_weight")
def latest_weight():
    """Get the latest weight directly from Withings (live, not from DB)."""
    try:
        access_token = _get_access_token()
    except WithingsAuthError:
        raise HTTPException(status_code=401, detail="Not authenticated. Visit /auth to connect.")

    meastypes = ",".join(str(t) for t in MEAS_TYPES.keys())
    resp = requests.post(WITHINGS_MEASURE_URL, headers={
        "Authorization": f"Bearer {access_token}",
    }, data={"action": "getmeas", "meastypes": meastypes, "category": 1}, timeout=15)

    data = resp.json()
    if data.get("status") != 0:
        if data.get("status") in (401, 403):
            _tokens["access_token"] = ""
            raise HTTPException(status_code=401, detail="Token expired. Visit /auth to re-authenticate.")
        raise HTTPException(status_code=502, detail=f"Withings API error: {data}")

    groups = data.get("body", {}).get("measuregrps", [])
    if not groups:
        raise HTTPException(status_code=404, detail="No measurements found")

    latest = groups[0]
    response = {"timestamp": latest.get("date"), "device_id": latest.get("deviceid")}
    for measure in latest.get("measures", []):
        mtype = measure.get("type")
        if mtype in MEAS_TYPES:
            response[MEAS_TYPES[mtype]] = _parse_measure(measure["value"], measure["unit"])
    if "weight_kg" in response:
        response["weight_lb"] = round(response["weight_kg"] * 2.20462, 2)
    return response


# --- Dashboard ---

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return open(os.path.join(os.path.dirname(__file__), "dashboard.html")).read()
