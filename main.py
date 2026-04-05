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


def _refresh_access_token():
    if not _tokens["refresh_token"]:
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
        return False
    body = data["body"]
    _tokens["access_token"] = body["access_token"]
    _tokens["refresh_token"] = body["refresh_token"]
    _tokens["expires_at"] = time.time() + body.get("expires_in", 10800)
    return True


def _get_access_token() -> str:
    if _tokens["access_token"] and time.time() < _tokens["expires_at"] - 60:
        return _tokens["access_token"]
    if _refresh_access_token():
        return _tokens["access_token"]
    return ""


def _parse_measure(value: int, unit: int) -> float:
    return round(value * (10 ** unit), 2)


def fetch_withings_measurements(startdate: int = 0, enddate: int = 0) -> list[dict]:
    """Fetch measurements from Withings API and return parsed list."""
    access_token = _get_access_token()
    if not access_token:
        return []

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


def daily_sync():
    """Cron job: fetch today's measurements and store them."""
    now = int(time.time())
    start = now - 86400  # last 24 hours
    measurements = fetch_withings_measurements(startdate=start, enddate=now)
    for m in measurements:
        store_measurement(m)
    print(f"[cron] Synced {len(measurements)} measurements at {datetime.now(TZ)}")


# --- App lifecycle ---

scheduler = BackgroundScheduler(timezone="America/Los_Angeles")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if DATABASE_URL:
        init_db()
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
    """Manually trigger a sync from Withings API to database."""
    measurements = fetch_withings_measurements()
    for m in measurements:
        store_measurement(m)
    return {"synced": len(measurements)}


@app.get("/latest_weight")
def latest_weight():
    """Get the latest weight directly from Withings (live, not from DB)."""
    access_token = _get_access_token()
    if not access_token:
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
