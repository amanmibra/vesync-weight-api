import os
import time

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse

app = FastAPI(title="Withings Weight API")

CLIENT_ID = os.environ.get("WITHINGS_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("WITHINGS_CLIENT_SECRET", "")
REDIRECT_URI = os.environ.get("WITHINGS_REDIRECT_URI", "")

WITHINGS_AUTH_URL = "https://account.withings.com/oauth2_user/authorize2"
WITHINGS_TOKEN_URL = "https://wbsapi.withings.net/v2/oauth2"
WITHINGS_MEASURE_URL = "https://wbsapi.withings.net/measure"

# Measurement type IDs
MEAS_TYPES = {
    1: "weight_kg",
    5: "fat_free_mass_kg",
    6: "fat_ratio_pct",
    8: "fat_mass_kg",
    76: "muscle_mass_kg",
    77: "hydration_kg",
    88: "bone_mass_kg",
}

# In-memory token store (use env vars for initial refresh token)
_tokens = {
    "access_token": "",
    "refresh_token": os.environ.get("WITHINGS_REFRESH_TOKEN", ""),
    "expires_at": 0,
}


def _refresh_access_token():
    """Refresh the access token using the stored refresh token."""
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
    """Return a valid access token, refreshing if needed."""
    if _tokens["access_token"] and time.time() < _tokens["expires_at"] - 60:
        return _tokens["access_token"]
    if _refresh_access_token():
        return _tokens["access_token"]
    return ""


def _parse_measure(value: int, unit: int) -> float:
    """Convert Withings value/unit pair to float. Actual = value * 10^unit."""
    return round(value * (10 ** unit), 2)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


@app.get("/auth")
def auth():
    """Redirect to Withings OAuth2 authorization page."""
    if not CLIENT_ID or not REDIRECT_URI:
        raise HTTPException(status_code=500, detail="WITHINGS_CLIENT_ID and WITHINGS_REDIRECT_URI must be set")
    url = (
        f"{WITHINGS_AUTH_URL}"
        f"?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope=user.metrics"
        f"&state=withings-weight-api"
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


@app.get("/latest_weight")
def latest_weight():
    """Get the latest weight and body composition from Withings."""
    access_token = _get_access_token()
    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated. Visit /auth to connect your Withings account.")

    meastypes = ",".join(str(t) for t in MEAS_TYPES.keys())

    resp = requests.post(WITHINGS_MEASURE_URL, headers={
        "Authorization": f"Bearer {access_token}",
    }, data={
        "action": "getmeas",
        "meastypes": meastypes,
        "category": 1,
    }, timeout=15)

    data = resp.json()
    if data.get("status") != 0:
        # Token might be expired
        if data.get("status") in (401, 403):
            _tokens["access_token"] = ""
            raise HTTPException(status_code=401, detail="Token expired. Visit /auth to re-authenticate.")
        raise HTTPException(status_code=502, detail=f"Withings API error: {data}")

    groups = data.get("body", {}).get("measuregrps", [])
    if not groups:
        raise HTTPException(status_code=404, detail="No measurements found")

    # Latest measurement group (already sorted by most recent)
    latest = groups[0]

    response = {
        "timestamp": latest.get("date"),
        "device_id": latest.get("deviceid"),
    }

    for measure in latest.get("measures", []):
        mtype = measure.get("type")
        if mtype in MEAS_TYPES:
            response[MEAS_TYPES[mtype]] = _parse_measure(measure["value"], measure["unit"])

    # Add weight_lb if weight_kg present
    if "weight_kg" in response:
        response["weight_lb"] = round(response["weight_kg"] * 2.20462, 2)

    return response
