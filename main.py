import hashlib
import os
import time
import uuid

import requests
from fastapi import FastAPI, HTTPException

app = FastAPI(title="VeSync Weight API")

VESYNC_EMAIL = os.environ.get("VESYNC_EMAIL")
VESYNC_PASSWORD = os.environ.get("VESYNC_PASSWORD")
VESYNC_TIMEZONE = os.environ.get("VESYNC_TIMEZONE", "America/Los_Angeles")

API_BASE = "https://smartapi.vesync.com"
APP_VERSION = "5.6.60"
APP_ID = "eldodkfj"
TERMINAL_ID = str(uuid.uuid4())

HEADERS = {
    "User-Agent": "okhttp/3.12.1",
    "Content-Type": "application/json; charset=UTF-8",
}


def _base_body(method: str) -> dict:
    return {
        "acceptLanguage": "en",
        "accountID": "",
        "appID": APP_ID,
        "sourceAppID": APP_ID,
        "clientInfo": "pyvesync",
        "clientType": "vesyncApp",
        "clientVersion": f"VeSync {APP_VERSION}",
        "debugMode": False,
        "method": method,
        "osInfo": "Android",
        "terminalId": TERMINAL_ID,
        "timeZone": VESYNC_TIMEZONE,
        "token": "",
        "traceId": str(int(time.time())),
        "userCountryCode": "US",
    }


def vesync_login() -> dict:
    password_md5 = hashlib.md5(VESYNC_PASSWORD.encode("utf-8")).hexdigest()

    # Step 1: Get authorization code
    body = _base_body("authByPWDOrOTM")
    body["authProtocolType"] = "generic"
    body["email"] = VESYNC_EMAIL
    body["password"] = password_md5

    resp = requests.post(
        f"{API_BASE}/globalPlatform/api/accountAuth/v1/authByPWDOrOTM",
        json=body, headers=HEADERS, timeout=15,
    )
    data = resp.json()
    if data.get("code") != 0:
        return None

    auth_code = data.get("result", {}).get("authorizeCode")
    if not auth_code:
        return None

    # Step 2: Exchange auth code for token
    body2 = _base_body("loginByAuthorizeCode4Vesync")
    body2["authorizeCode"] = auth_code
    body2["emailSubscriptions"] = False

    resp2 = requests.post(
        f"{API_BASE}/user/api/accountManage/v1/loginByAuthorizeCode4Vesync",
        json=body2, headers=HEADERS, timeout=15,
    )
    data2 = resp2.json()
    if data2.get("code") != 0:
        return None

    result = data2.get("result", {})
    return {"token": result.get("token"), "accountID": result.get("accountID")}


def get_weight_data(token: str, account_id: str) -> list:
    """Try the fat scale endpoint first, then fall back to basic scale endpoint."""
    now = int(time.time())
    auth_headers = {**HEADERS, "tk": token, "accountid": account_id}

    # Try fat scale endpoint (ESF00+ / body composition scales)
    body = _base_body("getWeighData")
    body["token"] = token
    body["accountID"] = account_id
    body["startTime"] = 0
    body["endTime"] = now
    body["pageSize"] = 5
    body["order"] = "desc"

    resp = requests.post(
        f"{API_BASE}/cloud/v1/deviceManaged/fatScale/getWeighData",
        json=body, headers=auth_headers, timeout=15,
    )
    data = resp.json()
    records = data.get("result", {}).get("data", [])
    if records:
        return records

    # Fallback: basic scale endpoint
    body["method"] = "getWeighingDataV2"
    resp = requests.post(
        f"{API_BASE}/cloud/v2/deviceManaged/getWeighingDataV2",
        json=body, headers=auth_headers, timeout=15,
    )
    data = resp.json()
    return data.get("result", {}).get("weightDatas", [])


def estimate_body_fat(weight_kg: float, impedance: float, height_cm: float, age: int, gender: str) -> float | None:
    """Estimate body fat % from bioelectrical impedance using standard BIA formula."""
    if not all([weight_kg, impedance, height_cm, age]):
        return None
    height_m = height_cm / 100.0
    bmi = weight_kg / (height_m ** 2)
    is_male = gender and gender.lower() in ("male", "m", "1")
    sex_factor = 1 if is_male else 0
    # Standard BIA-derived body fat estimation
    body_fat = (1.20 * bmi) + (0.23 * age) - (10.8 * sex_factor) - 5.4
    return round(max(0, min(body_fat, 70)), 1)


@app.get("/latest_weight")
def latest_weight():
    if not VESYNC_EMAIL or not VESYNC_PASSWORD:
        raise HTTPException(status_code=500, detail="VESYNC_EMAIL and VESYNC_PASSWORD must be set")

    auth = vesync_login()
    if not auth:
        raise HTTPException(status_code=401, detail="VeSync login failed")

    records = get_weight_data(auth["token"], auth["accountID"])
    if not records:
        raise HTTPException(status_code=404, detail="No weight data found")

    latest = records[0]

    # Fat scale fields
    weight_kg = latest.get("weigh_kg")
    weight_lb = latest.get("weigh_lb")
    # Basic scale fallback
    weight_g = latest.get("weightG")

    if weight_kg:
        weight = weight_kg
        unit = "kg"
        weight_lb = weight_lb or round(weight_kg * 2.20462, 2)
    elif weight_g:
        weight = weight_g
        unit = "g"
        weight_kg = weight_g / 1000.0
        weight_lb = round(weight_kg * 2.20462, 2)
    else:
        weight = latest.get("weight", 0)
        unit = latest.get("unit", "unknown")
        weight_kg = None
        weight_lb = None

    # Body composition from impedance
    impedance = latest.get("impedence") or latest.get("impedance")
    height_cm = latest.get("heightCm")
    age = latest.get("age")
    gender = latest.get("gender")
    body_fat_pct = estimate_body_fat(weight_kg, impedance, height_cm, age, gender) if impedance else None

    timestamp = latest.get("timestamp")

    response = {
        "timestamp": timestamp,
        "weight_kg": weight_kg,
        "weight_lb": weight_lb,
        "unit": latest.get("unit", unit),
        "body_fat_pct": body_fat_pct,
        "impedance": impedance,
    }

    # Include demographic context if present
    if height_cm:
        response["height_cm"] = height_cm
    if age:
        response["age"] = age
    if gender:
        response["gender"] = gender

    return response


@app.get("/", include_in_schema=False)
def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/docs")
