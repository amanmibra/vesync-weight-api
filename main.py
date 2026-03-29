import hashlib
import math
import os
import time

import requests
from fastapi import FastAPI, HTTPException

app = FastAPI(title="VeSync Weight API")

VESYNC_EMAIL = os.environ.get("VESYNC_EMAIL")
VESYNC_PASSWORD = os.environ.get("VESYNC_PASSWORD")
VESYNC_TIMEZONE = os.environ.get("VESYNC_TIMEZONE", "America/Los_Angeles")

API_BASE = "https://smartapi.vesync.com"


def vesync_login() -> dict:
    password_md5 = hashlib.md5(VESYNC_PASSWORD.encode()).hexdigest()
    body = {
        "acceptLanguage": "en",
        "appVersion": "2.8.6",
        "phoneBrand": "api",
        "phoneOS": "api",
        "timeZone": VESYNC_TIMEZONE,
        "traceId": str(int(time.time())),
        "email": VESYNC_EMAIL,
        "password": password_md5,
        "devToken": "",
        "userType": "1",
        "method": "loginV2",
    }
    resp = requests.post(f"{API_BASE}/cloud/v2/user/login", json=body, timeout=15)
    data = resp.json()
    if data.get("code") != 0:
        return None
    result = data.get("result", {})
    return {"token": result.get("token"), "accountID": result.get("accountID")}


def get_scale_devices(token: str, account_id: str) -> list:
    body = {
        "acceptLanguage": "en",
        "appVersion": "2.8.6",
        "phoneBrand": "api",
        "phoneOS": "api",
        "timeZone": VESYNC_TIMEZONE,
        "traceId": str(int(time.time())),
        "token": token,
        "accountID": account_id,
        "method": "devices",
        "pageNo": 1,
        "pageSize": 100,
    }
    resp = requests.post(f"{API_BASE}/cloud/v2/deviceManaged/devices", json=body, timeout=15)
    data = resp.json()
    devices = data.get("result", {}).get("list", [])
    return [d for d in devices if "scale" in d.get("deviceType", "").lower()
            or "scale" in d.get("configModule", "").lower()
            or "scale" in d.get("type", "").lower()]


def get_weight_data(token: str, account_id: str) -> list:
    """Try the fat scale endpoint first, then fall back to basic scale endpoint."""
    now = int(time.time())
    headers = {"tk": token, "accountid": account_id}

    # Try fat scale endpoint (ESF00+ / body composition scales)
    body = {
        "acceptLanguage": "en",
        "appVersion": "2.8.6",
        "phoneBrand": "api",
        "phoneOS": "api",
        "timeZone": VESYNC_TIMEZONE,
        "traceId": str(now),
        "token": token,
        "accountID": account_id,
        "method": "getWeighData",
        "startTime": 0,
        "endTime": now,
        "pageSize": 5,
        "order": "desc",
    }
    resp = requests.post(
        f"{API_BASE}/cloud/v1/deviceManaged/fatScale/getWeighData",
        json=body, headers=headers, timeout=15,
    )
    data = resp.json()
    records = data.get("result", {}).get("data", [])
    if records:
        return records

    # Fallback: basic scale endpoint
    body["method"] = "getWeighingDataV2"
    resp = requests.post(
        f"{API_BASE}/cloud/v2/deviceManaged/getWeighingDataV2",
        json=body, headers=headers, timeout=15,
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


@app.get("/")
def root():
    return {"status": "ok", "docs": "/docs"}
