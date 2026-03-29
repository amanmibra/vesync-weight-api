import os

from fastapi import FastAPI, HTTPException
from pyvesync import VeSync

app = FastAPI()

VESYNC_EMAIL = os.environ.get("VESYNC_EMAIL")
VESYNC_PASSWORD = os.environ.get("VESYNC_PASSWORD")
VESYNC_TIMEZONE = os.environ.get("VESYNC_TIMEZONE", "America/Los_Angeles")


@app.get("/latest_weight")
def latest_weight():
    if not VESYNC_EMAIL or not VESYNC_PASSWORD:
        raise HTTPException(status_code=500, detail="VESYNC_EMAIL and VESYNC_PASSWORD must be set")

    manager = VeSync(VESYNC_EMAIL, VESYNC_PASSWORD, VESYNC_TIMEZONE)

    if not manager.login():
        raise HTTPException(status_code=401, detail="VeSync login failed")

    manager.update()

    scales = manager.scales
    if not scales:
        raise HTTPException(status_code=404, detail="No scale devices found")

    scale = scales[0]

    weight_data = scale.device_status
    if not weight_data:
        raise HTTPException(status_code=404, detail="No weight data available")

    return {
        "timestamp_utc": scale.last_update if hasattr(scale, "last_update") else None,
        "weight": scale.weight if hasattr(scale, "weight") else weight_data,
        "unit": scale.unit if hasattr(scale, "unit") else "kg",
        "device_name": scale.device_name,
    }
