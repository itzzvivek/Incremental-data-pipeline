import os
import json
from datetime import datetime, timezone

BASE_DIR = os.getenv("APP_BASE_DIR", os.getcwd())
META_PATH = os.path.join(BASE_DIR, "data", "metadata", "last_load.json")

DEFAULT_TS = "2025-12-16T00:00:00Z"

def load_metadata():
    os.makedirs(os.path.dirname(META_PATH), exist_ok=True)

    # If file does not exist OR is empty → initialize
    if not os.path.exists(META_PATH) or os.path.getsize(META_PATH) == 0:
        with open(META_PATH, "w") as f:
            json.dump({"last_loaded": DEFAULT_TS}, f)

        return datetime.fromisoformat(
            DEFAULT_TS.replace("Z", "+00:00")
        )

    # If file exists, try reading JSON safely
    try:
        with open(META_PATH, "r") as f:
            obj = json.load(f)

        ts = obj.get("last_loaded", DEFAULT_TS)
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    except (json.JSONDecodeError, ValueError):
        # Corrupted file → reset safely
        with open(META_PATH, "w") as f:
            json.dump({"last_loaded": DEFAULT_TS}, f)

        return datetime.fromisoformat(
            DEFAULT_TS.replace("Z", "+00:00")
        )


def update_metadata(dt):
    os.makedirs(os.path.dirname(META_PATH), exist_ok=True)

    with open(META_PATH, "w") as f:
        json.dump(
            {"last_loaded": dt.astimezone(timezone.utc).isoformat()},
            f
        )   