import os
import json
from datetime import datetime

META_PATH = os.getenv("META_PATH", "/app/data/metadata/last_load.json")

DEFAULT = {"last_loaded": "1970-01-01T00:00:00"}

def load_metadata():
    if not os.path.exists(META_PATH):
        os.makedirs(os.path.dirname(META_PATH), exist_ok=True)
        with open(META_PATH, "w") as f:
            json.dump(DEFAULT, f)
        return datetime.fromisoformat(DEFAULT["last_loaded"].replace("Z", "+00:00"))
    
    with open(META_PATH) as f:
        obj = json.load(f)
    return datetime.fromisoformat(obj.get("last_loaded").replace("Z", "+00:00"))

def update_metadata(dt):
    with open(META_PATH, "w") as f:
        json.dump({"last_loaded": iso}, f)