from datetime import datetime, timezone
from pyspark.sql import DataFrame
from metadata import load_metadata


def build_incremental(raw_df: "DataFrame") -> "DataFrame":
    last_loaded = load_metadata()
    return raw_df.filter(raw_df.event_time > last_loaded)       