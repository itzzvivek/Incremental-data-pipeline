from datetime import datetime, timezone
from metadata import load_metadata
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

def build_incremental(raw_df):
    last_loaded = load_metadata()

    df = raw_df.selectExpr(
        "cast(raw_timestamp_ms/1000 as timestamp) as event_time",
        "raw_price as price"
    )

    return df.filter(df.event_time > last_loaded)