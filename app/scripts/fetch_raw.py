import os
import requests
from dotenv import load_dotenv

from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    TimestampType,
    LongType,
)
from pyspark.sql.functions import col, from_unixtime
from spark_session import get_spark

# Load env (local runs)
load_dotenv()

spark = get_spark("FetchRawData")

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")

if not COINGECKO_API_KEY:
    raise RuntimeError("COINGECKO_API_KEY not set")

RAW_DATA_PATH = "s3a://delta/raw/bitcoin_prices"


def fetch_raw(spark):
    headers = {
        "x-cg-demo-api-key": COINGECKO_API_KEY,
        "Accept": "application/json"
    }

    params = {
        "vs_currency": "usd",
        "days": "30",
    }

    print(f"Fetching raw data from {COINGECKO_URL}")
    response = requests.get(    
        COINGECKO_URL,
        headers=headers,    
        params=params,
        timeout=90
    )
    response.raise_for_status()

    prices = response.json().get("prices", [])
    market_caps = response.json().get("market_caps", [])
    volumes = response.json().get("total_volumes", [])


    if not prices:
        return None

    price_map = {p[0]: float(p[1]) for p in prices}
    market_cap_map = {m[0]: float(m[1]) for m in market_caps}
    volume_map = {v[0]: float(v[1]) for v in volumes}

    timestamps = sorted(price_map.keys())

    rows = [
        (
            ts,
            price_map.get(ts),
            market_cap_map.get(ts),
            volume_map.get(ts)
        )
        for ts in timestamps
    ]

    schema = StructType([
        StructField("event_time_ms", LongType(), False),
        StructField("price", DoubleType(), False),
        StructField("market_cap", DoubleType(), True),
        StructField("volume", DoubleType(), True),
    ])

    df = spark.createDataFrame(rows, schema)

    df = (
        df.withColumn(
            "event_time",
            from_unixtime(col("event_time_ms") / 1000)
            .cast(TimestampType())
        )
        .drop("event_time_ms")
    )

    return df


def fetch_and_store_raw():
    df = fetch_raw()

    if df is None or df.rdd.isEmpty():
        print("No raw data fetched")
        return

    df.write.format("delta").mode("append").save(RAW_DATA_PATH)
    print(f"Raw data written to {RAW_DATA_PATH}")


if __name__ == "__main__":
    fetch_and_store_raw()
