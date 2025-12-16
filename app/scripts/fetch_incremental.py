import os
import requests
import pyspark as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timezone

from metadata import load_metadata

spark = SparkSession.builder.appName("CoingekoFetchIncremental").getOrCreate()

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"


def fetch_incremental():
    last_loaded = load_metadata()

    params = {
        "vs_currency": "usd",
        "days": 1  # last 24 hours
    }

    r = requests.get(COINGECKO_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    prices = data.get("prices", [])

    if not prices:
        return spark.createDataFrame([], schema=None)   

    # Convert to Spark rows
    rows = [
        (
            datetime.fromtimestamp(p[0] / 1000),
            float(p[1])
        )
        for p in prices
        if datetime.fromtimestamp(p[0] / 1000, tz=timezone.utc) > last_loaded 
    ]

    if not rows:
        return spark.createDataFrame([], schema=None)

    df = spark.createDataFrame(rows, ["event_time", "price"])
    return df
