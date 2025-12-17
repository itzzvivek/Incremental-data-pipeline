import requests
from pyspark.sql import SparkSession
from datetime import datetime, timezone

spark = SparkSession.builder.appName("FetchRawData").getOrCreate()

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"


def fetch_raw():
    params = {
        "vs_currency": "usd",
        "days": "max"  # fetch all available data
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
            p[0], float(p[1]), datetime.now(timezone.utc),
            float(p[1])
        )
        for p in prices
    ]

    df = spark.createDataFrame(rows, ["event_time", "price"])
    return df