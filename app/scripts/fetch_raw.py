import requests
from datetime import datetime, timezone

from spark_session import get_spark

spark = get_spark("FetchRawData")

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"


def fetch_raw():
    params = {
        "vs_currency": "usd",
        "days": 1  # fetch all available data
    }

    r = requests.get(COINGECKO_URL, params=params, timeout=30)
    print(f"Fetching raw data from {r}")
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