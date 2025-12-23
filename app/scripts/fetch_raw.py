import requests
from datetime import datetime, timezone

from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import col, from_unixtime
from spark_session import get_spark

spark = get_spark("FetchRawData")

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"


def fetch_raw():
    params = {
        "vs_currency": "usd",
        "days": "max"
    }

    r = requests.get(COINGECKO_URL, params=params, timeout=30)
    print(f"Fetching raw data from {r}")
    r.raise_for_status()
    
    data = r.json()
    prices = data.get("prices", [])

    if not prices:
        return spark.createDataFrame([], schema=None)   

    # prices = [[timestamp_ms, price], ...]
    rows = [(p[0], float(p[1]))for p in prices]

    schema = StructType([
        StructField("event_time_ms", DoubleType(), False),
        StructField("price", DoubleType(), False)
    ])

    df = spark.createDataFrame(rows, schema=schema)

    df = df.withColumn(
        "event_time",
        from_unixtime(col("event_time_ms") / 1000).cast("TimestampType()")
    ).drop("event_time_ms")

    return df