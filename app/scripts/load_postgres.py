from pyspark.sql import DataFrame
import os


POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/analytics_db")

POSTGRES_PROPS = {
    "user": os.getenv("POSTGRES_USER", "root"),
    "password": os.getenv("POSTGRES_PASSWORD", "root"),
    "driver": "org.postgresql.Driver"
}

TABLE_NAME = "bitcoin_prices"


def write_gold(df: DataFrame):
    (
        df.write
        .mode("append")
        .jdbc(POSTGRES_URL, TABLE_NAME, properties=POSTGRES_PROPS)
        
    )