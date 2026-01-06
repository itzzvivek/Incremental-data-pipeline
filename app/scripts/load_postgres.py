from pyspark.sql import DataFrame
from dotenv import load_dotenv
import os

load_dotenv()   

POSTGRES_URL = f"{os.getenv('POSTGRES_URL')}/{os.getenv('POSTGRES_DB')}"

POSTGRES_PROPS = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

TABLE_NAME = "bitcoin_prices"


def write_gold(df: DataFrame):
    (
        df.write
        .mode("append")
        .jdbc(POSTGRES_URL, TABLE_NAME, properties=POSTGRES_PROPS)
        
    )