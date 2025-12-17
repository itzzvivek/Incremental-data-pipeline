import os
import pyspark as pd
import boto3
from deltalake.writer import write_deltalake


# S3 / Minio config via env
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
BUCKET_NAME = os.getenv("incremental-data", "delta")
    
TABLE_PATH = f"s3://{BUCKET_NAME}/crypto/bitcoin_ohlcv"

def write_delta(df: pd):
    if df.empty:
        return
    
    write_deltalake(TABLE_PATH, df, mode="append")