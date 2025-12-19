from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

def get_spark(app_name: str):
    return (
        SparkSession.builder
        .appName("incremental_data")

        # S3A + MinIO
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark