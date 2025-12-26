from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

def get_spark(app_name: str = "incremental_data"):
    # Change hadoop-aws version to 3.3.4 for maximum stability with Spark 3.x
    hadoop_aws_pkg = "org.apache.hadoop:hadoop-aws:3.3.4"
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        # ---------- S3A + MinIO Configuration ----------
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ---------- Delta Lake Configuration ----------
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    # Use extra_packages to fix the ClassNotFoundException
    return configure_spark_with_delta_pip(builder, extra_packages=[hadoop_aws_pkg]).getOrCreate()