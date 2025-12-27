from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os


def get_spark(app_name: str = "incremental_data"):
    hadoop_aws_pkg = "org.apache.hadoop:hadoop-aws:3.3.4"
    aws_sdk_pkg = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

    builder = (
        SparkSession.builder
        .appName(app_name)

        # ---------- S3A + MinIO ----------
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("S3_ENDPOINT", "http://minio:9000")
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # ---------- STABLE TIMEOUTS (NUMERIC ONLY) ----------
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")

        # ---------- Delta Lake ----------
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=[hadoop_aws_pkg, aws_sdk_pkg]
    ).getOrCreate()

    print("S3A timeout =", spark.sparkContext._jsc.hadoopConfiguration().get("fs.s3a.connection.timeout"))

    return spark
