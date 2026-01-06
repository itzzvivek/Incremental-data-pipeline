import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# JDBC packages
HADOOP_AWS_PKG = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_PKG = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
POSTGRES_JDBC_PKG = "org.postgresql:postgresql:42.7.3"


def get_spark(app_name: str = "incremental_data"):
    # 1. Stop any existing Spark session
    active_session = SparkSession.getActiveSession()
    if active_session:
        active_session.stop()

    # 2. Build Spark Session
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # --- Networking stability ---
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")

        # --- FIX: S3A timeout ("60s" bug) ---
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")

        # --- S3A / MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # --- Delta Lake ---
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    # 3. Create Spark session with ALL required JVM packages
    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            HADOOP_AWS_PKG,
            AWS_SDK_PKG,
            POSTGRES_JDBC_PKG,   # ✅ THIS FIXES org.postgresql.Driver
        ]
    ).getOrCreate()

    # 4. Deep JVM override (final safety net)
    try:
        sc = spark.sparkContext
        h_conf = sc._jsc.hadoopConfiguration()

        for key in [
            "connection.timeout",
            "connection.establish.timeout",
            "connection.request.timeout",
            "socket.timeout",
        ]:
            h_conf.set(f"fs.s3a.{key}", "60000")

        sc._gateway.jvm.java.lang.System.setProperty(
            "fs.s3a.connection.timeout", "60000"
        )

        print(
            "Successfully initialized Spark with Timeout:",
            h_conf.get("fs.s3a.connection.timeout")
        )
    except Exception as e:
        print(f"Warning: JVM override failed: {e}")

    return spark
