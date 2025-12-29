import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark(app_name: str = "incremental_data"):
    """
    Creates a Spark Session with specific overrides to fix the '60s' NumberFormatException
    and ensures S3A/MinIO compatibility.
    """
    
    # 1. Kill any existing active sessions to prevent 'Using an existing Spark session' warnings
    active_session = SparkSession.getActiveSession()
    if active_session:
        active_session.stop()

    # 2. Define Stable Package Versions
    # Hadoop 3.3.4 + AWS SDK 1.12.262 is the most stable pairing for local MinIO setups
    hadoop_aws_pkg = "org.apache.hadoop:hadoop-aws:3.3.4"
    aws_sdk_pkg = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

    # 3. Build the Session
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Force the driver to loopback to avoid WSL networking glitches
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        
        # --- FIX: NumberFormatException "60s" ---
        # We disable the FileSystem cache to force Spark to re-read our numeric configs
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        
        # --- S3A / MinIO Setup ---
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # --- Delta Lake Configuration ---
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )

    # 4. Apply Delta PIP and Create Session
    spark = configure_spark_with_delta_pip(
        builder, 
        extra_packages=[hadoop_aws_pkg, aws_sdk_pkg]
    ).getOrCreate()

    # 5. --- THE FINAL OVERRIDE ---
    # We reach into the Java Virtual Machine to set the property globally.
    # This prevents Delta Lake's internal 'LogStore' from seeing the "60s" string.
    try:
        sc = spark.sparkContext
        h_conf = sc._jsc.hadoopConfiguration()
        
        # Set values in the Hadoop Configuration object
        for key in ["connection.timeout", "connection.establish.timeout", "connection.request.timeout", "socket.timeout"]:
            h_conf.set(f"fs.s3a.{key}", "60000")
            
        # Set values in the Java System Properties (Global JVM level)
        jvm = sc._gateway.jvm
        jvm.java.lang.System.setProperty("fs.s3a.connection.timeout", "60000")
        
        print(f"Successfully initialized Spark with Timeout: {h_conf.get('fs.s3a.connection.timeout')}")
    except Exception as e:
        print(f"Warning: Failed to apply deep-level JVM overrides: {e}")

    return spark