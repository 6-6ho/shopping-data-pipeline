"""
Spark Common Utilities

Provides shared SparkSession factory and Iceberg table utilities.
Eliminates code duplication across streaming and batch jobs.
"""

import os
from typing import Optional
from pyspark.sql import SparkSession


class SparkConfig:
    """Configuration for Spark and storage connections."""

    # MinIO / S3 Configuration
    S3_ENDPOINT: str = os.getenv("S3_ENDPOINT", "http://minio:9000")
    S3_ACCESS_KEY: str = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    S3_SECRET_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

    # Iceberg Configuration
    # Force trailing slash and hardcode to avoid environment issues for now
    ICEBERG_WAREHOUSE: str = "s3a://iceberg-warehouse/data/"
    print(f"DEBUG: ICEBERG_WAREHOUSE set to '{ICEBERG_WAREHOUSE}' from {__file__}", flush=True)
    ICEBERG_CATALOG: str = "iceberg"

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    # Checkpoint paths
    CHECKPOINT_BASE: str = os.getenv("CHECKPOINT_BASE", "s3a://raw/checkpoints")


def create_spark_session(
    app_name: str,
    master: Optional[str] = None,
    enable_streaming: bool = False,
    extra_configs: Optional[dict] = None
) -> SparkSession:
    """
    Create a configured SparkSession for Iceberg and MinIO.

    Args:
        app_name: Name of the Spark application.
        master: Spark master URL. If None, uses default from environment.
        enable_streaming: If True, adds streaming-specific configurations.
        extra_configs: Additional Spark configurations to apply.

    Returns:
        Configured SparkSession instance.

    Example:
        >>> spark = create_spark_session("MyBatchJob")
        >>> spark = create_spark_session("MyStreamJob", enable_streaming=True)
    """
    config = SparkConfig()

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    # Iceberg configurations
    builder = builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{config.ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{config.ICEBERG_CATALOG}.type", "hadoop") \
        .config(f"spark.sql.catalog.{config.ICEBERG_CATALOG}.warehouse", config.ICEBERG_WAREHOUSE)

    # S3/MinIO configurations
    builder = builder \
        .config("spark.hadoop.fs.s3a.endpoint", config.S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", config.S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Streaming configurations
    if enable_streaming:
        builder = builder \
            .config("spark.sql.streaming.checkpointLocation", f"{config.CHECKPOINT_BASE}/default")

    # Apply extra configurations
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def ensure_namespace(spark: SparkSession, namespace: str) -> None:
    """
    Create an Iceberg namespace if it doesn't exist.

    Args:
        spark: Active SparkSession.
        namespace: Namespace path (e.g., "iceberg.shopping").
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def run_idempotent_overwrite(
    spark: SparkSession,
    df,
    table_name: str,
    partition_col: str,
    partition_value: str
) -> None:
    """
    Perform idempotent write by deleting existing partition data first.

    This ensures that re-running a batch job for the same date doesn't
    create duplicate records.

    Args:
        spark: Active SparkSession.
        df: DataFrame to write.
        table_name: Full table name (e.g., "iceberg.shopping.daily_summary").
        partition_col: Partition column name (e.g., "report_date").
        partition_value: Partition value to overwrite (e.g., "2024-01-15").
    """
    # Delete existing data for this partition
    spark.sql(f"""
        DELETE FROM {table_name}
        WHERE {partition_col} = '{partition_value}'
    """)

    # Insert new data
    df.writeTo(table_name).append()


def get_checkpoint_path(job_name: str) -> str:
    """
    Get checkpoint path for a streaming job.

    Args:
        job_name: Name of the streaming job.

    Returns:
        Full checkpoint path.
    """
    config = SparkConfig()
    return f"{config.CHECKPOINT_BASE}/{job_name}"
