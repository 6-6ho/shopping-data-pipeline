"""
Spark Streaming Job: Session Events

Reads complex nested JSON session events from Kafka and writes to Iceberg.
Demonstrates handling of deeply nested unstructured data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, explode, size,
    when, struct, array, element_at
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    ArrayType, BooleanType, DoubleType, MapType
)


def create_spark_session():
    """Create Spark session."""

    spark = SparkSession.builder \
        .appName("SessionEventsStreaming") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_session_schema():
    """Define schema for nested session events."""

    # Nested event schema
    product_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("quantity", IntegerType(), True)
    ])

    search_schema = StructType([
        StructField("query", StringType(), True),
        StructField("result_count", IntegerType(), True),
        StructField("filters", ArrayType(StringType()), True)
    ])

    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("total_amount", IntegerType(), True),
        StructField("discount_amount", IntegerType(), True),
        StructField("coupon_used", BooleanType(), True)
    ])

    event_schema = StructType([
        StructField("event_index", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("page", StringType(), True),
        StructField("duration_on_page", IntegerType(), True),
        StructField("product", product_schema, True),
        StructField("search", search_schema, True),
        StructField("transaction", transaction_schema, True)
    ])

    # Device schema
    screen_schema = StructType([
        StructField("width", IntegerType(), True),
        StructField("height", IntegerType(), True),
        StructField("density", DoubleType(), True)
    ])

    network_schema = StructType([
        StructField("type", StringType(), True),
        StructField("carrier", StringType(), True)
    ])

    device_schema = StructType([
        StructField("type", StringType(), True),
        StructField("os", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("screen", screen_schema, True),
        StructField("network", network_schema, True),
        StructField("manufacturer", StringType(), True),
        StructField("model", StringType(), True),
        StructField("locale", StringType(), True),
        StructField("timezone", StringType(), True)
    ])

    # Context schema
    utm_schema = StructType([
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("term", StringType(), True),
        StructField("content", StringType(), True)
    ])

    campaign_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("discount_rate", DoubleType(), True)
    ])

    location_schema = StructType([
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True)
    ])

    context_schema = StructType([
        StructField("referrer", StringType(), True),
        StructField("landing_page", StringType(), True),
        StructField("utm", utm_schema, True),
        StructField("campaign", campaign_schema, True),
        StructField("is_first_visit", BooleanType(), True),
        StructField("previous_sessions_count", IntegerType(), True),
        StructField("days_since_last_visit", IntegerType(), True),
        StructField("location", location_schema, True)
    ])

    # Summary schema
    summary_schema = StructType([
        StructField("total_events", IntegerType(), True),
        StructField("unique_products_viewed", IntegerType(), True),
        StructField("searches_count", IntegerType(), True),
        StructField("views_count", IntegerType(), True),
        StructField("add_cart_count", IntegerType(), True),
        StructField("purchase_count", IntegerType(), True),
        StructField("converted", BooleanType(), True),
        StructField("bounce", BooleanType(), True)
    ])

    # Main session schema
    return StructType([
        StructField("session_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("started_at", StringType(), True),
        StructField("ended_at", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("event_count", IntegerType(), True),
        StructField("device", device_schema, True),
        StructField("context", context_schema, True),
        StructField("user_agent", StringType(), True),
        StructField("events", ArrayType(event_schema), True),
        StructField("summary", summary_schema, True)
    ])


def create_iceberg_tables(spark):
    """Create Iceberg tables for session data."""

    # Session summary table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.sessions (
            session_id STRING,
            user_id STRING,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            duration_seconds INT,
            event_count INT,
            device_type STRING,
            device_os STRING,
            device_model STRING,
            referrer STRING,
            utm_source STRING,
            utm_medium STRING,
            region STRING,
            city STRING,
            is_first_visit BOOLEAN,
            total_events INT,
            unique_products_viewed INT,
            searches_count INT,
            views_count INT,
            add_cart_count INT,
            purchase_count INT,
            converted BOOLEAN,
            bounce BOOLEAN,
            session_date DATE
        )
        USING iceberg
        PARTITIONED BY (session_date, device_type)
    """)

    # Session events table (exploded)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.session_events (
            session_id STRING,
            user_id STRING,
            event_index INT,
            event_type STRING,
            event_timestamp TIMESTAMP,
            page STRING,
            duration_on_page INT,
            product_id STRING,
            product_category STRING,
            product_price INT,
            search_query STRING,
            search_result_count INT,
            transaction_id STRING,
            transaction_amount INT,
            event_date DATE
        )
        USING iceberg
        PARTITIONED BY (event_date, event_type)
    """)

    print("Iceberg tables created for sessions")


def process_sessions(spark):
    """Read sessions from Kafka and write to Iceberg."""

    schema = get_session_schema()

    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "session-events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON
    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    # Transform session summary
    session_df = parsed_df \
        .withColumn("started_at", to_timestamp(col("started_at"))) \
        .withColumn("ended_at", to_timestamp(col("ended_at"))) \
        .withColumn("session_date", col("started_at").cast("date")) \
        .withColumn("device_type", col("device.type")) \
        .withColumn("device_os", col("device.os")) \
        .withColumn("device_model", col("device.model")) \
        .withColumn("referrer", col("context.referrer")) \
        .withColumn("utm_source", col("context.utm.source")) \
        .withColumn("utm_medium", col("context.utm.medium")) \
        .withColumn("region", col("context.location.region")) \
        .withColumn("city", col("context.location.city")) \
        .withColumn("is_first_visit", col("context.is_first_visit")) \
        .withColumn("total_events", col("summary.total_events")) \
        .withColumn("unique_products_viewed", col("summary.unique_products_viewed")) \
        .withColumn("searches_count", col("summary.searches_count")) \
        .withColumn("views_count", col("summary.views_count")) \
        .withColumn("add_cart_count", col("summary.add_cart_count")) \
        .withColumn("purchase_count", col("summary.purchase_count")) \
        .withColumn("converted", col("summary.converted")) \
        .withColumn("bounce", col("summary.bounce")) \
        .drop("device", "context", "summary", "events", "user_agent", "metadata")

    # Write session summary
    session_query = session_df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://raw/checkpoints/sessions") \
        .toTable("iceberg.shopping.sessions")

    return session_query


def main():
    print("Starting Session Events Streaming Job...")

    spark = create_spark_session()

    # Create namespace and tables
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.shopping")
    create_iceberg_tables(spark)

    # Start streaming
    query = process_sessions(spark)

    print("Streaming started. Waiting for session data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
