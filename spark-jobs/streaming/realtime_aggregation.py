"""
Spark Streaming Job: Real-time Aggregation

Performs windowed aggregations on shopping events for real-time dashboards.
Outputs to a separate table for Grafana to query.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, sum as spark_sum,
    avg, max as spark_max, min as spark_min, approx_count_distinct,
    current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)


def create_spark_session():
    """Create Spark session."""

    spark = SparkSession.builder \
        .appName("RealtimeAggregation") \
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


def get_event_schema():
    """Simplified schema for aggregation."""

    device_schema = StructType([
        StructField("type", StringType(), True),
        StructField("os", StringType(), True)
    ])

    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("total_amount", IntegerType(), True),
        StructField("device", device_schema, True)
    ])


def create_aggregation_tables(spark):
    """Create tables for aggregated metrics."""

    # 5-minute window aggregations
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.metrics_5min (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            total_events LONG,
            unique_users LONG,
            total_revenue LONG,
            purchase_count LONG,
            view_count LONG,
            cart_count LONG,
            search_count LONG,
            avg_order_value DOUBLE,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(window_start))
    """)

    # Category metrics
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.metrics_category_5min (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            category STRING,
            event_count LONG,
            revenue LONG,
            unique_products LONG,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(window_start), category)
    """)

    print("Aggregation tables created")


def run_overall_aggregation(spark):
    """Run overall metrics aggregation."""

    schema = get_event_schema()

    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "shopping-events") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse and prepare
    events_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .withWatermark("event_timestamp", "1 minute")

    # 5-minute window aggregation
    agg_df = events_df \
        .groupBy(window(col("event_timestamp"), "5 minutes")) \
        .agg(
            count("*").alias("total_events"),
            approx_count_distinct("user_id").alias("unique_users"),
            spark_sum(
                col("total_amount")
            ).alias("total_revenue"),
            spark_sum(
                (col("event_type") == "purchase").cast("long")
            ).alias("purchase_count"),
            spark_sum(
                (col("event_type") == "view").cast("long")
            ).alias("view_count"),
            spark_sum(
                (col("event_type") == "add_cart").cast("long")
            ).alias("cart_count"),
            spark_sum(
                (col("event_type") == "search").cast("long")
            ).alias("search_count")
        ) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .withColumn("avg_order_value",
            col("total_revenue") / col("purchase_count")
        ) \
        .withColumn("updated_at", current_timestamp()) \
        .drop("window")

    # Write to Iceberg
    query = agg_df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://raw/checkpoints/metrics-5min") \
        .toTable("iceberg.shopping.metrics_5min")

    return query


def run_category_aggregation(spark):
    """Run category-level metrics aggregation."""

    schema = get_event_schema()

    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "shopping-events") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse and prepare
    events_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .withWatermark("event_timestamp", "1 minute")

    # Category aggregation
    cat_agg_df = events_df \
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("category")
        ) \
        .agg(
            count("*").alias("event_count"),
            spark_sum(col("total_amount")).alias("revenue"),
            approx_count_distinct("product_id").alias("unique_products")
        ) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .withColumn("updated_at", current_timestamp()) \
        .drop("window")

    # Write to Iceberg
    query = cat_agg_df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://raw/checkpoints/metrics-category-5min") \
        .toTable("iceberg.shopping.metrics_category_5min")

    return query


def main():
    print("Starting Real-time Aggregation Job...")

    spark = create_spark_session()

    # Create tables
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.shopping")
    create_aggregation_tables(spark)

    # Start both aggregations
    overall_query = run_overall_aggregation(spark)
    # category_query = run_category_aggregation(spark)  # Run separately if needed

    print("Real-time aggregation started...")
    overall_query.awaitTermination()


if __name__ == "__main__":
    main()
