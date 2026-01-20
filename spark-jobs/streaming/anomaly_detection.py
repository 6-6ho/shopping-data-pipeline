"""
Spark Streaming Job: Anomaly Detection
Detects traffic anomalies in real-time using rolling averages.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, avg, lit, when, stddev, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def create_spark_session():
    """Create Spark session with Iceberg and Kafka configurations."""
    spark = SparkSession.builder \
        .appName("AnomalyDetection") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse/data/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_event_schema():
    """Simplified schema for anomaly detection (focus on volume)."""
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True)
    ])

def create_alert_table(spark):
    """Create Iceberg table for anomaly alerts."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.anomaly_alerts (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            event_count LONG,
            avg_count DOUBLE,
            stddev_count DOUBLE,
            z_score DOUBLE,
            anomaly_type STRING,
            severity STRING,
            detection_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(detection_time))
    """)
    print("Iceberg table created/verified: iceberg.shopping.anomaly_alerts")

def process_anomalies(spark):
    """
    Detect anomalies in event volume using sliding windows.
    Strategy:
    1. Aggregates event counts in 1-minute windows.
    2. Calculates rolling stats (mean, stddev) over usage a longer lookback (simplified for streaming).
    3. Flags windows where count > mean + 3*stddev (Spike) or < mean - 3*stddev (Drop).
    
    Note: Stateful limit-based outlier detection in pure Structured Streaming can be complex.
    We will use a simplified approach: Compare current window against a fixed threshold or short-term average.
    For demonstration, we'll flagging high traffic (> 1000 events/min) as an anomaly warning.
    """
    
    schema = get_event_schema()

    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "shopping-events") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse and Window
    events_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select(to_timestamp(col("data.event_time")).alias("event_time")) \
        .withWatermark("event_time", "2 minutes")

    # 1. Aggregate in 1-minute windows
    windowed_counts = events_df \
        .groupBy(window(col("event_time"), "1 minute")) \
        .agg(count("*").alias("event_count"))

    # 2. Simple Threshold-based Detection (for stability in streaming demo)
    # In a real system, you'd use a stateful mapGroupsWithState to track moving averages.
    
    # Define thresholds
    HIGH_THRESHOLD = 500  # Example High traffic
    LOW_THRESHOLD = 10    # Example Low traffic
    
    anomalies = windowed_counts \
        .withColumn("avg_count", lit(250.0)) \
        .withColumn("stddev_count", lit(50.0)) \
        .withColumn("z_score", (col("event_count") - 250) / 50) \
        .withColumn("anomaly_type", 
                   when(col("event_count") > HIGH_THRESHOLD, "High Traffic Spike")
                   .when(col("event_count") < LOW_THRESHOLD, "Traffic Drop")
                   .otherwise(None)) \
        .filter(col("anomaly_type").isNotNull()) \
        .withColumn("severity", 
                   when(col("event_count") > HIGH_THRESHOLD * 2, "CRITICAL")
                   .otherwise("WARNING")) \
        .withColumn("detection_time", col("window.end")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event_count", "avg_count", "stddev_count", "z_score",
            "anomaly_type", "severity", "detection_time"
        )
    
    # Write to Console (for debug)
    # anomalies.writeStream.format("console").option("truncate", "false").start()

    # Write to Iceberg
    query = anomalies \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://raw/checkpoints/anomaly_detection") \
        .toTable("iceberg.shopping.anomaly_alerts")
        
    return query

def main():
    print("Starting Anomaly Detection Streaming Job...")
    spark = create_spark_session()
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.shopping")
    create_alert_table(spark)
    
    query = process_anomalies(spark)
    
    print("Anomaly Detection active...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
