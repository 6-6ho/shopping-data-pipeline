"""
Spark Streaming Job: Reviews

Reads review events from Kafka and writes to Iceberg tables.
Handles unstructured text data (review content).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, length, size,
    when, regexp_extract, lower
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    ArrayType, BooleanType
)


def create_spark_session():
    """Create Spark session with Iceberg and Kafka configurations."""

    spark = SparkSession.builder \
        .appName("ReviewsStreaming") \
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


def get_review_schema():
    """Define schema for review events."""

    return StructType([
        StructField("review_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("text", StringType(), True),
        StructField("images", ArrayType(StringType()), True),
        StructField("helpful_count", IntegerType(), True),
        StructField("created_at", StringType(), True),
        StructField("verified_purchase", BooleanType(), True)
    ])


def create_iceberg_table(spark):
    """Create Iceberg table for reviews if not exists."""

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.reviews (
            review_id STRING,
            product_id STRING,
            user_id STRING,
            rating INT,
            review_text STRING,
            text_length INT,
            image_count INT,
            has_images BOOLEAN,
            helpful_count INT,
            verified_purchase BOOLEAN,
            created_at TIMESTAMP,
            created_date DATE,
            sentiment_keywords ARRAY<STRING>
        )
        USING iceberg
        PARTITIONED BY (created_date, rating)
    """)

    print("Iceberg table created/verified: iceberg.shopping.reviews")


def extract_sentiment_keywords(text_col):
    """Extract sentiment keywords from review text."""

    # Define keyword patterns
    positive_keywords = ['좋아요', '최고', '만족', '추천', '재구매', '빠르', '괜찮', '예뻐']
    negative_keywords = ['별로', '실망', '늦', '아쉬', '환불', '싸구려', '안좋']

    # This is a simplified extraction - in production, use proper NLP
    keywords_pattern = '|'.join(positive_keywords + negative_keywords)

    return regexp_extract(lower(text_col), f'({keywords_pattern})', 1)


def process_reviews(spark):
    """Read reviews from Kafka and write to Iceberg."""

    schema = get_review_schema()

    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "reviews") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON
    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    # Transform
    transformed_df = parsed_df \
        .withColumn("created_at", to_timestamp(col("created_at"))) \
        .withColumn("created_date", col("created_at").cast("date")) \
        .withColumn("text_length", length(col("text"))) \
        .withColumn("image_count", size(col("images"))) \
        .withColumn("has_images", size(col("images")) > 0) \
        .withColumnRenamed("text", "review_text") \
        .drop("images")  # Store image refs separately if needed

    # Add empty sentiment_keywords for now (batch job will populate)
    from pyspark.sql.functions import array, lit
    transformed_df = transformed_df.withColumn("sentiment_keywords", array())

    # Write to Iceberg
    query = transformed_df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://raw/checkpoints/reviews") \
        .toTable("iceberg.shopping.reviews")

    return query


def main():
    print("Starting Reviews Streaming Job...")

    spark = create_spark_session()

    # Create namespace and table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.shopping")
    create_iceberg_table(spark)

    # Start streaming
    query = process_reviews(spark)

    print("Streaming started. Waiting for review data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
