"""
Spark Batch Job: Cohort Analysis
Calculates weekly retention rates for user cohorts based on their first visit.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, date_trunc, datediff, count, lit, when
from pyspark.sql.window import Window
import sys
from datetime import datetime, timedelta

def create_spark_session():
    """Create Spark session with Iceberg configurations."""
    spark = SparkSession.builder \
        .appName("CohortAnalysis") \
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

def create_cohort_table(spark):
    """Create Iceberg table for cohort retention results."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.cohort_retention (
            cohort_week DATE,
            cohort_size LONG,
            period_week INT,
            retained_users LONG,
            retention_rate FLOAT,
            calculation_date DATE
        )
        USING iceberg
        PARTITIONED BY (cohort_week)
    """)
    print("Iceberg table created/verified: iceberg.shopping.cohort_retention")

def calculate_cohort_retention(spark, target_date_str):
    """
    Calculate weekly retention for cohorts.
    
    Logic:
    1. Identify each user's first visit date (Cohort Date).
    2. Group users into weekly cohorts (Cohort Week).
    3. Calculate active weeks for each user relative to their cohort week.
    4. Aggregate retention counts per cohort per week.
    """
    print(f"Calculating cohort retention up to {target_date_str}")
    
    # 1. Load user activity data (From events table)
    # Using 'view' or 'purchase' events as activity
    events_df = spark.read.table("iceberg.shopping.events") \
        .filter(col("event_type").isin("view", "purchase")) \
        .select("user_id", "event_time") \
        .withColumn("event_date", col("event_time").cast("date"))
        
    if not events_df.stat.count():
        print("No events found. Skipping analysis.")
        return

    # 2. Determine User's First Visit (Cohort Definition)
    user_first_visit = events_df.groupBy("user_id") \
        .agg(min("event_date").alias("first_visit_date")) \
        .withColumn("cohort_week", date_trunc("week", col("first_visit_date")).cast("date"))

    # 3. Join events with user cohort info
    activity_with_cohort = events_df.join(user_first_visit, "user_id") \
        .withColumn("activity_week", date_trunc("week", col("event_date")).cast("date")) \
        .withColumn("period_week", 
                   (datediff(col("activity_week"), col("cohort_week")) / 7).cast("int")) \
        .filter(col("period_week") >= 0)  # Filter out data anomalies
    
    # 4. Calculate Cohort Size (Total users in each cohort)
    cohort_sizes = user_first_visit.groupBy("cohort_week") \
        .agg(count("user_id").alias("cohort_size"))
    
    # 5. Calculate Retained Users per Period
    retention_counts = activity_with_cohort.groupBy("cohort_week", "period_week") \
        .agg(count("user_id").alias("retained_users_raw")) # Note: This counts events, need distinct users
        
    # Correcting step 5 for Distinct Users
    retention_counts = activity_with_cohort.groupBy("cohort_week", "period_week") \
        .agg(count("user_id").alias("retained_users")) # This is still event count if not distinct
        
    # Rewrite Step 5 with distinct user count
    retention_counts = activity_with_cohort.groupBy("cohort_week", "period_week") \
        .agg(count("user_id").distinct().alias("retained_users"))

    # 6. Join to calculate rates
    final_metrics = retention_counts.join(cohort_sizes, "cohort_week") \
        .withColumn("retention_rate", col("retained_users") / col("cohort_size")) \
        .withColumn("calculation_date", lit(target_date_str).cast("date")) \
        .select("cohort_week", "cohort_size", "period_week", "retained_users", "retention_rate", "calculation_date") \
        .orderBy("cohort_week", "period_week")

    # 7. Write to Iceberg
    final_metrics.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("iceberg.shopping.cohort_retention")
        
    print("Cohort analysis saved to Iceberg.")

    # 8. Write to Postgres (Dashboard DB)
    print("Writing to Postgres...")
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    props = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        final_metrics.write \
            .jdbc(url=jdbc_url, table="analytics_cohort_retention", mode="overwrite", properties=props)
        print("Cohort analysis synced to Postgres.")
    except Exception as e:
        print(f"Failed to write to Postgres: {e}")
        print("Ensure Postgres is running and accessible.")
    
    final_metrics.show(20)

def main():
    if len(sys.argv) < 2:
        target_date = datetime.now().strftime("%Y-%m-%d")
    else:
        target_date = sys.argv[1]

    spark = create_spark_session()
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.shopping")
    create_cohort_table(spark)
    
    calculate_cohort_retention(spark, target_date)
    spark.stop()

if __name__ == "__main__":
    main()
