"""
Spark Batch Job: Performance Testing

Compares query performance between sorted and unsorted data.
Used to demonstrate the benefits of data organization in Iceberg.
"""

import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand


def create_spark_session():
    """Create Spark session."""

    spark = SparkSession.builder \
        .appName("PerformanceTest") \
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

    return spark


def create_test_tables(spark):
    """Create test tables for performance comparison."""

    # Unsorted table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.perf_test.events_unsorted (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            product_id STRING,
            category STRING,
            price INT,
            event_timestamp TIMESTAMP,
            event_date DATE
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)

    # Sorted by timestamp
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.perf_test.events_sorted_time (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            product_id STRING,
            category STRING,
            price INT,
            event_timestamp TIMESTAMP,
            event_date DATE
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)

    # Sorted by product_id + timestamp
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.perf_test.events_sorted_product (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            product_id STRING,
            category STRING,
            price INT,
            event_timestamp TIMESTAMP,
            event_date DATE
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)

    print("Test tables created")


def generate_test_data(spark, num_records: int = 10000000):
    """Generate test data."""

    print(f"Generating {num_records:,} test records...")

    # Generate random data
    df = spark.range(num_records) \
        .withColumn("event_id", col("id").cast("string")) \
        .withColumn("event_type",
            (col("id") % 4).cast("string")) \
        .withColumn("user_id",
            (col("id") % 100000).cast("string")) \
        .withColumn("product_id",
            (col("id") % 10000).cast("string")) \
        .withColumn("category",
            (col("id") % 5).cast("string")) \
        .withColumn("price",
            (rand() * 1000000).cast("int")) \
        .withColumn("event_timestamp",
            (col("id") * 1000 + 1609459200000).cast("timestamp")) \
        .withColumn("event_date",
            col("event_timestamp").cast("date")) \
        .drop("id")

    return df


def load_test_data(spark, df):
    """Load data into test tables with different sorting strategies."""

    print("Loading unsorted data...")
    start = time.time()
    df.writeTo("iceberg.perf_test.events_unsorted") \
        .option("write-format", "parquet") \
        .append()
    unsorted_time = time.time() - start
    print(f"Unsorted load time: {unsorted_time:.2f}s")

    print("Loading data sorted by timestamp...")
    start = time.time()
    df.orderBy("event_timestamp") \
        .writeTo("iceberg.perf_test.events_sorted_time") \
        .option("write-format", "parquet") \
        .append()
    sorted_time_time = time.time() - start
    print(f"Time-sorted load time: {sorted_time_time:.2f}s")

    print("Loading data sorted by product_id + timestamp...")
    start = time.time()
    df.orderBy("product_id", "event_timestamp") \
        .writeTo("iceberg.perf_test.events_sorted_product") \
        .option("write-format", "parquet") \
        .append()
    sorted_product_time = time.time() - start
    print(f"Product-sorted load time: {sorted_product_time:.2f}s")

    return {
        'unsorted': unsorted_time,
        'sorted_time': sorted_time_time,
        'sorted_product': sorted_product_time
    }


def run_performance_tests(spark) -> dict:
    """Run performance comparison queries."""

    results = {}

    # Test queries
    queries = {
        'time_range': """
            SELECT COUNT(*), SUM(price)
            FROM {table}
            WHERE event_timestamp BETWEEN '2021-01-01' AND '2021-01-02'
        """,
        'product_lookup': """
            SELECT COUNT(*), SUM(price)
            FROM {table}
            WHERE product_id = '1234'
        """,
        'product_time_range': """
            SELECT COUNT(*), SUM(price)
            FROM {table}
            WHERE product_id = '1234'
            AND event_timestamp BETWEEN '2021-01-01' AND '2021-06-01'
        """,
        'aggregation': """
            SELECT category, COUNT(*), SUM(price)
            FROM {table}
            GROUP BY category
        """,
    }

    tables = [
        'iceberg.perf_test.events_unsorted',
        'iceberg.perf_test.events_sorted_time',
        'iceberg.perf_test.events_sorted_product'
    ]

    for query_name, query_template in queries.items():
        results[query_name] = {}

        for table in tables:
            table_short = table.split('.')[-1]
            query = query_template.format(table=table)

            # Warm up
            spark.sql(query).collect()

            # Timed run (average of 3)
            times = []
            for _ in range(3):
                start = time.time()
                spark.sql(query).collect()
                times.append(time.time() - start)

            avg_time = sum(times) / len(times)
            results[query_name][table_short] = avg_time

            print(f"{query_name} on {table_short}: {avg_time:.3f}s")

    return results


def generate_report(results: dict) -> str:
    """Generate performance comparison report."""

    report = """
================================================================================
PERFORMANCE TEST RESULTS
================================================================================

Query Performance Comparison (seconds):
"""

    # Table header
    report += f"{'Query':<25} {'Unsorted':<15} {'Time-sorted':<15} {'Product-sorted':<15} {'Improvement':<15}\n"
    report += "-" * 85 + "\n"

    for query_name, times in results.items():
        unsorted = times.get('events_unsorted', 0)
        sorted_time = times.get('events_sorted_time', 0)
        sorted_product = times.get('events_sorted_product', 0)

        best = min(sorted_time, sorted_product)
        improvement = ((unsorted - best) / unsorted * 100) if unsorted > 0 else 0

        report += f"{query_name:<25} {unsorted:<15.3f} {sorted_time:<15.3f} {sorted_product:<15.3f} {improvement:<15.1f}%\n"

    report += """
================================================================================
KEY FINDINGS:
================================================================================
"""

    # Find best improvements
    for query_name, times in results.items():
        unsorted = times.get('events_unsorted', 0)
        best_table = min(times.items(), key=lambda x: x[1])
        if unsorted > 0:
            improvement = ((unsorted - best_table[1]) / unsorted * 100)
            report += f"- {query_name}: {improvement:.1f}% faster with {best_table[0]}\n"

    return report


def save_results(spark, results: dict, load_times: dict):
    """Save results to Iceberg table."""

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.perf_test.benchmark_results (
            run_timestamp TIMESTAMP,
            query_name STRING,
            table_name STRING,
            execution_time_seconds DOUBLE,
            load_time_seconds DOUBLE
        )
        USING iceberg
    """)

    from pyspark.sql import Row
    rows = []
    run_time = datetime.now()

    for query_name, times in results.items():
        for table_name, exec_time in times.items():
            rows.append(Row(
                run_timestamp=run_time,
                query_name=query_name,
                table_name=table_name,
                execution_time_seconds=exec_time,
                load_time_seconds=load_times.get(table_name.replace('events_', ''), 0)
            ))

    if rows:
        df = spark.createDataFrame(rows)
        df.writeTo("iceberg.perf_test.benchmark_results").append()
        print("Results saved to iceberg.perf_test.benchmark_results")


def main():
    """Main entry point."""

    num_records = int(sys.argv[1]) if len(sys.argv) > 1 else 1000000

    print(f"Starting performance test with {num_records:,} records")

    spark = create_spark_session()

    # Create namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.perf_test")

    # Create tables
    create_test_tables(spark)

    # Generate and load data
    df = generate_test_data(spark, num_records)
    load_times = load_test_data(spark, df)

    # Run performance tests
    print("\nRunning performance tests...")
    results = run_performance_tests(spark)

    # Generate report
    report = generate_report(results)
    print(report)

    # Save results
    save_results(spark, results, load_times)

    spark.stop()


if __name__ == "__main__":
    main()
