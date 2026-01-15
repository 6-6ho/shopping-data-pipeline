"""
Spark Batch Job: Data Quality Check

Performs data quality checks on ingested data.
Validates completeness, accuracy, and consistency.
"""

import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, when, isnan, isnull,
    lit, countDistinct, min as spark_min, max as spark_max
)


def create_spark_session():
    """Create Spark session."""

    spark = SparkSession.builder \
        .appName("DataQualityCheck") \
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


def create_quality_table(spark):
    """Create data quality results table."""

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.shopping.data_quality (
            check_date DATE,
            table_name STRING,
            check_name STRING,
            check_type STRING,
            status STRING,
            expected_value STRING,
            actual_value STRING,
            details STRING,
            checked_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (check_date)
    """)

    print("Data quality table created")


def log_check_result(spark, results: list, target_date: str):
    """Log check results to quality table."""

    from pyspark.sql import Row

    rows = [Row(
        check_date=datetime.strptime(target_date, "%Y-%m-%d").date(),
        table_name=r['table_name'],
        check_name=r['check_name'],
        check_type=r['check_type'],
        status=r['status'],
        expected_value=str(r.get('expected', '')),
        actual_value=str(r.get('actual', '')),
        details=r.get('details', ''),
        checked_at=datetime.now()
    ) for r in results]

    if rows:
        df = spark.createDataFrame(rows)
        df.writeTo("iceberg.shopping.data_quality").append()


def check_completeness(spark, target_date: str) -> list:
    """Check data completeness - are required fields populated?"""

    results = []

    # Check shopping events
    events_df = spark.read.table("iceberg.shopping.events") \
        .filter(col("event_date") == target_date)

    total_count = events_df.count()

    if total_count == 0:
        results.append({
            'table_name': 'shopping.events',
            'check_name': 'has_data',
            'check_type': 'completeness',
            'status': 'FAIL',
            'expected': '> 0',
            'actual': 0,
            'details': f'No data found for {target_date}'
        })
        return results

    results.append({
        'table_name': 'shopping.events',
        'check_name': 'has_data',
        'check_type': 'completeness',
        'status': 'PASS',
        'expected': '> 0',
        'actual': total_count,
        'details': f'Found {total_count} records'
    })

    # Check null rates for required fields
    required_fields = ['event_id', 'event_type', 'user_id', 'product_id', 'event_timestamp']

    for field in required_fields:
        null_count = events_df.filter(col(field).isNull()).count()
        null_rate = (null_count / total_count) * 100

        status = 'PASS' if null_rate < 1 else 'WARN' if null_rate < 5 else 'FAIL'

        results.append({
            'table_name': 'shopping.events',
            'check_name': f'{field}_null_rate',
            'check_type': 'completeness',
            'status': status,
            'expected': '< 1%',
            'actual': f'{null_rate:.2f}%',
            'details': f'{null_count} nulls out of {total_count}'
        })

    return results


def check_accuracy(spark, target_date: str) -> list:
    """Check data accuracy - are values within expected ranges?"""

    results = []

    events_df = spark.read.table("iceberg.shopping.events") \
        .filter(col("event_date") == target_date)

    if events_df.count() == 0:
        return results

    # Check price range
    price_stats = events_df.agg(
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price")
    ).collect()[0]

    # Price should be positive and reasonable
    if price_stats.min_price is not None:
        status = 'PASS' if price_stats.min_price >= 0 else 'FAIL'
        results.append({
            'table_name': 'shopping.events',
            'check_name': 'price_positive',
            'check_type': 'accuracy',
            'status': status,
            'expected': '>= 0',
            'actual': price_stats.min_price,
            'details': f'Min price: {price_stats.min_price}'
        })

    # Check valid event types
    valid_types = ['search', 'view', 'add_cart', 'purchase']
    invalid_types = events_df \
        .filter(~col("event_type").isin(valid_types)) \
        .count()

    status = 'PASS' if invalid_types == 0 else 'FAIL'
    results.append({
        'table_name': 'shopping.events',
        'check_name': 'valid_event_types',
        'check_type': 'accuracy',
        'status': status,
        'expected': '0 invalid',
        'actual': invalid_types,
        'details': f'{invalid_types} records with invalid event_type'
    })

    # Check timestamp validity (should be within target date)
    out_of_range = events_df \
        .filter(col("event_date") != target_date) \
        .count()

    status = 'PASS' if out_of_range == 0 else 'WARN'
    results.append({
        'table_name': 'shopping.events',
        'check_name': 'timestamp_in_range',
        'check_type': 'accuracy',
        'status': status,
        'expected': '0 out of range',
        'actual': out_of_range,
        'details': f'{out_of_range} records outside target date'
    })

    return results


def check_consistency(spark, target_date: str) -> list:
    """Check data consistency - are there duplicates or inconsistencies?"""

    results = []

    events_df = spark.read.table("iceberg.shopping.events") \
        .filter(col("event_date") == target_date)

    total_count = events_df.count()
    if total_count == 0:
        return results

    # Check for duplicate event IDs
    unique_events = events_df.select("event_id").distinct().count()
    duplicates = total_count - unique_events
    dup_rate = (duplicates / total_count) * 100

    status = 'PASS' if dup_rate < 0.1 else 'WARN' if dup_rate < 1 else 'FAIL'
    results.append({
        'table_name': 'shopping.events',
        'check_name': 'duplicate_events',
        'check_type': 'consistency',
        'status': status,
        'expected': '< 0.1%',
        'actual': f'{dup_rate:.2f}%',
        'details': f'{duplicates} duplicate event_ids'
    })

    # Check purchase events have total_amount
    purchases_without_amount = events_df \
        .filter((col("event_type") == "purchase") & col("total_amount").isNull()) \
        .count()

    total_purchases = events_df.filter(col("event_type") == "purchase").count()

    if total_purchases > 0:
        missing_rate = (purchases_without_amount / total_purchases) * 100
        status = 'PASS' if missing_rate == 0 else 'FAIL'
        results.append({
            'table_name': 'shopping.events',
            'check_name': 'purchase_has_amount',
            'check_type': 'consistency',
            'status': status,
            'expected': '0 missing',
            'actual': purchases_without_amount,
            'details': f'{purchases_without_amount}/{total_purchases} purchases missing amount'
        })

    return results


def check_freshness(spark, target_date: str) -> list:
    """Check data freshness - is data arriving on time?"""

    results = []

    # Check if we have data for target date
    events_df = spark.read.table("iceberg.shopping.events") \
        .filter(col("event_date") == target_date)

    count = events_df.count()

    # Compare with previous day
    prev_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_count = spark.read.table("iceberg.shopping.events") \
        .filter(col("event_date") == prev_date) \
        .count()

    if prev_count > 0:
        change_rate = ((count - prev_count) / prev_count) * 100

        # Alert if more than 50% change
        status = 'PASS' if abs(change_rate) < 50 else 'WARN'
        results.append({
            'table_name': 'shopping.events',
            'check_name': 'volume_change',
            'check_type': 'freshness',
            'status': status,
            'expected': '< 50% change',
            'actual': f'{change_rate:.1f}%',
            'details': f'Today: {count}, Yesterday: {prev_count}'
        })

    return results


def generate_quality_report(results: list) -> str:
    """Generate summary report."""

    total = len(results)
    passed = sum(1 for r in results if r['status'] == 'PASS')
    warned = sum(1 for r in results if r['status'] == 'WARN')
    failed = sum(1 for r in results if r['status'] == 'FAIL')

    report = f"""
========================================
DATA QUALITY REPORT
========================================
Total Checks: {total}
Passed: {passed} ({passed/total*100:.1f}%)
Warnings: {warned} ({warned/total*100:.1f}%)
Failed: {failed} ({failed/total*100:.1f}%)
========================================
"""

    if failed > 0:
        report += "\nFAILED CHECKS:\n"
        for r in results:
            if r['status'] == 'FAIL':
                report += f"  - {r['table_name']}.{r['check_name']}: {r['details']}\n"

    if warned > 0:
        report += "\nWARNINGS:\n"
        for r in results:
            if r['status'] == 'WARN':
                report += f"  - {r['table_name']}.{r['check_name']}: {r['details']}\n"

    return report


def main():
    """Main entry point."""

    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Starting data quality check for {target_date}")

    spark = create_spark_session()
    create_quality_table(spark)

    all_results = []

    try:
        # Run all checks
        all_results.extend(check_completeness(spark, target_date))
        all_results.extend(check_accuracy(spark, target_date))
        all_results.extend(check_consistency(spark, target_date))
        all_results.extend(check_freshness(spark, target_date))

        # Log results
        log_check_result(spark, all_results, target_date)

        # Generate report
        report = generate_quality_report(all_results)
        print(report)

        # Fail job if critical checks failed
        critical_failures = [r for r in all_results if r['status'] == 'FAIL']
        if critical_failures:
            print(f"CRITICAL: {len(critical_failures)} checks failed!")
            sys.exit(1)

    except Exception as e:
        print(f"Error in data quality check: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
