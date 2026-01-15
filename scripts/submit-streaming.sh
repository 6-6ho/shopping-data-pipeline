#!/bin/bash

# ==============================================
# Submit Spark Streaming Jobs
# ==============================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

JOB=${1:-shopping}

echo "Submitting Spark Streaming job: $JOB"

case $JOB in
    shopping)
        SCRIPT="streaming/shopping_stream.py"
        ;;
    reviews)
        SCRIPT="streaming/reviews_stream.py"
        ;;
    sessions)
        SCRIPT="streaming/session_stream.py"
        ;;
    aggregation)
        SCRIPT="streaming/realtime_aggregation.py"
        ;;
    *)
        echo "Unknown job: $JOB"
        echo "Usage: $0 [shopping|reviews|sessions|aggregation]"
        exit 1
        ;;
esac

docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hadoop \
    --conf spark.sql.catalog.iceberg.warehouse=s3a://iceberg-warehouse/ \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    /opt/spark-jobs/$SCRIPT

echo "Job submitted."
