
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

def test_warehouse_path(path_variant, description):
    print(f"\n--- Testing warehouse path: {description} ('{path_variant}') ---")
    try:
        spark = SparkSession.builder \
            .appName("IcebergTest") \
            .master("local[*]") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", path_variant) \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        print("SparkSession created. Attempting to create namespace...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.test_ns")
        print("SUCCESS: Namespace created.")
        spark.stop()
    except Exception as e:
        print(f"FAILURE: {e}")

if __name__ == "__main__":
    # Test 1: With trailing slash
    test_warehouse_path("s3a://iceberg-warehouse/", "With Trailing Slash")
    
    # Test 2: Without trailing slash
    test_warehouse_path("s3a://iceberg-warehouse", "Without Trailing Slash")
    
    # Test 3: With subpath
    test_warehouse_path("s3a://iceberg-warehouse/wh/", "Subpath With Slash")
