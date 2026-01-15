"""
Airflow DAG: Performance Testing

On-demand DAG to run performance comparison tests.
Demonstrates the benefits of sorted data in Iceberg.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

SPARK_SUBMIT = """
/opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 4g \
    --executor-cores 2 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hadoop \
    --conf spark.sql.catalog.iceberg.warehouse=s3a://iceberg-warehouse/ \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    {script} {args}
"""


with DAG(
    dag_id='performance_test',
    default_args=default_args,
    description='Run performance comparison tests',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['performance', 'test', 'benchmark'],
    params={
        'num_records': 1000000,  # Default 1M records
    }
) as dag:

    start = DummyOperator(task_id='start')

    # Run performance test
    run_performance_test = BashOperator(
        task_id='run_performance_test',
        bash_command=SPARK_SUBMIT.format(
            script='/opt/spark-jobs/batch/performance_test.py',
            args='{{ params.num_records }}'
        ),
    )

    end = DummyOperator(task_id='end')

    start >> run_performance_test >> end
