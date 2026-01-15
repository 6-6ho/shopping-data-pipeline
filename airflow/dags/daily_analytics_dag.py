"""
Airflow DAG: Daily Analytics Pipeline

Schedules daily batch jobs for data aggregation and analysis.
Runs after midnight to process the previous day's data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup


# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Spark submit command template
SPARK_SUBMIT = """
/opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
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
    dag_id='daily_analytics_pipeline',
    default_args=default_args,
    description='Daily batch processing for shopping analytics',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analytics', 'daily', 'batch'],
) as dag:

    # Start marker
    start = DummyOperator(task_id='start')

    # Data Quality Check
    data_quality_check = BashOperator(
        task_id='data_quality_check',
        bash_command=SPARK_SUBMIT.format(
            script='/opt/spark-jobs/batch/data_quality.py',
            args='{{ ds }}'
        ),
    )

    # Daily Aggregations
    with TaskGroup(group_id='daily_aggregations') as aggregations:

        daily_summary = BashOperator(
            task_id='daily_summary',
            bash_command=SPARK_SUBMIT.format(
                script='/opt/spark-jobs/batch/daily_aggregation.py',
                args='{{ ds }}'
            ),
        )

        review_analysis = BashOperator(
            task_id='review_analysis',
            bash_command=SPARK_SUBMIT.format(
                script='/opt/spark-jobs/batch/review_analysis.py',
                args='{{ ds }}'
            ),
        )

        # These can run in parallel
        [daily_summary, review_analysis]

    # End marker
    end = DummyOperator(task_id='end')

    # Define dependencies
    start >> data_quality_check >> aggregations >> end
