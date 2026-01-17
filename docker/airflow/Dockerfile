FROM apache/airflow:2.8.0-python3.10

USER root

# Install Java for Spark
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install additional Python packages - keep it minimal to avoid conflicts
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.0 \
    pyspark==3.5.0 \
    boto3
