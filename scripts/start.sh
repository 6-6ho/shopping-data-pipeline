#!/bin/bash

# ==============================================
# Shopping Data Pipeline - Start Script
# ==============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Starting Shopping Data Pipeline"
echo "=========================================="

cd "$PROJECT_DIR/docker"

# Start core services
echo "[1/4] Starting core infrastructure..."
docker compose up -d zookeeper kafka minio minio-init postgres

echo "Waiting for services to be ready..."
sleep 15

# Start Spark cluster
echo "[2/4] Starting Spark cluster..."
docker compose up -d spark-master spark-worker-1

sleep 10

# Start Airflow
echo "[3/4] Starting Airflow..."
docker compose up -d airflow-init
sleep 30
docker compose up -d airflow-webserver airflow-scheduler

# Start Grafana
echo "[4/4] Starting Grafana..."
docker compose up -d grafana

echo ""
echo "=========================================="
echo "Pipeline Started Successfully!"
echo "=========================================="
echo ""
echo "Access URLs:"
echo "  - Spark Master:  http://localhost:8080"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
echo "  - Airflow:       http://localhost:8090 (admin/admin)"
echo "  - Grafana:       http://localhost:3000 (admin/admin)"
echo ""
echo "To start data generator:"
echo "  docker compose --profile generator up -d data-generator"
echo ""
echo "To scale up workers:"
echo "  docker compose --profile scale up -d"
echo ""
