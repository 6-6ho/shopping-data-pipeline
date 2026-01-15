#!/bin/bash

# ==============================================
# Shopping Data Pipeline - Stop Script
# ==============================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Stopping Shopping Data Pipeline..."

cd "$PROJECT_DIR/docker"

docker compose --profile generator --profile scale down

echo "Pipeline stopped."
