#!/bin/bash
# Run script for Kafka-JR

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="$PROJECT_DIR/target/classes"
DATA_DIR="$PROJECT_DIR/data/kafka-jr"
PORT=${1:-8080}

# Auto-build if needed
if [ ! -d "$TARGET_DIR" ]; then
    echo "Classes not found. Building..."
    bash "$PROJECT_DIR/build.sh"
    echo ""
fi

# Ensure data directory exists
mkdir -p "$DATA_DIR"

echo "Starting broker on http://localhost:$PORT"
echo "Data stored in: $DATA_DIR"
echo ""
echo "Test: curl http://localhost:$PORT/health"
echo "Stop: Press Ctrl+C"
echo ""

java -cp "$TARGET_DIR" com.kafka.jr.broker.KafkaJRBroker --port "$PORT" --data-dir "$DATA_DIR"
