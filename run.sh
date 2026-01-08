#!/bin/bash
# Run script for Kafka-JR

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="$PROJECT_DIR/kafka-jr.jar"
TARGET_DIR="$PROJECT_DIR/target/classes"

# Build if needed
if [ ! -f "$JAR_FILE" ] && [ ! -d "$TARGET_DIR" ]; then
    echo "Building Kafka-JR..."
    bash "$PROJECT_DIR/build.sh"
fi

# Determine classpath
if [ -f "$JAR_FILE" ]; then
    CLASSPATH="$JAR_FILE"
else
    CLASSPATH="$TARGET_DIR"
fi

PORT=${1:-8080}
DATA_DIR=${2:-data/kafka-jr}

echo "=== Starting Kafka-JR Broker ==="
echo "Port: $PORT"
echo "Data Dir: $DATA_DIR"
echo "Classpath: $CLASSPATH"
echo ""

java -cp "$CLASSPATH" com.kafka.jr.broker.KafkaJRBroker --port "$PORT" --data-dir "$DATA_DIR"
