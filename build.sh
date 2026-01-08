#!/bin/bash
# Build script for Kafka-JR

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$PROJECT_DIR/src/main/java"
TARGET_DIR="$PROJECT_DIR/target/classes"
JAR_FILE="$PROJECT_DIR/kafka-jr.jar"

echo "=== Building Kafka-JR ==="
echo "Source: $SOURCE_DIR"
echo "Target: $TARGET_DIR"

# Create target directory
mkdir -p "$TARGET_DIR"

# Compile all Java files
echo "Compiling Java files..."
find "$SOURCE_DIR" -name "*.java" -print0 | xargs -0 javac -d "$TARGET_DIR" -sourcepath "$SOURCE_DIR"

echo "✓ Compilation successful"

# Create JAR
echo "Creating JAR..."
cd "$TARGET_DIR"
jar cfm "$JAR_FILE" /dev/null com/
cd "$PROJECT_DIR"

echo "✓ JAR created: $JAR_FILE"
echo ""
echo "To run:"
echo "  java -cp $JAR_FILE com.kafka.jr.broker.KafkaJRBroker --port 8080 --data-dir data/kafka-jr"
