#!/bin/bash
# Build script for Kafka-JR

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$PROJECT_DIR/src/main/java"
TARGET_DIR="$PROJECT_DIR/target/classes"

echo "Building kafka-jr..."

# Create target directory
mkdir -p "$TARGET_DIR"

# Compile all Java files
if find "$SOURCE_DIR" -name "*.java" -print0 | xargs -0 javac -d "$TARGET_DIR" -sourcepath "$SOURCE_DIR" 2>/dev/null; then
    echo "Build successful"
    echo ""
    echo "Next: bash run.sh"
else
    echo "Build failed. Check Java installation and source files."
    exit 1
fi
