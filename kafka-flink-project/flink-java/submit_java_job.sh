#!/bin/bash
# Script to build and submit the Flink Java job

# Get directory where script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# 1. Build the project using Docker
echo "Building Maven project..."
docker-compose up flink-java-build

# 2. Submit the job to JobManager
FLINK_JOBMANAGER="localhost:8081"
JAR_FILE="/opt/flink/usrlib/java/flink-order-processor-1.0-SNAPSHOT.jar"

echo "Submitting Flink Java job..."
docker exec jobmanager flink run \
    -m jobmanager:8081 \
    -c com.flink.orderprocessor.OrderProcessor \
    $JAR_FILE \
    -d

echo ""
echo "Job submitted. Check status at http://localhost:8081"
