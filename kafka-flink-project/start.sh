#!/bin/bash
# Startup script for the Kafka-Flink-PostgreSQL demo

set -e

echo "========================================"
echo "Kafka-Flink-PostgreSQL Demo Startup"
echo "========================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running"
    echo "Please start Docker Desktop and try again"
    exit 1
fi

# Navigate to project directory
cd "$(dirname "$0")"

echo ""
echo "Step 1: Downloading Flink connector JARs..."
chmod +x flink/download_jars.sh
./flink/download_jars.sh

echo ""
echo "Step 2: Starting all services..."
docker-compose up -d

echo ""
echo "Step 3: Waiting for services to be healthy..."
echo ""

# Wait for Kafka
echo "Waiting for Kafka..."
until docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    sleep 2
done
echo "Kafka is ready!"

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do
    sleep 2
done
echo "PostgreSQL is ready!"

# Wait for ClickHouse
echo "Waiting for ClickHouse..."
until docker-compose exec -T clickhouse clickhouse client -q 'SELECT 1' > /dev/null 2>&1; do
    sleep 2
done
echo "ClickHouse is ready!"

echo ""
echo "Step 4: Creating Kafka topic..."
docker-compose exec -T kafka kafka-topics --create \
    --topic orders \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 2>/dev/null || echo "Topic already exists"

echo ""
echo "========================================"
echo "Services are ready!"
echo "========================================"
echo ""
echo "Flink Web UI:      http://localhost:8081"
echo "Kafka UI:          http://localhost:8080"
echo "Kafka:             localhost:9092"
echo "PostgreSQL:        localhost:5432 (password: postgres)"
echo "ClickHouse:        localhost:8123 (HTTP), localhost:9000 (Native)"
echo ""
echo "To start the Flink job:"
echo "  docker-compose exec jobmanager flink run -py /opt/flink/usrlib/process_orders_table.py -d"
echo ""
echo "To start the PySpark job:"
echo "  docker-compose up -d spark-job"
echo ""
echo "To start the producer:"
echo "  docker-compose up -d producer"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f"
echo ""
echo "To stop everything:"
echo "  docker-compose down"
echo ""
