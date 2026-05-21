# Kafka-Flink-PostgreSQL Demo Project

A complete streaming data pipeline demo using Apache Kafka, Apache Flink, and PostgreSQL with Docker Compose.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Producer  │────▶│    Kafka    │────▶│    Flink    │────▶│ PostgreSQL  │
│  (Python)   │     │  (Broker)   │     │ (JobManager │     │   (Sink)    │
│             │     │             │     │ TaskManager)│     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

## Project Structure

```
kafka-flink-project/
├── start.sh                    # Automated startup script (RECOMMENDED)
├── docker-compose.yml          # Infrastructure orchestration
├── postgres/
│   └── init.sql                # DB schema initialization
├── producer/
│   ├── generator.py            # Python-based Kafka producer
│   └── requirements.txt
├── flink/                      # PyFlink Implementation
│   ├── process_orders_table.py  # Table API version
│   ├── process_orders.py        # DataStream API version
│   └── download_jars.sh        # Connector downloader
└── flink-java/                 # Java Flink Implementation
    ├── src/                    # Java source code (POJOs, Deserializers)
    ├── pom.xml                 # Maven configuration
    └── Dockerfile              # Multi-stage build for Java job
```

## Quick Start

### Prerequisites
- Docker Desktop
- 6GB+ RAM allocated to Docker
- Python 3.9+ (optional, for local producer)

### Step 1: Start Everything Automatically
The included `start.sh` script handles JAR downloads, service orchestration, health checks, and topic creation.
```bash
chmod +x start.sh
./start.sh
```

### Step 2: Choose Your Flink Job
You can run either the Python or the Java implementation.

**Option A: Java (High Performance)**
The Java job is built automatically via the `flink-java-build` service. To submit it:
```bash
docker-compose up -d flink-java-job
```

**Option B: Python (PyFlink)**
```bash
docker-compose exec jobmanager flink run \
  -py /opt/flink/usrlib/process_orders_table.py \
  -d
```

### Step 3: Start the Data Producer
```bash
docker-compose up -d producer
```

## Verification

### 1. Flink Web UI
Access `http://localhost:8081` to monitor job status and checkpoints.

### 2. PostgreSQL Data
Verify that processed orders are arriving in the database:
```bash
docker-compose exec postgres psql -U postgres -d orders_db -c "SELECT * FROM processed_orders LIMIT 10;"
```

## Troubleshooting
- **ARM64 (Apple Silicon):** The project is optimized to use `eclipse-temurin` based Maven images to avoid "no matching manifest" errors.
- **Memory:** If services fail to start, ensure Docker has at least 6GB of RAM.
- **Reset:** To wipe all data and start fresh: `docker-compose down -v`.
