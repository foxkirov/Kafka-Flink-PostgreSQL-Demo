# AGENT GUIDANCE: Kafka-Flink-PostgreSQL Demo Project

This document provides essential, non-obvious guidance for OpenCode agents working with this repository. Prioritize executable sources of truth (e.g., `docker-compose.yml`, `.sh` scripts) over descriptive prose.

## 1. Project Overview & Architecture

*   **Core Components**: Apache Kafka, Apache Flink (PyFlink & Java Flink), PostgreSQL.
*   **Orchestration**: Docker Compose (`docker-compose.yml`).
*   **Data Flow**: `producer/generator.py` (Python) -> Kafka (`orders` topic) -> `flink/process_orders.py` (PyFlink job) -> PostgreSQL (`processed_orders` table).
*   **Java Flink**: The `flink-java/` directory contains an alternative Java-based Flink job. The `docker-compose.yml` includes services for building (`flink-java-build`) and submitting (`flink-java-job`) this Java job.

## 2. Key Developer Commands & Workflow

### 2.1. Full System Startup (Recommended)

*   **Automated Start**: Use `./start.sh` to initialize, start all Docker services, wait for them to be healthy, and create the Kafka topic.
    ```bash
    ./start.sh
    ```
*   **Manual Steps (if not using `start.sh`)**:
    1.  Download Flink connector JARs: `./flink/download_jars.sh`
    2.  Start Docker services: `docker-compose up -d`
    3.  Create Kafka topic (optional, auto-creation is enabled but `start.sh` does it explicitly):
        `docker-compose exec kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`

### 2.2. Running Flink Jobs

*   **PyFlink Job Submission**:
    ```bash
    docker-compose exec jobmanager flink run -py /opt/flink/usrlib/process_orders.py -d
    ```
    (Note: `flink/` directory is mounted to `/opt/flink/usrlib` in Flink containers.)

*   **Java Flink Job (Requires Build)**:
    1.  Build the Java Flink job (usually handled by `flink-java-build` service at startup):
        `docker-compose run --rm flink-java-build`
    2.  Submit the Java Flink job:
        `docker-compose exec jobmanager flink run -c com.flink.orderprocessor.OrderProcessor /opt/flink/usrlib/java/flink-order-processor.jar -d`
        (Note: Java JARs are typically found in `/opt/flink/usrlib/java/`)

### 2.3. Running Kafka Producer

*   **Dockerized Producer (Recommended)**:
    ```bash
    docker-compose up -d producer
    ```
*   **Local Python Producer**:
    1.  Install dependencies: `pip install -r producer/requirements.txt`
    2.  Run: `KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python producer/generator.py`

### 2.4. Verification & Debugging

*   **Service Status**: `docker-compose ps`
*   **Flink Web UI**: Access at `http://localhost:8081`
*   **Kafka UI**: Access at `http://localhost:8080`
*   **PostgreSQL Data Check**:
    ```bash
    docker-compose exec postgres psql -U postgres -d orders_db
    # Then inside psql:
    SELECT * FROM processed_orders ORDER BY processing_timestamp DESC LIMIT 10;
    SELECT COUNT(*) FROM processed_orders;
    ```
*   **View Service Logs**: `docker-compose logs <service_name>` (e.g., `kafka`, `jobmanager`, `postgres`).

### 2.5. Stopping & Resetting

*   **Stop Services**: `docker-compose down`
*   **Reset All Data (Stop & Remove Volumes)**: `docker-compose down -v`

## 3. Environment Variables (Common Defaults)

These are set in `docker-compose.yml` and are internal to the Docker network unless explicitly exposed:

*   `KAFKA_BOOTSTRAP_SERVERS`: `kafka:29092` (internal to Docker network), `localhost:9092` (for local access)
*   `KAFKA_TOPIC`: `orders`
*   `POSTGRES_DB`: `orders_db`
*   `POSTGRES_USER`: `postgres`
*   `POSTGRES_PASSWORD`: `postgres`

## 4. Directory Mappings & Quirks

*   `./flink` is mounted as `/opt/flink/usrlib` in Flink Job/Task Managers. This is where PyFlink scripts and any downloaded JARs (via `download_jars.sh`) are located for Flink to pick up.
*   `./flink-java/target` is mounted to `/opt/flink/usrlib/java` for compiled Java Flink jobs.
*   `./producer` is mounted as `/app` in the `producer` service.
