# Kafka-Flink-PostgreSQL Demo Project

A complete streaming data pipeline demo using Apache Kafka, Apache Flink, and PostgreSQL with Docker Compose.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Producer  │────▶│    Kafka    │────▶│    Flink    │────▶│ PostgreSQL  │
│  (Python)   │     │  (Broker)   │     │ (JobManager │     │   (Sink)    │
└─────────────┘     └─────────────┘     │ TaskManager)│     └─────────────┘
                                         └─────────────┘
```

## Project Structure

```
kafka-flink-project/
├── docker-compose.yml          # All services configuration
├── postgres/
│   └── init.sql                 # Database schema initialization
├── producer/
│   ├── generator.py             # Kafka producer for order data
│   └── requirements.txt         # Python dependencies
├── flink/
│   ├── process_orders.py        # PyFlink job for processing
│   ├── requirements.txt          # Python dependencies
│   ├── download_jars.sh         # Download Flink connectors
│   └── jars/                    # Flink connector JARs
└── README.md                    # This file
```

## Quick Start

### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- Python 3.9+ (for running producer locally)
- 4GB+ RAM available

### Step 1: Start All Services

```bash
# Navigate to project directory
cd kafka-flink-project

# Download required JAR files
chmod +x flink/download_jars.sh
./flink/download_jars.sh

# Start all services
docker-compose up -d
```

### Step 2: Verify Services

Check if all services are running:

```bash
docker-compose ps
```

You should see:
- `zookeeper` - Running
- `kafka` - Running
- `postgres` - Running
- `jobmanager` - Running
- `taskmanager` - Running

### Step 3: Create Kafka Topic

```bash
# Connect to Kafka container
docker-compose exec kafka kafka-topics --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Or simply wait for auto-creation (enabled in docker-compose.yml).

### Step 4: Start the Flink Job

```bash
# Submit the Flink job
docker-compose exec jobmanager flink run \
  -py /opt/flink/usrlib/process_orders.py \
  -d
```

Or use the Python API directly:

```bash
# Install Python dependencies
pip install -r flink/requirements.txt

# Run the Flink job (if running locally with proper setup)
python flink/process_orders.py
```

### Step 5: Start the Producer

Option A - Run locally:

```bash
# Install Python dependencies
pip install -r producer/requirements.txt

# Run the producer
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python producer/generator.py
```

Option B - Use the Docker producer:

```bash
docker-compose up -d producer
```

### Step 6: Verify Data Flow

Check PostgreSQL for processed data:

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U postgres -d orders_db

# View processed orders
SELECT * FROM processed_orders ORDER BY processing_timestamp DESC LIMIT 10;

# View count
SELECT COUNT(*) FROM processed_orders;
```

Check Flink job status:

```bash
# Open Flink Web UI
open http://localhost:8081
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `KAFKA_TOPIC` | `orders` | Kafka topic name |
| `POSTGRES_URL` | `jdbc:postgresql://postgres:5432/orders_db` | PostgreSQL JDBC URL |
| `POSTGRES_USER` | `postgres` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `postgres` | PostgreSQL password |

### Producer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | `5` | Number of orders per batch |
| `INTERVAL` | `2` | Seconds between batches |
| `MAX_ORDERS` | `0` | Max orders (0 = unlimited) |

## Data Flow

1. **Producer** generates random e-commerce orders with:
   - `order_id`, `customer_id`, `product_id`
   - `quantity`, `price`
   - `order_timestamp`, `status`

2. **Kafka** receives orders on topic `orders`

3. **Flink Job** processes each order:
   - Calculates `total_price = quantity * price`
   - Adds `processing_timestamp`
   - Converts `status` to uppercase

4. **PostgreSQL** stores processed orders in `processed_orders` table

## Services

### Kafka
- Port: `9092` (host), `29092` (container)
- Topic: `orders`

### PostgreSQL
- Port: `5432`
- Database: `orders_db`
- Table: `processed_orders`

### Flink
- Web UI: `http://localhost:8081`
- REST API: `http://localhost:8081`

## Troubleshooting

### Check Kafka logs
```bash
docker-compose logs kafka
```

### Check Flink logs
```bash
docker-compose logs jobmanager
docker-compose logs taskmanager
```

### Check PostgreSQL logs
```bash
docker-compose logs postgres
```

### Reset everything
```bash
docker-compose down -v
docker-compose up -d
```

## Stopping the Project

```bash
docker-compose down
```

To remove all data:
```bash
docker-compose down -v
```

## Example Output

Producer output:
```
Published: ORD-A1B2C3D4 - PROD-001 x2 - $199.99 - pending
Published: ORD-E5F6G7H8 - PROD-002 x1 - $699.99 - processing
Published: ORD-I9J0K1L2 - PROD-003 x3 - $449.99 - shipped
```

PostgreSQL query:
```sql
orders_db=# SELECT order_id, total_price, status FROM processed_orders LIMIT 5;
    order_id     | total_price |   status
-----------------+-------------+------------
 ORD-A1B2C3D4    |      399.98 | PENDING
 ORD-E5F6G7H8    |      699.99 | PROCESSING
 ORD-I9J0K1L2   |     1349.97 | SHIPPED
(3 rows)
```

## License

MIT License - Feel free to use this for learning and testing.
