#!/usr/bin/env python3
"""
PyFlink Job - Process orders from Kafka and write to PostgreSQL

This job:
1. Reads raw orders from Kafka topic "orders"
2. Transforms data:
   - Calculates total_price = quantity * price
   - Adds processing_timestamp
   - Converts status to uppercase
3. Writes transformed data to PostgreSQL table "processed_orders"

Requirements:
- flink-sql-connector-kafka_2.12-1.17.1.jar
- flink-connector-jdbc-1.17.1.jar
- postgresql-42.6.0.jar
"""

import os
import json
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcSink
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.types import Row
from pyflink.common import Types, JsonRowTypeInfo


# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/orders_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")


class OrderDeserializationSchema:
    """Custom deserialization schema for Kafka messages"""
    
    def __init__(self):
        self.types = Types.ROW_NAMED(
            ["order_id", "customer_id", "product_id", "quantity", "price", "order_timestamp", "status"],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.DOUBLE(), Types.STRING(), Types.STRING()]
        )
    
    def deserialize(self, message):
        """Deserialize Kafka message"""
        if message is None:
            return None
        
        try:
            # Parse JSON string
            data = json.loads(message.decode('utf-8'))
            
            return Row(
                order_id=data.get("order_id"),
                customer_id=data.get("customer_id"),
                product_id=data.get("product_id"),
                quantity=int(data.get("quantity", 0)),
                price=float(data.get("price", 0.0)),
                order_timestamp=data.get("order_timestamp"),
                status=data.get("status")
            )
        except Exception as e:
            print(f"Error deserializing message: {e}")
            return None
    
    def get_produced_type(self):
        return self.types


def transform_order(order):
    """
    Transform order data:
    - Calculate total_price = quantity * price
    - Add processing_timestamp
    - Convert status to uppercase
    """
    if order is None:
        return None
    
    try:
        # Parse timestamp
        order_timestamp = datetime.fromisoformat(order.order_timestamp)
        
        # Calculate total price
        total_price = order.quantity * order.price
        total_price = round(total_price, 2)
        
        # Add processing timestamp
        processing_timestamp = datetime.now()
        
        # Transform to Row
        return Row(
            order_id=order.order_id,
            customer_id=order.customer_id,
            product_id=order.product_id,
            quantity=order.quantity,
            price=order.price,
            total_price=total_price,
            order_timestamp=order_timestamp,
            processing_timestamp=processing_timestamp,
            status=order.status.upper()
        )
        
    except Exception as e:
        print(f"Error transforming order: {e}")
        return None


def create_postgres_sink():
    """Create PostgreSQL sink for writing processed orders"""
    
    # JDBC connection options
    connection_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_url(POSTGRES_URL) \
        .with_driver_name("org.postgresql.Driver") \
        .with_user_name(POSTGRES_USER) \
        .with_password(POSTGRES_PASSWORD) \
        .with_batch_size(1000) \
        .build()
    
    # JDBC sink with upsert logic
    return JdbcSink.builder() \
        .set_sql("""
            INSERT INTO processed_orders 
            (order_id, customer_id, product_id, quantity, price, total_price, 
             order_timestamp, processing_timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                total_price = EXCLUDED.total_price,
                order_timestamp = EXCLUDED.order_timestamp,
                processing_timestamp = EXCLUDED.processing_timestamp,
                status = EXCLUDED.status
        """) \
        .set_connection_options(connection_options) \
        .build()


def create_env():
    """Create and configure Flink streaming environment"""
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set parallelism
    env.set_parallelism(2)
    
    # Enable checkpointing
    env.enable_checkpointing(10000)
    
    # Set restart strategy
    env.set_restart_strategy(
        type="failure-rate",
        failure_rate=0.1,
        failure_interval=60000,
        delay_interval=10000
    )
    
    return env


def main():
    """Main function to run the Flink job"""
    
    print("="*60)
    print("Starting Flink Order Processing Job")
    print("="*60)
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"PostgreSQL URL: {POSTGRES_URL}")
    print("="*60 + "\n")
    
    # Create environment
    env = create_env()
    
    # Define the type info for deserialization
    order_type_info = Types.ROW_NAMED(
        ["order_id", "customer_id", "product_id", "quantity", "price", "order_timestamp", "status"],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.DOUBLE(), Types.STRING(), Types.STRING()]
    )
    
    # Create Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id("flink-consumer-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(OrderDeserializationSchema()) \
        .build()
    
    # Add Kafka source to the environment
    stream = env.add_source(kafka_source, name="Kafka Source")
    
    # Transform the data
    transformed_stream = stream.map(
        transform_order,
        output_type=Types.ROW_NAMED(
            ["order_id", "customer_id", "product_id", "quantity", "price", "total_price", 
             "order_timestamp", "processing_timestamp", "status"],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.DOUBLE(), 
             Types.DOUBLE(), Types.LOCAL_DATE_TIME(), Types.LOCAL_DATE_TIME(), Types.STRING()]
        ),
        name="Transform Orders"
    ).filter(
        lambda x: x is not None,
        name="Filter None Values"
    )
    
    # Add processing timestamp to the data for logging
    def add_logging_info(record):
        print(f"Processing: {record.order_id} | Status: {record.status} | Total: ${record.total_price}")
        return record
    
    logged_stream = transformed_stream.map(
        add_logging_info,
        name="Log Processed Orders"
    )
    
    # Write to PostgreSQL
    postgres_sink = create_postgres_sink()
    logged_stream.add_sink(postgres_sink)
    
    # Execute the job
    print("\nExecuting Flink job...")
    env.execute("Process Orders from Kafka to PostgreSQL")


if __name__ == "__main__":
    main()
