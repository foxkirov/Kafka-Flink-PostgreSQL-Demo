#!/usr/bin/env python3
"""
PyFlink Table API Job - Process orders from Kafka and write to PostgreSQL

This version uses Flink SQL Table API which is simpler and more reliable.
"""

import os
from datetime import datetime

from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.types import DataTypes


# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/orders_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")


def main():
    """Main function to run the Flink job"""
    
    print("="*60)
    print("Starting Flink Order Processing Job (Table API)")
    print("="*60)
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"PostgreSQL URL: {POSTGRES_URL}")
    print("="*60 + "\n")
    
    # Create streaming environment
    env_settings = EnvironmentSettings.Builder() \
        .use_blink_planner() \
        .in_streaming_mode() \
        .build()
    
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)
    
    # Add JARs to classpath
    t_env.get_config().set(
        "pipeline.jars",
        "file:///opt/flink/lib/flink-sql-connector-kafka_2.12-1.17.1.jar"
        ":file:///opt/flink/lib/flink-connector-jdbc-1.17.1.jar"
        ":file:///opt/flink/lib/postgresql-42.6.0.jar"
        ":file:///opt/flink/lib/flink-json-1.17.1.jar"
    )
    
    # Create Kafka source table
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW orders_raw (
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            price DECIMAL(10, 2),
            order_timestamp TIMESTAMP(3),
            status STRING,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-consumer-group',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    
    # Create PostgreSQL sink table
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE processed_orders_sink (
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            price DECIMAL(10, 2),
            total_price DECIMAL(10, 2),
            order_timestamp TIMESTAMP(3),
            processing_timestamp TIMESTAMP(3),
            status STRING,
            PRIMARY KEY (order_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{POSTGRES_URL}',
            'table-name' = 'processed_orders',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '1s'
        )
    """)
    
    # Process and insert data
    result = t_env.sql_query("""
        SELECT 
            order_id,
            customer_id,
            product_id,
            quantity,
            price,
            CAST(quantity * price AS DECIMAL(10, 2)) AS total_price,
            order_timestamp,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS processing_timestamp,
            UPPER(status) AS status
        FROM orders_raw
    """)
    
    result.execute_insert("processed_orders_sink")


if __name__ == "__main__":
    main()
