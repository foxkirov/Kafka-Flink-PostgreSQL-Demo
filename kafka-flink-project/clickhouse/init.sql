-- Create a database for orders if it doesn't exist
CREATE DATABASE IF NOT EXISTS orders_db;

-- Use the orders_db database
USE orders_db;

-- Create a Kafka engine table to consume raw order data
CREATE TABLE IF NOT EXISTS kafka_orders_raw (
    order_id String,
    customer_id String,
    product_id String,
    quantity UInt32,
    price Float64,
    order_timestamp DateTime64(3),
    status String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'orders',
    kafka_group_name = 'clickhouse_consumer_group',
    kafka_format = 'JSONEachRow',
    kafka_row_delimiter = '\n',
    kafka_max_block_size = 1000000; -- Max messages per block

-- Create a target table for processed orders in ClickHouse
CREATE TABLE IF NOT EXISTS processed_orders_clickhouse (
    order_id String,
    customer_id String,
    product_id String,
    quantity UInt32,
    price Float64,
    total_price Float64,
    order_timestamp DateTime64(3),
    processing_timestamp DateTime64(3),
    status String
)
ENGINE = MergeTree()
PRIMARY KEY (order_id)
ORDER BY (order_id, processing_timestamp);

-- Create a materialized view to process data from Kafka and insert into the target table
-- This view will automatically process new messages from kafka_orders_raw
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_processed_orders_clickhouse TO processed_orders_clickhouse AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    price,
    round(quantity * price, 2) AS total_price,
    order_timestamp,
    now64(3) AS processing_timestamp,
    upper(status) AS status
FROM kafka_orders_raw;
