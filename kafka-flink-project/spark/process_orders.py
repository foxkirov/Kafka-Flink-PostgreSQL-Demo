import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, upper, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def main():
    # Configuration from environment variables
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "orders")
    postgres_url = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/orders_db")
    postgres_user = os.environ.get("POSTGRES_USER", "postgres")
    postgres_password = os.environ.get("POSTGRES_PASSWORD", "postgres")

    # Spark Session
    # Note: Packages are usually managed via spark-submit --packages, 
    # but we can also set them here if needed.
    spark = SparkSession.builder \
        .appName("PySpark-Kafka-Postgres-Orders") \
        .getOrCreate()

    # Define schema for the incoming JSON data from Kafka
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("status", StringType(), True)
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", "my_pyspark_orders_group") \
        .load()

    # Parse JSON and transform
    orders_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("total_price", round(col("quantity") * col("price"), 2)) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("status", upper(col("status"))) \
        .withColumn("spark_engine", col("status").cast(StringType())) # Placeholder for additional logic if needed

    # Note: Spark Structured Streaming JDBC Sink doesn't support 'append' mode directly 
    # for some versions/drivers in the same way as file sinks. 
    # We use foreachBatch for reliable JDBC writing.

    def write_to_postgres(batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", "spark_processed_orders") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} processed and written to Postgres.")

    # Start the stream
    query = orders_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "/app/spark_checkpoints") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
