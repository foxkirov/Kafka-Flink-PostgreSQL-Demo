package com.flink.orderprocessor;

import com.flink.orderprocessor.pojo.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.time.LocalDateTime;

public class OrderProcessor {

    public static void main(String[] args) throws Exception {
        // Configuration from environment variables
        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String kafkaTopic = System.getenv().getOrDefault("KAFKA_TOPIC", "orders");
        String postgresUrl = System.getenv().getOrDefault("POSTGRES_URL", "jdbc:postgresql://postgres:5432/orders_db");
        String postgresUser = System.getenv().getOrDefault("POSTGRES_USER", "postgres");
        String postgresPassword = System.getenv().getOrDefault("POSTGRES_PASSWORD", "postgres");

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000);

        // Kafka Source
        KafkaSource<Order> source = KafkaSource.<Order>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(kafkaTopic)
                .setGroupId("flink-java-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new OrderDeserializationSchema())
                .build();

        DataStream<Order> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Transformation
        DataStream<Order> transformedStream = stream.map(new MapFunction<Order, Order>() {
            @Override
            public Order map(Order order) throws Exception {
                if (order == null) return null;
                
                // Calculate total_price = quantity * price
                double totalPrice = order.getQuantity() * order.getPrice();
                order.setTotalPrice(Math.round(totalPrice * 100.0) / 100.0);

                // Add processing_timestamp
                order.setProcessingTimestamp(LocalDateTime.now());

                // Convert status to uppercase
                if (order.getStatus() != null) {
                    order.setStatus(order.getStatus().toUpperCase());
                }

                System.out.println("Processing: " + order.getOrderId() + " | Status: " + order.getStatus() + " | Total: $" + order.getTotalPrice());
                return order;
            }
        }).filter(order -> order != null);

        // JDBC Sink
        transformedStream.addSink(JdbcSink.sink(
                "INSERT INTO processed_orders " +
                        "(order_id, customer_id, product_id, quantity, price, total_price, " +
                        "order_timestamp, processing_timestamp, status) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (order_id) DO UPDATE SET " +
                        "customer_id = EXCLUDED.customer_id, " +
                        "product_id = EXCLUDED.product_id, " +
                        "quantity = EXCLUDED.quantity, " +
                        "price = EXCLUDED.price, " +
                        "total_price = EXCLUDED.total_price, " +
                        "order_timestamp = EXCLUDED.order_timestamp, " +
                        "processing_timestamp = EXCLUDED.processing_timestamp, " +
                        "status = EXCLUDED.status",
                (statement, order) -> {
                    statement.setString(1, order.getOrderId());
                    statement.setString(2, order.getCustomerId());
                    statement.setString(3, order.getProductId());
                    statement.setInt(4, order.getQuantity());
                    statement.setDouble(5, order.getPrice());
                    statement.setDouble(6, order.getTotalPrice());
                    statement.setObject(7, order.getOrderTimestamp());
                    statement.setObject(8, order.getProcessingTimestamp());
                    statement.setString(9, order.getStatus());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(postgresUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(postgresUser)
                        .withPassword(postgresPassword)
                        .build()
        ));

        // Execute the job
        env.execute("Java Flink Order Processing Job");
    }

    public static class OrderDeserializationSchema extends AbstractDeserializationSchema<Order> {
        private transient ObjectMapper objectMapper;

        @Override
        public void open(InitializationContext context) throws Exception {
            super.open(context);
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }

        @Override
        public Order deserialize(byte[] message) throws IOException {
            if (message == null) {
                return null;
            }
            try {
                return objectMapper.readValue(message, Order.class);
            } catch (Exception e) {
                System.err.println("Error deserializing message: " + e.getMessage());
                return null;
            }
        }
    }
}
