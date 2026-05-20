#!/bin/bash
# Download required JAR files for Flink connectors
# Run this script from the project root directory

JARS_DIR="flink/jars"

echo "Downloading Flink connector JARs..."

# Kafka connector for Flink 1.17.1
echo "Downloading flink-sql-connector-kafka_2.12-1.17.1.jar..."
curl -L -o "$JARS_DIR/flink-sql-connector-kafka_2.12-1.17.1.jar" \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar"

# JDBC connector for Flink 1.17.1
echo "Downloading flink-connector-jdbc-1.17.1.jar..."
curl -L -o "$JARS_DIR/flink-connector-jdbc-1.17.1.jar" \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.17.1/flink-connector-jdbc-1.17.1.jar"

# PostgreSQL JDBC driver
echo "Downloading postgresql-42.6.0.jar..."
curl -L -o "$JARS_DIR/postgresql-42.6.0.jar" \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar"

# Flink JSON serializer
echo "Downloading flink-json-1.17.1.jar..."
curl -L -o "$JARS_DIR/flink-json-1.17.1.jar" \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.17.1/flink-json-1.17.1.jar"

echo "Download complete!"
ls -la "$JARS_DIR/"
