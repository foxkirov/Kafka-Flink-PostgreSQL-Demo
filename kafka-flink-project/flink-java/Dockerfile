# Build stage
FROM maven:3.9.5-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
RUN mvn package -DskipTests

# Final stage
FROM flink:1.17.1-scala_2.12-java11
WORKDIR /opt/flink
COPY --from=build /app/target/flink-order-processor-1.0-SNAPSHOT.jar /opt/flink/usrlib/flink-order-processor.jar

# Command to submit the job
# Note: In a real cluster, you'd submit via the JobManager, 
# but here we can use the flink image to submit as well or let JobManager pick it up if configured.
# For simplicity, we can provide a script or use the job-cluster mode if needed.
# However, given the existing setup, we'll just have the JAR ready.
