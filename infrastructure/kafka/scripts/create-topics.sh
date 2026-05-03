#!/bin/bash

# Define common settings to keep it DRY
KAFKA_CONTAINER="flightops-kafka"
BOOTSTRAP="localhost:9092"

echo "Creating Kafka topics..."

# Main Ingestion Topic
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists \
  --topic flight-ops.ingestion.v1 \
  --partitions 3 --replication-factor 1

# Retry Topic
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists \
  --topic flight-ops.ingestion.retry.v1 \
  --partitions 3 --replication-factor 1

# Dead Letter Queue (DLQ) Topic
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists \
  --topic flight-ops.ingestion.dlq.v1 \
  --partitions 3 --replication-factor 1

echo "Topics created successfully."

