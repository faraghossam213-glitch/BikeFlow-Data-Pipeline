#!/bin/bash
# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:9092 1 20

echo "Creating Kafka topics..."

# 1. Raw topic: 3 partitions, custom retention (e.g., 24 hours = 86400000ms or 7 days)
kafka-topics --create --if-not-exists \
  --bootstrap-server kafka:9092 \
  --topic bikeflow.raw \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000

# 2. DLQ topic: for invalid or failed records
kafka-topics --create --if-not-exists \
  --bootstrap-server kafka:9092 \
  --topic bikeflow.dlq \
  --partitions 1 \
  --replication-factor 1

# 3. Alerts topic: for Spark streaming alert rules
kafka-topics --create --if-not-exists \
  --bootstrap-server kafka:9092 \
  --topic bikeflow.alerts \
  --partitions 1 \
  --replication-factor 1

echo "Kafka topics created successfully!"
kafka-topics --list --bootstrap-server kafka:9092
```[cite: 7]