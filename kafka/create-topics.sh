#!/bin/bash
set -euo pipefail
K=/opt/kafka/bin/kafka-topics.sh
for topic in bikeflow.raw bikeflow.dlq bikeflow.alerts; do
  if [[ "$topic" == "bikeflow.raw" ]]; then
    "$K" --bootstrap-server kafka:9092 --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 --config retention.ms=259200000
  else
    "$K" --bootstrap-server kafka:9092 --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 --config retention.ms=604800000
  fi
done
