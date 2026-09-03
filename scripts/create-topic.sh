#!/usr/bin/env bash
set -e

echo "Waiting for Kafka to be ready..."
kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1

echo "Creating topic 'product-events' if it doesn't exist..."
kafka-topics --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic product-events \
  --partitions 1 \
  --replication-factor 1

echo "Topic 'product-events' ready!"