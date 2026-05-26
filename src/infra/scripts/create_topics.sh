#!/bin/bash

BROKER="localhost:9092"
CONTAINER="infra-kafka-1"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

topics=(
    "raw.stocks"
    "raw.crypto"
    "clean.stocks"
    "clean.crypto"
)

echo "Waiting for Kafka to be ready..."
until docker exec $CONTAINER $KAFKA_TOPICS --bootstrap-server $BROKER --list > /dev/null 2>&1; do
  echo " Kafka not ready yet, retrying in 3s..."
  sleep 3
done
echo "Kafka is ready."

for topic in "${topics[@]}"; do
  docker exec $CONTAINER $KAFKA_TOPICS --create --if-not-exists --bootstrap-server $BROKER --replication-factor 1 --partitions 3 --topic "$topic"
  echo "Created topic: $topic"
done

echo ""
echo "=== Verifying topics ==="
docker exec $CONTAINER $KAFKA_TOPICS --list --bootstrap-server $BROKER