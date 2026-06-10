#!/bin/bash

BROKER="localhost:9092"
CONTAINER="kafka"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
KAFKA_API="/opt/kafka/bin/kafka-broker-api-versions.sh"

echo "=== Kafka Health Check ==="

docker exec $CONTAINER $KAFKA_API \
  --bootstrap-server $BROKER > /dev/null 2>&1 \
  && echo "Kafka broker: reachable" \
  || echo "Kafka broker: unreachable"


  echo ""
  echo "=== Topics ==="
  docker exec $CONTAINER $KAFKA_TOPICS \
    --list \
    --bootstrap-server $BROKER