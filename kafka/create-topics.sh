#!/usr/bin/env bash
set -euo pipefail
BROKERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"

topics=("rides.source" "rides.result")

for T in "${topics[@]}"; do
  docker exec -i kafka-1 kafka-topics --create     --topic "$T" --partitions 12 --replication-factor 3     --if-not-exists --bootstrap-server "$BROKERS"
done

docker exec -i kafka-1 kafka-topics --describe --bootstrap-server "$BROKERS"
