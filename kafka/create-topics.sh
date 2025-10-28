#!/usr/bin/env bash
set -euo pipefail

# === Configuration ===
BROKER="${KAFKA_BROKER:-kafka:9092}"
TOPIC_SOURCE="${KAFKA_TOPIC_SOURCE:-source}"
TOPIC_RESULT="${KAFKA_TOPIC_RESULT:-result}"
KAFKA_TOPICS_BIN="/opt/kafka/bin/kafka-topics.sh"

echo "[create-topics] Waiting for Kafka broker at ${BROKER}..."

# === Attendre que Kafka soit prêt ===
for i in {1..30}; do
  if "$KAFKA_TOPICS_BIN" --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
    echo "[create-topics] Kafka is up!"
    break
  fi
  echo "[create-topics] Still waiting... ($i/30)"
  sleep 2
done

# === Fonction utilitaire pour créer un topic ===
create_topic() {
  local topic="$1"
  if "$KAFKA_TOPICS_BIN" --bootstrap-server "$BROKER" --list | grep -q "^${topic}$"; then
    echo "[create-topics] Topic '${topic}' already exists."
  else
    echo "[create-topics] Creating topic '${topic}'..."
    "$KAFKA_TOPICS_BIN" --bootstrap-server "$BROKER" \
      --create \
      --topic "$topic" \
      --partitions 3 \
      --replication-factor 1
    echo "[create-topics] Topic '${topic}' created successfully."
  fi
}

# === Création des topics ===
create_topic "$TOPIC_SOURCE"
create_topic "$TOPIC_RESULT"

echo "[create-topics] All topics verified."