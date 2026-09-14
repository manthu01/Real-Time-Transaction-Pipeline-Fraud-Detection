#!/bin/bash
# Creates the PulseGuard Kafka topics with the partitioning/retention laid
# out in the build plan. Run automatically by the kafka-topics-init service
# in docker-compose.yml; safe to re-run (--if-not-exists).
set -euo pipefail

BOOTSTRAP="${1:-localhost:9092}"
BIN=/opt/kafka/bin/kafka-topics.sh

wait_for_kafka() {
  for _ in $(seq 1 30); do
    if $BIN --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "kafka did not become ready in time" >&2
  exit 1
}

create_topic() {
  local name="$1" partitions="$2" retention_ms="$3" extra="${4:-}"
  echo "creating topic: $name (partitions=$partitions retention_ms=$retention_ms)"
  $BIN --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "retention.ms=$retention_ms" \
    $extra
}

wait_for_kafka

SEVEN_DAYS_MS=$((7 * 24 * 60 * 60 * 1000))
THIRTY_DAYS_MS=$((30 * 24 * 60 * 60 * 1000))

create_topic "raw.transactions" 6 "$SEVEN_DAYS_MS"
create_topic "fraud.alerts" 3 "$THIRTY_DAYS_MS"
create_topic "dead.letter" 1 "$THIRTY_DAYS_MS"
create_topic "analyst.dispositions" 1 -1 "--config cleanup.policy=compact"

echo "topics ready:"
$BIN --bootstrap-server "$BOOTSTRAP" --list
