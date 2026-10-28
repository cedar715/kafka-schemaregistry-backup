#!/usr/bin/env bash
# The real proof: messages written using the SOURCE registry
# must be readable using the RESTORED registry.
set -euo pipefail

ID=$(curl -s http://localhost:8081/subjects/orders-value/versions/latest | jq .id)
echo "Producing 2 Avro messages to topic 'orders' with schema id $ID (via sr-source)"

printf '%s\n' '{"id":"o1","amount":100.5,"currency":"INR"}' '{"id":"o2","amount":42.0,"currency":"SGD"}' |
docker exec -i sr-source kafka-avro-console-producer \
  --bootstrap-server kafka:9092 --topic orders \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema.id="$ID" \
  --property auto.register.schemas=false

echo "First bytes of the raw message (magic byte 00 + 4-byte schema id):"
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic orders --from-beginning --max-messages 1 2>/dev/null | head -c 5 | xxd

echo
echo "Consuming with the RESTORED registry (sr-target). Inside its container it listens on 8081."
docker exec sr-target kafka-avro-console-consumer \
  --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 2 \
  --property schema.registry.url=http://localhost:8081
