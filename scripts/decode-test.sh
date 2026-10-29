#!/usr/bin/env bash
# The real proof: messages written using the SOURCE registry
# must be readable using the RESTORED registry.
#
#   1. every schema id in the backup resolves to the same schema on source and target
#      (this is the lookup a consumer does; it covers soft-deleted ids too)
#   2. real messages: produced with an explicit schema id via sr-source, consumed via sr-target
#
# usage: ./scripts/decode-test.sh [backup.json]
set -euo pipefail

BACKUP=${1:-backup.json}
SRC=http://localhost:8081
TGT=http://localhost:8082
FAILED=0

echo "== 1. Schema id lookups: source vs restored target"
for id in $(jq '[.subjects[].versions[].id] | unique | .[]' "$BACKUP"); do
  src=$(curl -sf "$SRC/schemas/ids/$id" | jq -cS '{schemaType, schema, references}')
  tgt=$(curl -sf "$TGT/schemas/ids/$id" | jq -cS '{schemaType, schema, references}' || echo "MISSING")
  if [[ "$src" == "$tgt" ]]; then
    echo "  id=$id OK"
  else
    echo "  id=$id FAIL  source=$src  target=$tgt"
    FAILED=1
  fi
done

id_of() {  # subject, version -> schema id (works for soft-deleted versions too)
  curl -sf "$SRC/subjects/$1/versions/$2?deleted=true" | jq -e .id
}

# case-name, format (avro|protobuf|json-schema), topic, schema id, message, text expected in the decoded output
check() {
  local name=$1 fmt=$2 topic=$3 id=$4 msg=$5 expect=$6
  printf '  %-46s id=%-3s ' "$name" "$id"

  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$topic" >/dev/null
  # start reading where the topic ends now, so re-runs only see this run's message
  local offset
  offset=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 \
    --topic "$topic" --partitions 0 | cut -d: -f3)

  # the Confluent console tools log to stdout, so keep their output only for errors
  local log
  if ! log=$(echo "$msg" | docker exec -i sr-source "kafka-$fmt-console-producer" \
      --bootstrap-server kafka:9092 --topic "$topic" \
      --property schema.registry.url=http://localhost:8081 \
      --property value.schema.id="$id" \
      --property auto.register.schemas=false 2>&1); then
    echo "FAIL  producer: $(grep -m1 -E 'Exception|Error' <<<"$log")"
    FAILED=1
    return
  fi

  local out
  out=$(docker exec sr-target "kafka-$fmt-console-consumer" \
    --bootstrap-server kafka:9092 --topic "$topic" --partition 0 --offset "$offset" \
    --max-messages 1 --timeout-ms 30000 \
    --property schema.registry.url=http://localhost:8081 2>&1 | grep -m1 '^{' || true)

  if [[ "$out" == *"$expect"* ]]; then
    echo "OK    $out"
  else
    echo "FAIL  got: '${out}'"
    FAILED=1
  fi
}

echo
echo "== 2. Produce via sr-source, consume via sr-target"
check "Avro, latest version"                 avro        orders         "$(id_of orders-value 2)" \
  '{"id":"o1","amount":100.5,"currency":"INR"}'          '"currency":"INR"'
# orders-value v1 is soft-deleted, so nothing can produce with it any more - but its id is
# shared with orders-archive-value, so this message carries exactly the bytes old orders did
check "Avro, id of soft-deleted orders v1"   avro        orders-archive "$(id_of orders-value 1)" \
  '{"id":"o0","amount":7.0}'                             '"id":"o0"'
check "Avro, rollback v3 (version gap)"      avro        rollback       "$(id_of rollback-value 3)" \
  '{"a":42}'                                             '"a":42'
check "JSON Schema"                          json-schema customers      "$(id_of customers-value 1)" \
  '{"id":"c1","name":"Asha"}'                            '"name":"Asha"'
check "Protobuf with a reference"            protobuf    payments       "$(id_of payments-value 1)" \
  '{"id":"p1","amount":{"currency":"SGD","units":"42"}}' '"currency":"SGD"'

echo
if [[ $FAILED -ne 0 ]]; then
  echo "DECODE TEST FAILED"
  exit 1
fi
echo "Everything written with the source registry decodes with the restored registry."
