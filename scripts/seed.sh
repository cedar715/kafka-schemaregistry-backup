#!/usr/bin/env bash
# Seeds the SOURCE registry with every tricky case a backup must survive.
set -euo pipefail
SR=${SR:-http://localhost:8081}
CT='Content-Type: application/vnd.schemaregistry.v1+json'

reg() {  # subject, json-body
  printf '%-22s -> ' "$1"
  curl -sf -X POST -H "$CT" "$SR/subjects/$1/versions" -d "$2"; echo
}
avro() { jq -nc --arg s "$1" '{schema:$s}'; }

ORDER_V1='{"type":"record","name":"Order","namespace":"bank","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"}]}'
ORDER_V2='{"type":"record","name":"Order","namespace":"bank","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"},{"name":"currency","type":"string","default":"INR"}]}'

echo "== 1. Avro subject with two versions (schema evolution)"
reg orders-value "$(avro "$ORDER_V1")"
reg orders-value "$(avro "$ORDER_V2")"

echo "== 2. Same schema under a second subject -> SAME id is reused"
reg orders-archive-value "$(avro "$ORDER_V1")"

echo "== 3. JSON Schema"
reg customers-value "$(jq -nc --arg s '{"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id"]}' \
  '{schemaType:"JSON", schema:$s}')"

echo "== 4. Protobuf with a reference (payments imports money.proto)"
reg common-money "$(jq -nc --arg s 'syntax = "proto3"; package bank; message Money { string currency = 1; int64 units = 2; }' \
  '{schemaType:"PROTOBUF", schema:$s}')"
reg payments-value "$(jq -nc --arg s 'syntax = "proto3"; package bank; import "money.proto"; message Payment { string id = 1; Money amount = 2; }' \
  '{schemaType:"PROTOBUF", schema:$s, references:[{name:"money.proto", subject:"common-money", version:1}]}')"

echo "== 5. A subject that gets HARD-deleted (must NOT come back after restore)"
reg tmp-value "$(avro '{"type":"record","name":"Tmp","fields":[{"name":"x","type":"int"}]}')"
curl -sf -X DELETE "$SR/subjects/tmp-value" >/dev/null
curl -sf -X DELETE "$SR/subjects/tmp-value?permanent=true" >/dev/null

echo "== 6. Soft-delete orders-value v1 (old messages still carry its id!)"
curl -sf -X DELETE "$SR/subjects/orders-value/versions/1"; echo

echo "== 7. Rollback: v1, v2, soft-delete v1, re-register v1's schema"
echo "   SR drops v1 for good and creates v3 with v1's id -> version gap, and v3 has a LOWER id than v2"
R_V1='{"type":"record","name":"R","namespace":"bank","fields":[{"name":"a","type":"int"}]}'
R_V2='{"type":"record","name":"R","namespace":"bank","fields":[{"name":"a","type":"int"},{"name":"b","type":"int","default":0}]}'
reg rollback-value "$(avro "$R_V1")"
reg rollback-value "$(avro "$R_V2")"
curl -sf -X DELETE "$SR/subjects/rollback-value/versions/1"; echo
reg rollback-value "$(avro "$R_V1")"

echo "== 8. Soft-deleted Protobuf schema AND the schema it references"
echo "   SR only allows deleting the referenced one AFTER its user -> restore must delete in that order"
reg common-addr "$(jq -nc --arg s 'syntax = "proto3"; package bank; message Addr { string city = 1; }' \
  '{schemaType:"PROTOBUF", schema:$s}')"
reg shipping-value "$(jq -nc --arg s 'syntax = "proto3"; package bank; import "addr.proto"; message Ship { Addr to = 1; }' \
  '{schemaType:"PROTOBUF", schema:$s, references:[{name:"addr.proto", subject:"common-addr", version:1}]}')"
curl -sf -X DELETE "$SR/subjects/shipping-value/versions/1"; echo
curl -sf -X DELETE "$SR/subjects/common-addr/versions/1"; echo

echo "== 9. Compatibility: global + per subject"
curl -sf -X PUT -H "$CT" "$SR/config" -d '{"compatibility":"BACKWARD_TRANSITIVE"}'; echo
curl -sf -X PUT -H "$CT" "$SR/config/orders-value" -d '{"compatibility":"FULL"}'; echo

echo "== 10. Mode: one subject locked READONLY"
curl -sf -X PUT -H "$CT" "$SR/mode/customers-value" -d '{"mode":"READONLY"}'; echo

echo
echo "== Source state"
echo "subjects (incl. soft-deleted): $(curl -s "$SR/subjects?deleted=true")"
echo "schemas by id:"
curl -s "$SR/schemas?deleted=true" | jq -c '.[] | {id, subject, version, type:(.schemaType // "AVRO")}'
