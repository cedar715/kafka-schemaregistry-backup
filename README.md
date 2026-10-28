# Schema Registry backup & restore — POC

Spring Boot CLI that backs up a Confluent Schema Registry to **one JSON file** and
restores it into an **empty** registry with **schema IDs preserved**.

```
backup   SR (source) ──REST──►  backup.json
restore  backup.json ──REST──►  SR (target, empty)   via IMPORT mode
verify   SR (target) ──REST──►  compare with backup.json
```

## Prerequisites
JDK 17+, Maven 3.9+, Docker (with Compose), `curl`, `jq`.

## Run it (≈10 minutes)

```bash
# 1. Kafka + two independent Schema Registries (source :8081, empty target :8082)
docker compose up -d
until curl -sf localhost:8081/subjects >/dev/null && curl -sf localhost:8082/subjects >/dev/null; do sleep 2; done

# 2. Put realistic + tricky data into the source
chmod +x scripts/*.sh
./scripts/seed.sh

# 3. Build
mvn -q package

# 4. Backup the source
java -jar target/sr-backup-poc.jar backup  --sr.url=http://localhost:8081 --file=backup.json

# 5. Restore into the empty target
java -jar target/sr-backup-poc.jar restore --sr.url=http://localhost:8082 --file=backup.json

# 6. Verify target == backup (exit code 1 if anything differs)
java -jar target/sr-backup-poc.jar verify  --sr.url=http://localhost:8082 --file=backup.json

# 7. The real proof: produce with source registry, consume with restored registry
./scripts/decode-test.sh
```

## What the seed data tests

| Case | Why a backup must handle it |
|---|---|
| Avro subject with 2 versions | Normal evolution |
| Same schema under 2 subjects | Both share **one** ID; restore must keep that |
| JSON Schema | Non-Avro types carry `schemaType` |
| Protobuf with a reference | Referenced schema must be restored **first** (we sort by ID) |
| Soft-deleted v1 | Hidden, but old messages still use its ID → must be backed up |
| Hard-deleted subject | Must **not** reappear after restore |
| Global + subject compatibility | Governance settings are part of the state |
| Subject in READONLY mode | Restored **last**, otherwise it blocks the restore itself |

## Things to try (and explain to yourself)

1. Open `backup.json`. Find the soft-deleted version and the two subjects sharing an ID.
2. Run `restore` again into `:8082` → it **refuses** (target not empty). Why is that the right default?
3. Run `restore` into `:8081` → **refuses** (same registry as the source).
4. Delete `"deleted": true` from v1 in `backup.json`, wipe the target, restore, verify. What changed?
5. Reset the target completely and redo steps 5–6:
   ```bash
   docker compose stop sr-target
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic _schemas_restore
   docker compose start sr-target
   ```
6. Restore the backup into a target that already has one unrelated schema, using `--force`.
   Why can IDs now collide?

## POC limits (what production needs on top)
- Local file only → production writes to object storage **outside the PR site**, versioned + immutable.
- Logical (REST) backup only → add a raw `_schemas` topic dump as a second safety net.
- No snapshot consistency check (a schema registered mid-backup could be half-captured).
- TLS uses the JVM truststore (`-Djavax.net.ssl.trustStore=...`); BASIC auth via `--sr.username/--sr.password`.
- Not handled: schema contexts (`:.ctx:subject`), subject configs on subjects with no versions.
- Restore needs `mode.mutability=true` on the target registry (IMPORT mode).
- No scheduling, metrics or alerting yet.
