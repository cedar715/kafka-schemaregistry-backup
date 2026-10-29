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

# 7. The real proof: every schema id resolves the same on both registries, and
#    messages produced with the source registry decode with the restored one
./scripts/decode-test.sh

# 8. Automated: the whole seed -> backup -> restore -> verify round trip in Testcontainers
mvn verify
```

Exit codes: `0` ok, `1` verify found differences (or any error), `2` bad usage
(unknown command, missing `--file` or `--sr.url`). `--sr.url` has no default on purpose.

BASIC auth: set `SR_USERNAME` / `SR_PASSWORD` env vars rather than `--sr.password`, which
lands in shell history and `ps`.

### Restore options

| Flag | Allows |
|---|---|
| `--allow-non-empty` | Target already has subjects. Also **resumes** a restore that stopped part-way: versions already present with the right ID are skipped; one present with a *different* ID stops the restore. |
| `--allow-same-source` | Target is the registry the backup came from. Compared by registry identity (`/v1/metadata/id`: Kafka cluster id + SR group id), not URL, so `127.0.0.1` vs `localhost` or a load balancer doesn't fool it. |

If registration fails, the target is **left in IMPORT mode on purpose** — nothing else can
register a schema and take an ID that is still waiting to be restored. Fix the cause and
re-run with `--allow-non-empty`.

## What the seed data tests

| Case | Why a backup must handle it |
|---|---|
| Avro subject with 2 versions | Normal evolution |
| Same schema under 2 subjects | Both share **one** ID; restore must keep that |
| JSON Schema | Non-Avro types carry `schemaType` |
| Protobuf with a reference | Referenced schema must be restored **first** (we sort by ID) |
| Soft-deleted v1 | Hidden, but old messages still use its ID → must be backed up |
| Hard-deleted subject | Must **not** reappear after restore |
| Rollback (soft-delete v1, re-register its schema) | SR drops v1 and makes v3 with v1's ID: a version gap, and v3's ID is *lower* than v2's |
| Soft-deleted Protobuf schema + the schema it references | SR refuses to delete a referenced version first, so restore soft-deletes **highest ID first** |
| Global + subject compatibility | Governance settings are part of the state |
| Subject in READONLY mode | Restored **last**, otherwise it blocks the restore itself |

## Things to try (and explain to yourself)

1. Open `backup.json`. Find the soft-deleted version and the two subjects sharing an ID.
2. Run `restore` again into `:8082` → it **refuses** (target not empty). Why is that the right default?
3. Run `restore` into `:8081` → **refuses** (same registry as the source). Try `http://127.0.0.1:8081` — why is it still refused?
4. Delete `"deleted": true` from v1 in `backup.json`, wipe the target, restore, verify. What changed?
5. Reset the target completely and redo steps 5–6:
   ```bash
   docker compose stop sr-target
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic _schemas_restore
   docker compose start sr-target
   ```
6. Restore the backup into a target that already has one unrelated schema, using `--allow-non-empty`.
   Why can IDs now collide, and what does restore do when they do?
7. Why does `verify` ignore the `ts` field of each version? (Hint: register in IMPORT mode with a `ts` and read it back.)

## POC limits (what production needs on top)
- Local file only → production writes to object storage **outside the PR site**, versioned + immutable.
- Logical (REST) backup only → add a raw `_schemas` topic dump as a second safety net.
- No snapshot consistency check (a schema registered mid-backup could be half-captured; a version
  hard-deleted mid-backup makes the backup fail with a clear message).
- **ID high-water mark is not carried over.** A hard-deleted schema's ID is invisible over REST
  (`/schemas/ids/{id}` → 404), so if it was the highest ID, the restored registry will hand that
  ID to the next new schema — while old messages may still carry it. Only the raw `_schemas`
  topic shows it; production must capture and reserve it.
- Version `ts` (registration time) cannot be restored — SR stamps its own even in IMPORT mode.
- TLS uses the JVM truststore (`-Djavax.net.ssl.trustStore=...`); BASIC auth via `SR_USERNAME/SR_PASSWORD`.
- Not handled: schema contexts (`:.ctx:subject`), subject configs on subjects with no versions,
  the DEK registry (`/dek-registry/v1/keks`) — if you use client-side field encryption, losing it
  makes encrypted fields unrecoverable.
- Restore needs `mode.mutability=true` on the target registry (IMPORT mode).
- No scheduling, metrics or alerting yet.
