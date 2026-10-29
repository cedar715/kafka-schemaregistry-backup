package com.example.kafka.srbackup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Restores a snapshot into an EMPTY Schema Registry, preserving schema IDs.
 *
 * Order matters:
 *   1. safety checks
 *   2. mode = IMPORT          (the only mode that accepts an explicit id + version)
 *   3. register every version, ascending id (referenced schemas exist before their users)
 *   4. mode = READWRITE
 *   5. re-apply soft deletes, DEscending id (users are deleted before what they reference)
 *   6. compatibility configs (global, then per subject)
 *   7. modes (per subject, then global) - last, because READONLY would block steps 5-6
 *
 * Re-running with allowNonEmpty resumes a restore that stopped part-way: versions already
 * present with the right id are skipped, and steps 4-7 are safe to repeat.
 */
@Service
public class RestoreService {

    private static final Logger log = LoggerFactory.getLogger(RestoreService.class);

    /** Fields of a version that Schema Registry accepts on register. Everything else is dropped. */
    private static final List<String> REGISTER_FIELDS =
            List.of("schema", "schemaType", "references", "metadata", "ruleSet", "id", "version");

    private final SrClient sr;

    public RestoreService(SrClient sr) {
        this.sr = sr;
    }

    /**
     * @param allowNonEmpty   restore into a registry that already has subjects (also: resume a failed restore)
     * @param allowSameSource restore into the very registry the backup was taken from
     */
    public void restore(JsonNode snapshot, boolean allowNonEmpty, boolean allowSameSource) {
        boolean targetEmpty = preflight(snapshot, allowNonEmpty, allowSameSource);

        List<ObjectNode> versions = flattenVersions(snapshot);
        log.info("Restoring {} versions into {}", versions.size(), sr.baseUrl());

        // 2. IMPORT mode (Schema Registry refuses it on a non-empty registry unless force=true)
        sr.put(targetEmpty ? "/mode" : "/mode?force=true", Map.of("mode", "IMPORT"));

        // 3. register with the ORIGINAL id and version
        int done = 0;
        try {
            for (ObjectNode v : versions) {
                register(v, targetEmpty);
                done++;
            }
        } catch (RuntimeException e) {
            // Deliberately NOT switching back to READWRITE: while in IMPORT mode nobody can
            // register a new schema and grab an id that is still waiting to be restored.
            log.error("Restore stopped after {} of {} versions. The target is LEFT IN IMPORT MODE on purpose. "
                    + "Fix the cause, then re-run with --allow-non-empty to resume.", done, versions.size());
            throw e;
        }

        // 4. back to normal so we can delete / configure
        sr.put("/mode", Map.of("mode", "READWRITE"));

        // 5. soft deletes, highest id first. A schema always has a higher id than the schemas it
        //    references, and SR refuses (42206) to delete a version that a live schema still references.
        List<ObjectNode> highestIdFirst = new ArrayList<>(versions);
        Collections.reverse(highestIdFirst);
        Map<String, Set<Integer>> live = new HashMap<>();
        for (ObjectNode v : highestIdFirst) {
            if (!v.path("deleted").asBoolean(false)) {
                continue;
            }
            String subject = v.get("subject").asText();
            int version = v.get("version").asInt();
            if (!targetEmpty && !live.computeIfAbsent(subject, this::liveVersions).contains(version)) {
                log.info("  already soft-deleted {} v{}", subject, version);
                continue;
            }
            sr.delete("/subjects/{s}/versions/{v}", subject, version);
            log.info("  soft-deleted {} v{}", subject, version);
        }

        // 6. compatibility
        JsonNode globalConfig = snapshot.get("globalConfig");
        if (isSet(globalConfig)) {
            sr.put("/config", toConfigUpdate(globalConfig));
            log.info("  global config {}", globalConfig);
        }
        for (JsonNode s : snapshot.get("subjects")) {
            if (isSet(s.get("config"))) {
                sr.put("/config/{s}", toConfigUpdate(s.get("config")), s.get("name").asText());
                log.info("  config {} {}", s.get("name").asText(), s.get("config"));
            }
        }

        // 7. modes
        for (JsonNode s : snapshot.get("subjects")) {
            if (isSet(s.get("mode"))) {
                sr.put("/mode/{s}", Map.of("mode", s.get("mode").get("mode").asText()), s.get("name").asText());
                log.info("  mode {} {}", s.get("name").asText(), s.get("mode").get("mode").asText());
            }
        }
        JsonNode globalMode = snapshot.get("globalMode");
        if (isSet(globalMode) && !"READWRITE".equals(globalMode.get("mode").asText())) {
            sr.put("/mode", Map.of("mode", globalMode.get("mode").asText()));
            log.info("  global mode {}", globalMode.get("mode").asText());
        }

        log.info("Restore finished. Now run 'verify' against the same target.");
    }

    private void register(ObjectNode v, boolean targetEmpty) {
        String subject = v.get("subject").asText();
        int version = v.get("version").asInt();
        int expectedId = v.get("id").asInt();

        if (!targetEmpty) {
            JsonNode existing = sr.get("/subjects/{s}/versions/{v}?deleted=true", subject, version);
            if (existing != null) {
                int existingId = existing.path("id").asInt(-1);
                if (existingId != expectedId) {
                    throw new IllegalStateException(subject + " v" + version + " already exists in the target with id "
                            + existingId + ", but the backup has id " + expectedId + ". Stop and investigate.");
                }
                log.info("  already present {} v{} id={}", subject, version, existingId);
                return;
            }
        }

        ObjectNode body = v.deepCopy();
        body.retain(REGISTER_FIELDS);

        JsonNode res = sr.post("/subjects/{s}/versions", body, subject);
        int gotId = res.path("id").asInt(-1);
        // SR 8.x echoes the registered version; older versions return only the id
        int gotVersion = res.path("version").asInt(version);
        if (gotId != expectedId || gotVersion != version) {
            throw new IllegalStateException("Mismatch for " + subject + ": expected v" + version + " id=" + expectedId
                    + ", got v" + gotVersion + " id=" + gotId + ". Stop and investigate.");
        }
        log.info("  registered {} v{} id={}", subject, version, gotId);
    }

    /** @return true if the target registry is empty */
    private boolean preflight(JsonNode snapshot, boolean allowNonEmpty, boolean allowSameSource) {
        if (snapshot.path("formatVersion").asInt() != BackupService.FORMAT_VERSION) {
            throw new IllegalStateException("Unsupported backup formatVersion " + snapshot.path("formatVersion"));
        }
        if (!allowSameSource && isSameRegistry(snapshot)) {
            throw new IllegalStateException("Target " + sr.baseUrl()
                    + " is the same registry this backup came from. Refusing without --allow-same-source.");
        }
        JsonNode existing = sr.get("/subjects?deleted=true");
        boolean empty = existing == null || existing.isEmpty();
        if (!empty && !allowNonEmpty) {
            throw new IllegalStateException("Target is not empty (" + existing.size() + " subjects). "
                    + "Restore needs an empty registry. Refusing without --allow-non-empty "
                    + "(which is also how you resume a restore that stopped part-way).");
        }
        log.info("Preflight OK: backup from {} taken {}", snapshot.path("sourceUrl").asText(),
                snapshot.path("createdAt").asText());
        return empty;
    }

    /**
     * Compares registry identities (Kafka cluster id + SR group id), so the same registry reached
     * through a different hostname, IP or load balancer is still recognised. Falls back to comparing
     * URLs for backups or registries that don't have the identity.
     */
    private boolean isSameRegistry(JsonNode snapshot) {
        JsonNode sourceId = snapshot.get("sourceRegistryId");
        JsonNode targetId = sr.registryId();
        if (isSet(sourceId) && isSet(targetId)) {
            return sourceId.equals(targetId);
        }
        return trim(snapshot.path("sourceUrl").asText()).equals(trim(sr.baseUrl()));
    }

    private Set<Integer> liveVersions(String subject) {
        Set<Integer> out = new HashSet<>();
        JsonNode versions = sr.get("/subjects/{s}/versions", subject);
        if (versions != null) {
            versions.forEach(n -> out.add(n.asInt()));
        }
        return out;
    }

    /** All versions of all subjects, sorted by id, then subject, then version. */
    private static List<ObjectNode> flattenVersions(JsonNode snapshot) {
        List<ObjectNode> out = new ArrayList<>();
        for (JsonNode s : snapshot.get("subjects")) {
            for (JsonNode v : s.get("versions")) {
                out.add((ObjectNode) v);
            }
        }
        out.sort(Comparator.comparingInt((ObjectNode v) -> v.get("id").asInt())
                .thenComparing(v -> v.get("subject").asText())
                .thenComparingInt(v -> v.get("version").asInt()));
        return out;
    }

    /** GET /config returns "compatibilityLevel"; PUT /config expects "compatibility". */
    private static ObjectNode toConfigUpdate(JsonNode config) {
        ObjectNode u = (ObjectNode) config.deepCopy();
        JsonNode level = u.remove("compatibilityLevel");
        if (level != null) {
            u.set("compatibility", level);
        }
        List<String> nulls = new ArrayList<>();
        u.fieldNames().forEachRemaining(f -> {
            if (u.get(f).isNull()) nulls.add(f);
        });
        u.remove(nulls);
        return u;
    }

    private static boolean isSet(JsonNode n) {
        return n != null && !n.isNull() && !n.isMissingNode();
    }

    private static String trim(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
