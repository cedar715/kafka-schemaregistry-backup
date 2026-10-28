package com.example.kafka.srbackup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Restores a snapshot into an EMPTY Schema Registry, preserving schema IDs.
 *
 * Order matters:
 *   1. safety checks
 *   2. mode = IMPORT          (the only mode that accepts an explicit id + version)
 *   3. register every version, ascending id (referenced schemas exist before their users)
 *   4. mode = READWRITE
 *   5. re-apply soft deletes
 *   6. compatibility configs (global, then per subject)
 *   7. modes (per subject, then global) - last, because READONLY would block steps 5-6
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

    public void restore(JsonNode snapshot, boolean force) {
        preflight(snapshot, force);

        List<ObjectNode> versions = flattenVersions(snapshot);
        log.info("Restoring {} versions into {}", versions.size(), sr.baseUrl());

        // 2. IMPORT mode (Schema Registry refuses it on a non-empty registry unless force=true)
        sr.put(force ? "/mode?force=true" : "/mode", Map.of("mode", "IMPORT"));

        // 3. register with the ORIGINAL id and version
        for (ObjectNode v : versions) {
            String subject = v.get("subject").asText();
            int expectedId = v.get("id").asInt();

            ObjectNode body = v.deepCopy();
            body.retain(REGISTER_FIELDS);

            JsonNode res = sr.post("/subjects/{s}/versions", body, subject);
            int gotId = res.path("id").asInt(-1);
            if (gotId != expectedId) {
                throw new IllegalStateException("ID mismatch for " + subject + " v" + v.get("version")
                        + ": expected " + expectedId + ", got " + gotId + ". Stop and investigate.");
            }
            log.info("  registered {} v{} id={}", subject, v.get("version"), gotId);
        }

        // 4. back to normal so we can delete / configure
        sr.put("/mode", Map.of("mode", "READWRITE"));

        // 5. soft deletes
        for (ObjectNode v : versions) {
            if (v.path("deleted").asBoolean(false)) {
                sr.delete("/subjects/{s}/versions/{v}", v.get("subject").asText(), v.get("version").asInt());
                log.info("  soft-deleted {} v{}", v.get("subject").asText(), v.get("version"));
            }
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

    private void preflight(JsonNode snapshot, boolean force) {
        if (snapshot.path("formatVersion").asInt() != BackupService.FORMAT_VERSION) {
            throw new IllegalStateException("Unsupported backup formatVersion " + snapshot.path("formatVersion"));
        }
        String source = trim(snapshot.path("sourceUrl").asText());
        if (source.equals(trim(sr.baseUrl())) && !force) {
            throw new IllegalStateException("Target " + sr.baseUrl()
                    + " is the same registry this backup came from. Refusing without --force.");
        }
        JsonNode existing = sr.get("/subjects?deleted=true");
        if (existing != null && existing.size() > 0 && !force) {
            throw new IllegalStateException("Target is not empty (" + existing.size()
                    + " subjects). Restore needs an empty registry. Refusing without --force.");
        }
        log.info("Preflight OK: backup from {} taken {}", source, snapshot.path("createdAt").asText());
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
