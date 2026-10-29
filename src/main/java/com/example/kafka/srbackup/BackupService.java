package com.example.kafka.srbackup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Reads everything from Schema Registry into one JSON snapshot:
 *   - global compatibility + mode
 *   - every subject, INCLUDING soft-deleted ones
 *   - every version, INCLUDING soft-deleted ones, with its global schema ID
 *   - per-subject compatibility + mode (only if explicitly set on the subject)
 */
@Service
public class BackupService {

    private static final Logger log = LoggerFactory.getLogger(BackupService.class);
    public static final int FORMAT_VERSION = 1;

    private final SrClient sr;
    private final ObjectMapper mapper;

    public BackupService(SrClient sr, ObjectMapper mapper) {
        this.sr = sr;
        this.mapper = mapper;
    }

    public ObjectNode export() {
        log.info("Exporting from {}", sr.baseUrl());

        ObjectNode snapshot = mapper.createObjectNode();
        snapshot.put("formatVersion", FORMAT_VERSION);
        snapshot.put("createdAt", Instant.now().toString());
        snapshot.put("sourceUrl", sr.baseUrl());
        snapshot.set("sourceRegistryId", sr.registryId()); // null if the endpoint is missing
        snapshot.set("globalConfig", sr.get("/config"));
        snapshot.set("globalMode", sr.get("/mode"));

        ArrayNode subjects = snapshot.putArray("subjects");
        int versionCount = 0;
        int softDeleted = 0;
        Set<Integer> ids = new TreeSet<>();

        JsonNode names = sr.get("/subjects?deleted=true");
        for (JsonNode nameNode : names) {
            String subject = nameNode.asText();

            // versions still visible normally; anything else is soft-deleted
            Set<Integer> live = toIntSet(sr.get("/subjects/{s}/versions", subject));
            JsonNode all = sr.get("/subjects/{s}/versions?deleted=true", subject);

            ObjectNode s = subjects.addObject();
            s.put("name", subject);
            s.set("config", sr.get("/config/{s}?defaultToGlobal=false", subject)); // null if not set
            s.set("mode", sr.get("/mode/{s}?defaultToGlobal=false", subject));     // null if not set
            ArrayNode versions = s.putArray("versions");

            if (all == null) {
                continue;
            }
            for (JsonNode v : all) {
                int version = v.asInt();
                // raw JSON: subject, version, id, schemaType, schema, references, metadata, ruleSet ...
                ObjectNode ver = (ObjectNode) sr.get("/subjects/{s}/versions/{v}?deleted=true", subject, version);
                if (ver == null) {
                    throw new IllegalStateException(subject + " v" + version
                            + " disappeared during the backup (hard-deleted?). Re-run the backup.");
                }
                boolean deleted = !live.contains(version);
                ver.put("deleted", deleted);
                versions.add(ver);

                versionCount++;
                if (deleted) softDeleted++;
                ids.add(ver.get("id").asInt());
            }
        }

        ObjectNode summary = snapshot.putObject("summary");
        summary.put("subjects", subjects.size());
        summary.put("versions", versionCount);
        summary.put("softDeletedVersions", softDeleted);
        summary.put("distinctSchemaIds", ids.size());

        log.info("Exported {} subjects, {} versions ({} soft-deleted), {} distinct schema IDs",
                subjects.size(), versionCount, softDeleted, ids.size());
        return snapshot;
    }

    private static Set<Integer> toIntSet(JsonNode array) {
        Set<Integer> out = new HashSet<>();
        if (array != null) {
            array.forEach(n -> out.add(n.asInt()));
        }
        return out;
    }
}
