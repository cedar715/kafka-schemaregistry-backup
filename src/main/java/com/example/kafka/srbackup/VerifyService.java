package com.example.kafka.srbackup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Exports the target registry again and compares it with the backup file:
 * subjects, versions, IDs, schema text, soft-delete flags, configs and modes.
 */
@Service
public class VerifyService {

    private static final Logger log = LoggerFactory.getLogger(VerifyService.class);

    private final BackupService backup;

    public VerifyService(BackupService backup) {
        this.backup = backup;
    }

    public boolean verify(JsonNode expected) {
        JsonNode actual = backup.export();
        List<String> diffs = new ArrayList<>();

        same(diffs, "global config", expected.get("globalConfig"), actual.get("globalConfig"));
        same(diffs, "global mode", expected.get("globalMode"), actual.get("globalMode"));

        Map<String, JsonNode> exp = bySubject(expected);
        Map<String, JsonNode> act = bySubject(actual);
        TreeSet<String> names = new TreeSet<>(exp.keySet());
        names.addAll(act.keySet());

        for (String name : names) {
            JsonNode e = exp.get(name);
            JsonNode a = act.get(name);
            if (e == null) { diffs.add("unexpected subject in target: " + name); continue; }
            if (a == null) { diffs.add("missing subject in target: " + name); continue; }

            same(diffs, name + " config", e.get("config"), a.get("config"));
            same(diffs, name + " mode", e.get("mode"), a.get("mode"));

            Map<Integer, JsonNode> ev = byVersion(e), av = byVersion(a);
            TreeSet<Integer> vs = new TreeSet<>(ev.keySet());
            vs.addAll(av.keySet());
            for (Integer v : vs) {
                // whole version JSON must match: id, schema text, type, references, deleted flag ...
                same(diffs, name + " v" + v, withoutTimestamp(ev.get(v)), withoutTimestamp(av.get(v)));
            }
        }

        if (diffs.isEmpty()) {
            log.info("VERIFY OK: target matches backup ({})", expected.get("summary"));
            return true;
        }
        log.error("VERIFY FAILED: {} difference(s)", diffs.size());
        diffs.forEach(d -> log.error("  {}", d));
        return false;
    }

    private static void same(List<String> diffs, String what, JsonNode expected, JsonNode actual) {
        JsonNode e = (expected == null || expected.isMissingNode()) ? null : expected;
        JsonNode a = (actual == null || actual.isMissingNode()) ? null : actual;
        if (e != null && e.isNull()) e = null;
        if (a != null && a.isNull()) a = null;
        if (!Objects.equals(e, a)) {
            diffs.add(what + ": expected " + e + " but found " + a);
        }
    }

    /**
     * SR 8.x adds "ts" (registration time) to every version. Even IMPORT mode stamps its own
     * time, so a restore can never reproduce it - comparing it would fail every verify.
     */
    private static JsonNode withoutTimestamp(JsonNode version) {
        if (!(version instanceof ObjectNode o)) {
            return version;
        }
        ObjectNode copy = o.deepCopy();
        copy.remove("ts");
        return copy;
    }

    private static Map<String, JsonNode> bySubject(JsonNode snapshot) {
        Map<String, JsonNode> m = new TreeMap<>();
        snapshot.get("subjects").forEach(s -> m.put(s.get("name").asText(), s));
        return m;
    }

    private static Map<Integer, JsonNode> byVersion(JsonNode subject) {
        Map<Integer, JsonNode> m = new TreeMap<>();
        subject.get("versions").forEach(v -> m.put(v.get("version").asInt(), v));
        return m;
    }
}
