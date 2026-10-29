package com.example.kafka.srbackup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.io.File;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * seed.sh -> backup -> restore -> verify against real registries, plus the safety checks.
 * Same setup as docker-compose.yml: one broker, two registries on separate topics + group ids.
 * Needs Docker, curl and jq. Run with `mvn verify`.
 */
@Testcontainers(disabledWithoutDocker = true)
class RoundTripIT {

    static final Network NET = Network.newNetwork();

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.1")
            .withNetwork(NET)
            .withListener("kafka:19092");

    @Container
    static final GenericContainer<?> SOURCE = registry("sr-source", "_schemas");

    @Container
    static final GenericContainer<?> TARGET = registry("sr-target", "_schemas_restore");

    final ObjectMapper mapper = new ObjectMapper();

    @Test
    void restoredRegistryMatchesSource() throws Exception {
        seed(url(SOURCE));
        SrClient src = client(SOURCE);
        SrClient tgt = client(TARGET);
        RestoreService restore = new RestoreService(tgt);
        VerifyService verify = new VerifyService(new BackupService(tgt, mapper));

        JsonNode snapshot = new BackupService(src, mapper).export();
        assertThat(snapshot.at("/summary/softDeletedVersions").asInt()).isEqualTo(3);

        restore.restore(snapshot, false, false);
        assertThat(verify.verify(snapshot)).isTrue();

        // what consumers actually do: resolve the id from the message header
        TreeSet<Integer> ids = new TreeSet<>();
        snapshot.get("subjects").forEach(s -> s.get("versions").forEach(v -> ids.add(v.get("id").asInt())));
        for (int id : ids) {
            JsonNode a = src.get("/schemas/ids/{id}", id);
            JsonNode b = tgt.get("/schemas/ids/{id}", id);
            assertThat(b).as("schema id %d", id).isNotNull();
            assertThat(b.get("schema")).as("schema id %d", id).isEqualTo(a.get("schema"));
            assertThat(b.get("references")).as("schema id %d references", id).isEqualTo(a.get("references"));
        }

        // safety checks
        assertThatThrownBy(() -> restore.restore(snapshot, false, false))
                .hasMessageContaining("not empty");
        assertThatThrownBy(() -> new RestoreService(src).restore(snapshot, true, false))
                .hasMessageContaining("same registry");

        // resuming over a complete restore changes nothing
        restore.restore(snapshot, true, false);
        assertThat(verify.verify(snapshot)).isTrue();
    }

    private static GenericContainer<?> registry(String group, String topic) {
        return new GenericContainer<>("confluentinc/cp-schema-registry:8.1.0")
                .withNetwork(NET)
                .dependsOn(KAFKA)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", group)
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", topic)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID", group)
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
    }

    private static String url(GenericContainer<?> sr) {
        return "http://" + sr.getHost() + ":" + sr.getMappedPort(8081);
    }

    private SrClient client(GenericContainer<?> sr) {
        return new SrClient(url(sr), "", "", mapper);
    }

    private static void seed(String srUrl) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", "scripts/seed.sh")
                .redirectErrorStream(true)
                .redirectOutput(new File("target/seed.log"));
        pb.environment().put("SR", srUrl);
        int rc = pb.start().waitFor();
        assertThat(rc).as("scripts/seed.sh exit code, see target/seed.log").isZero();
    }
}
