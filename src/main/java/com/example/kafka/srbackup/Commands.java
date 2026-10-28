package com.example.kafka.srbackup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/** Command-line entry point: backup | restore | verify. */
@Component
public class Commands implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(Commands.class);

    private final BackupService backup;
    private final RestoreService restore;
    private final VerifyService verify;
    private final ObjectMapper mapper;

    public Commands(BackupService backup, RestoreService restore, VerifyService verify, ObjectMapper mapper) {
        this.backup = backup;
        this.restore = restore;
        this.verify = verify;
        this.mapper = mapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<String> cmd = args.getNonOptionArgs();
        if (cmd.isEmpty() || !args.containsOption("file")) {
            usage();
            return;
        }
        Path file = Path.of(args.getOptionValues("file").get(0));
        boolean force = args.containsOption("force");

        switch (cmd.get(0)) {
            case "backup" -> write(backup.export(), file);
            case "restore" -> restore.restore(read(file), force);
            case "verify" -> {
                if (!verify.verify(read(file))) {
                    throw new IllegalStateException("verify failed");
                }
            }
            default -> usage();
        }
    }

    /** Write to a temp file first, then rename: a crash never leaves a half-written backup. */
    private void write(JsonNode snapshot, Path file) throws Exception {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), snapshot);
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        log.info("Backup written to {} ({} bytes)", file.toAbsolutePath(), Files.size(file));
    }

    private JsonNode read(Path file) throws Exception {
        return mapper.readTree(file.toFile());
    }

    private static void usage() {
        System.out.println("""
                usage:
                  java -jar sr-backup-poc.jar backup  --sr.url=<url> --file=<backup.json>
                  java -jar sr-backup-poc.jar restore --sr.url=<url> --file=<backup.json> [--force]
                  java -jar sr-backup-poc.jar verify  --sr.url=<url> --file=<backup.json>
                optional: --sr.username=<u> --sr.password=<p>
                """);
    }
}
