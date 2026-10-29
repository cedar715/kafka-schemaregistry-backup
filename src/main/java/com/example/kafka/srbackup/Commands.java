package com.example.kafka.srbackup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Set;

/**
 * Command-line entry point: backup | restore | verify.
 *
 * Exit codes: 0 ok, 1 verify found differences (or any error), 2 bad usage.
 * A typo'd command in a cron job must not look like a successful backup.
 */
@Component
public class Commands implements ApplicationRunner, ExitCodeGenerator {

    private static final Logger log = LoggerFactory.getLogger(Commands.class);
    private static final Set<String> COMMANDS = Set.of("backup", "restore", "verify");

    private final BackupService backup;
    private final RestoreService restore;
    private final VerifyService verify;
    private final SrClient sr;
    private final ObjectMapper mapper;
    private int exitCode = 0;

    public Commands(BackupService backup, RestoreService restore, VerifyService verify,
                    SrClient sr, ObjectMapper mapper) {
        this.backup = backup;
        this.restore = restore;
        this.verify = verify;
        this.sr = sr;
        this.mapper = mapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<String> cmd = args.getNonOptionArgs();
        if (cmd.size() != 1 || !COMMANDS.contains(cmd.get(0))
                || !args.containsOption("file") || sr.baseUrl().isBlank()) {
            usage();
            exitCode = 2;
            return;
        }
        Path file = Path.of(args.getOptionValues("file").get(0));

        switch (cmd.get(0)) {
            case "backup" -> write(backup.export(), file);
            case "restore" -> restore.restore(read(file),
                    args.containsOption("allow-non-empty"), args.containsOption("allow-same-source"));
            case "verify" -> exitCode = verify.verify(read(file)) ? 0 : 1;
        }
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    /** Write to a temp file first, then rename: a crash never leaves a half-written backup. */
    private void write(JsonNode snapshot, Path file) throws Exception {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), snapshot);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(tmp);
        }
        log.info("Backup written to {} ({} bytes)", file.toAbsolutePath(), Files.size(file));
    }

    private JsonNode read(Path file) throws Exception {
        return mapper.readTree(file.toFile());
    }

    private static void usage() {
        System.err.println("""
                usage:
                  java -jar sr-backup-poc.jar backup  --sr.url=<url> --file=<backup.json>
                  java -jar sr-backup-poc.jar restore --sr.url=<url> --file=<backup.json> [--allow-non-empty] [--allow-same-source]
                  java -jar sr-backup-poc.jar verify  --sr.url=<url> --file=<backup.json>

                restore options:
                  --allow-non-empty    target already has subjects; also resumes a restore that stopped part-way
                  --allow-same-source  target is the registry the backup was taken from

                BASIC auth: env vars SR_USERNAME / SR_PASSWORD (or --sr.username / --sr.password)
                """);
    }
}
