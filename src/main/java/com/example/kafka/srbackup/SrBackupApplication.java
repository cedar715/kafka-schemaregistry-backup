package com.example.kafka.srbackup;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Schema Registry backup/restore POC.
 *
 * <pre>
 * java -jar sr-backup-poc.jar backup  --sr.url=http://localhost:8081 --file=backup.json
 * java -jar sr-backup-poc.jar restore --sr.url=http://localhost:8082 --file=backup.json [--allow-non-empty]
 * java -jar sr-backup-poc.jar verify  --sr.url=http://localhost:8082 --file=backup.json
 * </pre>
 */
@SpringBootApplication
public class SrBackupApplication {
    public static void main(String[] args) {
        // propagate Commands' exit code (usage error, verify failure) to the shell
        System.exit(SpringApplication.exit(SpringApplication.run(SrBackupApplication.class, args)));
    }
}
