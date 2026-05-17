package com.vaut.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.vaut.config.PersistenceProperties;
import com.vaut.entity.KEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;

/**
 * Service responsible for saving events to the filesystem.
 * This is an alternative to database persistence when saveInFile is enabled.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class FilePersistenceService {

    private final PersistenceProperties properties;
    private final ObjectMapper jsonMapper;
    private final XmlMapper xmlMapper = new XmlMapper();

    /**
     * Initializes the trace directory on startup.
     */
    @PostConstruct
    public void init() {
        try {
            Path tracePath = Paths.get(properties.getTracePath());
            if (!Files.exists(tracePath)) {
                Files.createDirectories(tracePath);
                log.info("Created trace directory at: {}", tracePath.toAbsolutePath());
            }
        } catch (IOException e) {
            log.error("Failed to create trace directory: {}", e.getMessage());
        }
    }

    /**
     * Saves a list of events to individual files on disk asynchronously.
     * File naming pattern: [topic]_[offset].[extension]
     *
     * @param events List of events to save.
     */
    @Async
    public void saveEvents(List<KEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        String format = properties.getFormat().toLowerCase();
        String extension = "json".equals(format) ? "json" : "xml";
        Path traceDir = Paths.get(properties.getTracePath());

        for (KEvent event : events) {
            String fileName = String.format("%s_%d.%s",
                event.getKafkaTopic(),
                event.getKafkaOffset(),
                extension);

            Path filePath = traceDir.resolve(fileName);

            try {
                if ("json".equals(format)) {
                    jsonMapper.writeValue(filePath.toFile(), event);
                } else {
                    xmlMapper.writeValue(filePath.toFile(), event);
                }
                log.debug("Saved event to file: {}", filePath);
            } catch (IOException e) {
                log.error("Failed to save event {} to file {}: {}", event.getEventId(), filePath, e.getMessage());
            }
        }
        log.info("Successfully saved {} events to disk in {} format", events.size(), format);
    }

    /**
     * Scheduled task to clean up old trace files.
     * Runs every day at 2 AM.
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public void cleanupOldFiles() {
        log.info("Starting cleanup of old trace files...");
        Path traceDir = Paths.get(properties.getTracePath());
        if (!Files.exists(traceDir)) return;

        Instant threshold = Instant.now().minus(7, ChronoUnit.DAYS); // TODO: Make configurable

        try (Stream<Path> files = Files.walk(traceDir)) {
            long deletedCount = files
                .filter(Files::isRegularFile)
                .filter(p -> {
                    try {
                        return Files.getLastModifiedTime(p).toInstant().isBefore(threshold);
                    } catch (IOException e) {
                        return false;
                    }
                })
                .map(p -> {
                    try {
                        Files.delete(p);
                        return true;
                    } catch (IOException e) {
                        log.error("Failed to delete old trace file {}: {}", p, e.getMessage());
                        return false;
                    }
                })
                .filter(deleted -> deleted)
                .count();

            log.info("Cleanup complete. Deleted {} old trace files.", deletedCount);
        } catch (IOException e) {
            log.error("Error during trace files cleanup: {}", e.getMessage());
        }
    }
}
