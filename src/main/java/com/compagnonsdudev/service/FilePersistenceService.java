package com.compagnonsdudev.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.compagnonsdudev.config.PersistenceProperties;
import com.compagnonsdudev.entity.KEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

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
     * Saves a list of events to individual files on disk.
     * File naming pattern: [topic]_[offset].[extension]
     *
     * @param events List of events to save.
     */
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
}
