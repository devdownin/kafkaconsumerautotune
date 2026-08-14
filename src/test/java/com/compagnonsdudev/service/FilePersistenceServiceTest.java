package com.compagnonsdudev.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.compagnonsdudev.config.PersistenceProperties;
import com.compagnonsdudev.entity.KEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class FilePersistenceServiceTest {

    private PersistenceProperties properties;
    private ObjectMapper objectMapper;
    private FilePersistenceService filePersistenceService;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        properties = new PersistenceProperties();
        properties.setTracePath(tempDir.toString());
        objectMapper = new ObjectMapper();
        filePersistenceService = new FilePersistenceService(properties, objectMapper);
        filePersistenceService.init();
    }

    @Test
    void shouldSaveEventsAsJson() throws Exception {
        // Given
        properties.setFormat("json");
        KEvent event = KEvent.builder()
                .eventId("evt-json")
                .kafkaTopic("test-topic")
                .kafkaOffset(123L)
                .payload("{\"key\":\"value\"}")
                .build();

        // When
        filePersistenceService.saveEvents(List.of(event));

        // Then
        Path expectedFile = tempDir.resolve("test-topic_123.json");
        assertThat(Files.exists(expectedFile)).isTrue();
        String content = Files.readString(expectedFile);
        assertThat(content).contains("evt-json");
    }

    @Test
    void shouldSaveEventsAsXml() throws Exception {
        // Given
        properties.setFormat("xml");
        KEvent event = KEvent.builder()
                .eventId("evt-xml")
                .kafkaTopic("xml-topic")
                .kafkaOffset(456L)
                .payload("<key>value</key>")
                .build();

        // When
        filePersistenceService.saveEvents(List.of(event));

        // Then
        Path expectedFile = tempDir.resolve("xml-topic_456.xml");
        assertThat(Files.exists(expectedFile)).isTrue();
        String content = Files.readString(expectedFile);
        assertThat(content).contains("evt-xml");
        assertThat(content).contains("<KEvent>");
    }
}
