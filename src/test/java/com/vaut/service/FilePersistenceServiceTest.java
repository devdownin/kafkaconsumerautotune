package com.vaut.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaut.config.PersistenceProperties;
import com.vaut.entity.KEvent;
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
                .kafkaPartition(2)
                .kafkaOffset(123L)
                .payload("{\"key\":\"value\"}")
                .build();

        // When
        filePersistenceService.saveEvents(List.of(event));

        // Then
        Path expectedFile = tempDir.resolve("test-topic_2_123.json");
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
                .kafkaPartition(0)
                .kafkaOffset(456L)
                .payload("<key>value</key>")
                .build();

        // When
        filePersistenceService.saveEvents(List.of(event));

        // Then
        Path expectedFile = tempDir.resolve("xml-topic_0_456.xml");
        assertThat(Files.exists(expectedFile)).isTrue();
        String content = Files.readString(expectedFile);
        assertThat(content).contains("evt-xml");
        assertThat(content).contains("<KEvent>");
    }

    @Test
    void shouldNotOverwriteEventsSharingAnOffsetAcrossPartitions() throws Exception {
        // Given: offsets are only unique within a partition, so two different records can share one
        properties.setFormat("json");
        KEvent fromPartition0 = KEvent.builder()
                .eventId("evt-p0")
                .kafkaTopic("shared-topic")
                .kafkaPartition(0)
                .kafkaOffset(99L)
                .build();
        KEvent fromPartition1 = KEvent.builder()
                .eventId("evt-p1")
                .kafkaTopic("shared-topic")
                .kafkaPartition(1)
                .kafkaOffset(99L)
                .build();

        // When
        filePersistenceService.saveEvents(List.of(fromPartition0, fromPartition1));

        // Then: both records survive
        assertThat(Files.readString(tempDir.resolve("shared-topic_0_99.json"))).contains("evt-p0");
        assertThat(Files.readString(tempDir.resolve("shared-topic_1_99.json"))).contains("evt-p1");
    }
}
