package com.vaut.service;

import com.vaut.config.PersistenceProperties;
import com.vaut.entity.KEvent;
import com.vaut.repository.KEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventPersistenceServiceTest {

    @Mock
    private KEventRepository keventRepository;

    @Mock
    private FilePersistenceService filePersistenceService;

    private PersistenceProperties persistenceProperties;
    private EventPersistenceService eventPersistenceService;

    @BeforeEach
    void setUp() {
        persistenceProperties = new PersistenceProperties();
        eventPersistenceService = new EventPersistenceService(keventRepository, filePersistenceService, persistenceProperties);
    }

    @Test
    void shouldSaveToDatabaseWhenSaveInFileIsFalse() {
        // Given
        persistenceProperties.setSaveInFile(false);
        KEvent event = KEvent.builder().eventId("evt1").kafkaTopic("topic1").kafkaOffset(100L).build();
        List<KEvent> events = List.of(event);

        when(keventRepository.findExistingEventIds(any())).thenReturn(Set.of());
        when(keventRepository.saveAll(any())).thenReturn(events);

        // When
        List<KEvent> result = eventPersistenceService.saveEventsBatch(events);

        // Then
        assertThat(result).hasSize(1);
        verify(keventRepository).saveAll(any());
        verifyNoInteractions(filePersistenceService);
    }

    @Test
    void shouldSaveToFileWhenSaveInFileIsTrue() {
        // Given
        persistenceProperties.setSaveInFile(true);
        KEvent event = KEvent.builder().eventId("evt1").kafkaTopic("topic1").kafkaOffset(100L).build();
        List<KEvent> events = List.of(event);

        // When
        List<KEvent> result = eventPersistenceService.saveEventsBatch(events);

        // Then
        assertThat(result).hasSize(1);
        verify(filePersistenceService).saveEvents(events);
        verifyNoInteractions(keventRepository);
    }
}
