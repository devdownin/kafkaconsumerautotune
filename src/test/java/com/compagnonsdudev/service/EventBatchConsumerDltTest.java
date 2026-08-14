package com.compagnonsdudev.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.compagnonsdudev.repository.DltEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.compagnonsdudev.entity.DltEvent;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class EventBatchConsumerDltTest {

    @Mock private EventPersistenceService persistenceService;
    @Mock private EventProcessingService processingService;
    @Mock private DltService dltService;
    @Mock private DltEventRepository dltEventRepository;
    @Mock private WebSocketService webSocketService;
    @Mock private Acknowledgment acknowledgment;

    private MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private EventBatchConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new EventBatchConsumer(persistenceService, processingService, dltService, dltEventRepository, meterRegistry, webSocketService);
    }

    @Test
    void shouldRouteToDltOnProcessingError() {
        // Given
        String originalTopic = "orders.topic";
        String badJson = "invalid-json";
        ConsumerRecord<String, String> record = new ConsumerRecord<>(originalTopic, 0, 100L, "key", badJson);
        List<ConsumerRecord<String, String>> records = Collections.singletonList(record);

        when(processingService.processRecord(record)).thenReturn(Optional.empty());
        DltEvent dltEvent = DltEvent.builder().id(1L).build();
        when(dltService.routeToDlt(eq(record), anyString())).thenReturn(dltEvent);
        when(dltEventRepository.saveAll(any())).thenReturn(Collections.singletonList(dltEvent));

        // When
        consumer.consumeBatch(records, acknowledgment);

        // Then
        verify(dltService).routeToDlt(eq(record), anyString());
        verify(dltEventRepository).saveAll(any());
        verify(webSocketService).sendDltEvent(dltEvent);
        verify(acknowledgment).acknowledge();
    }
}
