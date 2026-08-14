package com.compagnonsdudev.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.compagnonsdudev.dto.dashboard.DashboardStatsDTO;
import com.compagnonsdudev.repository.DltEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventBatchConsumerStatsTest {

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
    void shouldNotSendStatsAfterBatchToOptimizePerformance() {
        // Given
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test", 0, 0, "key", "{}");
        List<ConsumerRecord<String, String>> records = Collections.singletonList(record);

        // When
        consumer.consumeBatch(records, acknowledgment);

        // Then
        verify(webSocketService, never()).sendStats(any());
        verify(acknowledgment).acknowledge();
    }
}
