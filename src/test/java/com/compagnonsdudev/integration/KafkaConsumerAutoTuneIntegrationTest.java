package com.compagnonsdudev.integration;

import com.compagnonsdudev.entity.DltEvent;
import com.compagnonsdudev.entity.KEvent;
import com.compagnonsdudev.repository.DltEventRepository;
import com.compagnonsdudev.repository.KEventRepository;
import com.compagnonsdudev.service.EventPersistenceService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9095", "port=9095" })
@ActiveProfiles("dev")
public class KafkaConsumerAutoTuneIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KEventRepository eventRepository;

    @Autowired
    private DltEventRepository dltEventRepository;

    @SpyBean
    private EventPersistenceService persistenceService;

    @Value("${kafka.topic.name}")
    private String topicName;

    @BeforeEach
    void setUp() {
        eventRepository.deleteAll();
        dltEventRepository.deleteAll();
        reset(persistenceService);
    }

    @Test
    void shouldProcessValidMessage() {
        String payload = "{\"idPassage\": \"EVENT-001\", \"data\": \"sample data\"}";
        kafkaTemplate.send(topicName, "EVENT-001", payload);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            List<KEvent> events = eventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getEventId()).isEqualTo("EVENT-001");
        });
    }

    @Test
    void shouldRouteToDltOnInvalidJson() {
        String payload = "invalid-json";
        kafkaTemplate.send(topicName, "BAD-001", payload);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            List<DltEvent> dltEvents = dltEventRepository.findAll();
            assertThat(dltEvents).hasSize(1);
            assertThat(dltEvents.get(0).getErrorMessage()).contains("Processing failed");
        });
    }

    @Test
    void shouldRouteToDltOnMissingId() {
        String payload = "{\"something\": \"else\"}";
        kafkaTemplate.send(topicName, "BAD-002", payload);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            List<DltEvent> dltEvents = dltEventRepository.findAll();
            assertThat(dltEvents).hasSize(1);
            assertThat(dltEvents.get(0).getErrorMessage()).contains("Processing failed");
        });
    }

    @Test
    void shouldSkipDuplicateId() {
        String payload = "{\"idPassage\": \"DUP-001\", \"data\": \"first\"}";
        eventRepository.save(KEvent.builder().eventId("DUP-001").payload(payload).build());

        kafkaTemplate.send(topicName, "DUP-001", payload);

        // Wait to ensure processing attempt
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> true);

        assertThat(eventRepository.findAll()).hasSize(1);
        assertThat(dltEventRepository.findAll()).isEmpty();
    }

    @Test
    void shouldHandlePermanentDbErrorWithFallback() {
        String payload = "{\"idPassage\": \"FAIL-DB-001\", \"data\": \"should go to DLT after fallback\"}";

        // Mock a permanent DB error for the first attempt of this batch
        // In a real scenario, this could be a DataIntegrityViolationException
        doThrow(new org.springframework.dao.DataIntegrityViolationException("Permanent DB Error"))
            .when(persistenceService).saveEventsBatch(argThat(list ->
                list.stream().anyMatch(e -> e.getEventId().equals("FAIL-DB-001"))
            ));

        kafkaTemplate.send(topicName, "FAIL-DB-001", payload);

        // We expect the system to:
        // 1. Try to persist the batch -> Fails
        // 2. Catch the error in EventBatchConsumer
        // 3. Fallback to individual persistence OR direct routing to DLT
        // 4. Eventually, the message should be recorded as a DLT event to avoid infinite retry
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            List<DltEvent> dltEvents = dltEventRepository.findAll();
            assertThat(dltEvents).anyMatch(e -> e.getPayload().contains("FAIL-DB-001")
                    && e.getErrorMessage().contains("Persistence failed"));
        });
    }
}
