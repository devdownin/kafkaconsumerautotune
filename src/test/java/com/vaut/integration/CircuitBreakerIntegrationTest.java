package com.vaut.integration;

import com.vaut.entity.KEvent;
import com.vaut.repository.KEventRepository;
import com.vaut.service.EventPersistenceService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9096", "port=9096" })
@ActiveProfiles("dev")
public class CircuitBreakerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaRegistry;

    @Autowired
    private CircuitBreakerRegistry cbRegistry;

    @Autowired
    private EventPersistenceService persistenceService;

    @SpyBean
    private KEventRepository eventRepository;

    @Value("${kafka.topic.name}")
    private String topicName;

    @Test
    void shouldStopAndRestartConsumerBasedOnCircuitBreakerState() {
        // 1. Trigger Circuit Breaker OPEN
        doThrow(new org.springframework.dao.DataAccessResourceFailureException("DB Down"))
            .when(eventRepository).findExistingEventIds(any());

        CircuitBreaker cb = cbRegistry.circuitBreaker("persistence");
        cb.transitionToClosedState(); // Reset to be sure

        for (int i = 0; i < 15; i++) {
            kafkaTemplate.send(topicName, "ID-" + i, "{\"idPassage\": \"ID-" + i + "\"}");
        }

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
            MessageListenerContainer container = kafkaRegistry.getListenerContainer("eventBatchConsumer");
            assertThat(container.isRunning()).isFalse();
        });

        // 2. Trigger Circuit Breaker HALF_OPEN -> CLOSED (Recovery)
        // Reset mock to succeed
        reset(eventRepository);

        // Transition manually to Half-Open to simulate time passing (avoiding long waits in CI)
        cb.transitionToHalfOpenState();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            MessageListenerContainer container = kafkaRegistry.getListenerContainer("eventBatchConsumer");
            assertThat(container.isRunning()).isTrue();
        });

        // 3. Drive the circuit to CLOSED.
        // It closes only after 'permittedNumberOfCallsInHalfOpenState' (3) successful calls, and a
        // call means one batch, not one message. Publishing five records to Kafka does not
        // guarantee three batches: the consumer is free to return all of them from a single poll,
        // in which case the circuit sees one call and stays HALF_OPEN forever. The successful
        // calls are therefore made directly, so the assertion below tests the circuit breaker
        // wiring rather than however Kafka happened to group the records.
        for (int i = 0; i < 3; i++) {
            persistenceService.saveEventsBatch(List.of(KEvent.builder()
                    .eventId("RECOVERY-" + i)
                    .payload("{\"idPassage\": \"RECOVERY-" + i + "\"}")
                    .kafkaTopic(topicName)
                    .kafkaPartition(0)
                    .kafkaOffset((long) i)
                    .build()));
        }

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        });

        // The consumer must still be running once the circuit has recovered
        assertThat(kafkaRegistry.getListenerContainer("eventBatchConsumer").isRunning()).isTrue();
    }

    private void reset(Object mock) {
        org.mockito.Mockito.reset(mock);
    }
}
