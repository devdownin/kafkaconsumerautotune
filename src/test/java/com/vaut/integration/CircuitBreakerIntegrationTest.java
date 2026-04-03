package com.vaut.integration;

import com.vaut.repository.KEventRepository;
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

        // Send a message to move it to CLOSED
        // The circuit breaker transitions to CLOSED only after 'permittedNumberOfCallsInHalfOpenState' successful calls (which is 3).
        for (int i = 0; i < 5; i++) {
             kafkaTemplate.send(topicName, "RECOVERY-" + i, "{\"idPassage\": \"RECOVERY-" + i + "\"}");
        }

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        });
    }

    private void reset(Object mock) {
        org.mockito.Mockito.reset(mock);
    }
}
