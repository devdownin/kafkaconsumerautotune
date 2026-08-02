package com.vaut.service;

import com.vaut.dto.dashboard.SystemEventDTO;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Service that listens to Circuit Breaker state transitions and controls the Kafka consumer.
 * If the database becomes unavailable (Circuit OPEN), the consumer is stopped to avoid message loss or unnecessary processing.
 * When the circuit is CLOSED or HALF_OPEN, the consumer is restarted.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class CircuitBreakerStateListener {

    private final KafkaListenerEndpointRegistry registry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final WebSocketService webSocketService;

    /**
     * Container start/stop is dispatched here rather than run inline. Resilience4j publishes state
     * transitions synchronously on the thread that caused them, which for the 'persistence' breaker
     * is the Kafka listener thread itself; calling container.stop() from that thread would block
     * waiting for the very thread that is executing the call.
     */
    private final ExecutorService lifecycleExecutor =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "cb-consumer-lifecycle");
                t.setDaemon(true);
                return t;
            });

    /**
     * Registers a listener for state transition events on the 'persistence' circuit breaker.
     */
    @PostConstruct
    public void registerListeners() {
        circuitBreakerRegistry.circuitBreaker("persistence").getEventPublisher()
            .onStateTransition(event -> {
                CircuitBreaker.StateTransition transition = event.getStateTransition();
                log.warn("Circuit Breaker 'persistence' transitioned from {} to {}",
                        transition.getFromState(), transition.getToState());

                String state = transition.getToState().name();
                SystemEventDTO systemEvent = SystemEventDTO.builder()
                        .category("CIRCUIT_BREAKER")
                        .title("Persistence Circuit Breaker")
                        .message("State changed to " + state)
                        .type(transition.getToState() == CircuitBreaker.State.OPEN ? "ERROR" :
                              (transition.getToState() == CircuitBreaker.State.HALF_OPEN ? "WARNING" : "SUCCESS"))
                        .timestamp(LocalDateTime.now())
                        .build();
                webSocketService.sendSystemEvent(systemEvent);

                if (transition.getToState() == CircuitBreaker.State.OPEN) {
                    stopConsumer();
                } else if (transition.getToState() == CircuitBreaker.State.CLOSED ||
                           transition.getToState() == CircuitBreaker.State.HALF_OPEN) {
                    startConsumer();
                }
            });
    }

    /**
     * Shuts down the lifecycle executor when the application context closes.
     */
    @PreDestroy
    public void shutdown() {
        lifecycleExecutor.shutdownNow();
    }

    private void stopConsumer() {
        lifecycleExecutor.execute(() -> {
            MessageListenerContainer container = registry.getListenerContainer("eventBatchConsumer");
            if (container != null && container.isRunning()) {
                log.warn("Stopping Kafka consumer 'eventBatchConsumer' due to open circuit breaker");
                container.stop();
            }
        });
    }

    private void startConsumer() {
        lifecycleExecutor.execute(() -> {
            MessageListenerContainer container = registry.getListenerContainer("eventBatchConsumer");
            if (container != null && !container.isRunning()) {
                log.info("Restarting Kafka consumer 'eventBatchConsumer' (Circuit is {})",
                        circuitBreakerRegistry.circuitBreaker("persistence").getState());
                container.start();
            }
        });
    }
}
