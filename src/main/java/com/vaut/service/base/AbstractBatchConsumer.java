package com.vaut.service.base;

import com.vaut.config.AppConstants;
import com.vaut.dto.dashboard.SystemEventDTO;
import com.vaut.entity.DltEvent;
import com.vaut.repository.DltEventRepository;
import com.vaut.service.DltService;
import com.vaut.service.WebSocketService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.support.Acknowledgment;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for Kafka batch consumers.
 * Provides the core logic for batch processing, persistence with fallback, DLT routing, and metrics.
 *
 * @param <T> The type of the entity produced by the consumer.
 */
@Slf4j
public abstract class AbstractBatchConsumer<T> {

    protected final DltService dltService;
    protected final DltEventRepository dltEventRepository;
    protected final MeterRegistry meterRegistry;
    protected final WebSocketService webSocketService;

    /**
     * Constructs a new AbstractBatchConsumer with shared dependencies.
     */
    protected AbstractBatchConsumer(DltService dltService,
                                   DltEventRepository dltEventRepository,
                                   MeterRegistry meterRegistry,
                                   WebSocketService webSocketService) {
        this.dltService = dltService;
        this.dltEventRepository = dltEventRepository;
        this.meterRegistry = meterRegistry;
        this.webSocketService = webSocketService;
    }

    /**
     * Processes a single Kafka record into an entity.
     *
     * @param record The Kafka record to process.
     * @return An Optional containing the entity, or empty if processing fails.
     */
    protected abstract Optional<T> processRecord(ConsumerRecord<String, String> record);

    /**
     * Persists a batch of entities.
     *
     * @param entities The entities to save.
     * @return The list of persisted entities.
     */
    protected abstract List<T> saveBatch(List<T> entities);

    /**
     * Broadcasts newly persisted entities via WebSockets.
     *
     * @param entities The persisted entities.
     */
    protected abstract void broadcastPersisted(List<T> entities);

    /**
     * Extracts the business ID from the entity.
     */
    protected abstract String getEntityId(T entity);

    /**
     * Extracts the Kafka topic from the entity.
     */
    protected abstract String getEntityTopic(T entity);

    /**
     * Extracts the Kafka partition from the entity.
     */
    protected abstract Integer getEntityPartition(T entity);

    /**
     * Extracts the Kafka offset from the entity.
     */
    protected abstract Long getEntityOffset(T entity);

    /**
     * Main entry point for the Kafka listener.
     * Handles the batch processing lifecycle.
     */
    protected void consume(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        // Populate MDC for Kafka processing
        MDC.put("correlationId", UUID.randomUUID().toString());
        MDC.put("rgpd", "false");
        MDC.put("eventCategory", "kafka");
        MDC.put("eventType", "process");
        MDC.put("eventOutcome", "success");

        try {
            long startTime = System.currentTimeMillis();
            log.info("Received batch of {} records from Kafka", records.size());

            meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_RECEIVED_COUNT).increment(records.size());

            List<T> entitiesToPersist = new ArrayList<>();
            List<DltEvent> dltEventsToPersist = new ArrayList<>();

            for (ConsumerRecord<String, String> record : records) {
                try {
                    recordSizeMetric(record);

                    processRecord(record).ifPresentOrElse(
                        entitiesToPersist::add,
                        () -> dltEventsToPersist.add(dltService.routeToDlt(record, "Processing failed (Check logs)"))
                    );
                } catch (Exception e) {
                    MDC.put("eventOutcome", "failure");
                    log.error("Unexpected error processing record at partition {} offset {}: {}",
                            record.partition(), record.offset(), e.getMessage());
                    dltEventsToPersist.add(dltService.routeToDlt(record, "Unexpected error: " + e.getMessage()));
                    // Reset outcome for next logs if needed, but here we continue the loop
                    MDC.put("eventOutcome", "success");
                }
            }

            persistBatches(entitiesToPersist, dltEventsToPersist, records);

            acknowledgment.acknowledge();

            long duration = System.currentTimeMillis() - startTime;
            meterRegistry.timer(AppConstants.METRIC_KAFKA_EVENTS_BATCH_DURATION).record(duration, TimeUnit.MILLISECONDS);
            log.info("Batch of {} records processed in {}ms", records.size(), duration);
        } catch (Exception e) {
            MDC.put("eventOutcome", "failure");
            log.error("Critical error in batch consumption: {}", e.getMessage());
            throw e;
        } finally {
            MDC.clear();
        }
    }

    /**
     * Persists entities and DLT events with a fallback mechanism for resilience.
     * Integrates with the Circuit Breaker: if the circuit is open, it avoids further processing.
     */
    private void persistBatches(List<T> entities, List<DltEvent> dltEvents, List<ConsumerRecord<String, String>> originalRecords) {
        if (!entities.isEmpty()) {
            try {
                List<T> persisted = saveBatch(entities);
                meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS, "type", "success").increment(entities.size());
                broadcastPersisted(persisted);
            } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                MDC.put("eventOutcome", "failure");
                log.error("Circuit breaker is OPEN. Database is likely unavailable. Stopping batch processing.");
                throw e; // Re-throw to trigger Kafka retry/stop
            } catch (Exception e) {
                log.error("Batch persistence failed, falling back to individual processing: {}", e.getMessage());
                webSocketService.sendSystemEvent(SystemEventDTO.builder()
                        .type("WARNING")
                        .category("BATCH")
                        .title("Batch Persistence Failure")
                        .message("Falling back to individual processing for " + entities.size() + " records")
                        .timestamp(java.time.LocalDateTime.now())
                        .build());
                for (T entity : entities) {
                    try {
                        List<T> persisted = saveBatch(List.of(entity));
                        meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS, "type", "success").increment(1);
                        broadcastPersisted(persisted);
                    } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException ex) {
                        MDC.put("eventOutcome", "failure");
                        log.error("Circuit breaker opened during individual persistence. Stopping.");
                        throw ex;
                    } catch (Exception ex) {
                        MDC.put("eventOutcome", "failure");
                        log.error("Failed to persist individual entity {}: {}", getEntityId(entity), ex.getMessage());
                        findOriginalRecord(originalRecords, entity).ifPresentOrElse(
                            record -> {
                                try {
                                    DltEvent dltEvent = dltService.routeToDlt(record, "Persistence failed: " + ex.getMessage());
                                    List<DltEvent> saved = dltEventRepository.saveAll(List.of(dltEvent));
                                    saved.forEach(webSocketService::sendDltEvent);
                                } catch (Exception dltEx) {
                                    log.error("Critical: Could not route to DLT as database is also failing DLT persistence: {}", dltEx.getMessage());
                                }
                            },
                            () -> log.error("Could not find original record for failed entity {}", getEntityId(entity))
                        );
                        MDC.put("eventOutcome", "success"); // Reset for next entity
                    }
                }
            }
        }

        if (!dltEvents.isEmpty()) {
            try {
                List<DltEvent> saved = dltEventRepository.saveAll(dltEvents);
                saved.forEach(webSocketService::sendDltEvent);
            } catch (Exception e) {
                MDC.put("eventOutcome", "failure");
                log.error("Failed to persist DLT events batch: {}", e.getMessage());
                MDC.put("eventOutcome", "success");
            }
        }
    }

    private Optional<ConsumerRecord<String, String>> findOriginalRecord(List<ConsumerRecord<String, String>> records, T entity) {
        String topic = getEntityTopic(entity);
        Integer partition = getEntityPartition(entity);
        Long offset = getEntityOffset(entity);

        return records.stream()
                .filter(r -> r.topic().equals(topic)
                        && r.partition() == partition
                        && r.offset() == offset)
                .findFirst();
    }

    private void recordSizeMetric(ConsumerRecord<String, String> record) {
        int size = record.serializedValueSize() > -1 ? record.serializedValueSize() : (record.value() != null ? record.value().length() : 0);
        meterRegistry.summary(AppConstants.METRIC_KAFKA_EVENT_RECEIVED_SIZE, "topic", record.topic()).record(size);
    }
}
