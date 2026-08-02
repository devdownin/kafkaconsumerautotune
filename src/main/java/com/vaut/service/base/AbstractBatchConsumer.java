package com.vaut.service.base;

import com.vaut.config.AppConstants;
import com.vaut.dto.dashboard.SystemEventDTO;
import com.vaut.exception.PermanentException;
import com.vaut.exception.TransientException;
import com.vaut.entity.DltEvent;
import com.vaut.repository.DltEventRepository;
import com.vaut.service.DltService;
import com.vaut.service.WebSocketService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for high-performance Kafka batch consumers with built-in resilience.
 *
 * <p>This class implements the "Template Method" pattern to standardize the batch consumption
 * lifecycle while allowing concrete implementations to define specific parsing and persistence logic.</p>
 *
 * <h3>Key Features:</h3>
 * <ul>
 *     <li><b>Batch Efficiency:</b> Processes groups of messages in a single transaction.</li>
 *     <li><b>Individual Fallback:</b> If a batch persistence fails, it automatically switches to
 *     one-by-one processing to isolate "poison messages" and allow healthy ones to proceed.</li>
 *     <li><b>Observability:</b> Automatic MDC management (correlation IDs, Kafka coordinates) and Micrometer metrics.</li>
 *     <li><b>Circuit Breaker Integration:</b> Cooperates with Resilience4j to stop consumption if downstream systems are failing.</li>
 * </ul>
 *
 * @param <T> The type of the business entity produced by the consumer.
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
        // Default MDC for the batch
        MDC.put(AppConstants.MDC_CORRELATION_ID, UUID.randomUUID().toString());
        MDC.put(AppConstants.MDC_RGPD, "false");
        MDC.put(AppConstants.MDC_EVENT_CATEGORY, "kafka");
        MDC.put(AppConstants.MDC_EVENT_TYPE, "process");
        MDC.put(AppConstants.MDC_EVENT_OUTCOME, "success");

        try {
            long startTime = System.currentTimeMillis();
            MDC.put(AppConstants.MDC_EVENT_OUTCOME, "success");
            log.info("Received batch of {} records from Kafka", records.size());

            meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_RECEIVED_COUNT).increment(records.size());

            List<T> entitiesToPersist = new ArrayList<>();
            List<DltEvent> dltEventsToPersist = new ArrayList<>();

            for (ConsumerRecord<String, String> record : records) {
                // Extract correlation ID from headers if available
                String correlationId = extractHeader(record, AppConstants.HEADER_CORRELATION_ID)
                        .orElse(UUID.randomUUID().toString());

                MDC.put(AppConstants.MDC_CORRELATION_ID, correlationId);
                MDC.put(AppConstants.MDC_KAFKA_TOPIC, record.topic());
                MDC.put(AppConstants.MDC_KAFKA_PARTITION, String.valueOf(record.partition()));
                MDC.put(AppConstants.MDC_KAFKA_OFFSET, String.valueOf(record.offset()));
                MDC.put(AppConstants.MDC_KAFKA_KEY, record.key());

                try {
                    recordSizeMetric(record);

                    processRecord(record).ifPresentOrElse(
                        entitiesToPersist::add,
                        () -> {
                            MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
                            dltEventsToPersist.add(dltService.routeToDlt(record, "Processing failed (Check logs)"));
                        }
                    );
                } catch (PermanentException e) {
                    MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
                    log.error("Permanent error processing record (Routing to DLT): {}", e.getMessage());
                    dltEventsToPersist.add(dltService.routeToDlt(record, "Permanent error: " + e.getMessage()));
                } catch (TransientException e) {
                    MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
                    log.warn("Transient error processing record (Rethrowing for Kafka retry): {}", e.getMessage());
                    throw e; // Rethrow to trigger Kafka retry
                } catch (Exception e) {
                    MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
                    log.error("Unexpected error processing record: {}", e.getMessage());
                    dltEventsToPersist.add(dltService.routeToDlt(record, "Unexpected error: " + e.getMessage()));
                } finally {
                    MDC.put(AppConstants.MDC_EVENT_OUTCOME, "success");
                    // We don't remove correlationId here to keep it for the persistBatches if it's individual fallback
                    // but we will reset it at the start of next iteration anyway.
                    MDC.remove(AppConstants.MDC_KAFKA_TOPIC);
                    MDC.remove(AppConstants.MDC_KAFKA_PARTITION);
                    MDC.remove(AppConstants.MDC_KAFKA_OFFSET);
                    MDC.remove(AppConstants.MDC_KAFKA_KEY);
                    MDC.remove(AppConstants.MDC_EVENT_ID);
                }
            }

            persistBatches(entitiesToPersist, dltEventsToPersist, records);

            acknowledgment.acknowledge();

            long duration = System.currentTimeMillis() - startTime;
            meterRegistry.timer(AppConstants.METRIC_KAFKA_EVENTS_BATCH_DURATION).record(duration, TimeUnit.MILLISECONDS);
            log.info("Batch of {} records processed in {}ms", records.size(), duration);
        } catch (Exception e) {
            MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
            log.error("Critical error in batch consumption: {}", e.getMessage());
            throw e;
        } finally {
            MDC.remove(AppConstants.MDC_EVENT_OUTCOME);
            MDC.clear();
        }
    }

    private Optional<String> extractHeader(ConsumerRecord<String, String> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header != null && header.value() != null) {
            return Optional.of(new String(header.value(), StandardCharsets.UTF_8));
        }
        return Optional.empty();
    }

    /**
     * Handles the complex task of persisting both valid entities and DLT events.
     *
     * <p>The logic follows a multi-level fallback strategy:</p>
     * <ol>
     *     <li><b>Batch Attempt:</b> Try to save all valid entities in one go.</li>
     *     <li><b>Circuit Breaker Check:</b> If the DB is down (Circuit Open), stop immediately and let Kafka retry later.</li>
     *     <li><b>Individual Fallback:</b> If batch save fails (e.g. data error), try each entity one by one.
     *         Successful entities are committed; failed ones are routed to DLT.</li>
     * </ol>
     *
     * @param entities Entities that were successfully parsed and are ready for DB.
     * @param dltEvents Events that failed parsing and should be sent to DLT.
     * @param originalRecords The raw Kafka records (needed for DLT routing in case of persistence failure).
     */
    private void persistBatches(List<T> entities, List<DltEvent> dltEvents, List<ConsumerRecord<String, String>> originalRecords) {
        if (!entities.isEmpty()) {
            try {
                List<T> persisted = saveBatch(entities);
                meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS, "type", "success").increment(entities.size());
                broadcastPersisted(persisted);
            } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
                log.error("Circuit breaker is OPEN. Database is likely unavailable. Stopping batch processing.");
                throw e; // Re-throw to trigger Kafka retry/stop
            } catch (Exception e) {
                MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
                log.error("Batch persistence failed, falling back to individual processing: {}", e.getMessage());
                MDC.put(AppConstants.MDC_EVENT_OUTCOME, "success");
                webSocketService.sendSystemEvent(SystemEventDTO.builder()
                        .type("WARNING")
                        .category("BATCH")
                        .title("Batch Persistence Failure")
                        .message("Falling back to individual processing for " + entities.size() + " records")
                        .timestamp(java.time.LocalDateTime.now())
                        .build());
                // Indexed once up front: this fallback resolves an original record per failed entity,
                // which is a full scan of the batch each time if done by search.
                Map<String, ConsumerRecord<String, String>> recordsByCoordinates = indexByCoordinates(originalRecords);

                for (T entity : entities) {
                    MDC.put(AppConstants.MDC_KAFKA_TOPIC, getEntityTopic(entity));
                    MDC.put(AppConstants.MDC_KAFKA_PARTITION, String.valueOf(getEntityPartition(entity)));
                    MDC.put(AppConstants.MDC_KAFKA_OFFSET, String.valueOf(getEntityOffset(entity)));

                    try {
                        List<T> persisted = saveBatch(List.of(entity));
                        meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS, "type", "success").increment(1);
                        broadcastPersisted(persisted);
                    } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException ex) {
                        MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
                        log.error("Circuit breaker opened during individual persistence. Stopping.");
                        throw ex;
                    } catch (Exception ex) {
                        MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
                        log.error("Failed to persist individual entity {}: {}", getEntityId(entity), ex.getMessage());
                        findOriginalRecord(recordsByCoordinates, entity).ifPresentOrElse(
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
                        MDC.put(AppConstants.MDC_EVENT_OUTCOME, "success"); // Reset for next entity
                    } finally {
                        MDC.remove(AppConstants.MDC_KAFKA_TOPIC);
                        MDC.remove(AppConstants.MDC_KAFKA_PARTITION);
                        MDC.remove(AppConstants.MDC_KAFKA_OFFSET);
                    }
                }
            }
        }

        if (!dltEvents.isEmpty()) {
            try {
                List<DltEvent> saved = dltEventRepository.saveAll(dltEvents);
                saved.forEach(webSocketService::sendDltEvent);
            } catch (Exception e) {
                MDC.put(AppConstants.MDC_EVENT_OUTCOME, "failure");
                log.error("Failed to persist DLT events batch: {}", e.getMessage());
                MDC.put(AppConstants.MDC_EVENT_OUTCOME, "success");
            }
        }
    }

    private Map<String, ConsumerRecord<String, String>> indexByCoordinates(List<ConsumerRecord<String, String>> records) {
        Map<String, ConsumerRecord<String, String>> index = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            index.putIfAbsent(coordinateKey(record.topic(), record.partition(), record.offset()), record);
        }
        return index;
    }

    private Optional<ConsumerRecord<String, String>> findOriginalRecord(
            Map<String, ConsumerRecord<String, String>> recordsByCoordinates, T entity) {
        String topic = getEntityTopic(entity);
        Integer partition = getEntityPartition(entity);
        Long offset = getEntityOffset(entity);

        if (topic == null || partition == null || offset == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(recordsByCoordinates.get(coordinateKey(topic, partition, offset)));
    }

    private static String coordinateKey(String topic, int partition, long offset) {
        return topic + '\0' + partition + '\0' + offset;
    }

    private void recordSizeMetric(ConsumerRecord<String, String> record) {
        int size = record.serializedValueSize() > -1 ? record.serializedValueSize() : (record.value() != null ? record.value().length() : 0);
        meterRegistry.summary(AppConstants.METRIC_KAFKA_EVENT_RECEIVED_SIZE, "topic", record.topic()).record(size);
    }
}
