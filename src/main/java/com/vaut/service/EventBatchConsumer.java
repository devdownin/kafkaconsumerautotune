package com.vaut.service;

import com.vaut.entity.DltEvent;
import com.vaut.entity.KEvent;
import com.vaut.repository.DltEventRepository;
import com.vaut.service.base.AbstractBatchConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Concrete implementation of a Kafka batch consumer for {@link KEvent}.
 */
@Service
@Slf4j
public class EventBatchConsumer extends AbstractBatchConsumer<KEvent> {

    private final EventPersistenceService persistenceService;
    private final EventProcessingService processingService;

    /**
     * Constructs a new EventBatchConsumer.
     */
    public EventBatchConsumer(EventPersistenceService persistenceService,
                              EventProcessingService processingService,
                              DltService dltService,
                              DltEventRepository dltEventRepository,
                              MeterRegistry meterRegistry,
                              WebSocketService webSocketService) {
        super(dltService, dltEventRepository, meterRegistry, webSocketService);
        this.persistenceService = persistenceService;
        this.processingService = processingService;
    }

    /**
     * Kafka listener entry point.
     */
    @KafkaListener(
            id = "eventBatchConsumer",
            topics = "${kafka.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "batchFactory")
    public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        super.consume(records, acknowledgment);
    }

                processingService.processRecord(record).ifPresentOrElse(
                    eventsToPersist::add,
                    () -> dltEventsToPersist.add(dltService.routeToDlt(record, "Processing failed (Check logs)"))
                );
            } catch (Exception e) {
                log.error("Unexpected error processing record at partition {} offset {}: {}",
                        record.partition(), record.offset(), e.getMessage());
                dltEventsToPersist.add(dltService.routeToDlt(record, "Unexpected error: " + e.getMessage()));
            }
        }
        
        persistBatches(eventsToPersist, dltEventsToPersist, records);
        
        acknowledgment.acknowledge();

    @Override
    protected List<KEvent> saveBatch(List<KEvent> entities) {
        return persistenceService.saveEventsBatch(entities);
    }

    /**
     * Persists batches of events and DLT events.
     * Includes a fallback mechanism: if a batch persistence fails, it attempts to persist
     * messages individually and routes those that still fail to the DLT.
     *
     * @param events List of successfully processed events to persist.
     * @param dltEvents List of events already marked for DLT.
     * @param originalRecords Original Kafka records (required for DLT fallback).
     */
    private void persistBatches(List<KEvent> events, List<DltEvent> dltEvents, List<ConsumerRecord<String, String>> originalRecords) {
        // Successful events
        if (!events.isEmpty()) {
            try {
                List<KEvent> persisted = persistenceService.saveEventsBatch(events);
                meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS, "type", "success").increment(events.size());
                webSocketService.sendNewEvents(persisted);
            } catch (Exception e) {
                log.error("Batch persistence failed, falling back to individual processing: {}", e.getMessage());
                for (KEvent event : events) {
                    try {
                        List<KEvent> persisted = persistenceService.saveEventsBatch(List.of(event));
                        meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS, "type", "success").increment(1);
                        webSocketService.sendNewEvents(persisted);
                    } catch (Exception ex) {
                        log.error("Failed to persist individual event {}: {}", event.getEventId(), ex.getMessage());
                        // Fallback: route to DLT
                        findOriginalRecord(originalRecords, event).ifPresentOrElse(
                            record -> {
                                DltEvent dltEvent = dltService.routeToDlt(record, "Persistence failed: " + ex.getMessage());
                                List<DltEvent> saved = dltEventRepository.saveAll(List.of(dltEvent));
                                saved.forEach(webSocketService::sendDltEvent);
                            },
                            () -> log.error("Could not find original record for failed event {}", event.getEventId())
                        );
                    }
                }
            }
        }

        // DLT events (from processing stage)
        if (!dltEvents.isEmpty()) {
            try {
                List<DltEvent> saved = dltEventRepository.saveAll(dltEvents);
                saved.forEach(webSocketService::sendDltEvent);
            } catch (Exception e) {
                log.error("Failed to persist DLT events batch: {}", e.getMessage());
                // In extremis, we log them. DLT persistence failure is rare but should be handled.
            }
        }
    }

    /**
     * Finds the original Kafka record corresponding to a KEvent.
     * Matches by topic, partition, and offset.
     */
    private java.util.Optional<ConsumerRecord<String, String>> findOriginalRecord(List<ConsumerRecord<String, String>> records, KEvent event) {
        return records.stream()
                .filter(r -> r.topic().equals(event.getKafkaTopic())
                        && r.partition() == event.getKafkaPartition()
                        && r.offset() == event.getKafkaOffset())
                .findFirst();
    }
}
