package com.vaut.service;


import com.vaut.config.AppConstants;
import com.vaut.entity.DltEvent;
import com.vaut.entity.KEvent;
import com.vaut.repository.DltEventRepository;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Kafka consumer service that processes events in batches.
 * This service handles generalized message processing, persistence, and DLT routing.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class EventBatchConsumer {

    private final EventPersistenceService persistenceService;
    private final EventProcessingService processingService;
    private final DltService dltService;
    private final DltEventRepository dltEventRepository;
    private final MeterRegistry meterRegistry;
    private final WebSocketService webSocketService;

    /**
     * Consumes a batch of records from Kafka.
     *
     * @param records The batch of Kafka records.
     * @param acknowledgment Acknowledgment handle for manual offset commit.
     */
    @KafkaListener(
            id = "eventBatchConsumer",
            topics = "${kafka.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "batchFactory")
    public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        long startTime = System.currentTimeMillis();
        log.info("Received batch of {} records from Kafka", records.size());
        
        meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_RECEIVED_COUNT).increment(records.size());
        
        List<KEvent> eventsToPersist = new ArrayList<>();
        List<DltEvent> dltEventsToPersist = new ArrayList<>();
        
        for (ConsumerRecord<String, String> record : records) {
            processingService.recordSizeMetric(record);

            processingService.processRecord(record).ifPresentOrElse(
                eventsToPersist::add,
                () -> dltEventsToPersist.add(dltService.routeToDlt(record, "Processing failed (Check logs)"))
            );
        }
        
        persistBatches(eventsToPersist, dltEventsToPersist);
        
        acknowledgment.acknowledge();

        long duration = System.currentTimeMillis() - startTime;
        meterRegistry.timer(AppConstants.METRIC_KAFKA_EVENTS_BATCH_DURATION).record(duration, TimeUnit.MILLISECONDS);
        log.info("Batch of {} records processed in {}ms", records.size(), duration);
    }

    private void persistBatches(List<KEvent> events, List<DltEvent> dltEvents) {
        // Successful events
        if (!events.isEmpty()) {
            List<KEvent> persisted = persistenceService.saveEventsBatch(events);
            meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS, "type", "success").increment(events.size());
            webSocketService.sendNewEvents(persisted);
        }

        // DLT events
        if (!dltEvents.isEmpty()) {
            List<DltEvent> saved = dltEventRepository.saveAll(dltEvents);
            saved.forEach(webSocketService::sendDltEvent);
        }
    }
}
