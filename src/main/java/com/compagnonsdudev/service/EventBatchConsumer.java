package com.compagnonsdudev.service;

import com.compagnonsdudev.entity.KEvent;
import com.compagnonsdudev.repository.DltEventRepository;
import com.compagnonsdudev.service.base.AbstractBatchConsumer;
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
 * Extends {@link AbstractBatchConsumer} to benefit from standardized batch processing,
 * persistence fallback, and metrics.
 */
@Service
@Slf4j
public class EventBatchConsumer extends AbstractBatchConsumer<KEvent> {

    private final EventPersistenceService persistenceService;
    private final EventProcessingService processingService;

    /**
     * Constructs a new EventBatchConsumer with its dependencies.
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
     * Kafka listener entry point for batch consumption.
     *
     * @param records The list of Kafka records to process.
     * @param acknowledgment The Kafka acknowledgment object.
     */
    @KafkaListener(
            id = "eventBatchConsumer",
            topics = "${kafka.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "batchFactory")
    public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        super.consume(records, acknowledgment);
    }

    @Override
    protected Optional<KEvent> processRecord(ConsumerRecord<String, String> record) {
        return processingService.processRecord(record);
    }

    @Override
    protected List<KEvent> saveBatch(List<KEvent> entities) {
        return persistenceService.saveEventsBatch(entities);
    }

    @Override
    protected void broadcastPersisted(List<KEvent> entities) {
        webSocketService.sendNewEvents(entities);
    }

    @Override
    protected String getEntityId(KEvent entity) {
        return entity.getEventId();
    }

    @Override
    protected String getEntityTopic(KEvent entity) {
        return entity.getKafkaTopic();
    }

    @Override
    protected Integer getEntityPartition(KEvent entity) {
        return entity.getKafkaPartition();
    }

    @Override
    protected Long getEntityOffset(KEvent entity) {
        return entity.getKafkaOffset();
    }
}
