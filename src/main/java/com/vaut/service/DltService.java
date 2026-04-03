package com.vaut.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaut.config.AppConstants;
import com.vaut.entity.DltEvent;
import com.vaut.repository.DltEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for managing the Dead Letter Topic (DLT) flow.
 * Handles routing failed messages to the DLT and provides operations for retrying or discarding them.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DltService {

    private final DltEventRepository dltEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @Value("${kafka.topic.dlt}")
    private String dltTopicName;

    /**
     * Routes a failed message to the Dead Letter Topic (DLT) and prepares it for database persistence.
     *
     * @param record The original Kafka record that failed processing.
     * @param reason The reason for the failure.
     * @return The DltEvent entity representing the failure.
     */
    public DltEvent routeToDlt(ConsumerRecord<String, String> record, String reason) {
        // 1. Send to Kafka DLT topic
        ProducerRecord<String, String> dltRecord = new ProducerRecord<>(dltTopicName, record.key(), record.value());
        dltRecord.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, record.topic().getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, (reason != null ? reason : "Unknown error").getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(dltRecord);

        // 2. Create DltEvent entity
        return DltEvent.builder()
                .originalTopic(record.topic())
                .originalPartition(record.partition())
                .originalOffset(record.offset())
                .errorMessage(reason)
                .payload(record.value())
                .headers(serializeHeaders(record))
                .dhm(LocalDateTime.now())
                .status("UNRESOLVED")
                .severity(determineSeverity(reason))
                .build();
    }

    private String serializeHeaders(ConsumerRecord<String, String> record) {
        try {
            Map<String, String> headersMap = new HashMap<>();
            for (Header header : record.headers()) {
                headersMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
            return headersMap.isEmpty() ? null : objectMapper.writeValueAsString(headersMap);
        } catch (Exception e) {
            log.warn("Failed to serialize headers for record at offset {}: {}", record.offset(), e.getMessage());
            return null;
        }
    }

    private void restoreHeaders(ProducerRecord<String, String> record, String serializedHeaders) {
        if (serializedHeaders == null || serializedHeaders.isEmpty()) return;
        try {
            Map<String, String> headersMap = objectMapper.readValue(serializedHeaders, new TypeReference<Map<String, String>>() {});
            headersMap.forEach((k, v) -> {
                if (v != null) {
                    record.headers().add(k, v.getBytes(StandardCharsets.UTF_8));
                }
            });
        } catch (Exception e) {
            log.warn("Failed to restore headers: {}", e.getMessage());
        }
    }

    private String determineSeverity(String reason) {
        if (reason == null) return "MEDIUM";
        if (reason.contains("Validation") || reason.contains("Parsing")) {
            return "HIGH";
        }
        return "MEDIUM";
    }

    /**
     * Retries a specific DLT event by re-sending it to its original Kafka topic.
     *
     * @param id The ID of the DltEvent to retry.
     */
    @Transactional
    public void retryEvent(Long id) {
        dltEventRepository.findById(id).ifPresent(event -> {
            if ("UNRESOLVED".equals(event.getStatus())) {
                log.info("Retrying event {} to topic {}", id, event.getOriginalTopic());
                ProducerRecord<String, String> record = new ProducerRecord<>(event.getOriginalTopic(), event.getPayload());
                restoreHeaders(record, event.getHeaders());
                kafkaTemplate.send(record);
                meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_RETRIED).increment();

                event.setStatus("RESOLVED");
                event.setResolvedAt(LocalDateTime.now());
                dltEventRepository.save(event);
            }
        });
    }

    /**
     * Marks a specific DLT event as discarded.
     *
     * @param id The ID of the DltEvent to discard.
     */
    @Transactional
    public void discardEvent(Long id) {
        dltEventRepository.findById(id).ifPresent(event -> {
            if ("UNRESOLVED".equals(event.getStatus())) {
                log.info("Discarding event {}", id);
                event.setStatus("DISCARDED");
                event.setResolvedAt(LocalDateTime.now());
                dltEventRepository.save(event);
            }
        });
    }

    /**
     * Retries all currently unresolved DLT events.
     */
    @Transactional
    public void retryAll() {
        List<DltEvent> unresolved = dltEventRepository.findByStatus("UNRESOLVED");
        log.info("Retrying all {} unresolved events", unresolved.size());
        unresolved.forEach(event -> retryEvent(event.getId()));
    }

    /**
     * Discards all currently unresolved DLT events.
     */
    @Transactional
    public void discardAll() {
        List<DltEvent> unresolved = dltEventRepository.findByStatus("UNRESOLVED");
        log.info("Discarding all {} unresolved events", unresolved.size());
        unresolved.forEach(event -> discardEvent(event.getId()));
    }

    /**
     * Retries a specific set of DLT events by their IDs.
     *
     * @param ids List of DltEvent IDs to retry.
     */
    @Transactional
    public void bulkRetry(List<Long> ids) {
        log.info("Bulk retrying {} events", ids.size());
        ids.forEach(this::retryEvent);
    }

    /**
     * Discards a specific set of DLT events by their IDs.
     *
     * @param ids List of DltEvent IDs to discard.
     */
    @Transactional
    public void bulkDiscard(List<Long> ids) {
        log.info("Bulk discarding {} events", ids.size());
        ids.forEach(this::discardEvent);
    }

    /**
     * Retries a specific DLT event with a modified payload.
     *
     * @param id The ID of the DltEvent to retry.
     * @param modifiedPayload The new payload to use for the retry.
     */
    @Transactional
    public void retryWithPayload(Long id, String modifiedPayload) {
        dltEventRepository.findById(id).ifPresent(event -> {
            if ("UNRESOLVED".equals(event.getStatus())) {
                log.info("Retrying event {} with MODIFIED payload to topic {}", id, event.getOriginalTopic());
                ProducerRecord<String, String> record = new ProducerRecord<>(event.getOriginalTopic(), modifiedPayload);
                restoreHeaders(record, event.getHeaders());
                kafkaTemplate.send(record);
                meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_RETRIED).increment();

                event.setStatus("RESOLVED");
                event.setPayload(modifiedPayload); // Update with the fixed payload
                event.setResolvedAt(LocalDateTime.now());
                dltEventRepository.save(event);
            }
        });
    }
}
