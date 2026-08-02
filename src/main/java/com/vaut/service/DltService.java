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
import java.util.concurrent.TimeUnit;

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

    /** How long to wait for the broker to acknowledge a republished DLT message. */
    private static final long SEND_TIMEOUT_SECONDS = 10;

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
                .originalKey(record.key())
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
                if (header.value() != null) {
                    headersMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                }
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
        dltEventRepository.findById(id).ifPresent(event -> republish(event, event.getPayload(), false));
    }

    /**
     * Re-sends an event to its original topic and marks it resolved once the broker has acknowledged.
     *
     * @param event The DLT event to republish.
     * @param payload The payload to send (either the stored one or an operator-supplied replacement).
     * @param persistPayload Whether the supplied payload should replace the stored one.
     */
    private void republish(DltEvent event, String payload, boolean persistPayload) {
        if (!"UNRESOLVED".equals(event.getStatus())) {
            return;
        }
        log.info("Retrying event {} to topic {}", event.getId(), event.getOriginalTopic());
        ProducerRecord<String, String> record =
                new ProducerRecord<>(event.getOriginalTopic(), event.getOriginalKey(), payload);
        restoreHeaders(record, event.getHeaders());

        try {
            // Wait for the broker acknowledgement: marking the event resolved on the strength of an
            // unconfirmed asynchronous send would lose the message if the send subsequently failed.
            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying DLT event " + event.getId(), e);
        } catch (Exception e) {
            log.error("Failed to republish DLT event {} to topic {}: {}", event.getId(), event.getOriginalTopic(), e.getMessage());
            throw new IllegalStateException("Failed to republish DLT event " + event.getId(), e);
        }

        meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_RETRIED).increment();

        event.setStatus("RESOLVED");
        if (persistPayload) {
            event.setPayload(payload); // Update with the fixed payload
        }
        event.setResolvedAt(LocalDateTime.now());
        dltEventRepository.save(event);
    }

    /**
     * Marks a specific DLT event as discarded.
     *
     * @param id The ID of the DltEvent to discard.
     */
    @Transactional
    public void discardEvent(Long id) {
        dltEventRepository.findById(id).ifPresent(event -> {
            log.info("Discarding event {}", id);
            markDiscarded(event);
            dltEventRepository.save(event);
        });
    }

    /**
     * Retries all currently unresolved DLT events.
     */
    @Transactional
    public void retryAll() {
        // The events are already loaded, so they are republished directly rather than re-read one
        // row at a time through retryEvent(id).
        List<DltEvent> unresolved = dltEventRepository.findByStatus("UNRESOLVED");
        log.info("Retrying all {} unresolved events", unresolved.size());
        unresolved.forEach(event -> republish(event, event.getPayload(), false));
    }

    /**
     * Discards all currently unresolved DLT events.
     */
    @Transactional
    public void discardAll() {
        List<DltEvent> unresolved = dltEventRepository.findByStatus("UNRESOLVED");
        log.info("Discarding all {} unresolved events", unresolved.size());
        unresolved.forEach(this::markDiscarded);
        dltEventRepository.saveAll(unresolved);
    }

    /**
     * Retries a specific set of DLT events by their IDs.
     *
     * @param ids List of DltEvent IDs to retry.
     */
    @Transactional
    public void bulkRetry(List<Long> ids) {
        log.info("Bulk retrying {} events", ids.size());
        List<DltEvent> events = dltEventRepository.findAllById(ids);
        events.forEach(event -> republish(event, event.getPayload(), false));
    }

    /**
     * Discards a specific set of DLT events by their IDs.
     *
     * @param ids List of DltEvent IDs to discard.
     */
    @Transactional
    public void bulkDiscard(List<Long> ids) {
        log.info("Bulk discarding {} events", ids.size());
        List<DltEvent> events = dltEventRepository.findAllById(ids);
        events.forEach(this::markDiscarded);
        dltEventRepository.saveAll(events);
    }

    private void markDiscarded(DltEvent event) {
        if ("UNRESOLVED".equals(event.getStatus())) {
            event.setStatus("DISCARDED");
            event.setResolvedAt(LocalDateTime.now());
        }
    }

    /**
     * Retries a specific DLT event with a modified payload.
     *
     * @param id The ID of the DltEvent to retry.
     * @param modifiedPayload The new payload to use for the retry.
     */
    @Transactional
    public void retryWithPayload(Long id, String modifiedPayload) {
        dltEventRepository.findById(id).ifPresent(event -> republish(event, modifiedPayload, true));
    }
}
