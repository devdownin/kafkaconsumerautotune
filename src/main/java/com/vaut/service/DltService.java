package com.vaut.service;

import com.vaut.entity.DltEvent;
import com.vaut.repository.DltEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class DltService {

    private final DltEventRepository dltEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.dlt}")
    private String dltTopicName;

    /**
     * Routes a failed message to the Dead Letter Topic (DLT) and prepares it for database persistence.
     *
     * @param record The original Kafka record that failed processing.
     * @param reason The reason for the failure.
     * @return The DltEvent entity.
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
                .dhm(LocalDateTime.now())
                .status("UNRESOLVED")
                .severity(determineSeverity(reason))
                .build();
    }

    private String determineSeverity(String reason) {
        if (reason == null) return "MEDIUM";
        if (reason.contains("Validation") || reason.contains("Parsing")) {
            return "HIGH";
        }
        return "MEDIUM";
    }

    @Transactional
    public void retryEvent(Long id) {
        dltEventRepository.findById(id).ifPresent(event -> {
            if ("UNRESOLVED".equals(event.getStatus())) {
                log.info("Retrying event {} to topic {}", id, event.getOriginalTopic());
                kafkaTemplate.send(event.getOriginalTopic(), event.getPayload());

                event.setStatus("RESOLVED");
                event.setResolvedAt(LocalDateTime.now());
                dltEventRepository.save(event);
            }
        });
    }

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

    @Transactional
    public void retryAll() {
        List<DltEvent> unresolved = dltEventRepository.findByStatus("UNRESOLVED");
        log.info("Retrying all {} unresolved events", unresolved.size());
        unresolved.forEach(event -> retryEvent(event.getId()));
    }

    @Transactional
    public void discardAll() {
        List<DltEvent> unresolved = dltEventRepository.findByStatus("UNRESOLVED");
        log.info("Discarding all {} unresolved events", unresolved.size());
        unresolved.forEach(event -> discardEvent(event.getId()));
    }

    @Transactional
    public void bulkRetry(List<Long> ids) {
        log.info("Bulk retrying {} events", ids.size());
        ids.forEach(this::retryEvent);
    }

    @Transactional
    public void bulkDiscard(List<Long> ids) {
        log.info("Bulk discarding {} events", ids.size());
        ids.forEach(this::discardEvent);
    }

    @Transactional
    public void retryWithPayload(Long id, String modifiedPayload) {
        dltEventRepository.findById(id).ifPresent(event -> {
            if ("UNRESOLVED".equals(event.getStatus())) {
                log.info("Retrying event {} with MODIFIED payload to topic {}", id, event.getOriginalTopic());
                kafkaTemplate.send(event.getOriginalTopic(), modifiedPayload);

                event.setStatus("RESOLVED");
                event.setPayload(modifiedPayload); // Update with the fixed payload
                event.setResolvedAt(LocalDateTime.now());
                dltEventRepository.save(event);
            }
        });
    }
}
