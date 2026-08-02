package com.vaut.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Kafka consumer service dedicated to monitoring the Dead Letter Topic (DLT).
 * Logically separates DLT monitoring from the main event processing flow.
 */
@Service
@Slf4j
public class EventDltConsumer {

    /**
     * Consumes messages from the DLT for logging and monitoring purposes.
     *
     * @param record The Kafka record from the DLT.
     */
    /** Payloads are truncated in the log; the full message is already persisted as a DltEvent. */
    private static final int MAX_LOGGED_PAYLOAD = 512;

    @KafkaListener(id = "eventDltConsumer", topics = "${kafka.topic.dlt}", groupId = "${spring.kafka.consumer.group-id}-dlt")
    public void consumeDlt(ConsumerRecord<String, String> record) {
        log.warn("Received message in DLT: topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), truncate(record.value()));
        // Monitoring/Alerting logic here
    }

    private static String truncate(String value) {
        if (value == null) return null;
        if (value.length() <= MAX_LOGGED_PAYLOAD) return value;
        return value.substring(0, MAX_LOGGED_PAYLOAD) + "... (" + value.length() + " chars total)";
    }
}
