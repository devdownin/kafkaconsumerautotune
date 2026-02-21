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
    @KafkaListener(id = "eventDltConsumer", topics = "${kafka.topic.dlt}", groupId = "${spring.kafka.consumer.group-id}-dlt")
    public void consumeDlt(ConsumerRecord<String, String> record) {
        log.error("Received message in DLT: offset={}, key={}, value={}", 
                record.offset(), record.key(), record.value());
        // Monitoring/Alerting logic here
    }
}
