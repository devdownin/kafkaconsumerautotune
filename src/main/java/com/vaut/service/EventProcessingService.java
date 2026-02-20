package com.vaut.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.vaut.config.AppConstants;
import com.vaut.entity.KEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Service dedicated to processing individual Kafka records into generalized KEvent entities.
 * Handles ID extraction via JsonPath, header processing, and metadata population.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class EventProcessingService {

    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @Value("${app.event.id-json-path:$.idPassage}")
    private String idJsonPath;

    /**
     * Processes a single Kafka record.
     *
     * @param record The Kafka record to process.
     * @return An Optional containing the mapped KEvent if successful, or empty if processing fails.
     */
    public Optional<KEvent> processRecord(ConsumerRecord<String, String> record) {
        try {
            String payload = record.value();
            if (payload == null) {
                log.error("Received record with null payload at offset {}", record.offset());
                return Optional.empty();
            }

            String eventId;
            try {
                Object result = JsonPath.read(payload, idJsonPath);
                eventId = result != null ? result.toString() : null;
            } catch (Exception e) {
                log.error("Failed to extract event ID using path {} at offset {}: {}", idJsonPath, record.offset(), e.getMessage());
                Counter.builder(AppConstants.METRIC_KAFKA_EVENTS_ERRORS)
                        .description("Number of technical errors encountered during event processing")
                        .tag("type", "id_extraction")
                        .register(meterRegistry)
                        .increment();
                return Optional.empty();
            }

            if (eventId == null || eventId.trim().isEmpty()) {
                log.error("Extracted event ID is null or empty at offset {}", record.offset());
                Counter.builder(AppConstants.METRIC_KAFKA_EVENTS_ERRORS)
                        .description("Number of technical errors encountered during event processing")
                        .tag("type", "missing_id")
                        .register(meterRegistry)
                        .increment();
                return Optional.empty();
            }

            KEvent event = KEvent.builder()
                    .eventId(eventId)
                    .payload(payload)
                    .partitionKey(record.key() != null ? record.key() : eventId)
                    .kafkaTopic(record.topic())
                    .kafkaPartition(record.partition())
                    .kafkaOffset(record.offset())
                    .headers(serializeHeaders(record))
                    .build();

            return Optional.of(event);

        } catch (Exception e) {
            log.error("Error processing message at offset {}: {}", record.offset(), e.getMessage());
            Counter.builder(AppConstants.METRIC_KAFKA_EVENTS_ERRORS)
                    .description("Number of technical errors encountered during event processing")
                    .tag("type", "generic_processing")
                    .register(meterRegistry)
                    .increment();
        }
        return Optional.empty();
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

    public void recordSizeMetric(ConsumerRecord<String, String> record) {
        int size = record.serializedValueSize() > -1 ? record.serializedValueSize() : (record.value() != null ? record.value().length() : 0);
        DistributionSummary.builder(AppConstants.METRIC_KAFKA_EVENT_RECEIVED_SIZE)
                .description("Size of received Kafka event payloads in bytes")
                .tag("topic", record.topic())
                .baseUnit("bytes")
                .register(meterRegistry)
                .record(size);
    }
}
