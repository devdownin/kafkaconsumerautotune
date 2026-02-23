package com.vaut.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaut.config.AppConstants;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

class MetricDescriptionTest {

    @Test
    void testMetricDescriptionIsSet() {
        // Given
        MeterRegistry registry = new SimpleMeterRegistry();
        EventProcessingService service = new EventProcessingService(new ObjectMapper(), registry);
        ReflectionTestUtils.setField(service, "idJsonPath", "$.id");

        // When - triggering an error that should record a metric
        // Providing a null payload should trigger a log.error but no metric in the current implementation?
        // Let's check the code:
        // if (payload == null) { log.error(...); return Optional.empty(); }
        // So I should provide a payload that fails JsonPath.

        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0, "key", "invalid-json");
        service.processRecord(record);

        // Then
        Meter meter = registry.find(AppConstants.METRIC_KAFKA_EVENTS_ERRORS).meter();
        assertThat(meter).isNotNull();
        assertThat(meter.getId().getDescription()).isEqualTo(AppConstants.METRIC_KAFKA_EVENTS_ERRORS_DESC);
    }
}
