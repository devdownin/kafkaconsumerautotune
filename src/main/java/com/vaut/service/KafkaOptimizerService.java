package com.vaut.service;

import com.vaut.dto.dashboard.KafkaPropertyOptimization;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Service that tracks and maintains a history of Kafka property optimizations.
 * Provides a log of parameter changes made by the auto-tuning service.
 */
@Service
public class KafkaOptimizerService {

    private final List<KafkaPropertyOptimization> optimizations = new ArrayList<>();

    /**
     * Initializes the optimizer service with some initial (mock) data for demonstration.
     */
    public KafkaOptimizerService() {
        // Initial mock data
        optimizations.add(KafkaPropertyOptimization.builder()
                .propertyName("max.poll.records")
                .oldValue("100")
                .newValue("500")
                .reason("High throughput detected, increasing batch size for better efficiency")
                .timestamp(LocalDateTime.now().minusHours(2))
                .build());

        optimizations.add(KafkaPropertyOptimization.builder()
                .propertyName("fetch.min.bytes")
                .oldValue("10240")
                .newValue("51200")
                .reason("Network overhead reduction: waiting for more data before fetching")
                .timestamp(LocalDateTime.now().minusMinutes(45))
                .build());

        optimizations.add(KafkaPropertyOptimization.builder()
                .propertyName("session.timeout.ms")
                .oldValue("30000")
                .newValue("45000")
                .reason("Detected transient network instability, increasing session timeout to prevent unnecessary rebalances")
                .timestamp(LocalDateTime.now().minusMinutes(120))
                .build());

        optimizations.add(KafkaPropertyOptimization.builder()
                .propertyName("max.partition.fetch.bytes")
                .oldValue("1048576")
                .newValue("2097152")
                .reason("Large messages detected in partition, increasing fetch size to avoid truncated batches")
                .timestamp(LocalDateTime.now().minusDays(1))
                .build());
    }

    /**
     * Retrieves the most recent optimization events.
     *
     * @return A list of the 10 most recent KafkaPropertyOptimization objects, sorted by timestamp descending.
     */
    public List<KafkaPropertyOptimization> getRecentOptimizations() {
        List<KafkaPropertyOptimization> sorted = new ArrayList<>(optimizations);
        sorted.sort((a, b) -> b.timestamp().compareTo(a.timestamp()));
        return sorted.stream().limit(10).toList();
    }

    /**
     * Records a new optimization event.
     *
     * @param property The Kafka property that was changed.
     * @param oldVal The previous value of the property.
     * @param newVal The new value applied to the property.
     * @param reason The reason for the change.
     */
    public void addOptimization(String property, String oldVal, String newVal, String reason) {
        optimizations.add(0, KafkaPropertyOptimization.builder()
                .propertyName(property)
                .oldValue(oldVal)
                .newValue(newVal)
                .reason(reason)
                .timestamp(LocalDateTime.now())
                .build());

        if (optimizations.size() > 50) {
            optimizations.remove(optimizations.size() - 1);
        }
    }
}
