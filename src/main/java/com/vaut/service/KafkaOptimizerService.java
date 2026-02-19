package com.vaut.service;

import com.vaut.dto.dashboard.KafkaPropertyOptimization;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class KafkaOptimizerService {

    private final List<KafkaPropertyOptimization> optimizations = new ArrayList<>();

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

    public List<KafkaPropertyOptimization> getRecentOptimizations() {
        List<KafkaPropertyOptimization> sorted = new ArrayList<>(optimizations);
        sorted.sort((a, b) -> b.getTimestamp().compareTo(a.getTimestamp()));
        return sorted.stream().limit(10).toList();
    }

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
