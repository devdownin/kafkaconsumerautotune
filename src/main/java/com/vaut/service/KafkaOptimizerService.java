package com.vaut.service;

import com.vaut.dto.dashboard.KafkaPropertyOptimization;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Service that tracks and maintains a history of Kafka property optimizations.
 * Provides a log of parameter changes made by the auto-tuning service.
 */
@Service
public class KafkaOptimizerService {

    private static final int MAX_HISTORY = 50;
    private static final int RECENT_LIMIT = 10;

    /**
     * Written by the tuning scheduler and read by request threads serving the optimizer view, so
     * the deque is a concurrent one. Newest entries are pushed at the head.
     */
    private final Deque<KafkaPropertyOptimization> optimizations = new ConcurrentLinkedDeque<>();

    /**
     * Retrieves the most recent optimization events.
     *
     * @return A list of the 10 most recent KafkaPropertyOptimization objects, most recent first.
     */
    public List<KafkaPropertyOptimization> getRecentOptimizations() {
        return optimizations.stream().limit(RECENT_LIMIT).toList();
    }

    /**
     * Records a new optimization event.
     *
     * @param property The Kafka property that was changed.
     * @param oldVal The previous value of the property.
     * @param newVal The new value applied to the property.
     * @param reason The reason for the change.
     * @param explanation A pedagogical explanation of the change.
     */
    public void addOptimization(String property, String oldVal, String newVal, String reason, String explanation) {
        optimizations.addFirst(KafkaPropertyOptimization.builder()
                .propertyName(property)
                .oldValue(oldVal)
                .newValue(newVal)
                .reason(reason)
                .explanation(explanation)
                .timestamp(LocalDateTime.now())
                .build());

        while (optimizations.size() > MAX_HISTORY) {
            optimizations.pollLast();
        }
    }
}
