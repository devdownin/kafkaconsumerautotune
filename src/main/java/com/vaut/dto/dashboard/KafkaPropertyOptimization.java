package com.vaut.dto.dashboard;

import lombok.Builder;
import java.time.LocalDateTime;

/**
 * Data Transfer Object representing a single Kafka property optimization event.
 * Records when and why a consumer parameter was adjusted by the auto-tuning service.
 */
@Builder
public record KafkaPropertyOptimization(
    /** The name of the Kafka property that was changed (e.g., max.poll.records). */
    String propertyName,
    /** The value of the property before the optimization. */
    String oldValue,
    /** The new value applied to the property. */
    String newValue,
    /** The reason for the adjustment (e.g., "Throughput target exceeded"). */
    String reason,
    /** The timestamp when the optimization was applied. */
    LocalDateTime timestamp
) {}
