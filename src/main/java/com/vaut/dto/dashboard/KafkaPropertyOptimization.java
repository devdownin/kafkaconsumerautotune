package com.vaut.dto.dashboard;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

/**
 * Data Transfer Object representing a single Kafka property optimization event.
 * Records when and why a consumer parameter was adjusted by the auto-tuning service.
 */
@Data
@Builder
public class KafkaPropertyOptimization {
    /** The name of the Kafka property that was changed (e.g., max.poll.records). */
    private String propertyName;
    /** The value of the property before the optimization. */
    private String oldValue;
    /** The new value applied to the property. */
    private String newValue;
    /** The reason for the adjustment (e.g., "Throughput target exceeded"). */
    private String reason;
    /** The timestamp when the optimization was applied. */
    private LocalDateTime timestamp;
}
