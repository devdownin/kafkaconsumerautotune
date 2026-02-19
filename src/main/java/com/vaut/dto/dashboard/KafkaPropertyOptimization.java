package com.vaut.dto.dashboard;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
public class KafkaPropertyOptimization {
    private String propertyName;
    private String oldValue;
    private String newValue;
    private String reason;
    private LocalDateTime timestamp;
}
