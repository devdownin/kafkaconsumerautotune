package com.vaut.dto.dashboard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Data Transfer Object for system-wide notifications and events.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SystemEventDTO {
    private String type; // INFO, WARNING, ERROR, SUCCESS
    private String category; // CIRCUIT_BREAKER, AUTO_TUNE, DLT, BATCH
    private String title;
    private String message;
    private LocalDateTime timestamp;
}
