package com.compagnonsdudev.dto.dashboard;

import lombok.Builder;
import java.time.LocalDateTime;

/**
 * Data Transfer Object for system-wide notifications and events.
 */
@Builder
public record SystemEventDTO(
    String type, // INFO, WARNING, ERROR, SUCCESS
    String category, // CIRCUIT_BREAKER, AUTO_TUNE, DLT, BATCH
    String title,
    String message,
    LocalDateTime timestamp
) {}
