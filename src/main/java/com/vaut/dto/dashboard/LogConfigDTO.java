package com.vaut.dto.dashboard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object representing the logging configuration for a specific logger.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LogConfigDTO {
    /** The name of the logger (e.g., com.vaut.service). */
    private String loggerName;
    /** The explicitly configured log level (e.g., INFO, DEBUG), or null if not explicitly set. */
    private String configuredLevel;
    /** The level currently in effect for this logger (taking inheritance into account). */
    private String effectiveLevel;
}
