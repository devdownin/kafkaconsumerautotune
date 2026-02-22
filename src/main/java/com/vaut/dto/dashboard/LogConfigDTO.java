package com.vaut.dto.dashboard;

import lombok.Builder;

/**
 * Data Transfer Object representing the logging configuration for a specific logger.
 */
@Builder
public record LogConfigDTO(
    /** The name of the logger (e.g., com.vaut.service). */
    String loggerName,
    /** The explicitly configured log level (e.g., INFO, DEBUG), or null if not explicitly set. */
    String configuredLevel,
    /** The level currently in effect for this logger (taking inheritance into account). */
    String effectiveLevel
) {}
