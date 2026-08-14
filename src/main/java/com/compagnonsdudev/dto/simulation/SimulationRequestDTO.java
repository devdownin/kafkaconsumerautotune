package com.compagnonsdudev.dto.simulation;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Parameters driving a traffic simulation run.
 *
 * <p>The bounds below are enforced server-side: the dashboard's own checks are
 * client-side only and any caller can post straight to the endpoint.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SimulationRequestDTO {

    @Min(value = 1, message = "must be at least 1")
    @Max(value = 10_000_000, message = "must not exceed 10000000")
    @lombok.Builder.Default
    private int totalMessages = 1000;

    @Min(value = 0, message = "must be between 0 and 100")
    @Max(value = 100, message = "must be between 0 and 100")
    @lombok.Builder.Default
    private int errorPercentage = 5; // Missing idPassage

    @Min(value = 0, message = "must be between 0 and 100")
    @Max(value = 100, message = "must be between 0 and 100")
    @lombok.Builder.Default
    private int malformedJsonPercentage = 2; // Not a valid JSON

    @Min(value = 0, message = "must not be negative")
    @Max(value = 60_000, message = "must not exceed 60000 ms")
    @lombok.Builder.Default
    private int delayBetweenMessagesMs = 10;

    @Min(value = 0, message = "must be between 0 and 100")
    @Max(value = 100, message = "must be between 0 and 100")
    @lombok.Builder.Default
    private int duplicatePercentage = 0;

    @Min(value = 0, message = "must not be negative")
    @Max(value = 1_000_000, message = "must not exceed 1000000 msg/s")
    @lombok.Builder.Default
    private int targetThroughputMsgPerSec = 0; // 0 means use delayBetweenMessagesMs
}
