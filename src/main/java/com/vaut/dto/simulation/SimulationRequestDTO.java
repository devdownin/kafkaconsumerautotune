package com.vaut.dto.simulation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SimulationRequestDTO {
    @lombok.Builder.Default
    private int totalMessages = 1000;
    @lombok.Builder.Default
    private int errorPercentage = 5; // Missing idPassage
    @lombok.Builder.Default
    private int malformedJsonPercentage = 2; // Not a valid JSON
    @lombok.Builder.Default
    private int delayBetweenMessagesMs = 10;
    @lombok.Builder.Default
    private int duplicatePercentage = 0;
    @lombok.Builder.Default
    private int targetThroughputMsgPerSec = 0; // 0 means use delayBetweenMessagesMs
}
