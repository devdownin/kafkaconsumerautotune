package com.vaut.dto.simulation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SimulationStatusDTO {
    private boolean running;
    private int processedMessages;
    private int totalMessages;
    private int sentValid;
    private int sentError;
    private int sentMalformed;
    private long startTime;
}
