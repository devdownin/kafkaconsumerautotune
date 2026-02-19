package com.vaut.dto.dashboard;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class JvmStatsDTO {
    private long heapUsed;
    private long heapMax;
    private long heapCommitted;
    private int threadCount;
    private int peakThreadCount;
    private double systemCpuLoad;
    private double processCpuLoad;
}
