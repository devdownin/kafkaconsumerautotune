package com.vaut.dto.dashboard;

import lombok.Builder;
import lombok.Data;

/**
 * Data Transfer Object representing JVM and system performance statistics.
 */
@Data
@Builder
public class JvmStatsDTO {
    /** Total amount of heap memory currently used (in bytes). */
    private long heapUsed;
    /** Maximum amount of heap memory available (in bytes). */
    private long heapMax;
    /** Amount of heap memory guaranteed to be available (in bytes). */
    private long heapCommitted;
    /** Current number of active threads in the JVM. */
    private int threadCount;
    /** Peak number of active threads in the JVM since startup. */
    private int peakThreadCount;
    /** Recent CPU load for the entire system (0.0 to 1.0). */
    private double systemCpuLoad;
    /** Recent CPU load for the JVM process (0.0 to 1.0). */
    private double processCpuLoad;
}
