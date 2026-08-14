package com.compagnonsdudev.dto.dashboard;

import lombok.Builder;

/**
 * Data Transfer Object representing JVM and system performance statistics.
 */
@Builder
public record JvmStatsDTO(
    /** Total amount of heap memory currently used (in bytes). */
    long heapUsed,
    /** Maximum amount of heap memory available (in bytes). */
    long heapMax,
    /** Amount of heap memory guaranteed to be available (in bytes). */
    long heapCommitted,
    /** Current number of active threads in the JVM. */
    int threadCount,
    /** Peak number of active threads in the JVM since startup. */
    int peakThreadCount,
    /** Recent CPU load for the entire system (0.0 to 1.0). */
    double systemCpuLoad,
    /** Recent CPU load for the JVM process (0.0 to 1.0). */
    double processCpuLoad
) {}
