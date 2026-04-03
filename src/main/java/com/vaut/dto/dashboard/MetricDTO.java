package com.vaut.dto.dashboard;

import lombok.Builder;
import java.util.List;

/**
 * Data Transfer Object for representing a single metric with its metadata and history.
 */
@Builder
public record MetricDTO(
    /** The unique name of the metric (e.g., jvm.memory.used). */
    String name,
    /** The type of the metric (e.g., GAUGE, COUNTER). */
    String type,
    /** A human-readable description of the metric. */
    String description,
    /** The current value of the metric formatted as a string. */
    String value,
    /** The unit of measurement (e.g., bytes, seconds). */
    String baseUnit,
    /** Flag indicating if this is an application-specific metric as opposed to standard JVM/system metrics. */
    boolean appSpecific,
    /** Historical data points for this metric to enable trend visualization. */
    List<Double> history,
    /** The detected trend of the metric (UP, DOWN, STABLE). */
    String trend,
    /** The current status of the metric based on predefined thresholds (NORMAL, WARNING, CRITICAL). */
    String status
) {}
