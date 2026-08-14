package com.compagnonsdudev.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for metric thresholds, used to determine the status (NORMAL, WARNING, CRITICAL) of various metrics.
 */
@Data
@Component
@ConfigurationProperties(prefix = "app.metrics")
public class MetricThresholdProperties {
    /** A map of metric names to their respective warning and critical thresholds. */
    private Map<String, Threshold> thresholds = new HashMap<>();

    /**
     * Threshold values for a specific metric.
     */
    @Data
    public static class Threshold {
        /** The value above which the metric is considered in a WARNING state. */
        private Double warning;
        /** The value above which the metric is considered in a CRITICAL state. */
        private Double critical;
    }
}
