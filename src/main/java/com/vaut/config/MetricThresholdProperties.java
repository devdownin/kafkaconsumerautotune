package com.vaut.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "app.metrics")
public class MetricThresholdProperties {
    private Map<String, Threshold> thresholds = new HashMap<>();

    @Data
    public static class Threshold {
        private Double warning;
        private Double critical;
        private String description;
    }
}
