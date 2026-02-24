package com.vaut.health;

import com.vaut.config.MetricThresholdProperties;
import com.vaut.service.DashboardService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

/**
 * Custom HealthIndicator that monitors Kafka consumer lag.
 * It compares the total lag against configurable thresholds to determine health status.
 */
@Component
@RequiredArgsConstructor
public class KafkaLagHealthIndicator implements HealthIndicator {

    private final DashboardService dashboardService;
    private final MetricThresholdProperties thresholdProperties;

    /**
     * Evaluates the health based on total Kafka lag.
     *
     * @return The health status: UP if lag is low, WARNING if above warning threshold, DOWN if above critical threshold.
     */
    @Override
    public Health health() {
        long lag = dashboardService.calculateTotalLag();
        MetricThresholdProperties.Threshold thresholds = thresholdProperties.getThresholds().get("kafka.lag");

        Health.Builder builder = Health.up()
                .withDetail("lag", lag);

        if (thresholds != null) {
            if (thresholds.getCritical() != null && lag >= thresholds.getCritical()) {
                return builder.down()
                        .withDetail("threshold.critical", thresholds.getCritical())
                        .withDetail("message", "Kafka lag is critical")
                        .build();
            } else if (thresholds.getWarning() != null && lag >= thresholds.getWarning()) {
                return builder.status(new Status("WARNING"))
                        .withDetail("threshold.warning", thresholds.getWarning())
                        .withDetail("message", "Kafka lag is high")
                        .build();
            }
            builder.withDetail("threshold.warning", thresholds.getWarning())
                   .withDetail("threshold.critical", thresholds.getCritical());
        }

        return builder.build();
    }
}
