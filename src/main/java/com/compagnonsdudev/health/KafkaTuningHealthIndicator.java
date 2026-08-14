package com.compagnonsdudev.health;

import com.compagnonsdudev.service.KafkaTuningService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Custom HealthIndicator that exposes the state and parameters of the Kafka auto-tuning service.
 */
@Component
@RequiredArgsConstructor
public class KafkaTuningHealthIndicator implements HealthIndicator {

    private final KafkaTuningService kafkaTuningService;

    /**
     * Reports the current status and parameters of the auto-tuning service.
     *
     * @return Health status UP with details about the tuning parameters.
     */
    @Override
    public Health health() {
        Map<String, Object> parameters = kafkaTuningService.getCurrentTuningParameters();

        return Health.up()
                .withDetails(parameters)
                .withDetail("service", "KafkaTuningService")
                .build();
    }
}
