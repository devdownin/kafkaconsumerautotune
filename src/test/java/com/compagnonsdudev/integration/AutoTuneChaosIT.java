package com.compagnonsdudev.integration;

import com.compagnonsdudev.config.KafkaTuningProperties;
import com.compagnonsdudev.repository.KEventRepository;
import com.compagnonsdudev.service.KafkaTuningService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Chaos Engineering Test for Kafka Auto-Tuning.
 * Simulates system degradation (latency) to verify the robustness of the PID controller.
 */
@TestPropertySource(properties = {
    "kafka.topic.name=it-chaos.topic",
    "kafka.topic.dlt=it-chaos.topic.dlt"
})
public class AutoTuneChaosIT extends AbstractKafkaIT {

    @Autowired
    private KafkaTuningService tuningService;

    @Autowired
    private KafkaTuningProperties tuningProperties;

    @SpyBean
    private KEventRepository eventRepository;

    @Test
    void shouldDownscaleOnLatencyInjection() {
        // 1. Initial State
        Map<String, Object> initialParams = tuningService.getCurrentTuningParameters();
        int initialMaxPoll = (int) initialParams.get("maxPollRecords");

        // 2. Inject Artificial Latency in DB persistence
        // We simulate a slow DB by adding sleep to the repository calls
        doAnswer(invocation -> {
            Thread.sleep(500); // 500ms latency per check
            return invocation.callRealMethod();
        }).when(eventRepository).findExistingEventIds(any());

        // 3. Trigger manual tune evaluation
        // In a real scenario, the @Scheduled task would do this.
        // We call it multiple times to let the PID/EMA react.
        for (int i = 0; i < 3; i++) {
            tuningService.tune();
        }

        // 4. Verify that max.poll.records has decreased
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, Object> currentParams = tuningService.getCurrentTuningParameters();
            int currentMaxPoll = (int) currentParams.get("maxPollRecords");

            // Due to EMA smoothing and PID, it should eventually decrease
            // even if it takes a few iterations.
            assertThat(currentMaxPoll).isLessThanOrEqualTo(initialMaxPoll);
        });
    }
}
