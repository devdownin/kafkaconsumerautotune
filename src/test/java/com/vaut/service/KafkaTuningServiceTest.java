package com.vaut.service;

import com.vaut.config.AppConstants;
import com.vaut.config.KafkaTuningProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaTuningServiceTest {

    private static final int INITIAL_MAX_POLL = 100;

    @Mock private DefaultKafkaConsumerFactory<String, String> consumerFactory;
    @Mock private KafkaListenerEndpointRegistry registry;
    @Mock private KafkaOptimizerService optimizerService;
    @Mock private DashboardService dashboardService;

    private MeterRegistry meterRegistry;
    private KafkaTuningService tuningService;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, INITIAL_MAX_POLL);
        configs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10240);
        configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        configs.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800);
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        when(consumerFactory.getConfigurationProperties()).thenReturn(configs);

        tuningService = new KafkaTuningService(consumerFactory, registry, meterRegistry,
                Optional.empty(), new KafkaTuningProperties(), optimizerService, dashboardService);

        // Traffic well over the 1200ms target, so the controller wants a large reduction
        meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_RECEIVED_COUNT).increment(5000);
        meterRegistry.timer(AppConstants.METRIC_KAFKA_EVENTS_BATCH_DURATION).record(5000, TimeUnit.MILLISECONDS);
        // Give the throughput calculation a non-zero interval to work with
        ReflectionTestUtils.setField(tuningService, "lastTimestamp", System.currentTimeMillis() - 60_000);
    }

    @Test
    void appliesParameterChangeWhenCooldownHasElapsed() {
        tuningService.tune();

        verify(consumerFactory).updateConfigs(any());
        assertThat(tuningService.getCurrentTuningParameters().get("maxPollRecords"))
                .isNotEqualTo(INITIAL_MAX_POLL);
    }

    @Test
    void keepsReportedParametersInSyncWithTheConsumerWhenChangeIsDeferred() {
        // Simulate a restart that just happened, putting us inside the cooldown window
        ReflectionTestUtils.setField(tuningService, "lastRestartTimestamp", System.currentTimeMillis());

        tuningService.tune();

        // The change is deferred, so nothing reaches the consumer...
        verify(consumerFactory, never()).updateConfigs(any());
        // ...and the tracked value must still describe what the consumer is actually running.
        // Advancing it here would both misreport the live config and suppress the change
        // permanently, since the next cycle would see no difference left to apply.
        assertThat(tuningService.getCurrentTuningParameters().get("maxPollRecords"))
                .isEqualTo(INITIAL_MAX_POLL);
        verify(optimizerService, never()).addOptimization(any(), any(), any(), any(), any());
    }

    @Test
    void reProposesADeferredChangeOnTheNextCycle() {
        ReflectionTestUtils.setField(tuningService, "lastRestartTimestamp", System.currentTimeMillis());
        tuningService.tune();
        verify(consumerFactory, never()).updateConfigs(any());

        // Cooldown expires; the change that was deferred must still be applied
        ReflectionTestUtils.setField(tuningService, "lastRestartTimestamp", 0L);
        ReflectionTestUtils.setField(tuningService, "lastTimestamp", System.currentTimeMillis() - 60_000);
        meterRegistry.counter(AppConstants.METRIC_KAFKA_EVENTS_RECEIVED_COUNT).increment(5000);
        meterRegistry.timer(AppConstants.METRIC_KAFKA_EVENTS_BATCH_DURATION).record(5000, TimeUnit.MILLISECONDS);

        tuningService.tune();

        verify(consumerFactory).updateConfigs(any());
        assertThat(tuningService.getCurrentTuningParameters().get("maxPollRecords"))
                .isNotEqualTo(INITIAL_MAX_POLL);
    }
}
