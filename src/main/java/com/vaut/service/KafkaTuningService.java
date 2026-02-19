package com.vaut.service;

import com.vaut.config.AppConstants;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.vaut.config.KafkaTuningProperties;

/**
 * Service that automatically tunes Kafka consumer parameters based on observed performance.
 * It monitors throughput and batch processing time to adjust max.poll.records.
 *
 * Includes safety mechanisms to avoid frequent rebalances:
 * - Minimum interval between restarts.
 * - Minimum threshold for parameter changes.
 * - Only tunes when active traffic is detected.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaTuningService {

    private final DefaultKafkaConsumerFactory<String, String> consumerFactory;
    private final KafkaListenerEndpointRegistry registry;
    private final MeterRegistry meterRegistry;
    private final Optional<AdminClient> adminClient;
    private final KafkaTuningProperties tuningProperties;

    @Value("${kafka.topic.name}")
    private String topicName;

    private double lastCount = 0;
    private long lastTimestamp = System.currentTimeMillis();
    private long lastRestartTimestamp = 0;

    // PID Controller State
    private double integral = 0;
    private double previousError = 0;

    // Current tuned values
    private int currentMaxPollRecords = -1;
    private int currentFetchMinBytes = -1;
    private int currentFetchMaxWaitMs = -1;

    /**
     * Periodic task to analyze metrics and adjust Kafka parameters if necessary.
     * It uses a PID controller to target an optimal batch duration and synchronizes
     * concurrency with the number of topic partitions.
     */
    @Scheduled(fixedRateString = "${kafka.tuning.fixed-rate:60000}", initialDelayString = "${kafka.tuning.initial-delay:30000}")
    public void tune() {
        try {
            if (currentMaxPollRecords == -1) {
                initializeCurrentValues();
            }

            long now = System.currentTimeMillis();
            double currentCount = getReceivedCount();
            double diff = currentCount - lastCount;
            long timeDiffMs = now - lastTimestamp;

            lastCount = currentCount;
            lastTimestamp = now;

            if (timeDiffMs <= 0 || diff <= 0) {
                // No messages consumed in this period, skip tuning
                return;
            }

            double throughput = (diff / timeDiffMs) * 1000.0; // msg/sec
            double avgBatchDuration = getAvgBatchDuration();

            log.info("Kafka Tuning [PID] - Throughput: {} msg/s, Avg Duration: {}ms, Target: {}ms, Current MaxPoll: {}",
                    String.format("%.2f", throughput), String.format("%.2f", avgBatchDuration), tuningProperties.getTargetBatchDurationMs(), currentMaxPollRecords);

            // Error: positive if faster than target (can increase batch), negative if slower (must decrease batch)
            double error = (tuningProperties.getTargetBatchDurationMs() - avgBatchDuration) / tuningProperties.getTargetBatchDurationMs();

            // Update PID state
            integral += error;
            // Anti-windup
            integral = Math.max(-10, Math.min(10, integral));
            double derivative = error - previousError;
            previousError = error;

            // PID Output
            double output = (tuningProperties.getKp() * error) + (tuningProperties.getKi() * integral) + (tuningProperties.getKd() * derivative);

            boolean needsRestart = false;
            Map<String, Object> newConfigs = new HashMap<>();

            // 1. Adjust Max Poll Records using PID
            int nextMaxPoll = currentMaxPollRecords + (int) Math.round(output);
            nextMaxPoll = Math.max(tuningProperties.getMinMaxPollRecords(), Math.min(tuningProperties.getMaxMaxPollRecords(), nextMaxPoll));

            if (shouldUpdate(currentMaxPollRecords, nextMaxPoll)) {
                log.info("AUTO-TUNE [PID]: Adjusting max.poll.records {} -> {} (Error: {})",
                        currentMaxPollRecords, nextMaxPoll, String.format("%.4f", error));
                currentMaxPollRecords = nextMaxPoll;
                newConfigs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, currentMaxPollRecords);
                needsRestart = true;
            }

            // 2. Adjust Fetch Max Wait based on throughput (separate logic or could be another PID)
            int nextWait = currentFetchMaxWaitMs;
            if (throughput < 5 && currentFetchMaxWaitMs < 1000) {
                nextWait = Math.min(currentFetchMaxWaitMs + 100, 1000);
            } else if (throughput > 100 && currentFetchMaxWaitMs > 100) {
                nextWait = Math.max(currentFetchMaxWaitMs - 100, 100);
            }

            if (shouldUpdate(currentFetchMaxWaitMs, nextWait)) {
                log.info("AUTO-TUNE: Adjusting fetch.max.wait.ms {} -> {}ms", currentFetchMaxWaitMs, nextWait);
                currentFetchMaxWaitMs = nextWait;
                newConfigs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, currentFetchMaxWaitMs);
                needsRestart = true;
            }

            // 3. Adjust Concurrency based on Partition Count
            Integer partitionCount = getPartitionCount();
            if (partitionCount != null) {
                MessageListenerContainer container = registry.getListenerContainer("eventBatchConsumer");
                if (container instanceof ConcurrentMessageListenerContainer<?, ?> concurrentContainer) {
                    int currentConcurrency = concurrentContainer.getConcurrency();
                    if (currentConcurrency != partitionCount) {
                        log.info("AUTO-TUNE: Adjusting concurrency {} -> {} (Topic partitions)", currentConcurrency, partitionCount);
                        concurrentContainer.setConcurrency(partitionCount);
                        needsRestart = true;
                    }
                }
            }

            if (needsRestart) {
                if (now - lastRestartTimestamp > tuningProperties.getMinRestartIntervalMs()) {
                    applyConfigs(newConfigs);
                    lastRestartTimestamp = now;
                } else {
                    log.info("AUTO-TUNE: Parameter change requested but deferred due to cooldown ({}ms since last restart)", now - lastRestartTimestamp);
                }
            }

        } catch (Exception e) {
            log.error("Error during Kafka tuning: {}", e.getMessage(), e);
        }
    }

    /**
     * Determines if a parameter update is significant enough to trigger a consumer restart.
     *
     * @param current The current value of the parameter.
     * @param proposed The proposed new value for the parameter.
     * @return true if the change exceeds the CHANGE_THRESHOLD or reaches the allowed limits.
     */
    private boolean shouldUpdate(int current, int proposed) {
        if (current == proposed) return false;
        double change = Math.abs((double) (proposed - current) / current);
        // Update if change is significant OR if hitting limits
        return change >= tuningProperties.getChangeThreshold() ||
               (proposed == tuningProperties.getMinMaxPollRecords() || proposed == tuningProperties.getMaxMaxPollRecords());
    }

    /**
     * Initializes the current tuning values by reading from the consumer factory's configuration.
     */
    private void initializeCurrentValues() {
        Map<String, Object> configs = consumerFactory.getConfigurationProperties();

        Object mpr = configs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        currentMaxPollRecords = mpr instanceof Number ? ((Number) mpr).intValue() : 100;

        Object fmb = configs.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        currentFetchMinBytes = fmb instanceof Number ? ((Number) fmb).intValue() : 10240;

        Object fmw = configs.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        currentFetchMaxWaitMs = fmw instanceof Number ? ((Number) fmw).intValue() : 500;

        log.info("Initial tuning values: MaxPollRecords={}, FetchMinBytes={}, FetchMaxWaitMs={}",
                currentMaxPollRecords, currentFetchMinBytes, currentFetchMaxWaitMs);
    }

    /**
     * Retrieves the total count of received Kafka events from the Micrometer registry.
     *
     * @return The number of events received.
     */
    private double getReceivedCount() {
        Counter counter = meterRegistry.find(AppConstants.METRIC_KAFKA_EVENTS_RECEIVED_COUNT).counter();
        return counter != null ? counter.count() : 0;
    }

    /**
     * Retrieves the average duration of batch processing from the Micrometer registry.
     *
     * @return The average duration in milliseconds.
     */
    private double getAvgBatchDuration() {
        Timer timer = meterRegistry.find(AppConstants.METRIC_KAFKA_EVENTS_BATCH_DURATION).timer();
        return timer != null ? timer.mean(TimeUnit.MILLISECONDS) : 0;
    }

    /**
     * Retrieves the partition count of the target Kafka topic using AdminClient.
     *
     * @return The number of partitions, or null if AdminClient is unavailable or an error occurs.
     */
    private Integer getPartitionCount() {
        if (adminClient.isEmpty()) return null;
        try {
            return adminClient.get()
                    .describeTopics(java.util.Collections.singletonList(topicName))
                    .allTopicNames()
                    .get(5, TimeUnit.SECONDS)
                    .get(topicName)
                    .partitions()
                    .size();
        } catch (Exception e) {
            log.warn("Failed to fetch partition count for topic {}: {}", topicName, e.getMessage());
            return null;
        }
    }

    /**
     * Applies new configurations to the consumer factory and restarts the listener container
     * to pick up the changes.
     *
     * @param newConfigs Map containing the new Kafka consumer properties.
     */
    private void applyConfigs(Map<String, Object> newConfigs) {
        log.info("Applying new Kafka consumer settings and restarting listener: {}", newConfigs);
        consumerFactory.updateConfigs(newConfigs);

        MessageListenerContainer container = registry.getListenerContainer("eventBatchConsumer");
        if (container != null) {
            container.stop();
            container.start();
            log.info("eventBatchConsumer successfully restarted.");
        } else {
            log.warn("Could not find container 'eventBatchConsumer' to restart.");
        }
    }

    /**
     * Retrieves the current values of tuned parameters for monitoring purposes.
     *
     * @return A map containing current maxPollRecords, fetchMinBytes, fetchMaxWaitMs, and concurrency.
     */
    public Map<String, Object> getCurrentTuningParameters() {
        if (currentMaxPollRecords == -1) initializeCurrentValues();
        Map<String, Object> params = new HashMap<>();
        params.put("maxPollRecords", currentMaxPollRecords);
        params.put("fetchMinBytes", currentFetchMinBytes);
        params.put("fetchMaxWaitMs", currentFetchMaxWaitMs);

        MessageListenerContainer container = registry.getListenerContainer("eventBatchConsumer");
        if (container instanceof ConcurrentMessageListenerContainer<?, ?> concurrentContainer) {
            params.put("concurrency", concurrentContainer.getConcurrency());
        }

        return params;
    }
}
