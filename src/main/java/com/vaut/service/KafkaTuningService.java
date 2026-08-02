package com.vaut.service;

import com.vaut.config.AppConstants;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.vaut.config.KafkaTuningProperties;

/**
 * Service dedicated to the "Auto-Tuning" of Kafka consumer parameters using a closed-loop control system.
 *
 * <p>The core of this service is a <b>PID Controller</b> (Proportional-Integral-Derivative) that aims to maintain
 * a stable batch processing duration. By adjusting {@code max.poll.records}, the service ensures that the
 * consumer is neither under-utilized nor overwhelmed, regardless of variations in message complexity or
 * database performance.</p>
 *
 * <p>Key optimizations performed:</p>
 * <ul>
 *     <li><b>Max Poll Records:</b> Adjusted via PID to reach the {@code targetBatchDurationMs}.</li>
 *     <li><b>Fetch Parameters:</b> {@code fetch.min.bytes} and {@code fetch.max.wait.ms} are tuned to optimize network throughput.</li>
 *     <li><b>Concurrency:</b> The number of consumer threads is dynamically scaled based on CPU load and Kafka lag.</li>
 * </ul>
 *
 * <p>Safety first: To avoid "rebalance storms", a cooldown period and a minimum change threshold are enforced
 * before any parameter change triggers a consumer restart.</p>
 */
@Service
@Slf4j
public class KafkaTuningService {

    private final DefaultKafkaConsumerFactory<String, String> consumerFactory;
    private final KafkaListenerEndpointRegistry registry;
    private final MeterRegistry meterRegistry;
    private final Optional<AdminClient> adminClient;
    private final KafkaTuningProperties tuningProperties;
    private final KafkaOptimizerService optimizerService;
    private final DashboardService dashboardService;

    public KafkaTuningService(DefaultKafkaConsumerFactory<String, String> consumerFactory,
                              KafkaListenerEndpointRegistry registry,
                              MeterRegistry meterRegistry,
                              Optional<AdminClient> adminClient,
                              KafkaTuningProperties tuningProperties,
                              KafkaOptimizerService optimizerService,
                              @Lazy DashboardService dashboardService) {
        this.consumerFactory = consumerFactory;
        this.registry = registry;
        this.meterRegistry = meterRegistry;
        this.adminClient = adminClient;
        this.tuningProperties = tuningProperties;
        this.optimizerService = optimizerService;
        this.dashboardService = dashboardService;

        // Register gauge for smoothed batch duration
        meterRegistry.gauge(AppConstants.METRIC_KAFKA_TUNING_BATCH_DURATION_SMOOTHED,
                io.micrometer.core.instrument.Tags.of("description", AppConstants.METRIC_KAFKA_TUNING_BATCH_DURATION_SMOOTHED_DESC),
                this,
                svc -> svc.smoothedBatchDuration > 0 ? svc.smoothedBatchDuration : 0);
    }

    @Value("${kafka.topic.name}")
    private String topicName;

    private double lastCount = 0;
    private long lastTimestamp = System.currentTimeMillis();
    private long lastRestartTimestamp = 0;

    // Previous readings of the cumulative meters, used to derive per-interval averages
    private long lastBatchTimerCount = 0;
    private double lastBatchTimerTotalMs = 0;
    private long lastSizeSummaryCount = 0;
    private double lastSizeSummaryTotal = 0;

    // PID Controller State
    private double integral = 0;
    private double previousError = 0;
    private double smoothedBatchDuration = -1;

    // Current tuned values. Written by the tuning scheduler, read by the dashboard and the health
    // indicator on their own threads, hence volatile. Not synchronized: applying a change restarts
    // the listener container, and holding a lock across that would stall every dashboard request.
    private volatile int currentMaxPollRecords = -1;
    private volatile int currentFetchMinBytes = -1;
    private volatile int currentFetchMaxWaitMs = -1;
    private volatile int currentFetchMaxBytes = -1;
    private volatile int currentMaxPollIntervalMs = -1;

    /**
     * The heart of the auto-tuning logic. Runs periodically to evaluate system performance.
     *
     * <p>The tuning logic follows these steps:</p>
     * <ol>
     *     <li>Calculate current throughput (msg/s) and average batch duration.</li>
     *     <li>Compute the <b>Error</b>: {@code (Target - Actual) / Target}.</li>
     *     <li>Update the PID state and calculate the new {@code max.poll.records}.</li>
     *     <li>Tune network fetch parameters based on observed throughput and message size.</li>
     *     <li>Evaluate if concurrency should be changed based on CPU usage and consumer lag.</li>
     *     <li>If changes are significant and the cooldown period has passed, restart the consumer container.</li>
     * </ol>
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
            double avgMsgSize = getAvgMsgSize();

            // Apply EMA smoothing to batch duration
            if (smoothedBatchDuration == -1) {
                smoothedBatchDuration = avgBatchDuration;
            } else {
                smoothedBatchDuration = (tuningProperties.getEmaAlpha() * avgBatchDuration) + ((1 - tuningProperties.getEmaAlpha()) * smoothedBatchDuration);
            }

            log.info("Kafka Tuning [PID] - Throughput: {} msg/s, Avg Duration: {}ms (Smoothed: {}ms), Target: {}ms, Avg Msg Size: {} bytes, Current MaxPoll: {}",
                    String.format("%.2f", throughput), String.format("%.2f", avgBatchDuration), String.format("%.2f", smoothedBatchDuration),
                    tuningProperties.getTargetBatchDurationMs(), String.format("%.2f", avgMsgSize), currentMaxPollRecords);

            // Error: positive if faster than target (can increase batch), negative if slower (must decrease batch)
            // Use smoothed duration to avoid overreaction to transient spikes
            double error = (tuningProperties.getTargetBatchDurationMs() - smoothedBatchDuration) / tuningProperties.getTargetBatchDurationMs();

            // Update PID state
            integral += error;
            // Anti-windup
            integral = Math.max(-10, Math.min(10, integral));
            double derivative = error - previousError;
            previousError = error;

            // PID Output
            double output = (tuningProperties.getKp() * error) + (tuningProperties.getKi() * integral) + (tuningProperties.getKd() * derivative);

            Map<String, Object> newConfigs = new HashMap<>();
            List<PendingChange> pendingChanges = new ArrayList<>();

            // 1. Adjust Max Poll Records using PID
            int nextMaxPoll = currentMaxPollRecords + (int) Math.round(output);
            nextMaxPoll = Math.max(tuningProperties.getMinMaxPollRecords(), Math.min(tuningProperties.getMaxMaxPollRecords(), nextMaxPoll));

            // 2. Throttling based on system health (Memory/CPU).
            // Folded in before the fetch sizing below so those parameters are scaled against the
            // value we will actually apply.
            OperatingSystemMXBean osBeanForHealth = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            double currentCpuLoad = osBeanForHealth.getCpuLoad();
            double memoryUsage = (double) (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / Runtime.getRuntime().maxMemory();
            boolean throttling = false;

            if (currentCpuLoad > 0.9 || memoryUsage > 0.9) {
                int throttledMaxPoll = Math.max(tuningProperties.getMinMaxPollRecords(), (int) (nextMaxPoll * 0.7));
                if (throttledMaxPoll < nextMaxPoll) {
                    log.warn("CRITICAL SYSTEM LOAD (CPU: {}%, MEM: {}%). Throttling max.poll.records {} -> {}",
                            String.format("%.1f", currentCpuLoad * 100), String.format("%.1f", memoryUsage * 100),
                            nextMaxPoll, throttledMaxPoll);
                    nextMaxPoll = throttledMaxPoll;
                    throttling = true;
                }
            }

            if (shouldUpdate(currentMaxPollRecords, nextMaxPoll)) {
                log.info("AUTO-TUNE [PID]: Adjusting max.poll.records {} -> {} (Error: {})",
                        currentMaxPollRecords, nextMaxPoll, String.format("%.4f", error));
                String explanation;
                String reason;
                if (throttling) {
                    reason = String.format("Emergency throttling: CPU=%.2f, MEM=%.2f", currentCpuLoad, memoryUsage);
                    explanation = "Réduction d'urgence de la charge pour préserver la stabilité du système face à une saturation des ressources.";
                } else {
                    reason = String.format("PID optimization: error=%.4f, throughput=%.2f msg/s", error, throughput);
                    explanation = nextMaxPoll > currentMaxPollRecords ?
                            "Augmentation de la taille du lot car le traitement est plus rapide que l'objectif. Cela améliore l'efficacité globale." :
                            "Réduction de la taille du lot car le traitement est trop lent par rapport à l'objectif. Cela permet de stabiliser le temps de réponse.";
                }
                pendingChanges.add(new PendingChange(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                        String.valueOf(currentMaxPollRecords), String.valueOf(nextMaxPoll), reason, explanation));
                newConfigs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, nextMaxPoll);
            }

            // 3. Adjust Fetch Max Wait based on throughput
            int nextWait = currentFetchMaxWaitMs;
            if (throughput < 5 && currentFetchMaxWaitMs < 1000) {
                nextWait = Math.min(currentFetchMaxWaitMs + 100, 1000);
            } else if (throughput > 100 && currentFetchMaxWaitMs > 100) {
                nextWait = Math.max(currentFetchMaxWaitMs - 100, 100);
            }

            if (shouldUpdate(currentFetchMaxWaitMs, nextWait)) {
                log.info("AUTO-TUNE: Adjusting fetch.max.wait.ms {} -> {}ms", currentFetchMaxWaitMs, nextWait);
                String explanation = nextWait > currentFetchMaxWaitMs ?
                        "Augmentation du temps d'attente car le débit est faible. On attend un peu plus pour grouper les messages et économiser le réseau." :
                        "Réduction du temps d'attente car le débit est élevé. On privilégie la réactivité pour traiter le flux important.";
                pendingChanges.add(new PendingChange(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
                        String.valueOf(currentFetchMaxWaitMs), String.valueOf(nextWait),
                        String.format("Optimizing wait time based on throughput (%.2f msg/s)", throughput), explanation));
                newConfigs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, nextWait);
            }

            // 4. Adjust fetch.min.bytes based on throughput
            // Aim for batches of roughly 1/10th of second of traffic or at least minFetchMinBytes
            int nextFetchMinBytes = (int) (throughput * avgMsgSize * 0.1);
            nextFetchMinBytes = Math.max(tuningProperties.getMinFetchMinBytes(), Math.min(tuningProperties.getMaxFetchMinBytes(), nextFetchMinBytes));

            if (shouldUpdate(currentFetchMinBytes, nextFetchMinBytes)) {
                log.info("AUTO-TUNE: Adjusting fetch.min.bytes {} -> {} bytes", currentFetchMinBytes, nextFetchMinBytes);
                pendingChanges.add(new PendingChange(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
                        String.valueOf(currentFetchMinBytes), String.valueOf(nextFetchMinBytes),
                        String.format("Optimizing fetch efficiency for %.2f msg/s", throughput),
                        "Ajustement du volume minimum de données pour optimiser les transferts réseau en fonction du débit constaté."));
                newConfigs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, nextFetchMinBytes);
            }

            // 5. Adjust fetch.max.bytes & max.partition.fetch.bytes to avoid clipping
            int nextFetchMaxBytes = (int) (nextMaxPoll * avgMsgSize * tuningProperties.getFetchMaxBytesSafetyFactor());
            nextFetchMaxBytes = Math.max(nextFetchMaxBytes, 1048576); // Minimum 1MB

            if (shouldUpdate(currentFetchMaxBytes, nextFetchMaxBytes)) {
                log.info("AUTO-TUNE: Adjusting fetch.max.bytes & max.partition.fetch.bytes {} -> {} bytes", currentFetchMaxBytes, nextFetchMaxBytes);
                pendingChanges.add(new PendingChange(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                        String.valueOf(currentFetchMaxBytes), String.valueOf(nextFetchMaxBytes),
                        String.format("Scaling fetch size for max.poll.records=%d and avgMsgSize=%.2f", nextMaxPoll, avgMsgSize),
                        "Adaptation de la mémoire tampon pour s'aligner sur la taille des lots et éviter de tronquer les messages."));
                newConfigs.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, nextFetchMaxBytes);
                newConfigs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, nextFetchMaxBytes);
            }

            // 6. Adjust max.poll.interval.ms based on target duration
            int nextMaxPollInterval = (int) (tuningProperties.getTargetBatchDurationMs() * tuningProperties.getMaxPollIntervalSafetyFactor());
            nextMaxPollInterval = Math.max(nextMaxPollInterval, 30000); // Minimum 30s

            if (shouldUpdate(currentMaxPollIntervalMs, nextMaxPollInterval)) {
                log.info("AUTO-TUNE: Adjusting max.poll.interval.ms {} -> {}ms", currentMaxPollIntervalMs, nextMaxPollInterval);
                pendingChanges.add(new PendingChange(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                        String.valueOf(currentMaxPollIntervalMs), String.valueOf(nextMaxPollInterval),
                        String.format("Extending poll interval safety for target batch duration of %.0fms", tuningProperties.getTargetBatchDurationMs()),
                        "Augmentation du délai de sécurité pour éviter que Kafka n'exclue le consumer si le traitement d'un lot prend plus de temps que prévu."));
                newConfigs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, nextMaxPollInterval);
            }

            // 7. Adjust Concurrency based on System Load and Lag
            ConcurrentMessageListenerContainer<?, ?> concurrentContainer = null;
            int currentConcurrency = 0;
            int nextConcurrency = 0;
            Integer partitionCount = getPartitionCount();
            if (partitionCount != null) {
                MessageListenerContainer container = registry.getListenerContainer("eventBatchConsumer");
                if (container instanceof ConcurrentMessageListenerContainer<?, ?> cc) {
                    concurrentContainer = cc;
                    currentConcurrency = cc.getConcurrency();
                    nextConcurrency = currentConcurrency;

                    OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                    double systemCpuLoad = osBean.getCpuLoad();
                    long lag = dashboardService.calculateTotalLag();

                    if (systemCpuLoad > tuningProperties.getCpuThresholdHigh()) {
                        nextConcurrency = Math.max(tuningProperties.getMinConcurrency(), currentConcurrency - 1);
                        if (nextConcurrency < currentConcurrency) {
                            log.info("AUTO-TUNE: High system load detected ({}), decreasing concurrency {} -> {}",
                                    String.format("%.2f", systemCpuLoad), currentConcurrency, nextConcurrency);
                            pendingChanges.add(new PendingChange("concurrency", String.valueOf(currentConcurrency), String.valueOf(nextConcurrency),
                                    String.format("High system load (%.2f) detected", systemCpuLoad),
                                    "Réduction du nombre de threads pour soulager le processeur et stabiliser l'application."));
                        }
                    } else if (systemCpuLoad >= 0 && systemCpuLoad < tuningProperties.getCpuThresholdLow() && lag > tuningProperties.getLagThresholdForScaling()) {
                        nextConcurrency = Math.min(partitionCount, currentConcurrency + 1);
                        if (nextConcurrency > currentConcurrency) {
                            log.info("AUTO-TUNE: Low system load ({}) and lag ({}) detected, increasing concurrency {} -> {}",
                                    String.format("%.2f", systemCpuLoad), lag, currentConcurrency, nextConcurrency);
                            pendingChanges.add(new PendingChange("concurrency", String.valueOf(currentConcurrency), String.valueOf(nextConcurrency),
                                    String.format("Low load (%.2f) and high lag (%d) detected", systemCpuLoad, lag),
                                    "Augmentation du parallélisme pour traiter le retard accumulé (lag) car les ressources système sont disponibles."));
                        }
                    } else if (currentConcurrency > partitionCount) {
                        // Safety: never exceed partition count
                        nextConcurrency = partitionCount;
                        log.info("AUTO-TUNE: Concurrency exceeds partition count, resetting to {}", partitionCount);
                    }
                }
            }

            boolean concurrencyChanged = concurrentContainer != null && nextConcurrency != currentConcurrency;

            if (newConfigs.isEmpty() && !concurrencyChanged) {
                return;
            }

            // Nothing above this point mutates tuning state. A change is only committed once it has
            // actually been pushed to the consumer factory, so a deferred change stays pending and is
            // re-proposed on the next cycle instead of being silently dropped.
            if (now - lastRestartTimestamp <= tuningProperties.getMinRestartIntervalMs()) {
                log.info("AUTO-TUNE: Parameter change requested but deferred due to cooldown ({}ms since last restart)", now - lastRestartTimestamp);
                return;
            }

            if (concurrencyChanged) {
                concurrentContainer.setConcurrency(nextConcurrency);
            }
            applyConfigs(newConfigs);
            lastRestartTimestamp = now;

            // Commit only what was actually pushed: a proposal that fell below the change threshold
            // was never sent to the factory and must not be recorded as the live value.
            currentMaxPollRecords = appliedValue(newConfigs, ConsumerConfig.MAX_POLL_RECORDS_CONFIG, currentMaxPollRecords);
            currentFetchMaxWaitMs = appliedValue(newConfigs, ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, currentFetchMaxWaitMs);
            currentFetchMinBytes = appliedValue(newConfigs, ConsumerConfig.FETCH_MIN_BYTES_CONFIG, currentFetchMinBytes);
            currentFetchMaxBytes = appliedValue(newConfigs, ConsumerConfig.FETCH_MAX_BYTES_CONFIG, currentFetchMaxBytes);
            currentMaxPollIntervalMs = appliedValue(newConfigs, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, currentMaxPollIntervalMs);
            pendingChanges.forEach(c -> optimizerService.addOptimization(
                    c.property(), c.oldValue(), c.newValue(), c.reason(), c.explanation()));

        } catch (Exception e) {
            log.error("Error during Kafka tuning: {}", e.getMessage(), e);
        }
    }

    /** A parameter change that has been proposed but not yet pushed to the consumer factory. */
    private record PendingChange(String property, String oldValue, String newValue, String reason, String explanation) {}

    private static int appliedValue(Map<String, Object> configs, String key, int fallback) {
        Object value = configs.get(key);
        return value instanceof Number n ? n.intValue() : fallback;
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
        if (current == 0) return true;
        double change = Math.abs((double) (proposed - current) / current);
        // Update if change is significant OR if hitting limits
        return change >= tuningProperties.getChangeThreshold() ||
               (proposed == tuningProperties.getMinMaxPollRecords() || proposed == tuningProperties.getMaxMaxPollRecords());
    }

    /**
     * Initializes the current tuning values by reading from the consumer factory's configuration.
     */
    private synchronized void initializeCurrentValues() {
        if (currentMaxPollRecords != -1) {
            // Another thread got here first
            return;
        }
        Map<String, Object> configs = consumerFactory.getConfigurationProperties();

        Object mpr = configs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        currentMaxPollRecords = mpr instanceof Number ? ((Number) mpr).intValue() : 100;

        Object fmb = configs.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        currentFetchMinBytes = fmb instanceof Number ? ((Number) fmb).intValue() : 10240;

        Object fmw = configs.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        currentFetchMaxWaitMs = fmw instanceof Number ? ((Number) fmw).intValue() : 500;

        Object fmb_max = configs.get(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        currentFetchMaxBytes = fmb_max instanceof Number ? ((Number) fmb_max).intValue() : 52428800;

        Object mpi = configs.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        currentMaxPollIntervalMs = mpi instanceof Number ? ((Number) mpi).intValue() : 300000;

        log.info("Initial tuning values: MaxPollRecords={}, FetchMinBytes={}, FetchMaxWaitMs={}, FetchMaxBytes={}, MaxPollIntervalMs={}",
                currentMaxPollRecords, currentFetchMinBytes, currentFetchMaxWaitMs, currentFetchMaxBytes, currentMaxPollIntervalMs);
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
     * Retrieves the average duration of batches processed since the previous tuning cycle.
     *
     * <p>Micrometer's {@code Timer.mean()} averages over the entire lifetime of the meter, so it
     * flattens out as the process runs and stops reflecting current conditions. The PID controller
     * needs a signal that tracks the present, so the mean is computed over the delta of the timer's
     * count and total time since the last cycle.</p>
     *
     * @return The average duration in milliseconds over the last interval.
     */
    private double getAvgBatchDuration() {
        Timer timer = meterRegistry.find(AppConstants.METRIC_KAFKA_EVENTS_BATCH_DURATION).timer();
        if (timer == null) return 0;

        long count = timer.count();
        double totalMs = timer.totalTime(TimeUnit.MILLISECONDS);
        long countDelta = count - lastBatchTimerCount;
        double totalDelta = totalMs - lastBatchTimerTotalMs;
        lastBatchTimerCount = count;
        lastBatchTimerTotalMs = totalMs;

        if (countDelta <= 0) {
            // No batch completed in this window; fall back to the lifetime mean.
            return timer.mean(TimeUnit.MILLISECONDS);
        }
        return totalDelta / countDelta;
    }

    /**
     * Retrieves the average size of messages received since the previous tuning cycle.
     * Computed over the interval delta for the same reason as {@link #getAvgBatchDuration()}.
     *
     * @return The average size in bytes.
     */
    private double getAvgMsgSize() {
        io.micrometer.core.instrument.DistributionSummary summary = meterRegistry.find(AppConstants.METRIC_KAFKA_EVENT_RECEIVED_SIZE).summary();
        if (summary == null) return 512.0; // Default to 512 bytes if no data

        long count = summary.count();
        double total = summary.totalAmount();
        long countDelta = count - lastSizeSummaryCount;
        double totalDelta = total - lastSizeSummaryTotal;
        lastSizeSummaryCount = count;
        lastSizeSummaryTotal = total;

        if (countDelta <= 0) {
            double mean = summary.mean();
            return mean > 0 ? mean : 512.0;
        }
        return totalDelta / countDelta;
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
        params.put("fetchMaxBytes", currentFetchMaxBytes);
        params.put("maxPollIntervalMs", currentMaxPollIntervalMs);

        MessageListenerContainer container = registry.getListenerContainer("eventBatchConsumer");
        if (container instanceof ConcurrentMessageListenerContainer<?, ?> concurrentContainer) {
            params.put("concurrency", concurrentContainer.getConcurrency());
        }

        return params;
    }
}
