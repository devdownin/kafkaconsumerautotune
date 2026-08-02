package com.vaut.service;

import com.vaut.dto.dashboard.ConsumerGroupDTO;
import com.vaut.dto.dashboard.DashboardStatsDTO;
import com.vaut.dto.dashboard.LogConfigDTO;
import com.vaut.dto.dashboard.PartitionLagDTO;
import com.vaut.repository.DltEventRepository;
import com.vaut.entity.DltEvent;
import com.vaut.repository.KEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import org.springframework.beans.factory.annotation.Value;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import javax.sql.DataSource;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.boot.logging.LogLevel;
import org.springframework.boot.logging.LoggerConfiguration;
import com.vaut.config.AppConstants;
import java.util.Optional;
import io.micrometer.core.instrument.MeterRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.scheduling.annotation.Scheduled;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import com.sun.management.OperatingSystemMXBean;
import com.vaut.dto.dashboard.JvmStatsDTO;
import com.vaut.dto.dashboard.MetricDTO;
import com.vaut.config.MetricThresholdProperties;
import io.micrometer.core.instrument.Measurement;

/**
 * Service that provides statistics and monitoring data for the dashboard.
 * It aggregates information from Kafka, the database, JVM, and application metrics.
 */
@Service
@RequiredArgsConstructor
@lombok.extern.slf4j.Slf4j
public class DashboardService {

    private final KEventRepository eventRepository;
    private final DltEventRepository dltEventRepository;
    private final EntityManager entityManager;
    private final Optional<BuildProperties> buildProperties;
    private final DataSource dataSource;
    private final LoggingSystem loggingSystem;
    private final MeterRegistry meterRegistry;
    private final Optional<AdminClient> adminClient;
    private final KafkaProperties kafkaProperties;
    private final WebSocketService webSocketService;
    private final KafkaTuningService kafkaTuningService;
    private final MetricThresholdProperties metricThresholdProperties;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    private static final int THROUGHPUT_5S_SIZE = 120; // 10 minutes at 5s interval
    private static final int THROUGHPUT_1M_SIZE = 1440; // 24 hours at 1m interval
    private final List<Double> successThroughput5s = new ArrayList<>(Collections.nCopies(THROUGHPUT_5S_SIZE, 0.0));
    private final List<Double> errorThroughput5s = new ArrayList<>(Collections.nCopies(THROUGHPUT_5S_SIZE, 0.0));
    private final List<Double> retryThroughput5s = new ArrayList<>(Collections.nCopies(THROUGHPUT_5S_SIZE, 0.0));
    private final List<Long> lagHistory5s = new ArrayList<>(Collections.nCopies(THROUGHPUT_5S_SIZE, 0L));
    private final List<Integer> maxPollRecordsHistory5s = new ArrayList<>(Collections.nCopies(THROUGHPUT_5S_SIZE, 0));
    private final List<Integer> concurrencyHistory5s = new ArrayList<>(Collections.nCopies(THROUGHPUT_5S_SIZE, 0));
    private final List<Long> timestamps5s = new ArrayList<>();
    private final List<Double> throughput1m = new ArrayList<>(Collections.nCopies(THROUGHPUT_1M_SIZE, 0.0));
    private final Map<String, List<Double>> metricsHistory = new java.util.concurrent.ConcurrentHashMap<>();
    private static final int MAX_HISTORY_POINTS = 20;
    private long lastSuccessCount = 0;
    private long lastErrorCount = 0;
    private long lastRetryCount = 0;
    private int minuteCounter = 0;
    private double minuteAccumulator = 0;

    // Populated on a request thread, read by others
    private volatile String cachedDbVendor;
    private volatile String cachedDbSchema;
    private volatile String cachedDbDriver;

    // Cache for Kafka Lag to avoid over-polling AdminClient.
    // Written by the scheduler, read by request threads.
    private final AtomicLong cachedTotalLag = new AtomicLong(0);
    private volatile List<ConsumerGroupDTO> cachedConsumerGroups = Collections.emptyList();
    private final AtomicLong lastKafkaUpdate = new AtomicLong(0);
    private final AtomicLong lastKafkaAttempt = new AtomicLong(0);
    private static final long KAFKA_CACHE_DURATION_MS = 30000; // 30 seconds

    @Value("${spring.kafka.bootstrap-servers:kafkadev:9093}")
    private String bootstrapServers;

    @Value("${kafka.topic.name}")
    private String topicName;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroup;

    @Value("${spring.kafka.ssl.enabled:false}")
    private boolean sslEnabled;

    @Value("${spring.application.name:KafkaMonitor}")
    private String appName;

    @Value("${app.edition:Enterprise Edition}")
    private String appEdition;

    @Value("${app.event.id-json-path:$.idPassage}")
    private String idJsonPath;

    @PostConstruct
    public void init() {
        long now = System.currentTimeMillis();
        synchronized (successThroughput5s) {
            for (int i = THROUGHPUT_5S_SIZE - 1; i >= 0; i--) {
                timestamps5s.add(now - (i * 5000L));
            }

            // Get initial tuning parameters for pre-filling history
            Map<String, Object> tuningParams = kafkaTuningService.getCurrentTuningParameters();
            int currentMaxPoll = 0;
            Object mpr = tuningParams.get("maxPollRecords");
            if (mpr instanceof Number n) currentMaxPoll = n.intValue();

            int currentConcurrency = 0;
            Object conc = tuningParams.get("concurrency");
            if (conc instanceof Number n) currentConcurrency = n.intValue();

            Collections.fill(maxPollRecordsHistory5s, currentMaxPoll);
            Collections.fill(concurrencyHistory5s, currentConcurrency);
        }
    }

    /**
     * Periodically refreshes Kafka-related metrics such as consumer group status and lag.
     * Uses the AdminClient to query the Kafka cluster.
     */
    @Scheduled(fixedRate = 30000)
    public synchronized void refreshKafkaMetrics() {
        if (adminClient.isEmpty()) return;
        AdminClient client = adminClient.get();
        lastKafkaAttempt.set(System.currentTimeMillis());

        try {
            List<String> groups = Arrays.asList(consumerGroup, consumerGroup + "-dlt");
            Map<String, ConsumerGroupDescription> descriptions = client.describeConsumerGroups(groups).all().get(5, TimeUnit.SECONDS);

            List<ConsumerGroupDTO> dtos = new ArrayList<>();
            long totalMainLag = 0;

            for (String groupId : groups) {
                ConsumerGroupDescription desc = descriptions.get(groupId);
                if (desc != null) {
                    Map<TopicPartition, OffsetAndMetadata> groupOffsets = client
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get(5, TimeUnit.SECONDS);

                    List<PartitionLagDTO> partitionLags = new ArrayList<>();
                    long groupLag = 0;

                    if (!groupOffsets.isEmpty()) {
                        Map<TopicPartition, OffsetSpec> latestOffsetSpecs = groupOffsets.keySet().stream()
                                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

                        Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = client
                                .listOffsets(latestOffsetSpecs)
                                .all()
                                .get(5, TimeUnit.SECONDS);

                        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : groupOffsets.entrySet()) {
                            TopicPartition tp = entry.getKey();
                            var latest = latestOffsets.get(tp);
                            if (latest == null) {
                                // Partition disappeared between the two admin calls
                                continue;
                            }
                            long currentOffset = entry.getValue().offset();
                            long latestOffset = latest.offset();
                            long partitionLag = Math.max(0, latestOffset - currentOffset);
                            groupLag += partitionLag;

                            partitionLags.add(PartitionLagDTO.builder()
                                    .partition(tp.partition())
                                    .currentOffset(currentOffset)
                                    .logEndOffset(latestOffset)
                                    .lag(partitionLag)
                                    .build());
                        }
                    }

                    if (groupId.equals(consumerGroup)) {
                        totalMainLag = groupLag;
                    }

                    partitionLags.sort((a, b) -> Integer.compare(a.partition(), b.partition()));

                    int assignedPartitions = (int) desc.members().stream()
                            .flatMap(m -> m.assignment().topicPartitions().stream())
                            .count();

                    dtos.add(ConsumerGroupDTO.builder()
                            .groupId(groupId)
                            .state(desc.state().toString())
                            .members(desc.members().size())
                            .partitions(assignedPartitions)
                            .lag(groupLag)
                            .partitionLags(partitionLags)
                            .build());
                }
            }

            this.cachedConsumerGroups = dtos;
            this.cachedTotalLag.set(totalMainLag);
            this.lastKafkaUpdate.set(System.currentTimeMillis());

        } catch (Exception e) {
            // Keep stale cache on error, but do not fail silently: without this the dashboard
            // simply shows nothing with no indication of why.
            log.warn("Failed to refresh Kafka consumer group metrics, serving stale values: {}", e.toString());
        }
    }

    /**
     * Refreshes the Kafka cache on demand, but only if it has never been populated and no attempt
     * was made recently.
     *
     * <p>Each refresh makes several AdminClient calls with a 5s timeout apiece. If the cluster is
     * unreachable, refreshing on every request would add that latency to every dashboard page and
     * every actuator health check, so an attempt is made at most once per cache window.</p>
     */
    private void refreshIfNeverPopulated() {
        if (adminClient.isEmpty() || lastKafkaUpdate.get() != 0) {
            return;
        }
        if (System.currentTimeMillis() - lastKafkaAttempt.get() < KAFKA_CACHE_DURATION_MS) {
            return;
        }
        refreshKafkaMetrics();
    }

    /**
     * Returns the cached information for the monitored Kafka consumer groups.
     *
     * @return A list of ConsumerGroupDTO objects.
     */
    public List<ConsumerGroupDTO> getConsumerGroupsInfo() {
        refreshIfNeverPopulated();
        return cachedConsumerGroups;
    }

    /**
     * Calculates the total consumer lag across all partitions.
     *
     * @return The total lag as a long.
     */
    public long calculateTotalLag() {
        refreshIfNeverPopulated();
        return cachedTotalLag.get();
    }

    /**
     * Periodically updates the historical data points for all tracked metrics.
     * Broadcasts the updated metrics to connected clients via WebSockets.
     */
    @Scheduled(fixedRate = 10000)
    public void updateMetricsHistory() {
        Set<String> currentMetricNames = new HashSet<>();
        meterRegistry.getMeters().forEach(meter -> {
            String name = meter.getId().getName();
            List<Measurement> measurements = new ArrayList<>();
            meter.measure().forEach(measurements::add);

            for (Measurement measurement : measurements) {
                String suffix = measurement.getStatistic().name().toLowerCase();
                String fullName = name + (measurements.size() > 1 ? "." + suffix : "");
                double value = measurement.getValue();
                currentMetricNames.add(fullName);

                metricsHistory.compute(fullName, (k, v) -> {
                    List<Double> history = (v == null) ? new CopyOnWriteArrayList<>() : v;
                    history.add(value);
                    if (history.size() > MAX_HISTORY_POINTS) {
                        history.remove(0);
                    }
                    return history;
                });
            }
        });
        metricsHistory.keySet().retainAll(currentMetricNames);
        webSocketService.broadcast(AppConstants.WEBSOCKET_TOPIC_METRICS_LIVE, getAllMetrics());
    }

    /**
     * Periodically updates throughput history (messages per second) and broadcasts
     * updated statistics and JVM status via WebSockets.
     */
    @Scheduled(fixedRate = 5000)
    public void updateThroughputHistory() {
        double currentSuccess = Optional.ofNullable(meterRegistry.find(AppConstants.METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS).counter())
                .map(counter -> counter.count())
                .orElse(0.0);
        double currentErrors = Optional.ofNullable(meterRegistry.find(AppConstants.METRIC_KAFKA_EVENTS_ERRORS).counter())
                .map(counter -> counter.count())
                .orElse(0.0);
        double currentRetries = Optional.ofNullable(meterRegistry.find(AppConstants.METRIC_KAFKA_EVENTS_RETRIED).counter())
                .map(counter -> counter.count())
                .orElse(0.0);

        long successDelta = (long) (currentSuccess - lastSuccessCount);
        if (successDelta < 0) successDelta = 0;
        lastSuccessCount = (long) currentSuccess;
        double successMsgPerSec = successDelta / 5.0;

        long errorDelta = (long) (currentErrors - lastErrorCount);
        if (errorDelta < 0) errorDelta = 0;
        lastErrorCount = (long) currentErrors;
        double errorMsgPerSec = errorDelta / 5.0;

        long retryDelta = (long) (currentRetries - lastRetryCount);
        if (retryDelta < 0) retryDelta = 0;
        lastRetryCount = (long) currentRetries;
        double retryMsgPerSec = retryDelta / 5.0;

        long currentLag = calculateTotalLag();
        long now = System.currentTimeMillis();
        Map<String, Object> tuningParams = kafkaTuningService.getCurrentTuningParameters();
        int currentMaxPoll = 0;
        Object mpr = tuningParams.get("maxPollRecords");
        if (mpr instanceof Number n) currentMaxPoll = n.intValue();

        int currentConcurrency = 0;
        Object conc = tuningParams.get("concurrency");
        if (conc instanceof Number n) currentConcurrency = n.intValue();

        synchronized (successThroughput5s) {
            successThroughput5s.add(successMsgPerSec);
            errorThroughput5s.add(errorMsgPerSec);
            retryThroughput5s.add(retryMsgPerSec);
            lagHistory5s.add(currentLag);
            maxPollRecordsHistory5s.add(currentMaxPoll);
            concurrencyHistory5s.add(currentConcurrency);
            timestamps5s.add(now);

            if (successThroughput5s.size() > THROUGHPUT_5S_SIZE) {
                successThroughput5s.remove(0);
                errorThroughput5s.remove(0);
                retryThroughput5s.remove(0);
                lagHistory5s.remove(0);
                maxPollRecordsHistory5s.remove(0);
                concurrencyHistory5s.remove(0);
                timestamps5s.remove(0);
            }
        }

        minuteAccumulator += successMsgPerSec + errorMsgPerSec;
        minuteCounter++;
        if (minuteCounter >= 12) {
            double avgMsgPerSec = minuteAccumulator / 12.0;
            synchronized (throughput1m) {
                throughput1m.add(avgMsgPerSec);
                if (throughput1m.size() > THROUGHPUT_1M_SIZE) {
                    throughput1m.remove(0);
                }
            }
            minuteAccumulator = 0;
            minuteCounter = 0;
        }

        webSocketService.sendStats(getStats());
        webSocketService.sendJvmStats(getJvmStats());
    }

    /**
     * Retrieves current JVM and system performance statistics.
     *
     * @return A JvmStatsDTO object.
     */
    public JvmStatsDTO getJvmStats() {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        return JvmStatsDTO.builder()
                .heapUsed(memoryBean.getHeapMemoryUsage().getUsed())
                .heapMax(memoryBean.getHeapMemoryUsage().getMax())
                .heapCommitted(memoryBean.getHeapMemoryUsage().getCommitted())
                .threadCount(threadBean.getThreadCount())
                .peakThreadCount(threadBean.getPeakThreadCount())
                .systemCpuLoad(osBean.getCpuLoad() * 100.0)
                .processCpuLoad(osBean.getProcessCpuLoad() * 100.0)
                .build();
    }

    /**
     * Retrieves the most recent events from the Dead Letter Topic (DLT).
     *
     * @param limit The maximum number of events to retrieve.
     * @return A list of DltEvent objects.
     */
    public List<DltEvent> getRecentDltEvents(int limit) {
        return dltEventRepository.findAll(PageRequest.of(0, limit, Sort.by("id").descending())).getContent();
    }

    /**
     * Retrieves the current logging configuration for tracked loggers.
     *
     * @return A list of LogConfigDTO objects.
     */
    public List<LogConfigDTO> getLogConfigs() {
        List<String> loggersToTrack = Arrays.asList("com.vaut", "org.springframework.kafka", "org.hibernate.SQL", "org.apache.kafka");
        return loggersToTrack.stream()
                .map(name -> {
                    LoggerConfiguration config = loggingSystem.getLoggerConfiguration(name);
                    String configured = "DEFAULT";
                    if (config != null && config.getConfiguredLevel() != null) {
                        configured = config.getConfiguredLevel().name();
                    }
                    return LogConfigDTO.builder()
                            .loggerName(name)
                            .configuredLevel(configured)
                            .effectiveLevel(config != null ? config.getEffectiveLevel().name() : "INFO")
                            .build();
                })
                .toList();
    }

    /**
     * Updates the log level for a specific logger.
     *
     * @param loggerName The name of the logger to update.
     * @param level The new log level (e.g., DEBUG, INFO).
     */
    public void updateLogLevel(String loggerName, String level) {
        loggingSystem.setLogLevel(loggerName, LogLevel.valueOf(level.toUpperCase(java.util.Locale.ROOT)));
    }

    /**
     * Retrieves all registered metrics with their current values, trends, and statuses.
     *
     * @return A list of MetricDTO objects.
     */
    public List<MetricDTO> getAllMetrics() {
        return meterRegistry.getMeters().stream()
                .flatMap(meter -> {
                    String name = meter.getId().getName();
                    String type = meter.getId().getType().name();
                    String description = meter.getId().getDescription();
                    String baseUnit = meter.getId().getBaseUnit();
                    boolean appSpecific = name.startsWith("kafka.events") || name.startsWith("app.") || name.startsWith("myconsumer.") || name.startsWith("process.");

                    List<MetricDTO> metrics = new ArrayList<>();
                    List<Measurement> measurements = new ArrayList<>();
                    meter.measure().forEach(measurements::add);

                    for (Measurement measurement : measurements) {
                        String suffix = measurement.getStatistic().name().toLowerCase();
                        String fullName = name + (measurements.size() > 1 ? "." + suffix : "");
                        double currentValue = measurement.getValue();

                        List<Double> history = new ArrayList<>(metricsHistory.getOrDefault(fullName, Collections.emptyList()));

                        String trend = "STABLE";
                        if (history.size() >= 2) {
                            double prevValue = history.get(history.size() - 2);
                            if (currentValue > prevValue) trend = "UP";
                            else if (currentValue < prevValue) trend = "DOWN";
                        }

                        String status = "NORMAL";
                        MetricThresholdProperties.Threshold threshold = metricThresholdProperties.getThresholds().get(fullName);
                        if (threshold == null) threshold = metricThresholdProperties.getThresholds().get(name);

                        if (threshold != null) {
                            if (threshold.getCritical() != null && currentValue >= threshold.getCritical()) {
                                status = "CRITICAL";
                            } else if (threshold.getWarning() != null && currentValue >= threshold.getWarning()) {
                                status = "WARNING";
                            }
                        }

                        metrics.add(MetricDTO.builder()
                                .name(fullName)
                                .type(type)
                                .description(description != null ? description : "N/A")
                                .value(String.format("%.2f", currentValue))
                                .baseUnit(baseUnit != null ? baseUnit : "")
                                .appSpecific(appSpecific)
                                .history(history)
                                .trend(trend)
                                .status(status)
                                .build());
                    }
                    return metrics.stream();
                })
                .sorted((a, b) -> a.name().compareToIgnoreCase(b.name()))
                .collect(Collectors.toList());
    }

    /**
     * Aggregates all application statistics for the main dashboard view.
     * Results are cached for 5 seconds to improve performance.
     *
     * @return A DashboardStatsDTO object.
     */
    @Cacheable(value = "stats", sync = true)
    public DashboardStatsDTO getStats() {
        long successCount = eventRepository.count();
        var dltStats = dltEventRepository.getDltStats(LocalDateTime.now().minusDays(1));

        long dltCount = dltStats.getTotalCount() != null ? dltStats.getTotalCount() : 0L;
        long total = successCount + dltCount;

        double successRate = total == 0 ? 100.0 : (double) successCount / total * 100.0;

        long totalDlt24h = dltStats.getCountLast24h() != null ? dltStats.getCountLast24h() : 0L;
        long unresolvedErrors = dltStats.getUnresolvedCount() != null ? dltStats.getUnresolvedCount() : 0L;

        // Optimized: We no longer fetch all events in memory to calculate average
        // For now, we set it to N/A or implement it via a more specific query if needed
        String avgResolutionTime = "N/A";

        long realLag = calculateTotalLag();
        List<Double> successThroughput;
        List<Double> errorThroughput;
        List<Double> retryThroughput;
        List<Long> lagHistory;
        List<Integer> maxPollRecordsHistory;
        List<Integer> concurrencyHistory;
        List<Long> timestamps;
        List<Double> throughput24h;
        synchronized (successThroughput5s) {
            successThroughput = new ArrayList<>(successThroughput5s);
            errorThroughput = new ArrayList<>(errorThroughput5s);
            retryThroughput = new ArrayList<>(retryThroughput5s);
            lagHistory = new ArrayList<>(lagHistory5s);
            maxPollRecordsHistory = new ArrayList<>(maxPollRecordsHistory5s);
            concurrencyHistory = new ArrayList<>(concurrencyHistory5s);
            timestamps = new ArrayList<>(timestamps5s);
        }
        synchronized (throughput1m) {
            throughput24h = new ArrayList<>(throughput1m);
        }

        long resolvedCount = dltStats.getResolvedCount() != null ? dltStats.getResolvedCount() : 0L;
        long discardedCount = dltStats.getDiscardedCount() != null ? dltStats.getDiscardedCount() : 0L;

        String dbStatus = "Connected";
        int activeConnections = 0;
        int idleConnections = 0;
        int totalConnections = 0;
        int maxPoolSize = 0;

        if (cachedDbVendor == null) {
            try (Connection connection = dataSource.getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                cachedDbVendor = metaData.getDatabaseProductName();
                cachedDbSchema = metaData.getUserName();
                cachedDbDriver = metaData.getDriverName();
            } catch (Exception e) {
                // Temporary failure to get metadata, don't cache yet
            }
        }

        String dbVendor = cachedDbVendor != null ? cachedDbVendor : "Database";
        String dbSchema = cachedDbSchema != null ? cachedDbSchema : "N/A";
        String dbDriver = cachedDbDriver != null ? cachedDbDriver : "N/A";

        try {
            String validationQuery = dbVendor.toLowerCase(java.util.Locale.ROOT).contains("oracle") ? "SELECT 1 FROM DUAL" : "SELECT 1";
            Query query = entityManager.createNativeQuery(validationQuery);
            query.getSingleResult();

            if (dataSource instanceof HikariDataSource hikari) {
                activeConnections = hikari.getHikariPoolMXBean().getActiveConnections();
                idleConnections = hikari.getHikariPoolMXBean().getIdleConnections();
                totalConnections = hikari.getHikariPoolMXBean().getTotalConnections();
                maxPoolSize = hikari.getMaximumPoolSize();
            }
        } catch (Exception e) {
            dbStatus = "Disconnected";
        }

        String version = buildProperties.map(BuildProperties::getVersion).orElse("1.0.0-SNAPSHOT");
        Map<String, Object> tuningParams = kafkaTuningService.getCurrentTuningParameters();
        String cbStatus = circuitBreakerRegistry.circuitBreaker("persistence").getState().name();

        Long sslCertExpiry = null;
        if (sslEnabled) {
            var gauge = meterRegistry.find("spring.ssl.bundle.certificate.validity").gauge();
            if (gauge != null) {
                sslCertExpiry = (long) gauge.value();
            }
        }

        return DashboardStatsDTO.builder()
                .totalProcessed(successCount + dltCount)
                .successRate(Math.round(successRate * 100.0) / 100.0)
                .dltCount(dltCount)
                .consumerLag(realLag)
                .successCount(successCount)
                .errorCount(unresolvedErrors)
                .retryCount(resolvedCount + discardedCount)
                .successThroughput(successThroughput)
                .errorThroughput(errorThroughput)
                .retryThroughput(retryThroughput)
                .lagHistory(lagHistory)
                .maxPollRecordsHistory(maxPollRecordsHistory)
                .concurrencyHistory(concurrencyHistory)
                .timestamps(timestamps)
                .throughput24h(throughput24h)
                .kafkaClusterName(bootstrapServers)
                .totalDlt24h(totalDlt24h)
                .unresolvedErrors(unresolvedErrors)
                .avgResolutionTime(avgResolutionTime)
                .appVersion(version)
                .systemUser(System.getProperty("user.name", "Admin"))
                .dbStatus(dbVendor + ": " + dbStatus)
                .dbSchema(dbSchema)
                .dbDriver(dbDriver)
                .dbActiveConnections(activeConnections)
                .dbIdleConnections(idleConnections)
                .dbTotalConnections(totalConnections)
                .dbMaxPoolSize(maxPoolSize)
                .topicName(topicName)
                .consumerGroup(consumerGroup)
                .idJsonPath(idJsonPath)
                .javaVersion(System.getProperty("java.version"))
                .springBootVersion(SpringBootVersion.getVersion())
                .consumerGroups(getConsumerGroupsInfo())
                .sslCertExpiryDays(sslCertExpiry)
                .kafkaSecurityProtocol(sslEnabled ? "SSL" : "PLAINTEXT")
                .appName(appName)
                .appEdition(appEdition)
                .maxPollRecords((Integer) tuningParams.get("maxPollRecords"))
                .fetchMinBytes((Integer) tuningParams.get("fetchMinBytes"))
                .fetchMaxWaitMs((Integer) tuningParams.get("fetchMaxWaitMs"))
                .fetchMaxBytes((Integer) tuningParams.get("fetchMaxBytes"))
                .maxPollIntervalMs((Integer) tuningParams.get("maxPollIntervalMs"))
                .concurrency((Integer) tuningParams.get("concurrency"))
                .circuitBreakerStatus(cbStatus)
                .build();
    }
}
