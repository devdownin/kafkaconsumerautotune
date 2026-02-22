package com.vaut.dto.dashboard;

import lombok.Builder;
import java.util.List;

/**
 * Data Transfer Object for aggregating all dashboard statistics.
 * Includes processing metrics, database status, Kafka information,
 * system details, and adaptive tuning parameters.
 */
@Builder
public record DashboardStatsDTO(
    /** Total number of messages processed by the application since startup. */
    long totalProcessed,
    /** Percentage of successfully processed messages. */
    double successRate,
    /** Total number of messages currently in the Dead Letter Topic (DLT). */
    long dltCount,
    /** Current Kafka consumer lag (sum of lags for all partitions). */
    long consumerLag,

    /** Recent throughput history (e.g., last hour, sampled periodically). */
    List<Double> throughput,
    /** Throughput history for the last 24 hours. */
    List<Double> throughput24h,

    /** Count of successfully processed messages. */
    long successCount,
    /** Count of failed processing attempts. */
    long errorCount,
    /** Count of retry attempts performed by the application. */
    long retryCount,

    /** Name or identifier of the Kafka cluster. */
    String kafkaClusterName,
    /** Total number of DLT events recorded in the last 24 hours. */
    long totalDlt24h,
    /** Count of errors that have not yet been resolved in the DLT management. */
    long unresolvedErrors,
    /** Average time taken to resolve an error in the DLT. */
    String avgResolutionTime,

    /** Current version of the application. */
    String appVersion,
    /** System user under which the application is running. */
    String systemUser,
    /** Status of the database connection (e.g., UP, DOWN). */
    String dbStatus,

    /** Database schema being used. */
    String dbSchema,
    /** Database driver version or name. */
    String dbDriver,
    /** Number of active database connections in the pool. */
    int dbActiveConnections,
    /** Number of idle database connections in the pool. */
    int dbIdleConnections,
    /** Total number of database connections (active + idle). */
    int dbTotalConnections,
    /** Maximum allowed size of the database connection pool. */
    int dbMaxPoolSize,

    /** Kafka topic name being consumed. */
    String topicName,
    /** Kafka consumer group identifier. */
    String consumerGroup,
    /** JsonPath used to extract the event ID from the payload. */
    String idJsonPath,

    /** Java version running the application. */
    String javaVersion,
    /** Spring Boot version used. */
    String springBootVersion,

    /** List of details for each consumer group being monitored. */
    List<ConsumerGroupDTO> consumerGroups,

    /** Number of days until SSL certificate expiry, if applicable. */
    Long sslCertExpiryDays,
    /** Security protocol used to connect to Kafka (e.g., PLAINTEXT, SSL). */
    String kafkaSecurityProtocol,

    /** Name of the application. */
    String appName,
    /** Edition of the application (e.g., Enterprise, Community). */
    String appEdition,

    /** Current max.poll.records Kafka consumer parameter. */
    Integer maxPollRecords,
    /** Current fetch.min.bytes Kafka consumer parameter. */
    Integer fetchMinBytes,
    /** Current fetch.max.wait.ms Kafka consumer parameter. */
    Integer fetchMaxWaitMs,
    /** Current number of concurrent consumer threads. */
    Integer concurrency,

    /** Current state of the persistence circuit breaker. */
    String circuitBreakerStatus
) {}
