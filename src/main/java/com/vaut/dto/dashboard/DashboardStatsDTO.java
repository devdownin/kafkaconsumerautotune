package com.vaut.dto.dashboard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Data Transfer Object for aggregating all dashboard statistics.
 * Includes processing metrics, database status, Kafka information,
 * system details, and adaptive tuning parameters.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DashboardStatsDTO {
    /** Total number of messages processed by the application since startup. */
    private long totalProcessed;
    /** Percentage of successfully processed messages. */
    private double successRate;
    /** Total number of messages currently in the Dead Letter Topic (DLT). */
    private long dltCount;
    /** Current Kafka consumer lag (sum of lags for all partitions). */
    private long consumerLag;

    /** Recent throughput history (e.g., last hour, sampled periodically). */
    private List<Double> throughput;
    /** Throughput history for the last 24 hours. */
    private List<Double> throughput24h;

    /** Count of successfully processed messages. */
    private long successCount;
    /** Count of failed processing attempts. */
    private long errorCount;
    /** Count of retry attempts performed by the application. */
    private long retryCount;

    /** Name or identifier of the Kafka cluster. */
    private String kafkaClusterName;
    /** Total number of DLT events recorded in the last 24 hours. */
    private long totalDlt24h;
    /** Count of errors that have not yet been resolved in the DLT management. */
    private long unresolvedErrors;
    /** Average time taken to resolve an error in the DLT. */
    private String avgResolutionTime;

    /** Current version of the application. */
    private String appVersion;
    /** System user under which the application is running. */
    private String systemUser;
    /** Status of the database connection (e.g., UP, DOWN). */
    private String dbStatus;

    /** Database schema being used. */
    private String dbSchema;
    /** Database driver version or name. */
    private String dbDriver;
    /** Number of active database connections in the pool. */
    private int dbActiveConnections;
    /** Number of idle database connections in the pool. */
    private int dbIdleConnections;
    /** Total number of database connections (active + idle). */
    private int dbTotalConnections;
    /** Maximum allowed size of the database connection pool. */
    private int dbMaxPoolSize;

    /** Kafka topic name being consumed. */
    private String topicName;
    /** Kafka consumer group identifier. */
    private String consumerGroup;
    /** JsonPath used to extract the event ID from the payload. */
    private String idJsonPath;

    /** Java version running the application. */
    private String javaVersion;
    /** Spring Boot version used. */
    private String springBootVersion;

    /** List of details for each consumer group being monitored. */
    private List<ConsumerGroupDTO> consumerGroups;

    /** Number of days until SSL certificate expiry, if applicable. */
    private Long sslCertExpiryDays;
    /** Security protocol used to connect to Kafka (e.g., PLAINTEXT, SSL). */
    private String kafkaSecurityProtocol;

    /** Name of the application. */
    private String appName;
    /** Edition of the application (e.g., Enterprise, Community). */
    private String appEdition;

    /** Current max.poll.records Kafka consumer parameter. */
    private Integer maxPollRecords;
    /** Current fetch.min.bytes Kafka consumer parameter. */
    private Integer fetchMinBytes;
    /** Current fetch.max.wait.ms Kafka consumer parameter. */
    private Integer fetchMaxWaitMs;
    /** Current number of concurrent consumer threads. */
    private Integer concurrency;
}
