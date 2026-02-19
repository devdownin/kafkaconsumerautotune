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
    private long totalProcessed;
    private double successRate;
    private long dltCount;
    private long consumerLag;

    private List<Double> throughput;
    private List<Double> throughput24h;

    private long successCount;
    private long errorCount;
    private long retryCount;

    private String kafkaClusterName;
    private long totalDlt24h;
    private long unresolvedErrors;
    private String avgResolutionTime;

    private String appVersion;
    private String systemUser;
    private String dbStatus;

    // Additional Database Details
    private String dbSchema;
    private String dbDriver;
    private int dbActiveConnections;
    private int dbIdleConnections;
    private int dbTotalConnections;
    private int dbMaxPoolSize;

    // Kafka Settings
    private String topicName;
    private String consumerGroup;
    private String idJsonPath;

    // System Info
    private String javaVersion;
    private String springBootVersion;

    private List<ConsumerGroupDTO> consumerGroups;

    // SSL Info
    private Long sslCertExpiryDays;
    private String kafkaSecurityProtocol;

    // App Info
    private String appName;
    private String appEdition;

    // Dynamic Tuning Info
    private Integer maxPollRecords;
    private Integer fetchMinBytes;
    private Integer fetchMaxWaitMs;
    private Integer concurrency;
}
