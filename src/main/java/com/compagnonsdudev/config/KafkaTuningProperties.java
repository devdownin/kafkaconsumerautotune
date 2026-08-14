package com.compagnonsdudev.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for Kafka Auto-Tuning.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "kafka.tuning")
public class KafkaTuningProperties {

    /**
     * Minimum interval between consumer restarts in milliseconds.
     * Prevents rebalance storms.
     */
    private long minRestartIntervalMs = 300000;

    /**
     * Threshold for parameter changes to trigger a restart (0.1 = 10%).
     */
    private double changeThreshold = 0.1;

    /**
     * Target batch duration in milliseconds.
     * The PID controller tries to reach this processing time.
     */
    private double targetBatchDurationMs = 1200.0;

    /**
     * PID Controller Proportional coefficient.
     */
    private double kp = 150.0;

    /**
     * PID Controller Integral coefficient.
     */
    private double ki = 20.0;

    /**
     * PID Controller Derivative coefficient.
     */
    private double kd = 50.0;

    /**
     * Minimum allowed max.poll.records.
     */
    private int minMaxPollRecords = 20;

    /**
     * Maximum allowed max.poll.records.
     */
    private int maxMaxPollRecords = 1000;

    /**
     * Minimum allowed fetch.min.bytes.
     */
    private int minFetchMinBytes = 1024;

    /**
     * Maximum allowed fetch.min.bytes.
     */
    private int maxFetchMinBytes = 1048576; // 1MB

    /**
     * Safety factor for fetch.max.bytes relative to (max.poll.records * avg.message.size).
     */
    private double fetchMaxBytesSafetyFactor = 1.5;

    /**
     * Safety factor for max.poll.interval.ms relative to target batch duration.
     */
    private double maxPollIntervalSafetyFactor = 3.0;

    /**
     * Initial delay for the tuning task in milliseconds.
     */
    private long initialDelay = 30000;

    /**
     * Fixed rate for the tuning task in milliseconds.
     */
    private long fixedRate = 60000;

    /**
     * CPU usage threshold (0.0 to 1.0) above which concurrency should be decreased.
     */
    private double cpuThresholdHigh = 0.8;

    /**
     * CPU usage threshold (0.0 to 1.0) below which concurrency can be increased if there is lag.
     */
    private double cpuThresholdLow = 0.4;

    /**
     * Minimum number of threads for Kafka consumption.
     */
    private int minConcurrency = 1;

    /**
     * Lag threshold above which we consider increasing concurrency (if CPU allows).
     */
    private long lagThresholdForScaling = 500;

    /**
     * Smoothing factor for Exponential Moving Average (EMA) of batch duration.
     * Value between 0 and 1. Lower values mean more smoothing.
     */
    private double emaAlpha = 0.2;
}
