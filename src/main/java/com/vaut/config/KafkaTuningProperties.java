package com.vaut.config;

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
     * Initial delay for the tuning task in milliseconds.
     */
    private long initialDelay = 30000;

    /**
     * Fixed rate for the tuning task in milliseconds.
     */
    private long fixedRate = 60000;
}
