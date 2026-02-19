package com.vaut.config;

public final class AppConstants {
    private AppConstants() {}

    // Metric Names
    public static final String METRIC_KAFKA_EVENTS_RECEIVED_COUNT = "kafka.events.received.count";
    public static final String METRIC_KAFKA_EVENT_RECEIVED_SIZE = "kafka.event.received.size";
    public static final String METRIC_KAFKA_EVENTS_ERRORS = "kafka.events.errors";
    public static final String METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS = "kafka.events.processed";
    public static final String METRIC_KAFKA_EVENTS_BATCH_DURATION = "kafka.events.batch.duration";

    // WebSocket Topics
    public static final String WEBSOCKET_TOPIC_EVENTS = "/topic/events";
    public static final String WEBSOCKET_TOPIC_STATS = "/topic/stats";
    public static final String WEBSOCKET_TOPIC_DLT = "/topic/dlt";
    public static final String WEBSOCKET_TOPIC_METRICS = "/topic/metrics";
}
