package com.vaut.config;

public final class AppConstants {
    private AppConstants() {}

    // Metric Names
    public static final String METRIC_KAFKA_EVENTS_RECEIVED_COUNT = "myconsumer.kafka.events.received.count";
    public static final String METRIC_KAFKA_EVENT_RECEIVED_SIZE = "myconsumer.kafka.event.received.size";
    public static final String METRIC_KAFKA_EVENTS_ERRORS = "myconsumer.kafka.events.errors";
    public static final String METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS = "myconsumer.kafka.events.processed";
    public static final String METRIC_KAFKA_EVENTS_BATCH_DURATION = "myconsumer.kafka.events.batch.duration";

    // WebSocket Topics
    public static final String WEBSOCKET_TOPIC_EVENTS = "/topic/events";
    public static final String WEBSOCKET_TOPIC_STATS = "/topic/stats";
    public static final String WEBSOCKET_TOPIC_DLT = "/topic/dlt";
    public static final String WEBSOCKET_TOPIC_METRICS = "/topic/metrics";
    public static final String WEBSOCKET_TOPIC_METRICS_LIVE = "/topic/metrics-live";
}
