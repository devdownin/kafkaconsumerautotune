package com.vaut.config;

/**
 * Global constants for the application, including metric names and WebSocket topics.
 */
public final class AppConstants {
    private AppConstants() {}

    /** Total count of events received from Kafka. */
    public static final String METRIC_KAFKA_EVENTS_RECEIVED_COUNT = "app.kafka.events.received.count";
    public static final String METRIC_KAFKA_EVENTS_RECEIVED_COUNT_DESC = "Total count of events received from Kafka";
    /** Size (in bytes) of individual events received from Kafka. */
    public static final String METRIC_KAFKA_EVENT_RECEIVED_SIZE = "app.kafka.event.received.size";
    public static final String METRIC_KAFKA_EVENT_RECEIVED_SIZE_DESC = "Size (in bytes) of individual events received from Kafka";
    /** Count of errors encountered during event processing. */
    public static final String METRIC_KAFKA_EVENTS_ERRORS = "app.kafka.events.errors";
    public static final String METRIC_KAFKA_EVENTS_ERRORS_DESC = "Count of errors encountered during event processing";
    /** Count of successfully processed and persisted events. */
    public static final String METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS = "app.kafka.events.processed";
    public static final String METRIC_KAFKA_EVENTS_PROCESSED_SUCCESS_DESC = "Count of successfully processed and persisted events";
    /** Count of events that have been retried from the DLT. */
    public static final String METRIC_KAFKA_EVENTS_RETRIED = "app.kafka.events.retried";
    public static final String METRIC_KAFKA_EVENTS_RETRIED_DESC = "Count of events that have been retried from the DLT";
    /** Duration (in milliseconds) taken to process a batch of events. */
    public static final String METRIC_KAFKA_EVENTS_BATCH_DURATION = "app.kafka.events.batch.duration";
    public static final String METRIC_KAFKA_EVENTS_BATCH_DURATION_DESC = "Duration (in milliseconds) taken to process a batch of events";

    /** WebSocket topic for broadcasting new successfully processed events. */
    public static final String WEBSOCKET_TOPIC_EVENTS = "/topic/events";
    /** WebSocket topic for broadcasting general dashboard statistics. */
    public static final String WEBSOCKET_TOPIC_STATS = "/topic/stats";
    /** WebSocket topic for broadcasting new Dead Letter Topic (DLT) events. */
    public static final String WEBSOCKET_TOPIC_DLT = "/topic/dlt";
    /** WebSocket topic for broadcasting aggregate metric updates. */
    public static final String WEBSOCKET_TOPIC_METRICS = "/topic/metrics";
    /** WebSocket topic for broadcasting real-time individual metric updates. */
    public static final String WEBSOCKET_TOPIC_METRICS_LIVE = "/topic/metrics-live";
    /** WebSocket topic for system-wide notifications and state changes. */
    public static final String WEBSOCKET_TOPIC_SYSTEM_EVENTS = "/topic/system-events";

    // Logging MDC Keys
    public static final String MDC_CORRELATION_ID = "correlationId";
    public static final String MDC_EVENT_CATEGORY = "eventCategory";
    public static final String MDC_EVENT_TYPE = "eventType";
    public static final String MDC_EVENT_OUTCOME = "eventOutcome";
    public static final String MDC_HTTP_METHOD = "httpMethod";
    public static final String MDC_URL_PATH = "urlPath";
    public static final String MDC_KAFKA_TOPIC = "kafkaTopic";
    public static final String MDC_KAFKA_PARTITION = "kafkaPartition";
    public static final String MDC_KAFKA_OFFSET = "kafkaOffset";
    public static final String MDC_KAFKA_KEY = "kafkaKey";
    public static final String MDC_EVENT_ID = "eventId";
    public static final String MDC_RGPD = "rgpd";
    public static final String MDC_AUTHENTICATION = "authentication";

    // Headers
    public static final String HEADER_CORRELATION_ID = "X-Correlation-Id";
}
