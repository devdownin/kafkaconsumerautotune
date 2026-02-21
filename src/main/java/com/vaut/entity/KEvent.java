package com.vaut.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * JPA Entity representing a generalized Kafka event processed by the application.
 */
@Entity
@Table(name = "KEVENTS", indexes = {
    @Index(name = "idx_kevent_event_id", columnList = "event_id", unique = true)
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KEvent {

    /** Technical primary key. */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "kevent_seq")
    @SequenceGenerator(name = "kevent_seq", sequenceName = "KEVENT_SEQ", allocationSize = 50)
    private Long id;

    /** Business identifier extracted from the payload. Must be unique. */
    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    /** The full JSON payload of the Kafka message. */
    @Lob
    @Column(name = "payload", columnDefinition = "CLOB")
    private String payload;

    /** Key used for Kafka partitioning. */
    @Column(name = "partition_key")
    private String partitionKey;

    /** Serialized Kafka headers. */
    @Lob
    @Column(name = "headers", columnDefinition = "CLOB")
    private String headers;

    /** Name of the source Kafka topic. */
    @Column(name = "kafka_topic")
    private String kafkaTopic;

    /** Partition number from which the message was read. */
    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    /** Offset of the message in the Kafka partition. */
    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    /** Timestamp when the event was persisted in the database. */
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    /**
     * Initializes default values before persistence.
     */
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        if (partitionKey == null || partitionKey.isEmpty()) {
            partitionKey = eventId;
        }
    }
}
