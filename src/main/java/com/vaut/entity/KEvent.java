package com.vaut.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "KEVENTS", indexes = {
    @Index(name = "idx_kevent_event_id", columnList = "event_id", unique = true)
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "kevent_seq")
    @SequenceGenerator(name = "kevent_seq", sequenceName = "KEVENT_SEQ", allocationSize = 50)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Lob
    @Column(name = "payload", columnDefinition = "CLOB")
    private String payload;

    @Column(name = "partition_key")
    private String partitionKey;

    @Lob
    @Column(name = "headers", columnDefinition = "CLOB")
    private String headers;

    @Column(name = "kafka_topic")
    private String kafkaTopic;

    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        if (partitionKey == null || partitionKey.isEmpty()) {
            partitionKey = eventId;
        }
    }
}
