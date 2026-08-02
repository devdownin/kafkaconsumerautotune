package com.vaut.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * JPA Entity representing a message that failed processing and was routed to the Dead Letter Topic (DLT).
 */
@Entity
@Table(name = "DLT_EVENTS")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DltEvent {

    /** Technical primary key. */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "dlt_seq")
    @SequenceGenerator(name = "dlt_seq", sequenceName = "DLT_SEQ", allocationSize = 50)
    private Long id;

    /** Name of the topic where the message originated. */
    @Column(name = "original_topic")
    private String originalTopic;

    /** Partition from which the message was read. */
    @Column(name = "original_partition")
    private Integer originalPartition;

    /** Offset of the failed message. */
    @Column(name = "original_offset")
    private Long originalOffset;

    /** Key of the failed message, preserved so a retry lands on the same partition. */
    @Column(name = "original_key")
    private String originalKey;

    /** Description of the error that caused the failure. */
    @Column(name = "error_message", length = 10000)
    private String errorMessage;

    /** The full JSON payload of the failed message. */
    @Column(name = "payload", length = 10000)
    private String payload;

    /** Serialized Kafka headers of the original message. */
    @Lob
    @Column(name = "headers", columnDefinition = "CLOB")
    private String headers;

    /** Timestamp when the failure occurred. */
    @Column(name = "dhm")
    private LocalDateTime dhm;

    /** Resolution status (e.g., UNRESOLVED, RESOLVED, DISCARDED). */
    @Column(name = "status")
    private String status;

    /** Timestamp when the error was resolved or discarded. */
    @Column(name = "resolved_at")
    private LocalDateTime resolvedAt;

    /** Severity of the error (e.g., HIGH, MEDIUM, LOW). */
    @Column(name = "severity")
    private String severity;
}
