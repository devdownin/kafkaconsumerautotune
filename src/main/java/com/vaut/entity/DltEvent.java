package com.vaut.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "DLT_EVENTS")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DltEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "dlt_seq")
    @SequenceGenerator(name = "dlt_seq", sequenceName = "DLT_SEQ", allocationSize = 50)
    private Long id;

    @Column(name = "original_topic")
    private String originalTopic;

    @Column(name = "original_partition")
    private Integer originalPartition;

    @Column(name = "original_offset")
    private Long originalOffset;

    @Column(name = "error_message", length = 10000)
    private String errorMessage;

    @Column(name = "payload", length = 10000)
    private String payload;

    @Lob
    @Column(name = "headers", columnDefinition = "CLOB")
    private String headers;

    @Column(name = "dhm")
    private LocalDateTime dhm;

    @Column(name = "status")
    private String status; // UNRESOLVED, RESOLVED, DISCARDED

    @Column(name = "resolved_at")
    private LocalDateTime resolvedAt;

    @Column(name = "severity")
    private String severity; // HIGH, MEDIUM, LOW
}
