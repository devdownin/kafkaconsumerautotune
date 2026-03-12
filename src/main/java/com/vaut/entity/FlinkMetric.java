package com.vaut.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Entity representing a Flink-based Prometheus metric configuration.
 */
@Entity
@Table(name = "FLINK_METRICS")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FlinkMetric {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "flink_metric_seq")
    @SequenceGenerator(name = "flink_metric_seq", sequenceName = "FLINK_METRIC_SEQ", allocationSize = 50)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false, length = 2000)
    private String query;

    private String description;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
