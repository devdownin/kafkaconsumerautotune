package com.vaut.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * JPA Entity representing an alert received from Prometheus Alertmanager.
 */
@Entity
@Table(name = "ALERT_EVENTS")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AlertEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "alert_seq")
    @SequenceGenerator(name = "alert_seq", sequenceName = "ALERT_SEQ", allocationSize = 50)
    private Long id;

    @Column(name = "alert_name")
    private String alertName;

    @Column(name = "status")
    private String status; // firing, resolved

    @Column(name = "severity")
    private String severity;

    @Column(name = "summary", length = 1000)
    private String summary;

    @Column(name = "description", length = 2000)
    private String description;

    @Column(name = "instance")
    private String instance;

    @Column(name = "starts_at")
    private LocalDateTime startsAt;

    @Column(name = "ends_at")
    private LocalDateTime endsAt;

    @Column(name = "received_at")
    private LocalDateTime receivedAt;

    @Column(name = "fingerprint")
    private String fingerprint;
}
