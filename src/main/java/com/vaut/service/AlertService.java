package com.vaut.service;

import com.vaut.entity.AlertEvent;
import com.vaut.repository.AlertEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class AlertService {

    private final AlertEventRepository alertEventRepository;
    private final WebSocketService webSocketService;

    @Transactional
    public void processWebhook(Map<String, Object> payload) {
        log.info("Received Alertmanager webhook: {}", payload);

        List<Map<String, Object>> alerts = (List<Map<String, Object>>) payload.get("alerts");
        if (alerts == null) return;

        for (Map<String, Object> alertData : alerts) {
            Map<String, String> labels = (Map<String, String>) alertData.get("labels");
            Map<String, String> annotations = (Map<String, String>) alertData.get("annotations");
            String fingerprint = (String) alertData.get("fingerprint");

            AlertEvent event = alertEventRepository.findByFingerprint(fingerprint)
                    .orElse(new AlertEvent());

            event.setAlertName(labels.get("alertname"));
            event.setStatus((String) alertData.get("status"));
            event.setSeverity(labels.get("severity"));
            event.setSummary(annotations.get("summary"));
            event.setDescription(annotations.get("description"));
            event.setInstance(labels.get("instance"));
            event.setFingerprint(fingerprint);
            event.setReceivedAt(LocalDateTime.now());

            String startsAtStr = (String) alertData.get("startsAt");
            if (startsAtStr != null) {
                event.setStartsAt(OffsetDateTime.parse(startsAtStr).toLocalDateTime());
            }

            String endsAtStr = (String) alertData.get("endsAt");
            if (endsAtStr != null && !"0001-01-01T00:00:00Z".equals(endsAtStr)) {
                event.setEndsAt(OffsetDateTime.parse(endsAtStr).toLocalDateTime());
            }

            alertEventRepository.save(event);
        }

        // Broadcast update via WebSocket
        webSocketService.broadcast("/topic/alerts", getRecentAlerts(10));
    }

    public List<AlertEvent> getRecentAlerts(int limit) {
        return alertEventRepository.findAll(PageRequest.of(0, limit, Sort.by("receivedAt").descending())).getContent();
    }
}
