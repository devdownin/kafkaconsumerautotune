package com.vaut.service;

import com.vaut.config.AppConstants;
import com.vaut.entity.KEvent;
import com.vaut.entity.DltEvent;
import com.vaut.dto.dashboard.DashboardStatsDTO;
import com.vaut.dto.dashboard.JvmStatsDTO;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class WebSocketService {

    private final SimpMessagingTemplate messagingTemplate;

    public WebSocketService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void sendNewEvents(List<KEvent> events) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_EVENTS, events);
    }

    public void sendStats(DashboardStatsDTO stats) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_STATS, stats);
    }

    public void sendDltEvent(DltEvent event) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_DLT, event);
    }

    public void sendJvmStats(JvmStatsDTO stats) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_METRICS, stats);
    }
}
