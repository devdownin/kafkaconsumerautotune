package com.compagnonsdudev.service;

import com.compagnonsdudev.config.AppConstants;
import com.compagnonsdudev.entity.KEvent;
import com.compagnonsdudev.entity.DltEvent;
import com.compagnonsdudev.dto.dashboard.DashboardStatsDTO;
import com.compagnonsdudev.dto.dashboard.JvmStatsDTO;
import com.compagnonsdudev.dto.dashboard.SystemEventDTO;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Service for sending real-time updates to connected clients via WebSockets.
 * Uses SimpMessagingTemplate to broadcast data to various STOMP topics.
 */
@Service
public class WebSocketService {

    private final SimpMessagingTemplate messagingTemplate;

    /**
     * Constructs a new WebSocketService with the provided messaging template.
     *
     * @param messagingTemplate The template used to send messages.
     */
    public WebSocketService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    /**
     * Broadcasts a system event notification.
     *
     * @param systemEvent The system event notification to broadcast.
     */
    public void sendSystemEvent(SystemEventDTO systemEvent) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_SYSTEM_EVENTS, systemEvent);
    }

    /**
     * Broadcasts a list of newly processed events.
     *
     * @param events The list of processed KEvent objects.
     */
    public void sendNewEvents(List<KEvent> events) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_EVENTS, events);
    }

    /**
     * Broadcasts updated dashboard statistics.
     *
     * @param stats The updated DashboardStatsDTO object.
     */
    public void sendStats(DashboardStatsDTO stats) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_STATS, stats);
    }

    /**
     * Broadcasts a new Dead Letter Topic (DLT) event.
     *
     * @param event The DltEvent object to broadcast.
     */
    public void sendDltEvent(DltEvent event) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_DLT, event);
    }

    /**
     * Broadcasts updated JVM performance statistics.
     *
     * @param stats The updated JvmStatsDTO object.
     */
    public void sendJvmStats(JvmStatsDTO stats) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_METRICS, stats);
    }

    /**
     * Broadcasts a generic payload to a specified WebSocket topic.
     *
     * @param topic The destination topic.
     * @param payload The object to broadcast.
     */
    public void broadcast(String topic, Object payload) {
        messagingTemplate.convertAndSend(topic, payload);
    }

    /**
     * Broadcasts the current status of the traffic simulation.
     *
     * @param status The simulation status DTO.
     */
    public void sendSimulationStatus(Object status) {
        messagingTemplate.convertAndSend(AppConstants.WEBSOCKET_TOPIC_SIMULATION, status);
    }
}
