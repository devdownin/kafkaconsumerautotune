package com.vaut.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaut.config.AppConstants;
import com.vaut.dto.simulation.SimulationRequestDTO;
import com.vaut.dto.simulation.SimulationStatusDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@RequiredArgsConstructor
public class SimulationService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final WebSocketService webSocketService;

    @Value("${kafka.topic.name}")
    private String topicName;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger sentValidCount = new AtomicInteger(0);
    private final AtomicInteger sentErrorCount = new AtomicInteger(0);
    private final AtomicInteger sentMalformedCount = new AtomicInteger(0);
    private final AtomicInteger sentDuplicateCount = new AtomicInteger(0);
    private final AtomicInteger totalMessages = new AtomicInteger(0);
    private final java.util.concurrent.atomic.AtomicLong startTime = new java.util.concurrent.atomic.AtomicLong(0);

    public SimulationStatusDTO getStatus() {
        return SimulationStatusDTO.builder()
                .running(running.get())
                .processedMessages(processedCount.get())
                .totalMessages(totalMessages.get())
                .sentValid(sentValidCount.get())
                .sentError(sentErrorCount.get())
                .sentMalformed(sentMalformedCount.get())
                .sentDuplicate(sentDuplicateCount.get())
                .startTime(startTime.get())
                .build();
    }

    public void stopSimulation() {
        running.set(false);
    }

    @Async
    public void startSimulation(SimulationRequestDTO request) {
        if (running.getAndSet(true)) {
            log.warn("Simulation already running");
            return;
        }

        MDC.put(AppConstants.MDC_EVENT_CATEGORY, "simulation");
        MDC.put(AppConstants.MDC_EVENT_TYPE, "produce");

        this.totalMessages.set(request.getTotalMessages());
        this.processedCount.set(0);
        this.sentValidCount.set(0);
        this.sentErrorCount.set(0);
        this.sentMalformedCount.set(0);
        this.sentDuplicateCount.set(0);
        this.startTime.set(System.currentTimeMillis());

        log.info("Starting simulation: {}", request);
        Random random = new Random();

        String lastKey = null;
        String lastPayload = null;

        try {
            for (int i = 0; i < request.getTotalMessages() && running.get(); i++) {
                int chance = random.nextInt(100);
                String payload;
                String key;
                String correlationId = UUID.randomUUID().toString();

                boolean isDuplicate = (lastKey != null && chance < request.getDuplicatePercentage());

                if (isDuplicate) {
                    key = lastKey;
                    payload = lastPayload;
                    sentDuplicateCount.incrementAndGet();
                    MDC.put(AppConstants.MDC_EVENT_TYPE, "produce_duplicate");
                } else {
                    key = UUID.randomUUID().toString();
                    MDC.put(AppConstants.MDC_EVENT_TYPE, "produce");
                    if (chance < (request.getDuplicatePercentage() + request.getMalformedJsonPercentage())) {
                        payload = "MALFORMED_JSON_CONTENT_{" + key + "}";
                        sentMalformedCount.incrementAndGet();
                    } else if (chance < (request.getDuplicatePercentage() + request.getMalformedJsonPercentage() + request.getErrorPercentage())) {
                        payload = objectMapper.writeValueAsString(Map.of(
                                "notIdPassage", key,
                                "timestamp", System.currentTimeMillis(),
                                "data", "Message missing idPassage"
                        ));
                        sentErrorCount.incrementAndGet();
                    } else {
                        payload = objectMapper.writeValueAsString(Map.of(
                                "idPassage", key,
                                "timestamp", System.currentTimeMillis(),
                                "data", "Valid message content"
                        ));
                        sentValidCount.incrementAndGet();
                    }
                    lastKey = key;
                    lastPayload = payload;
                }

                MDC.put(AppConstants.MDC_CORRELATION_ID, correlationId);
                MDC.put(AppConstants.MDC_KAFKA_KEY, key);
                MDC.put(AppConstants.MDC_KAFKA_TOPIC, topicName);

                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, payload);
                record.headers().add(AppConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));

                kafkaTemplate.send(record);
                log.debug("Simulated message sent with correlationId: {}", correlationId);

                processedCount.incrementAndGet();

                // Periodic broadcast for UI (every 50 messages or if last)
                if (processedCount.get() % 50 == 0 || processedCount.get() == totalMessages.get()) {
                    webSocketService.sendSimulationStatus(getStatus());
                }

                // Throttle control. Pacing is computed against the simulation's start time rather
                // than by sleeping a fixed amount per message: at rates above 1000 msg/s the
                // per-message interval rounds below a millisecond, and the send itself takes time
                // the fixed sleep never accounts for.
                if (request.getTargetThroughputMsgPerSec() > 0) {
                    long dueAt = startTime.get() + (long) ((i + 1) * 1000.0 / request.getTargetThroughputMsgPerSec());
                    long sleepMs = dueAt - System.currentTimeMillis();
                    if (sleepMs > 0) {
                        Thread.sleep(sleepMs);
                    }
                } else if (request.getDelayBetweenMessagesMs() > 0) {
                    Thread.sleep(request.getDelayBetweenMessagesMs());
                }

                MDC.remove(AppConstants.MDC_CORRELATION_ID);
                MDC.remove(AppConstants.MDC_KAFKA_KEY);
                MDC.remove(AppConstants.MDC_KAFKA_TOPIC);
            }
        } catch (InterruptedException e) {
            log.error("Simulation interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error during simulation", e);
        } finally {
            running.set(false);
            webSocketService.sendSimulationStatus(getStatus());
            log.info("Simulation finished: {}/{}", processedCount.get(), totalMessages.get());
            MDC.clear();
        }
    }
}
