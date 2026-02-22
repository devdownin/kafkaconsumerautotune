package com.vaut.service;

import com.vaut.entity.KEvent;
import com.vaut.repository.KEventRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service responsible for persisting generalized Kafka events to the database.
 * Handles idempotency by checking for existing event IDs before saving.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EventPersistenceService {

    private final KEventRepository keventRepository;

    /**
     * Persists a batch of generalized events.
     * Before saving, it checks the database for existing eventId values to avoid duplicates.
     *
     * @param events List of events to persist.
     * @return List of events that were actually saved.
     */
    @Transactional
    @Retryable(
        retryFor = { org.springframework.dao.TransientDataAccessException.class, org.springframework.dao.ConcurrencyFailureException.class },
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    @CircuitBreaker(name = "persistence")
    public List<KEvent> saveEventsBatch(List<KEvent> events) {
        if (events == null || events.isEmpty()) {
            return List.of();
        }
        log.debug("Processing batch of {} events for persistence", events.size());

        Set<String> idsToCheck = events.stream()
                                   .map(KEvent::getEventId)
                                   .collect(Collectors.toSet());

        Set<String> existingIds = keventRepository.findExistingEventIds(idsToCheck);

        List<KEvent> newEvents = events.stream()
                .filter(event -> {
                    boolean exists = existingIds.contains(event.getEventId());
                    if (exists) {
                        log.warn("KEvent with eventId {} already exists, skipping", event.getEventId());
                    }
                    return !exists;
                })
                .collect(Collectors.toList());

        if (!newEvents.isEmpty()) {
            List<KEvent> persistedEvents = keventRepository.saveAll(newEvents);
            log.info("Successfully persisted {} new events in batch", newEvents.size());
            return persistedEvents;
        } else {
            log.info("No new events to persist in this batch (all duplicates or empty)");
        }
        return newEvents;
    }
}
