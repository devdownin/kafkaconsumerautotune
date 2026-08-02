package com.vaut.service;

import com.vaut.config.AppConstants;
import com.vaut.entity.KEvent;
import com.vaut.repository.KEventRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service responsible for the reliable persistence of {@link KEvent} entities.
 *
 * <p>This service is fortified with multiple resilience patterns:</p>
 * <ul>
 *     <li><b>Idempotency:</b> Automatically detects and skips duplicate events based on their unique business ID.</li>
 *     <li><b>Circuit Breaker:</b> Stops calls to the database if the failure rate is too high, protecting the infrastructure.</li>
 *     <li><b>Retry:</b> Automatically retries transient failures (e.g., temporary lock issues) before giving up.</li>
 *     <li><b>Transactional:</b> Ensures that a batch of new events is saved atomically.</li>
 * </ul>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EventPersistenceService {

    private final KEventRepository keventRepository;
    private final FilePersistenceService filePersistenceService;
    private final com.vaut.config.PersistenceProperties persistenceProperties;

    /** Kept well under Oracle's 1000-expression ceiling for IN lists. */
    private static final int ID_LOOKUP_CHUNK_SIZE = 500;

    /**
     * Persists a batch of generalized events.
     * Before saving, it checks the database for existing eventId values to avoid duplicates.
     *
     * @param events List of events to persist.
     * @return List of events that were actually saved.
     */
    @Transactional
    @CircuitBreaker(name = "persistence")
    @Retry(name = "persistence")
    public List<KEvent> saveEventsBatch(List<KEvent> events) {
        if (events == null || events.isEmpty()) {
            return List.of();
        }

        if (persistenceProperties.isSaveInFile()) {
            log.debug("Saving batch of {} events to file system", events.size());
            filePersistenceService.saveEvents(events);
            return events;
        }

        log.debug("Processing batch of {} events for persistence", events.size());

        Set<String> idsToCheck = events.stream()
                                   .map(KEvent::getEventId)
                                   .collect(Collectors.toSet());

        Set<String> existingIds = findExistingIdsChunked(idsToCheck);

        // seenIds also filters duplicates that appear twice within this same batch. Checking only
        // against the database would let both copies through and break the unique constraint on
        // event_id, failing the whole batch.
        Set<String> seenIds = new HashSet<>();
        List<KEvent> newEvents = events.stream()
                .filter(event -> {
                    boolean exists = existingIds.contains(event.getEventId()) || !seenIds.add(event.getEventId());
                    if (exists) {
                        MDC.put(AppConstants.MDC_EVENT_ID, event.getEventId());
                        log.warn("KEvent with eventId {} already exists, skipping", event.getEventId());
                        MDC.remove(AppConstants.MDC_EVENT_ID);
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

    /**
     * Runs the idempotency lookup in chunks.
     *
     * <p>Oracle rejects an IN list of more than 1000 expressions. Auto-tuning raises
     * {@code max.poll.records} up to that same figure by default and higher if reconfigured, so a
     * single-shot query sits right on the limit.</p>
     */
    private Set<String> findExistingIdsChunked(Set<String> ids) {
        if (ids.size() <= ID_LOOKUP_CHUNK_SIZE) {
            return keventRepository.findExistingEventIds(ids);
        }

        Set<String> existing = new HashSet<>();
        Set<String> chunk = new HashSet<>(ID_LOOKUP_CHUNK_SIZE);
        for (String id : ids) {
            chunk.add(id);
            if (chunk.size() == ID_LOOKUP_CHUNK_SIZE) {
                existing.addAll(keventRepository.findExistingEventIds(chunk));
                chunk = new HashSet<>(ID_LOOKUP_CHUNK_SIZE);
            }
        }
        if (!chunk.isEmpty()) {
            existing.addAll(keventRepository.findExistingEventIds(chunk));
        }
        return existing;
    }
}
