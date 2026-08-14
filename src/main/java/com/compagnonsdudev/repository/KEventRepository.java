package com.compagnonsdudev.repository;

import com.compagnonsdudev.entity.KEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Set;

/**
 * Repository for managing {@link KEvent} entities.
 */
@Repository
public interface KEventRepository extends JpaRepository<KEvent, Long> {
    /**
     * Checks if an event with the given eventId already exists.
     *
     * @param eventId The event identifier to check.
     * @return true if it exists, false otherwise.
     */
    boolean existsByEventId(String eventId);

    /**
     * Finds a subset of the provided event IDs that already exist in the database.
     * Used for efficient idempotency checks during batch processing.
     *
     * @param eventIds The set of event IDs to check.
     * @return A set of event IDs that were found in the database.
     */
    @Query("SELECT k.eventId FROM KEvent k WHERE k.eventId IN :eventIds")
    Set<String> findExistingEventIds(Set<String> eventIds);
}
