package com.vaut.repository;

import com.vaut.entity.DltEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Repository for managing {@link DltEvent} entities.
 */
@Repository
public interface DltEventRepository extends JpaRepository<DltEvent, Long> {
    /**
     * Counts the number of DLT events recorded after a specific timestamp.
     *
     * @param dhm The threshold timestamp.
     * @return The count of events.
     */
    long countByDhmAfter(LocalDateTime dhm);

    /**
     * Counts the number of DLT events with a specific resolution status.
     *
     * @param status The status to count (e.g., UNRESOLVED).
     * @return The count of events.
     */
    long countByStatus(String status);

    /**
     * Finds all DLT events with a specific resolution status.
     *
     * @param status The status to filter by.
     * @return A list of matching DltEvent objects.
     */
    List<DltEvent> findByStatus(String status);
}
