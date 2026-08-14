package com.compagnonsdudev.repository;

import com.compagnonsdudev.entity.DltEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.compagnonsdudev.dto.repository.DltStatsProjection;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

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

    /**
     * Retrieves aggregated DLT statistics in a single query.
     *
     * @param last24h The timestamp for the last 24 hours.
     * @return A projection containing the aggregated stats.
     */
    @Query("SELECT " +
           "COUNT(e) as totalCount, " +
           "SUM(CASE WHEN e.status = 'UNRESOLVED' THEN 1L ELSE 0L END) as unresolvedCount, " +
           "SUM(CASE WHEN e.status = 'RESOLVED' THEN 1L ELSE 0L END) as resolvedCount, " +
           "SUM(CASE WHEN e.status = 'DISCARDED' THEN 1L ELSE 0L END) as discardedCount, " +
           "SUM(CASE WHEN e.dhm > :last24h THEN 1L ELSE 0L END) as countLast24h " +
           "FROM DltEvent e")
    DltStatsProjection getDltStats(@Param("last24h") LocalDateTime last24h);
}
