package com.vaut.service;

import com.vaut.dto.repository.DltStatsProjection;
import com.vaut.repository.DltEventRepository;
import com.vaut.repository.KEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Supplies the aggregate row counts shown on the dashboard.
 *
 * <p>Separate from {@link DashboardService} so the counts can be cached on their own, longer
 * schedule. They come from a full {@code COUNT(*)} over KEVENTS plus an aggregate over DLT_EVENTS,
 * which are by far the most expensive part of building the dashboard statistics and the part that
 * matters least when a few seconds stale. The dashboard cache expires every 5 seconds and the
 * scheduler refreshes it on exactly that interval, so without a second cache here these two
 * queries would run every 5 seconds for the lifetime of the process.</p>
 */
@Service
@RequiredArgsConstructor
public class EventCountsService {

    private final KEventRepository eventRepository;
    private final DltEventRepository dltEventRepository;

    /** Aggregate counts used to build the dashboard statistics. */
    public record Counts(long successCount, long dltCount, long countLast24h, long unresolvedCount,
                         long resolvedCount, long discardedCount) {}

    /**
     * Retrieves the current counts, cached for 30 seconds.
     *
     * @return A snapshot of the processed and DLT event counts.
     */
    @Cacheable(value = "eventCounts", sync = true)
    public Counts getCounts() {
        long successCount = eventRepository.count();
        DltStatsProjection dltStats = dltEventRepository.getDltStats(LocalDateTime.now().minusDays(1));

        return new Counts(
                successCount,
                orZero(dltStats.getTotalCount()),
                orZero(dltStats.getCountLast24h()),
                orZero(dltStats.getUnresolvedCount()),
                orZero(dltStats.getResolvedCount()),
                orZero(dltStats.getDiscardedCount()));
    }

    private static long orZero(Long value) {
        return value != null ? value : 0L;
    }
}
