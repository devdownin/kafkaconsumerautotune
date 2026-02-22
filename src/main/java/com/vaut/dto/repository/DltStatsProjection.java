package com.vaut.dto.repository;

/**
 * Interface projection for DLT statistics retrieved from the database.
 */
public interface DltStatsProjection {
    long getTotalCount();
    long getUnresolvedCount();
    long getResolvedCount();
    long getDiscardedCount();
    long getCountLast24h();
    Double getAvgResolutionTimeMinutes();
}
