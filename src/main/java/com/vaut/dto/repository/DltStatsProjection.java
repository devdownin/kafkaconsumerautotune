package com.vaut.dto.repository;

/**
 * Interface projection for DLT statistics retrieved from the database.
 * Uses wrapper types (Long, Double) to safely handle potential NULL values from SUM/AVG queries.
 */
public interface DltStatsProjection {
    Long getTotalCount();
    Long getUnresolvedCount();
    Long getResolvedCount();
    Long getDiscardedCount();
    Long getCountLast24h();
    Double getAvgResolutionTimeMinutes();
}
