package com.compagnonsdudev.dto.dashboard;

import lombok.Builder;

/**
 * Data Transfer Object representing Kafka lag for a specific partition.
 */
@Builder
public record PartitionLagDTO(
    /** The partition number. */
    int partition,
    /** The last committed offset for this partition. */
    long currentOffset,
    /** The latest offset available in the partition on the Kafka broker. */
    long logEndOffset,
    /** The difference between the log end offset and the current offset (messages remaining to be consumed). */
    long lag
) {}
