package com.vaut.dto.dashboard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object representing Kafka lag for a specific partition.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PartitionLagDTO {
    /** The partition number. */
    private int partition;
    /** The last committed offset for this partition. */
    private long currentOffset;
    /** The latest offset available in the partition on the Kafka broker. */
    private long logEndOffset;
    /** The difference between the log end offset and the current offset (messages remaining to be consumed). */
    private long lag;
}
