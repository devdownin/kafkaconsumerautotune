package com.vaut.dto.dashboard;

import lombok.Builder;
import java.util.List;

/**
 * Data Transfer Object representing the status and metrics of a Kafka consumer group.
 */
@Builder
public record ConsumerGroupDTO(
    /** The unique identifier of the consumer group. */
    String groupId,
    /** The current state of the consumer group (e.g., Stable, Rebalancing, Dead). */
    String state,
    /** The number of active members in the consumer group. */
    int members,
    /** The total number of partitions assigned to this consumer group. */
    int partitions,
    /** The total lag (number of messages behind) for all partitions in this consumer group. */
    long lag,
    /** Detailed lag information for each partition. */
    List<PartitionLagDTO> partitionLags
) {}
