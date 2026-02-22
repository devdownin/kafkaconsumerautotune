package com.vaut.dto.dashboard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

/**
 * Data Transfer Object representing the status and metrics of a Kafka consumer group.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConsumerGroupDTO {
    /**
     * The unique identifier of the consumer group.
     */
    private String groupId;

    /**
     * The current state of the consumer group (e.g., Stable, Rebalancing, Dead).
     */
    private String state;

    /**
     * The number of active members in the consumer group.
     */
    private int members;

    /**
     * The total number of partitions assigned to this consumer group.
     */
    private int partitions;

    /**
     * The total lag (number of messages behind) for all partitions in this consumer group.
     */
    private long lag;

    /**
     * Detailed lag information for each partition.
     */
    private List<PartitionLagDTO> partitionLags;
}
