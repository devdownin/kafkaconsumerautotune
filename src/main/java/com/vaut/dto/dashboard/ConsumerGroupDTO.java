package com.vaut.dto.dashboard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConsumerGroupDTO {
    private String groupId;
    private String state;
    private int members;
    private int partitions;
    private long lag;
    private List<PartitionLagDTO> partitionLags;
}
