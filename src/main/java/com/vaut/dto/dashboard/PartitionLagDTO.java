package com.vaut.dto.dashboard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PartitionLagDTO {
    private int partition;
    private long currentOffset;
    private long logEndOffset;
    private long lag;
}
