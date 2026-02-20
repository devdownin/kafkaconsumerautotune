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
public class MetricDTO {
    String name;
    String type;
    String description;
    String value;
    String baseUnit;
    boolean appSpecific;
    List<Double> history;
}
