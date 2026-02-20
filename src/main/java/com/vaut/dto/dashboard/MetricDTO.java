package com.vaut.dto.dashboard;

import lombok.Builder;

@Builder
public record MetricDTO(
    String name,
    String type,
    String description,
    String value,
    String baseUnit,
    boolean appSpecific
) {}
