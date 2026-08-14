package com.compagnonsdudev.repository;

import com.compagnonsdudev.entity.FlinkMetric;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface FlinkMetricRepository extends JpaRepository<FlinkMetric, Long> {
    Optional<FlinkMetric> findByName(String name);
}
