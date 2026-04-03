package com.vaut.repository;

import com.vaut.entity.FlinkMetric;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface FlinkMetricRepository extends JpaRepository<FlinkMetric, Long> {
    Optional<FlinkMetric> findByName(String name);
}
