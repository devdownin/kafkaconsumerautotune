package com.vaut.repository;

import com.vaut.entity.AlertEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AlertEventRepository extends JpaRepository<AlertEvent, Long> {
    Optional<AlertEvent> findByFingerprint(String fingerprint);
}
