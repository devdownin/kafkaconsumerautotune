package com.vaut.repository;

import com.vaut.entity.DltEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface DltEventRepository extends JpaRepository<DltEvent, Long> {
    long countByDhmAfter(LocalDateTime dhm);
    long countByStatus(String status);
    List<DltEvent> findByStatus(String status);
}
