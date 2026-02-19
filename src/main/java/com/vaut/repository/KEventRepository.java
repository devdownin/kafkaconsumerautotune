package com.vaut.repository;

import com.vaut.entity.KEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Set;

@Repository
public interface KEventRepository extends JpaRepository<KEvent, Long> {
    boolean existsByEventId(String eventId);

    @Query("SELECT k.eventId FROM KEvent k WHERE k.eventId IN :eventIds")
    Set<String> findExistingEventIds(Set<String> eventIds);
}
