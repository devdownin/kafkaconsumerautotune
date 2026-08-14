package com.compagnonsdudev.controller;

import com.compagnonsdudev.entity.KEvent;
import com.compagnonsdudev.repository.KEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST controller for managing generalized Kafka events.
 * Provides endpoints for paginated access to processed events.
 */
@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class KEventController {

    private final KEventRepository repository;

    /**
     * Retrieves a paginated list of processed events.
     *
     * @param page The page number to retrieve (default 0).
     * @param size The number of events per page (default 10).
     * @return A Page of KEvent objects.
     */
    @GetMapping
    public Page<KEvent> getEvents(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return repository.findAll(PageRequest.of(page, size, Sort.by("id").descending()));
    }

    /**
     * Retrieves the most recently processed events.
     *
     * @param limit The maximum number of recent events to return (default 10).
     * @return A list of the most recent KEvent objects.
     */
    @GetMapping("/recent")
    public List<KEvent> getRecentEvents(@RequestParam(defaultValue = "10") int limit) {
        return repository.findAll(PageRequest.of(0, limit, Sort.by("id").descending())).getContent();
    }
}
