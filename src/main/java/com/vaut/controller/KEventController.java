package com.vaut.controller;

import com.vaut.entity.KEvent;
import com.vaut.repository.KEventRepository;
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
 */
@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class KEventController {

    private final KEventRepository repository;

    @GetMapping
    public Page<KEvent> getEvents(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return repository.findAll(PageRequest.of(page, size, Sort.by("id").descending()));
    }

    @GetMapping("/recent")
    public List<KEvent> getRecentEvents(@RequestParam(defaultValue = "10") int limit) {
        return repository.findAll(PageRequest.of(0, limit, Sort.by("id").descending())).getContent();
    }
}
