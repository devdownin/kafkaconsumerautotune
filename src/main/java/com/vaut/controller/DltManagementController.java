package com.vaut.controller;

import com.vaut.service.DashboardService;
import com.vaut.service.DltService;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Controller for managing Dead Letter Topic (DLT) events.
 * Provides endpoints for viewing, retrying, and discarding failed messages.
 */
@Controller
@RequestMapping("/dlt-management")
@RequiredArgsConstructor
public class DltManagementController {

    private final DashboardService dashboardService;
    private final DltService dltService;

    /**
     * Renders the DLT management view with recent failure events.
     *
     * @param model The UI model.
     * @return The name of the dlt-management Thymeleaf template.
     */
    @GetMapping
    public String dltManagement(Model model) {
        var dltEvents = dashboardService.getRecentDltEvents(25);
        model.addAttribute("dltEvents", dltEvents);
        model.addAttribute("stats", dashboardService.getStats());
        model.addAttribute("activePage", "dlt");
        return "dlt-management";
    }

    /**
     * Discards a specific DLT event.
     *
     * @param id The ID of the event to discard.
     * @return A redirect to the DLT management page.
     */
    @PostMapping("/{id}/discard")
    public String discardEvent(@PathVariable Long id) {
        dltService.discardEvent(id);
        return "redirect:/dlt-management?success=discarded";
    }

    /**
     * Retries a specific DLT event.
     *
     * @param id The ID of the event to retry.
     * @return A redirect to the DLT management page.
     */
    @PostMapping("/{id}/retry")
    public String retryEvent(@PathVariable Long id) {
        dltService.retryEvent(id);
        return "redirect:/dlt-management?success=retried";
    }

    /**
     * Retries all currently unresolved DLT events.
     *
     * @return A redirect to the DLT management page.
     */
    @PostMapping("/retry-all")
    public String retryAll() {
        dltService.retryAll();
        return "redirect:/dlt-management?success=retried-all";
    }

    /**
     * Discards all currently unresolved DLT events.
     *
     * @return A redirect to the DLT management page.
     */
    @PostMapping("/discard-all")
    public String discardAll() {
        dltService.discardAll();
        return "redirect:/dlt-management?success=discarded-all";
    }

    /**
     * Retries a set of DLT events in bulk.
     *
     * @param ids The list of event IDs to retry.
     * @return A redirect to the DLT management page.
     */
    @PostMapping("/bulk-retry")
    public String bulkRetry(@RequestParam("ids") List<Long> ids) {
        dltService.bulkRetry(ids);
        return "redirect:/dlt-management?success=retried-bulk";
    }

    /**
     * Discards a set of DLT events in bulk.
     *
     * @param ids The list of event IDs to discard.
     * @return A redirect to the DLT management page.
     */
    @PostMapping("/bulk-discard")
    public String bulkDiscard(@RequestParam("ids") List<Long> ids) {
        dltService.bulkDiscard(ids);
        return "redirect:/dlt-management?success=discarded-bulk";
    }

    /**
     * Retries a DLT event with a manually modified payload.
     *
     * @param id The ID of the event to retry.
     * @param payloadMap A map containing the modified payload.
     * @return A JSON response indicating success.
     */
    @PostMapping("/{id}/retry-with-payload")
    @ResponseBody
    public Map<String, String> retryWithPayload(@PathVariable Long id, @RequestBody Map<String, String> payloadMap) {
        String modifiedPayload = payloadMap.get("payload");
        dltService.retryWithPayload(id, modifiedPayload);
        return Map.of("status", "success");
    }
}
