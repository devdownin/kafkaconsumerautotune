package com.vaut.controller;

import com.vaut.service.DashboardService;
import com.vaut.service.DltService;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/dlt-management")
@RequiredArgsConstructor
public class DltManagementController {

    private final DashboardService dashboardService;
    private final DltService dltService;

    @GetMapping
    public String dltManagement(Model model) {
        var dltEvents = dashboardService.getRecentDltEvents(25);
        model.addAttribute("dltEvents", dltEvents);
        model.addAttribute("stats", dashboardService.getStats());
        model.addAttribute("activePage", "dlt");
        return "dlt-management";
    }

    @PostMapping("/{id}/discard")
    public String discardEvent(@PathVariable Long id) {
        dltService.discardEvent(id);
        return "redirect:/dlt-management?success=discarded";
    }

    @PostMapping("/{id}/retry")
    public String retryEvent(@PathVariable Long id) {
        dltService.retryEvent(id);
        return "redirect:/dlt-management?success=retried";
    }

    @PostMapping("/retry-all")
    public String retryAll() {
        dltService.retryAll();
        return "redirect:/dlt-management?success=retried-all";
    }

    @PostMapping("/discard-all")
    public String discardAll() {
        dltService.discardAll();
        return "redirect:/dlt-management?success=discarded-all";
    }

    @PostMapping("/bulk-retry")
    public String bulkRetry(@RequestParam("ids") List<Long> ids) {
        dltService.bulkRetry(ids);
        return "redirect:/dlt-management?success=retried-bulk";
    }

    @PostMapping("/bulk-discard")
    public String bulkDiscard(@RequestParam("ids") List<Long> ids) {
        dltService.bulkDiscard(ids);
        return "redirect:/dlt-management?success=discarded-bulk";
    }

    @PostMapping("/{id}/retry-with-payload")
    @ResponseBody
    public Map<String, String> retryWithPayload(@PathVariable Long id, @RequestBody Map<String, String> payloadMap) {
        String modifiedPayload = payloadMap.get("payload");
        dltService.retryWithPayload(id, modifiedPayload);
        return Map.of("status", "success");
    }
}
