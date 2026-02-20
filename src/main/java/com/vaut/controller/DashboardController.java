package com.vaut.controller;

import com.vaut.repository.KEventRepository;
import com.vaut.service.DashboardService;
import com.vaut.service.KafkaOptimizerService;
import com.vaut.config.MessageViewerConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequiredArgsConstructor
public class DashboardController {

	private final KEventRepository repository;
	private final DashboardService dashboardService;
	private final KafkaOptimizerService optimizerService;
	private final MessageViewerConfig messageViewerConfig;

	@GetMapping("/")
	public String dashboard(Model model) {
		// Fetch the 10 most recent events
		var recentEvents = repository.findAll(PageRequest.of(0, 10, Sort.by("id").descending())).getContent();
		model.addAttribute("recentEvents", recentEvents);

		// Add stats for the dashboard
		var stats = dashboardService.getStats();
		model.addAttribute("stats", stats);
		model.addAttribute("totalProcessed", stats.getTotalProcessed());
		model.addAttribute("activePage", "dashboard");

		return "dashboard";
	}

	@GetMapping("/consumer-groups")
	public String consumerGroups(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("activePage", "consumer-groups");
		return "consumer-groups";
	}

	@GetMapping("/db-status")
	public String dbStatus(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("activePage", "db-status");
		return "db-status";
	}

	@GetMapping("/message-viewer")
	public String messageViewer(Model model) {
		var recentEvents = repository.findAll(PageRequest.of(0, 20, Sort.by("id").descending())).getContent();
		model.addAttribute("recentEvents", recentEvents);
		model.addAttribute("lastEvent", recentEvents.stream().findFirst().orElse(null));
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("messageViewerConfig", messageViewerConfig);
		model.addAttribute("activePage", "message-viewer");
		return "message-viewer";
	}

	@GetMapping("/settings")
	public String settings(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("logConfigs", dashboardService.getLogConfigs());
		model.addAttribute("activePage", "settings");
		return "settings";
	}

	@GetMapping("/metrics")
	public String metrics(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("metrics", dashboardService.getAllMetrics());
		model.addAttribute("activePage", "metrics");
		return "metrics";
	}

	@GetMapping("/optimizer")
	public String optimizer(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("optimizations", optimizerService.getRecentOptimizations());
		model.addAttribute("activePage", "optimizer");
		return "optimizer";
	}

	@PostMapping("/settings/logs")
	public String updateLogLevel(@RequestParam String loggerName, @RequestParam String level) {
		dashboardService.updateLogLevel(loggerName, level);
		return "redirect:/settings?success=true";
	}
}
