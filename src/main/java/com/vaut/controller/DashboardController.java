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

/**
 * Controller for the web dashboard interface.
 * Handles routing to various monitoring and management views.
 */
@Controller
@RequiredArgsConstructor
public class DashboardController {

	private final KEventRepository repository;
	private final DashboardService dashboardService;
	private final KafkaOptimizerService optimizerService;
	private final MessageViewerConfig messageViewerConfig;

	/**
	 * Renders the main dashboard view.
	 *
	 * @param model The UI model.
	 * @return The name of the dashboard Thymeleaf template.
	 */
	@GetMapping("/")
	public String dashboard(Model model) {
		// Fetch the 10 most recent events
		var recentEvents = repository.findAll(PageRequest.of(0, 10, Sort.by("id").descending())).getContent();
		model.addAttribute("recentEvents", recentEvents);

		// Add stats for the dashboard
		var stats = dashboardService.getStats();
		model.addAttribute("stats", stats);
		model.addAttribute("totalProcessed", stats.totalProcessed());
		model.addAttribute("activePage", "dashboard");

		return "dashboard";
	}

	/**
	 * Renders the consumer groups monitoring view.
	 *
	 * @param model The UI model.
	 * @return The name of the consumer-groups Thymeleaf template.
	 */
	@GetMapping("/consumer-groups")
	public String consumerGroups(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("activePage", "consumer-groups");
		return "consumer-groups";
	}

	/**
	 * Renders the database status view.
	 *
	 * @param model The UI model.
	 * @return The name of the db-status Thymeleaf template.
	 */
	@GetMapping("/db-status")
	public String dbStatus(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("activePage", "db-status");
		return "db-status";
	}

	/**
	 * Renders the message viewer view for inspecting individual events.
	 *
	 * @param model The UI model.
	 * @return The name of the message-viewer Thymeleaf template.
	 */
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

	/**
	 * Renders the application settings view.
	 *
	 * @param model The UI model.
	 * @return The name of the settings Thymeleaf template.
	 */
	@GetMapping("/settings")
	public String settings(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("logConfigs", dashboardService.getLogConfigs());
		model.addAttribute("activePage", "settings");
		return "settings";
	}

	/**
	 * Renders the metrics explorer view.
	 *
	 * @param model The UI model.
	 * @return The name of the metrics Thymeleaf template.
	 */
	@GetMapping("/metrics")
	public String metrics(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("metrics", dashboardService.getAllMetrics());
		model.addAttribute("activePage", "metrics");
		return "metrics";
	}

	/**
	 * Renders the Kafka parameter optimizer history view.
	 *
	 * @param model The UI model.
	 * @return The name of the optimizer Thymeleaf template.
	 */
	@GetMapping("/optimizer")
	public String optimizer(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("optimizations", optimizerService.getRecentOptimizations());
		model.addAttribute("activePage", "optimizer");
		return "optimizer";
	}

	/**
	 * Renders the system architecture documentation view.
	 *
	 * @param model The UI model.
	 * @return The name of the architecture Thymeleaf template.
	 */
	@GetMapping("/architecture")
	public String architecture(Model model) {
		model.addAttribute("stats", dashboardService.getStats());
		model.addAttribute("activePage", "architecture");
		return "architecture";
	}

	/**
	 * Endpoint to update the log level of a specific logger.
	 *
	 * @param loggerName The name of the logger to update.
	 * @param level The new log level.
	 * @return A redirect to the settings page.
	 */
	@PostMapping("/settings/logs")
	public String updateLogLevel(@RequestParam String loggerName, @RequestParam String level) {
		dashboardService.updateLogLevel(loggerName, level);
		return "redirect:/settings?success=true";
	}
}
