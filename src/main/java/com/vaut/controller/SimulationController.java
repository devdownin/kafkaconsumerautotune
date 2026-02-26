package com.vaut.controller;

import com.vaut.dto.simulation.SimulationRequestDTO;
import com.vaut.dto.simulation.SimulationStatusDTO;
import com.vaut.service.DashboardService;
import com.vaut.service.SimulationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
@RequiredArgsConstructor
public class SimulationController {

    private final SimulationService simulationService;
    private final DashboardService dashboardService;

    @GetMapping("/simulation")
    public String simulation(Model model) {
        model.addAttribute("stats", dashboardService.getStats());
        model.addAttribute("activePage", "simulation");
        model.addAttribute("status", simulationService.getStatus());
        return "simulation";
    }

    @PostMapping("/api/simulation/start")
    @ResponseBody
    public ResponseEntity<String> startSimulation(@RequestBody SimulationRequestDTO request) {
        simulationService.startSimulation(request);
        return ResponseEntity.ok("Simulation started");
    }

    @PostMapping("/api/simulation/stop")
    @ResponseBody
    public ResponseEntity<String> stopSimulation() {
        simulationService.stopSimulation();
        return ResponseEntity.ok("Simulation stopped");
    }

    @GetMapping("/api/simulation/status")
    @ResponseBody
    public ResponseEntity<SimulationStatusDTO> getStatus() {
        return ResponseEntity.ok(simulationService.getStatus());
    }
}
