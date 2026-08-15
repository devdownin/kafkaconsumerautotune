package com.compagnonsdudev.exception;

import com.compagnonsdudev.controller.DashboardController;
import com.compagnonsdudev.controller.SimulationController;
import com.compagnonsdudev.repository.KEventRepository;
import com.compagnonsdudev.service.DashboardService;
import com.compagnonsdudev.service.SimulationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * A handled exception used to answer 200 OK with an HTML page, whatever the
 * caller was. These tests pin down the status code and the negotiated format.
 */
@WebMvcTest({DashboardController.class, SimulationController.class})
class GlobalExceptionHandlerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean private KEventRepository repository;
    @MockBean private DashboardService dashboardService;
    @MockBean private SimulationService simulationService;
    @MockBean private com.compagnonsdudev.service.KafkaOptimizerService optimizerService;
    @MockBean private com.compagnonsdudev.config.MessageViewerConfig messageViewerConfig;

    @Test
    void browserRequestGetsTheErrorPageWithA500() throws Exception {
        // The controller reads the repository before the stats, so stub it out to
        // make sure the exception under test is the one that reaches the handler.
        when(repository.findAll(any(PageRequest.class))).thenReturn(new PageImpl<>(List.of()));
        when(dashboardService.getStats()).thenThrow(new IllegalStateException("database unreachable"));

        mockMvc.perform(get("/").accept(MediaType.TEXT_HTML))
                .andExpect(status().isInternalServerError())
                .andExpect(view().name("error"))
                .andExpect(model().attribute("error", "IllegalStateException"))
                .andExpect(model().attribute("message", "database unreachable"))
                .andExpect(model().attribute("path", "/"));
    }

    @Test
    void apiRequestGetsJsonWithA500() throws Exception {
        when(simulationService.getStatus()).thenThrow(new IllegalStateException("simulation registry down"));

        mockMvc.perform(get("/api/simulation/status"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(jsonPath("$.error").value("Internal Server Error"))
                .andExpect(jsonPath("$.message").value("simulation registry down"))
                .andExpect(jsonPath("$.path").value("/api/simulation/status"));
    }

    @Test
    void jsonIsAlsoNegotiatedOutsideTheApiPrefix() throws Exception {
        when(repository.findAll(any(PageRequest.class))).thenReturn(new PageImpl<>(List.of()));
        when(dashboardService.getStats()).thenThrow(new IllegalStateException("boom"));

        mockMvc.perform(get("/").header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(500));
    }

    @Test
    void outOfRangeSimulationParametersAreRejectedWith400() throws Exception {
        String body = """
                {"totalMessages": 0, "errorPercentage": 250, "delayBetweenMessagesMs": -1}
                """;

        mockMvc.perform(post("/api/simulation/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.path").value("/api/simulation/start"))
                .andExpect(jsonPath("$.fieldErrors.totalMessages").exists())
                .andExpect(jsonPath("$.fieldErrors.errorPercentage").exists())
                .andExpect(jsonPath("$.fieldErrors.delayBetweenMessagesMs").exists());
    }

    @Test
    void validSimulationParametersStillGoThrough() throws Exception {
        String body = """
                {"totalMessages": 500, "errorPercentage": 5, "malformedJsonPercentage": 2,
                 "delayBetweenMessagesMs": 0, "duplicatePercentage": 0, "targetThroughputMsgPerSec": 100}
                """;

        mockMvc.perform(post("/api/simulation/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk());
    }
}
