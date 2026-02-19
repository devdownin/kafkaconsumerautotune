package com.vaut.controller;

import com.vaut.dto.dashboard.DashboardStatsDTO;
import com.vaut.repository.KEventRepository;
import com.vaut.service.DashboardService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(DashboardController.class)
class DashboardControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KEventRepository repository;

    @MockBean
    private DashboardService dashboardService;

    @MockBean
    private com.vaut.service.KafkaOptimizerService optimizerService;

    @MockBean
    private com.vaut.config.MessageViewerConfig messageViewerConfig;

    @Test
    void shouldRenderDashboardWithStats() throws Exception {
        // Given
        when(repository.findAll(any(PageRequest.class))).thenReturn(new PageImpl<>(Collections.emptyList()));

        DashboardStatsDTO stats = DashboardStatsDTO.builder()
                .totalProcessed(100L)
                .successRate(95.0)
                .dltCount(5L)
                .consumerLag(500L)
                .dbStatus("Connected")
                .appVersion("1.0.0")
                .systemUser("admin")
                .build();
        when(dashboardService.getStats()).thenReturn(stats);

        // When & Then
        mockMvc.perform(get("/"))
                .andExpect(status().isOk())
                .andExpect(model().attributeExists("stats"))
                .andExpect(model().attribute("totalProcessed", 100L))
                .andExpect(view().name("dashboard"))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("95.0%")));
    }

    @Test
    void shouldRenderMessageViewer() throws Exception {
        // Given
        when(repository.findAll(any(PageRequest.class))).thenReturn(new PageImpl<>(Collections.emptyList()));

        DashboardStatsDTO stats = DashboardStatsDTO.builder()
                .dbStatus("Connected")
                .appVersion("1.0.0")
                .systemUser("admin")
                .build();
        when(dashboardService.getStats()).thenReturn(stats);

        // When & Then
        mockMvc.perform(get("/message-viewer"))
                .andExpect(status().isOk())
                .andExpect(model().attributeExists("stats"))
                .andExpect(model().attribute("activePage", "message-viewer"))
                .andExpect(view().name("message-viewer"));
    }
}
