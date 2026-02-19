package com.vaut.controller;

import com.vaut.service.DashboardService;
import com.vaut.service.DltService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DltManagementController.class)
public class DltManagementControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DashboardService dashboardService;

    @MockitoBean
    private DltService dltService;

    @Test
    public void testBulkRetry() throws Exception {
        mockMvc.perform(post("/dlt-management/bulk-retry")
                .param("ids", "1", "2"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/dlt-management?success=retried-bulk"));

        verify(dltService).bulkRetry(List.of(1L, 2L));
    }
}
