package com.vaut.service;

import com.vaut.dto.dashboard.DashboardStatsDTO;
import com.vaut.repository.DltEventRepository;
import com.vaut.repository.KEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.springframework.boot.logging.LoggingSystem;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Map;
import java.util.Optional;

import com.vaut.dto.repository.DltStatsProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DashboardServiceTest {

    @Mock
    private KEventRepository eventRepository;

    @Mock
    private DltEventRepository dltEventRepository;

    @Mock
    private EntityManager entityManager;

    @Mock
    private DataSource dataSource;

    @Mock
    private LoggingSystem loggingSystem;

    @Mock
    private MeterRegistry meterRegistry;

    @Mock
    private AdminClient adminClient;

    @Mock
    private KafkaProperties kafkaProperties;

    @Mock
    private WebSocketService webSocketService;

    @Mock
    private KafkaTuningService kafkaTuningService;

    @Mock
    private com.vaut.config.MetricThresholdProperties metricThresholdProperties;

    @Mock
    private io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry circuitBreakerRegistry;

    private DashboardService dashboardService;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        when(kafkaTuningService.getCurrentTuningParameters()).thenReturn(Map.of(
            "maxPollRecords", 100,
            "fetchMinBytes", 10240,
            "fetchMaxWaitMs", 500,
            "concurrency", 6
        ));
        io.github.resilience4j.circuitbreaker.CircuitBreaker cb = mock(io.github.resilience4j.circuitbreaker.CircuitBreaker.class);
        when(circuitBreakerRegistry.circuitBreaker("persistence")).thenReturn(cb);
        when(cb.getState()).thenReturn(io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED);

        dashboardService = new DashboardService(eventRepository, dltEventRepository, entityManager, Optional.empty(), dataSource, loggingSystem, meterRegistry, Optional.of(adminClient), kafkaProperties, webSocketService, kafkaTuningService, metricThresholdProperties, circuitBreakerRegistry);
    }

    @Test
    void shouldCalculateStatsCorrectly() throws Exception {
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getDatabaseProductName()).thenReturn("H2");
        when(entityManager.createNativeQuery(anyString())).thenReturn(mock(Query.class));

        // Given
        when(eventRepository.count()).thenReturn(95L);
        DltStatsProjection dltStats = mock(DltStatsProjection.class);
        when(dltEventRepository.getDltStats(any())).thenReturn(dltStats);
        when(dltStats.getTotalCount()).thenReturn(5L);
        when(dltStats.getUnresolvedCount()).thenReturn(3L);
        when(dltStats.getResolvedCount()).thenReturn(1L);
        when(dltStats.getDiscardedCount()).thenReturn(1L);

        // When
        DashboardStatsDTO stats = dashboardService.getStats();

        // Then
        assertThat(stats.totalProcessed()).isEqualTo(100L);
        assertThat(stats.successRate()).isEqualTo(95.0);
        assertThat(stats.dltCount()).isEqualTo(5L);
        assertThat(stats.successCount()).isEqualTo(95L);
        assertThat(stats.errorCount()).isEqualTo(3L);
        assertThat(stats.retryCount()).isEqualTo(2L);
    }

    @Test
    void shouldHandleZeroEvents() throws Exception {
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getDatabaseProductName()).thenReturn("H2");
        when(entityManager.createNativeQuery(anyString())).thenReturn(mock(Query.class));

        // Given
        when(eventRepository.count()).thenReturn(0L);
        DltStatsProjection dltStats = mock(DltStatsProjection.class);
        when(dltEventRepository.getDltStats(any())).thenReturn(dltStats);

        // When
        DashboardStatsDTO stats = dashboardService.getStats();

        // Then
        assertThat(stats.successRate()).isEqualTo(100.0);
    }

    @Test
    void shouldMaintainThroughputHistorySize() {
        io.micrometer.core.instrument.search.Search mockSearch = mock(io.micrometer.core.instrument.search.Search.class);
        when(meterRegistry.find(anyString())).thenReturn(mockSearch);
        when(mockSearch.counter()).thenReturn(mock(io.micrometer.core.instrument.Counter.class));
        DltStatsProjection dltStats = mock(DltStatsProjection.class);
        when(dltEventRepository.getDltStats(any())).thenReturn(dltStats);

        // Given
        DashboardStatsDTO statsInitial = dashboardService.getStats();
        assertThat(statsInitial.throughput()).hasSize(180);

        // When - simulation of throughput updates
        for (int i = 0; i < 200; i++) {
            dashboardService.updateThroughputHistory();
        }

        // Then
        DashboardStatsDTO statsAfter = dashboardService.getStats();
        assertThat(statsAfter.throughput()).hasSize(180);
    }
}
