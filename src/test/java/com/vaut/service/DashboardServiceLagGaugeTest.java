package com.vaut.service;

import com.vaut.config.AppConstants;
import com.vaut.repository.DltEventRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.test.util.ReflectionTestUtils;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers the consumer lag and group membership gauges.
 *
 * <p>Both values were computed for the dashboard but never registered with Micrometer, so the
 * HighKafkaLag, CriticalKafkaLag and KafkaConsumerStopped rules in {@code prometheus-rules.yml}
 * queried series that nothing in the stack produced and could never fire.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DashboardServiceLagGaugeTest {

    private static final String GROUP = "test.group";
    private static final String TOPIC = "test.topic";

    @Mock private DltEventRepository dltEventRepository;
    @Mock private EntityManager entityManager;
    @Mock private DataSource dataSource;
    @Mock private LoggingSystem loggingSystem;
    @Mock private AdminClient adminClient;
    @Mock private KafkaProperties kafkaProperties;
    @Mock private WebSocketService webSocketService;
    @Mock private KafkaTuningService kafkaTuningService;
    @Mock private com.vaut.config.MetricThresholdProperties metricThresholdProperties;
    @Mock private io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry circuitBreakerRegistry;
    @Mock private com.vaut.repository.KEventRepository eventRepository;

    private MeterRegistry meterRegistry;
    private DashboardService dashboardService;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        dashboardService = new DashboardService(dltEventRepository,
                new EventCountsService(eventRepository, dltEventRepository), entityManager,
                Optional.empty(), dataSource, loggingSystem, meterRegistry, Optional.of(adminClient),
                kafkaProperties, webSocketService, kafkaTuningService, metricThresholdProperties,
                circuitBreakerRegistry);
        ReflectionTestUtils.setField(dashboardService, "consumerGroup", GROUP);
    }

    /** Stubs one admin refresh reporting the given committed offset, end offset and member count. */
    private void givenGroupState(long committedOffset, long endOffset, int memberCount) {
        TopicPartition partition = new TopicPartition(TOPIC, 0);

        MemberAssignment assignment = new MemberAssignment(Set.of(partition));
        List<MemberDescription> members = java.util.stream.IntStream.range(0, memberCount)
                .mapToObj(i -> new MemberDescription("member-" + i, "client-" + i, "host-" + i, assignment))
                .toList();
        ConsumerGroupDescription description = new ConsumerGroupDescription(
                GROUP, false, members, "range", ConsumerGroupState.STABLE, null);

        DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(GROUP, description)));
        when(adminClient.describeConsumerGroups(any())).thenReturn(describeResult);

        ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
        when(offsetsResult.partitionsToOffsetAndMetadata())
                .thenReturn(KafkaFuture.completedFuture(Map.of(partition, new OffsetAndMetadata(committedOffset))));
        when(adminClient.listConsumerGroupOffsets(anyString())).thenReturn(offsetsResult);

        ListOffsetsResult listOffsets = mock(ListOffsetsResult.class);
        when(listOffsets.all()).thenReturn(KafkaFuture.completedFuture(Map.of(partition,
                new ListOffsetsResult.ListOffsetsResultInfo(endOffset, -1L, Optional.empty()))));
        when(adminClient.listOffsets(any(Map.class))).thenReturn(listOffsets);
    }

    private Gauge lagGauge() {
        return meterRegistry.find(AppConstants.METRIC_KAFKA_CONSUMER_LAG)
                .tag("group", GROUP).tag("topic", TOPIC).gauge();
    }

    private Gauge memberGauge() {
        return meterRegistry.find(AppConstants.METRIC_KAFKA_CONSUMER_GROUP_MEMBERS)
                .tag("group", GROUP).gauge();
    }

    @Test
    void publishesLagTaggedByGroupAndTopic() {
        // The alert rules aggregate with `sum by (group, topic)`, so both tags have to be present
        givenGroupState(400L, 1000L, 2);

        dashboardService.refreshKafkaMetrics();

        assertThat(lagGauge()).isNotNull();
        assertThat(lagGauge().value()).isEqualTo(600.0);
    }

    @Test
    void publishesGroupMemberCount() {
        givenGroupState(0L, 0L, 3);

        dashboardService.refreshKafkaMetrics();

        assertThat(memberGauge()).isNotNull();
        assertThat(memberGauge().value()).isEqualTo(3.0);
    }

    @Test
    void reportsZeroMembersRatherThanDroppingTheSeries() {
        givenGroupState(0L, 0L, 2);
        dashboardService.refreshKafkaMetrics();
        assertThat(memberGauge().value()).isEqualTo(2.0);

        // Every member left. The series has to stay and read 0, because KafkaConsumerStopped
        // matches on `== 0`; a disappearing series would silently never fire.
        DescribeConsumerGroupsResult empty = mock(DescribeConsumerGroupsResult.class);
        when(empty.all()).thenReturn(KafkaFuture.completedFuture(Collections.emptyMap()));
        when(adminClient.describeConsumerGroups(any())).thenReturn(empty);

        dashboardService.refreshKafkaMetrics();

        assertThat(memberGauge()).isNotNull();
        assertThat(memberGauge().value()).isZero();
    }

    @Test
    void keepsLastKnownValuesWhenTheAdminCallFails() {
        givenGroupState(400L, 1000L, 2);
        dashboardService.refreshKafkaMetrics();

        // A failing AdminClient is a monitoring problem, not a consumer outage. Zeroing here would
        // fire KafkaConsumerStopped on a transient admin error.
        when(adminClient.describeConsumerGroups(any())).thenThrow(new RuntimeException("broker unreachable"));

        dashboardService.refreshKafkaMetrics();

        assertThat(lagGauge().value()).isEqualTo(600.0);
        assertThat(memberGauge().value()).isEqualTo(2.0);
    }
}
