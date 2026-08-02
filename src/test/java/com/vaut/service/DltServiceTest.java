package com.vaut.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vaut.entity.DltEvent;
import com.vaut.repository.DltEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.support.SendResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DltServiceTest {

    @Mock
    private DltEventRepository dltEventRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private MeterRegistry meterRegistry;

    @Mock
    private Counter retryCounter;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private DltService dltService;

    @BeforeEach
    void setUp() {
        lenient().when(meterRegistry.counter(anyString(), any(String[].class))).thenReturn(retryCounter);
        // DltService now waits for the broker acknowledgement before marking an event resolved.
        lenient().when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    @Test
    public void testBulkRetry() {
        DltEvent event = DltEvent.builder().id(1L).originalTopic("topic").payload("{}").status("UNRESOLVED").build();
        when(dltEventRepository.findAllById(List.of(1L))).thenReturn(List.of(event));

        dltService.bulkRetry(List.of(1L));

        verify(kafkaTemplate).send(any(ProducerRecord.class));
        verify(dltEventRepository).save(any(DltEvent.class));
    }

    @Test
    public void testRetryPreservesOriginalKey() {
        DltEvent event = DltEvent.builder().id(1L).originalTopic("topic").originalKey("order-42")
                .payload("{}").status("UNRESOLVED").build();
        when(dltEventRepository.findById(1L)).thenReturn(Optional.of(event));

        dltService.retryEvent(1L);

        // The key decides the partition, so dropping it on replay would reorder the stream.
        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(sent.capture());
        assertThat(sent.getValue().key()).isEqualTo("order-42");
    }

    @Test
    public void testEventStaysUnresolvedWhenSendFails() {
        DltEvent event = DltEvent.builder().id(1L).originalTopic("topic").payload("{}").status("UNRESOLVED").build();
        when(dltEventRepository.findById(1L)).thenReturn(Optional.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        assertThatThrownBy(() -> dltService.retryEvent(1L)).isInstanceOf(IllegalStateException.class);

        // Marking it resolved here would silently drop the message.
        assertThat(event.getStatus()).isEqualTo("UNRESOLVED");
        verify(dltEventRepository, never()).save(any(DltEvent.class));
    }

    @Test
    public void testRetryWithPayload() {
        DltEvent event = DltEvent.builder().id(1L).originalTopic("topic").payload("{}").status("UNRESOLVED").build();
        when(dltEventRepository.findById(1L)).thenReturn(Optional.of(event));

        dltService.retryWithPayload(1L, "{\"fixed\":true}");

        verify(kafkaTemplate).send(any(ProducerRecord.class));
        verify(dltEventRepository).save(argThat(e -> "{\"fixed\":true}".equals(e.getPayload()) && "RESOLVED".equals(e.getStatus())));
    }
}
