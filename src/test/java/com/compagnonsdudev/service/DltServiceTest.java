package com.compagnonsdudev.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.compagnonsdudev.entity.DltEvent;
import com.compagnonsdudev.repository.DltEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Optional;

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
    }

    @Test
    public void testBulkRetry() {
        DltEvent event = DltEvent.builder().id(1L).originalTopic("topic").payload("{}").status("UNRESOLVED").build();
        when(dltEventRepository.findById(1L)).thenReturn(Optional.of(event));

        dltService.bulkRetry(List.of(1L));

        verify(kafkaTemplate).send(any(ProducerRecord.class));
        verify(dltEventRepository).save(any(DltEvent.class));
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
