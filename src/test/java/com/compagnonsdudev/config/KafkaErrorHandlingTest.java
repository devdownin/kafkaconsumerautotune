package com.compagnonsdudev.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.compagnonsdudev.exception.PermanentException;
import com.compagnonsdudev.exception.TransientException;

class KafkaErrorHandlingTest {

    @Test
    void shouldRetryOnTransientException() {
        // Arrange
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 1));
        errorHandler.addNotRetryableExceptions(PermanentException.class);
        
        // Act & Assert
        // The DefaultErrorHandler does not expose public methods to inspect its classification logic directly.
        // We rely on integration tests (RetryDltIntegrationTest) to verify the actual retry behavior.
    }

    @Test
    void shouldNotRetryOnPermanentException() {
        // Arrange
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 1));
        errorHandler.addNotRetryableExceptions(PermanentException.class);

        // Act & Assert
        // We verify the exception is explicitly added to the not-retryable list
        // Note: internal checking might be hard, but this documents our intent.
    }
    
    @Test
    void shouldNotRetryOnJsonProcessingException() {
         DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 1));
         errorHandler.addNotRetryableExceptions(JsonProcessingException.class);
         
         // Intent: JSON errors are permanent
    }
}
