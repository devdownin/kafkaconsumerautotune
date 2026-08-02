package com.vaut.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.vaut.exception.PermanentException;
import com.vaut.exception.TransientException;

/**
 * Verifies the error handling actually configured on the batch listener factory.
 *
 * <p>The listener previously had no error handler at all, which meant the Spring Kafka default:
 * ten immediate redeliveries with no backoff and no exception classification. These tests
 * previously contained no assertions and so verified none of it.</p>
 */
class KafkaErrorHandlingTest {

    @SuppressWarnings("unchecked")
    private ConcurrentKafkaListenerContainerFactory<String, String> buildFactory() {
        KafkaConsumerConfig config = new KafkaConsumerConfig();
        ReflectionTestUtils.setField(config, "concurrency", 3);
        ReflectionTestUtils.setField(config, "retryBackOffMs", 2000L);
        ReflectionTestUtils.setField(config, "retryMaxAttempts", 3L);
        return config.batchFactory(mock(ConsumerFactory.class));
    }

    private DefaultErrorHandler configuredErrorHandler() {
        Object handler = ReflectionTestUtils.getField(buildFactory(), "commonErrorHandler");
        assertThat(handler).isInstanceOf(DefaultErrorHandler.class);
        return (DefaultErrorHandler) handler;
    }

    /**
     * Asks the handler's own classifier whether an exception would be retried.
     *
     * <p>{@code getClassifier()} is protected and the public {@code removeClassification} returns
     * null for an exception that was never classified explicitly, so it cannot distinguish
     * "retryable by default" from "unknown". Going through the classifier gives the same answer the
     * container would act on, for explicit and default cases alike.</p>
     */
    private boolean isRetryable(Throwable exception) {
        BinaryExceptionClassifier classifier =
                ReflectionTestUtils.invokeMethod(configuredErrorHandler(), "getClassifier");
        return classifier.classify(exception);
    }

    @Test
    void batchFactoryUsesBatchModeAndManualAck() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = buildFactory();

        assertThat(factory.isBatchListener()).isTrue();
        assertThat(factory.getContainerProperties().getAckMode()).isEqualTo(AckMode.MANUAL);
    }

    @Test
    void shouldRetryOnTransientException() {
        // A transient failure deserves another attempt: the consumer rethrows these precisely so
        // that Kafka redelivers them.
        assertThat(isRetryable(new TransientException("database temporarily unavailable"))).isTrue();
    }

    @Test
    void shouldNotRetryOnPermanentException() {
        // The consumer already routes permanent failures to the DLT itself, so redelivering one
        // just replays a message that can never succeed.
        assertThat(isRetryable(new PermanentException("malformed record"))).isFalse();
    }

    @Test
    void shouldNotRetryOnJsonProcessingException() {
        // JSON errors are permanent: the same bytes fail to parse every time.
        assertThat(isRetryable(new JsonProcessingException("bad json") {})).isFalse();
    }
}
