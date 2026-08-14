package com.compagnonsdudev.integration;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

/**
 * Test de fumée : le contexte applicatif complet démarre contre un vrai broker.
 */
@TestPropertySource(properties = {
    "kafka.topic.name=it-context.topic",
    "kafka.topic.dlt=it-context.topic.dlt"
})
class ApplicationContextIT extends AbstractKafkaIT {

    @Test
    void contextLoads() {
    }
}
