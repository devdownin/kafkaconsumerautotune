package com.vaut.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import jakarta.persistence.EntityManagerFactory;

/**
 * Kafka Configuration class for the application.
 * Defines Consumer and Producer factories, AdminClient, and Batch Listener container factory.
 *
 * Key features:
 * - Supports SSL configuration.
 * - Optimized for high-throughput batch processing.
 * - Manual offset acknowledgment mode.
 * - Custom PlatformTransactionManager for JPA.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

 @Value("${spring.kafka.bootstrap-servers}")
 private String bootstrapServers;

 @Value("${spring.kafka.consumer.group-id}")
 private String groupId;

 @Value("${spring.kafka.consumer.key-deserializer}")
 private String keyDeserializer;

 @Value("${spring.kafka.consumer.value-deserializer}")
 private String valueDeserializer;

 @Value("${spring.kafka.consumer.enable-auto-commit:false}")
 private boolean enableAutoCommit;

 @Value("${spring.kafka.producer.key-serializer}")
 private String keySerializer;

 @Value("${spring.kafka.producer.value-serializer}")
 private String valueSerializer;

 @Value("${spring.kafka.ssl.enabled:false}")
 private boolean sslEnabled;

 @Value("${spring.kafka.ssl.trust-store-location:}")
 private String sslTrustStoreLocation;

 @Value("${spring.kafka.ssl.trust-store-password:}")
 private String sslTrustStorePassword;

 @Value("${spring.kafka.ssl.key-store-location:}")
 private String sslKeyStoreLocation;

 @Value("${spring.kafka.ssl.key-store-password:}")
 private String sslKeyStorePassword;

 @Value("${spring.kafka.ssl.key-password:}")
 private String sslKeyPassword;

 @Value("${spring.kafka.consumer.isolation-level:read_committed}")
 private String isolationLevel;

 @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
 private String autoOffsetReset;

 @Value("${spring.kafka.consumer.max-poll-records:100}")
 private int maxPollRecords;

 @Value("${spring.kafka.consumer.fetch-min-bytes:10240}")
 private int fetchMinBytes;

 @Value("${spring.kafka.consumer.fetch-max-wait-ms:500}")
 private int fetchMaxWaitMs;

 @Value("${spring.kafka.consumer.session-timeout-ms:30000}")
 private int sessionTimeoutMs;

 @Value("${spring.kafka.consumer.max-poll-interval-ms:300000}")
 private int maxPollIntervalMs;

// @Value("${spring.kafka.producer.transaction-id-prefix:tx-}")
// private String producerTransactionIdPrefix;

    /**
     * Creates the primary Kafka Consumer Factory.
     *
     * @return A DefaultKafkaConsumerFactory configured with application properties.
     */
    @Bean
    public DefaultKafkaConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(getConsumerConfigs());
    }

    /**
     * Aggregates Kafka consumer configuration properties.
     *
     * @return A map of Kafka consumer configurations.
     */
    public Map<String, Object> getConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

        // Optimized parameters for high-volume batch processing
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel.toLowerCase());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        if (sslEnabled) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTrustStoreLocation);
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTrustStorePassword);
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeyStoreLocation);
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeyStorePassword);
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        }
        return props;
    }

    /**
     * Configures the KafkaAdmin to manage topics and metadata.
     *
     * @return A KafkaAdmin bean.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (sslEnabled) {
            configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTrustStoreLocation);
            configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTrustStorePassword);
            configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeyStoreLocation);
            configs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeyStorePassword);
            configs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        }
        return new KafkaAdmin(configs);
    }

    /**
     * Provides an AdminClient for programmatic access to the Kafka cluster.
     *
     * @param kafkaAdmin The KafkaAdmin bean.
     * @return An AdminClient instance, or null if creation fails.
     */
    @Bean
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        try {
            return AdminClient.create(kafkaAdmin.getConfigurationProperties());
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Factory for the batch Kafka listener container.
     * Configures concurrency and manual acknowledgment.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> batchFactory(
         ConsumerFactory<String, String> consumerFactory) {
     
     ConcurrentKafkaListenerContainerFactory<String, String> factory = 
         new ConcurrentKafkaListenerContainerFactory<>();
     
     factory.setConsumerFactory(consumerFactory);
     factory.setConcurrency(6); // Adjust based on the number of partitions
     factory.setBatchListener(true); // BATCH MODE ENABLED

     // Manual acknowledgment mode to ensure at-least-once delivery
     factory.getContainerProperties().setAckMode(AckMode.MANUAL);
     
     return factory;
 }


    /**
     * Configures the Producer Factory for sending messages to Kafka.
     *
     * @return A ProducerFactory configured with application properties.
     */
    @Bean
    ProducerFactory<String, String> producerFactory() {
     Map<String, Object> props = new HashMap<>();
     props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
     props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
     props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
     if (sslEnabled) {
         props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
         props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTrustStoreLocation);
         props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTrustStorePassword);
         props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeyStoreLocation);
         props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeyStorePassword);
         props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
     }
     DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
 //    factory.setTransactionIdPrefix(producerTransactionIdPrefix);
     return factory;
 }


    /**
     * Provides a KafkaTemplate for high-level operations.
     *
     * @param producerFactory The ProducerFactory bean.
     * @return A KafkaTemplate bean.
     */
    @Bean
    KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
     return new KafkaTemplate<>(producerFactory);
 }

    /**
     * Configures the primary PlatformTransactionManager for the application using JPA.
     *
     * @param entityManagerFactory The EntityManagerFactory bean.
     * @return A PlatformTransactionManager bean.
     */
    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}