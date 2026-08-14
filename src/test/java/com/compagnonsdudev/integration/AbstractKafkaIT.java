package com.compagnonsdudev.integration;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Socle des tests d'intégration : un vrai broker Kafka, pas un broker embarqué.
 *
 * <p>Le conteneur est démarré une seule fois pour toute la JVM de test et
 * partagé par l'ensemble des classes qui héritent de cette base. Un conteneur
 * par classe coûterait plusieurs dizaines de secondes sans rien apporter :
 * l'isolation entre classes est assurée par des topics distincts, que chaque
 * classe déclare via {@code @TestPropertySource}. L'arrêt est laissé à Ryuk,
 * le conteneur de nettoyage de Testcontainers, à la fin de la JVM.
 *
 * <p>Ces tests exigent un démon Docker accessible. Ils sont exécutés par
 * Failsafe en phase {@code integration-test} — donc par {@code mvn verify},
 * pas par {@code mvn test}, qui reste rapide et sans dépendance externe.
 */
@SpringBootTest
@ActiveProfiles("dev")
public abstract class AbstractKafkaIT {

    /**
     * Image Apache Kafka officielle en mode KRaft : pas de ZooKeeper à démarrer.
     */
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka:3.8.0");

    protected static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE);

    static {
        KAFKA.start();
    }

    /**
     * Substitue l'adresse du broker à celle du profil dev, qui pointe vers
     * l'hôte {@code kafkadev} de la pile docker-compose.
     */
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }
}
