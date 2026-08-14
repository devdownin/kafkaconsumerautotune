# Observabilité : Logging Structuré, Tracing et Métriques

KafkaConsumerAutoTune intègre une pile d'observabilité complète basée sur le standard **OpenTelemetry (OTel)** et la suite **Grafana (Prometheus, Loki, Tempo/Jaeger)**.

## 1. Logging Structuré et Centralisé

L'application génère des logs au format JSON conformes à l'Elastic Common Schema (ECS), facilitant leur exploitation automatique.

### 1.1 Format JSON
Chaque log inclut les blocs suivants :
-   `@timestamp` : Horodatage ISO8601 UTC.
-   `event` : Métadonnées (`category`, `type`, `outcome`).
-   `correlation` : Identifiants de liaison (`id`, `traceId`, `spanId`).
-   `kafka` : Coordonnées du message (`topic`, `partition`, `offset`).
-   `service` : Identité du service (`id`, `name`, `version`).

### 1.2 Centralisation avec Loki
Les logs sont écrits dans un fichier tournant (`/logs/app.log`) via Logback. Un agent **Promtail** scrappe ce fichier en temps réel et expédie les logs vers **Grafana Loki**.
-   **Corrélation** : Grâce au `traceId` présent dans chaque log, il est possible de retrouver tous les logs associés à une trace spécifique dans Grafana.

---

## 2. Tracing Distribué (OpenTelemetry)

L'application utilise **Micrometer Tracing** avec un pont **OpenTelemetry** pour générer et propager des traces.

### 2.1 Fonctionnement
-   **Propagation** : L'ID de trace est propagé à travers les headers Kafka (via l'API Observation de Spring Kafka).
-   **Exemplars** : KafkaConsumerAutoTune active les **Exemplars** Prometheus. Cela permet d'associer un `traceId` directement à un point de mesure dans un graphique de métriques. Dans Grafana, un clic sur un "point bleu" dans un graphique de latence permet d'ouvrir instantanément la trace correspondante dans Jaeger.

---

## 3. Pile Infrastructure

Le fichier `docker-compose.yml` déploie une stack d'observabilité prête à l'emploi :

### Composants
-   **OTel Collector** : Pivot de la télémétrie, reçoit traces et métriques (port 4318).
-   **Prometheus** : Stockage des métriques avec support des Exemplars (port 9090).
-   **Loki** : Stockage centralisé des logs (port 3100).
-   **Promtail** : Agent de collecte des logs applicatifs.
-   **Jaeger** : Visualisation des traces distribuées (port 16686).
-   **Grafana** : Tableaux de bord unifiés (port 3000) intégrant toutes les sources de données.

---

## 4. Configuration

### Propriétés Spring Boot (`application.yml`)
```yaml
management:
  metrics:
    distribution:
      percentiles-histogram:
        kafka.events.batch.duration: true # Nécessaire pour les Exemplars
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: http://otel-collector:4318/v1/traces
```

### Alerting
Des règles d'alerte Prometheus sont définies dans `prometheus-rules.yml` pour surveiller :
-   Le lag Kafka excessif.
-   Le taux d'erreur de traitement (> 5%).
-   La durée anormale des batchs.
