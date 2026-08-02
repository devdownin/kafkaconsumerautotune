# Observabilité : Logging Structuré, Tracing et Métriques

Consotopic intègre une pile d'observabilité complète basée sur le standard **OpenTelemetry (OTel)** et la suite **Grafana (Prometheus, Loki, Tempo/Jaeger)**.

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
Les logs sont écrits dans un fichier tournant (`${LOG_PATH:-./logs}/app.log`) via Logback. Un agent **Promtail** scrappe ce fichier en temps réel et expédie les logs vers **Grafana Loki**.
-   **Corrélation** : Grâce au `traceId` présent dans chaque log, il est possible de retrouver tous les logs associés à une trace spécifique dans Grafana.
-   **Rotation** : bornée à la fois en durée et en taille — 50 Mo par fichier, 7 jours d'historique, 1 Go au total, archives compressées en `.gz`. Une rotation purement quotidienne laissait une seule journée chargée croître sans limite.
-   **Format par profil** : `dev` et `local-h2` écrivent sur la console en texte lisible ; les autres profils utilisent le format JSON ECS. Le fichier est toujours en JSON, quel que soit le profil.

---

## 1.3 Métriques applicatives

Tous les compteurs applicatifs sont préfixés `app.` (et donc `app_` une fois exposés à Prometheus).

| Compteur | Type | Description |
|---|---|---|
| `app.kafka.events.received.count` | Counter | Messages reçus depuis Kafka |
| `app.kafka.events.processed` | Counter | Messages persistés avec succès |
| `app.kafka.events.errors` | Counter | Échecs de traitement, ventilés par `type` (`id_extraction`, `missing_id`, `generic_processing`) |
| `app.kafka.events.retried` | Counter | Messages rejoués depuis la DLT |
| `app.kafka.events.batch.duration` | Timer | Durée de traitement d'un batch — l'entrée du contrôleur PID |
| `app.kafka.event.received.size` | DistributionSummary | Taille des messages en octets, tag `topic` |
| `app.kafka.tuning.batch.duration.smoothed` | Gauge | Durée de batch lissée (EMA) utilisée par le PID |
| `app.kafka.consumer.lag` | Gauge | Lag en messages, tags `group` et `topic` |
| `app.kafka.consumer.group.members` | Gauge | Membres actifs du groupe, tag `group` |

---

## 2. Tracing Distribué (OpenTelemetry)

L'application utilise **Micrometer Tracing** avec un pont **OpenTelemetry** pour générer et propager des traces.

### 2.1 Fonctionnement
-   **Propagation** : L'ID de trace est propagé à travers les headers Kafka (via l'API Observation de Spring Kafka).
-   **Exemplars** : Consotopic active les **Exemplars** Prometheus. Cela permet d'associer un `traceId` directement à un point de mesure dans un graphique de métriques. Dans Grafana, un clic sur un "point bleu" dans un graphique de latence permet d'ouvrir instantanément la trace correspondante dans Jaeger.

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
        app.kafka.events.batch.duration: true # Nécessaire pour les Exemplars
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: http://otel-collector:4318/v1/traces
```

> Le préfixe `app.` est obligatoire : la clé doit correspondre exactement au nom du compteur. Une
> clé qui ne correspond à aucun compteur est ignorée en silence, sans erreur au démarrage.

### Endpoints Actuator exposés

L'exposition est restreinte à `health,info,prometheus,metrics`. L'application n'ayant **aucune
authentification**, exposer `"*"` publierait notamment `/actuator/heapdump` — un dump mémoire
complet, donc les identifiants de connexion et le contenu des messages — ainsi que `/actuator/env`
et `/actuator/configprops`. Seul `/actuator/prometheus` est réellement consommé, par le scrape
Prometheus et la page `/metrics`.

Deux health indicators applicatifs s'ajoutent aux indicateurs standards :
-   `kafkaLag` : compare le lag total aux seuils `app.metrics.thresholds."kafka.lag"`.
-   `kafkaTuning` : expose les paramètres actuellement appliqués par l'auto-tuning.

### Alerting
Des règles d'alerte Prometheus sont définies dans `prometheus-rules.yml` pour surveiller :
-   Le lag Kafka excessif.
-   Le taux d'erreur de traitement (> 5%).
-   La durée anormale des batchs.
-   L'état du circuit breaker, la charge CPU et l'occupation mémoire.
-   L'absence de membres dans le groupe de consommation.

Les règles Kafka s'appuient sur les gauges publiées par l'application elle-même
(`app_kafka_consumer_lag`, `app_kafka_consumer_group_members`), rafraîchies toutes les 30 secondes
par `DashboardService`. Elles interrogeaient auparavant `kafka_consumer_group_lag` et
`kafka_consumer_group_members`, produites par `kafka_exporter` — absent de cette stack — et ne
pouvaient donc jamais se déclencher.

Deux comportements sont volontaires dans la publication de ces gauges :

-   **Une série disparue est mise à zéro, pas supprimée.** `KafkaConsumerStopped` matche sur `== 0` :
    une série qui s'évanouit ne déclencherait jamais l'alerte.
-   **En cas d'échec de l'AdminClient, les dernières valeurs connues sont conservées.** Un appel
    d'administration qui échoue est un incident de supervision, pas une panne du consumer ; remettre
    les compteurs à zéro déclencherait les alertes sur une erreur transitoire de monitoring.
