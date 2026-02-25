# Observabilité : Logging Structuré et Tracing

Consotopic intègre une pile d'observabilité moderne basée sur le format JSON ELK (Elasticsearch, Logstash, Kibana) et le standard OpenTelemetry (OTel).

## 1. Logging Structuré (ELK/ECS)

L'application génère des logs au format JSON conformes à un schéma hiérarchique inspiré de l'Elastic Common Schema (ECS). Cela facilite l'indexation et l'analyse dans une pile ELK.

### Format JSON
Chaque log inclut les blocs suivants :
-   `@timestamp` : Horodatage ISO8601 UTC.
-   `event` : Métadonnées de l'événement (`category`, `type`, `outcome`).
-   `correlation` : Identifiants de liaison (`id`, `traceId`, `spanId`).
-   `log` : Détails techniques (`level`, `logger`, `authentication` masqué).
-   `organization` : Métadonnées du service (`id`, `application`, `version`).

### Enrichissement des logs
-   **Web** : Les logs HTTP incluent la méthode (`http.method`) et le chemin (`url.path`).
-   **Kafka** : Les logs de consommation incluent le topic (`kafka.topic`), la partition (`kafka.partition`) et l'offset (`kafka.offset`).
-   **Sécurité** : Le header `Authorization` est automatiquement masqué dans les logs (ex: `Bearer eyJhbGci...`).

---

## 2. Tracing Distribué (OpenTelemetry)

L'application utilise **Micrometer Tracing** avec un pont **OpenTelemetry** pour générer des traces distribuées.

### Fonctionnement
-   **Génération d'ID** : Un `traceId` unique est généré pour chaque requête entrante (Web) ou message Kafka.
-   **Propagation** : L'ID de trace est propagé à travers les threads et les services (via les headers Kafka si l'Observation est activée).
-   **MDC Integration** : Le `traceId` et le `spanId` sont automatiquement injectés dans le Mapped Diagnostic Context (MDC) pour lier les logs JSON à la trace correspondante.

---

## 3. Pile Infrastructure OpenTelemetry

Le fichier `docker-compose.yml` inclut une stack complète pour collecter et visualiser ces données :

### Composants
-   **OTel Collector** : Reçoit les traces et métriques via le protocole OTLP (port 4318/HTTP). Il les redirige vers Jaeger et Prometheus.
-   **Jaeger** : Interface de visualisation des traces (disponible sur le port `16686`). Permet d'analyser la latence et les dépendances.
-   **Prometheus** : Collecte les métriques applicatives agrégées par le collecteur (exposées sur le port `8889`).
-   **Grafana** : Tableaux de bord de visualisation (port `3000`).

---

## 4. Configuration

### Propriétés Spring Boot (`application.yml`)
```yaml
management:
  tracing:
    sampling:
      probability: 1.0 # 100% des traces sont capturées en mode démo
  otlp:
    tracing:
      endpoint: http://otel-collector:4318/v1/traces
    metrics:
      export:
        url: http://otel-collector:4318/v1/metrics
```

### Variables d'Environnement Docker
-   `MANAGEMENT_OTLP_TRACING_ENDPOINT` : URL du collecteur pour les traces.
-   `MANAGEMENT_OTLP_METRICS_EXPORT_URL` : URL du collecteur pour les métriques.
