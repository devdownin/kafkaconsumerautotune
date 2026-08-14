# Modèles C4 - KafkaConsumerAutoTune

Ce document présente l'architecture de l'application KafkaConsumerAutoTune en suivant le modèle C4 (Context, Container, Component) au format **PlantUML**.

Une version au format **Mermaid** est également disponible : [c4-mermaid.md](c4-mermaid.md).

## 1. Niveau 1 : Diagramme de Contexte (System Context)

Ce diagramme montre le système KafkaConsumerAutoTune dans son environnement global.

```puml
@startuml C4_Elements
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Context.puml

LAYOUT_WITH_LEGEND()

Person(user, "Utilisateur / Opérateur", "Surveille les performances et gère les erreurs via le dashboard.")
System(kafkaconsumerautotune, "KafkaConsumerAutoTune", "Application de consommation Kafka haute performance avec auto-tuning intelligent et résilience avancée.")

System_Ext(kafka, "Kafka", "Flux de messages d'entrée (Message Broker).")
System_Ext(database, "Base de données", "Stockage persistant des événements et erreurs DLT (Oracle/H2).")
System_Ext(otel_stack, "Observability Stack", "Pile complète (Loki, Prometheus, Jaeger) pour la supervision, le log centralisé et le tracing.")

Rel(user, kafkaconsumerautotune, "Surveille et gère", "HTTPS/WebSocket")
Rel(kafkaconsumerautotune, kafka, "Consomme des messages", "Kafka Protocol")
Rel(kafkaconsumerautotune, database, "Persiste les données", "JDBC")
Rel(kafkaconsumerautotune, otel_stack, "Exporte logs, métriques et traces", "OTLP / File-Scraping")
@enduml
```

---

## 2. Niveau 2 : Diagramme de Conteneur (Container)

Ce diagramme décompose le système et détaille la pile d'observabilité intégrée.

```puml
@startuml C4_Containers
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Container.puml

LAYOUT_WITH_LEGEND()

Person(user, "Utilisateur / Opérateur", "Surveille les performances et gère les erreurs.")

System_Boundary(kafkaconsumerautotune_boundary, "Système KafkaConsumerAutoTune") {
    Container(ui, "Interface Web", "Thymeleaf, Tailwind CSS, ApexCharts", "Fournit le dashboard et les outils de gestion DLT.")
    Container(app, "Application Spring Boot", "Java 21, Spring Boot 3.5", "Gère la logique de consommation, l'auto-tuning (PID+EMA), la résilience et l'export télémétrique.")
}

System_Boundary(observability_boundary, "Pile d'Observabilité") {
    Container(collector, "OTel Collector", "OTLP", "Collecte et redirige les traces et métriques.")
    Container(prometheus, "Prometheus", "Metrics Store", "Stockage des métriques avec support des Exemplars.")
    Container(loki, "Grafana Loki", "Log Store", "Centralisation et indexation des logs JSON.")
    Container(promtail, "Promtail", "Log Agent", "Scrappe les fichiers de logs de l'application.")
    Container(jaeger, "Jaeger", "Tracing UI", "Visualisation des traces distribuées.")
}

ContainerDb_Ext(database, "Base de données", "Relational (Oracle/H2)", "Stocke les événements persistés et les messages DLT.")
Container_Ext(kafka, "Kafka Broker", "Message Broker", "Source des flux d'événements.")

Rel(user, ui, "Interagit avec", "HTTPS")
Rel(user, observability_boundary, "Consulte les dashboards et logs", "HTTPS (Grafana)")
Rel(ui, app, "Appels API & Flux temps réel", "HTTPS/WebSocket")
Rel(app, database, "Lit/Écrit dans", "JDBC")
Rel(app, kafka, "Consomme de", "Kafka Protocol")

Rel(app, collector, "Exporte traces & métriques", "OTLP/HTTP")
Rel(app, promtail, "Écrit les logs JSON", "File system")
Rel(promtail, loki, "Pousse les logs", "HTTP")
Rel(collector, prometheus, "Pousse les métriques", "Prometheus Remote Write")
Rel(collector, jaeger, "Pousse les traces", "OTLP/gRPC")
@enduml
```

---

## 3. Niveau 3 : Diagramme de Composant (Component)

Ce diagramme détaille les composants internes de l'application Spring Boot.

```puml
@startuml C4_Components
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Component.puml

LAYOUT_WITH_LEGEND()

Container_Boundary(app, "Application Spring Boot") {
    Component(controllers, "REST Controllers", "Spring MVC", "Gère les requêtes du dashboard et les actions DLT (Retry/Discard).")
    Component(batchConsumer, "EventBatchConsumer", "Spring Kafka", "Implémentation concrète de AbstractBatchConsumer.")
    Component(abstractConsumer, "AbstractBatchConsumer", "Base Class", "Gère le cycle de vie du batch, les métriques, le MDC et le fallback de persistance.")
    Component(tuningService, "KafkaTuningService", "PID Controller + EMA", "Ajuste dynamiquement le polling avec lissage EMA et throttling de santé.")
    Component(persistenceService, "EventPersistenceService", "Spring Data JPA", "Gère la persistance sécurisée par Circuit Breaker et Retry.")
    Component(cbStateListener, "CircuitBreakerStateListener", "Resilience4j Listener", "Pilote le cycle de vie du consommateur Kafka selon l'état du CB.")
    Component(dltService, "DltService", "Service", "Gère le routage vers la DLT (Kafka + Base).")
    Component(dashboardService, "DashboardService", "Service", "Agrège les métriques JVM, Kafka et Application.")
    Component(wsService, "WebSocketService", "Spring WebSocket", "Diffuse les mises à jour temps réel.")
    Component(mdcFilter, "LoggingMdcFilter", "Servlet Filter", "Prépare le contexte MDC (correlationId, auth) pour les logs.")
}

ContainerDb_Ext(database, "Base de données", "JDBC", "Stockage.")
Container_Ext(kafka, "Kafka Broker", "Kafka Protocol", "Source de données.")

Rel(batchConsumer, abstractConsumer, "Hérite de")
Rel(abstractConsumer, persistenceService, "Appelle pour persistance batch/individuelle")
Rel(abstractConsumer, dltService, "Route les messages en erreur")
Rel(persistenceService, database, "JDBC")

Rel(tuningService, batchConsumer, "Monitore et ajuste les paramètres de polling")
Rel(cbStateListener, batchConsumer, "Arrête/Démarre le container")
Rel(persistenceService, cbStateListener, "Déclenche des changements d'état")

Rel(mdcFilter, controllers, "Injecte les métadonnées de log")
Rel(controllers, dashboardService, "Récupère les données de monitoring")
Rel(dashboardService, wsService, "Envoie les mises à jour")
Rel(abstractConsumer, wsService, "Notifie les nouveaux événements")

Rel(batchConsumer, kafka, "Consomme", "Kafka Protocol")
@enduml
```
