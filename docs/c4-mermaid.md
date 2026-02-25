# Modèles C4 (Mermaid) - Consotopic

Ce document présente l'architecture de l'application Consotopic en suivant le modèle C4 (Context, Container, Component) au format Mermaid.

## 1. Niveau 1 : Diagramme de Contexte (System Context)

Ce diagramme montre le système Consotopic dans son environnement global, ses interactions avec les utilisateurs et les systèmes externes.

```mermaid
C4Context
    title Diagramme de Contexte pour le système Consotopic

    Person(user, "Utilisateur / Opérateur", "Surveille les performances et gère les erreurs via le dashboard.")
    System(consotopic, "Consotopic", "Application de consommation Kafka haute performance avec auto-tuning et résilience.")

    System_Ext(kafka, "Kafka", "Flux de messages d'entrée (Message Broker).")
    System_Ext(database, "Base de données", "Stockage persistant des événements et erreurs DLT (Oracle/H2).")
    System_Ext(otel, "OpenTelemetry Stack", "Collecte et visualise les traces et métriques (Collector, Jaeger, Prometheus).")

    Rel(user, consotopic, "Surveille et gère", "HTTPS/WebSocket")
    Rel(consotopic, kafka, "Consomme des messages", "Kafka Protocol")
    Rel(consotopic, database, "Persiste les données", "JDBC")
    Rel(consotopic, otel, "Exporte traces et métriques", "OTLP")
```

---

## 2. Niveau 2 : Diagramme de Conteneur (Container)

Ce diagramme décompose le système Consotopic en conteneurs logiques et inclut la pile d'observabilité.

```mermaid
C4Container
    title Diagramme de Conteneur pour le système Consotopic

    Person(user, "Utilisateur / Opérateur", "Surveille les performances et gère les erreurs.")

    Container_Boundary(consotopic_boundary, "Système Consotopic") {
        Container(ui, "Interface Web", "Thymeleaf, Tailwind CSS", "Fournit le dashboard et les outils de gestion DLT.")
        Container(app, "Application Spring Boot", "Java 21, Spring Boot 3.5", "Gère la logique de consommation, l'auto-tuning, la résilience et l'export OTel.")
    }

    Container_Boundary(otel_boundary, "Pile d'Observabilité") {
        Container(collector, "OTel Collector", "Go", "Collecte et redirige les données OTLP.")
        Container(jaeger, "Jaeger", "Distributed Tracing", "Visualisation des traces.")
        Container(prometheus, "Prometheus", "Metrics Store", "Stockage et requêtage des métriques.")
    }

    ContainerDb_Ext(database, "Base de données", "Relational (Oracle/H2)", "Stocke les événements et messages DLT.")
    Container_Ext(kafka, "Kafka Broker", "Message Broker", "Source des flux d'événements.")

    Rel(user, ui, "Interagit avec", "HTTPS")
    Rel(user, jaeger, "Consulte les traces", "HTTPS")
    Rel(ui, app, "Appels API & WebSocket", "HTTPS/WebSocket")
    Rel(app, database, "Lit/Écrit dans", "JDBC")
    Rel(app, kafka, "Consomme de", "Kafka Protocol")
    Rel(app, collector, "Exporte", "OTLP/HTTP")
    Rel(collector, jaeger, "Envoie les traces", "OTLP/gRPC")
    Rel(collector, prometheus, "Expose les métriques", "Prometheus Exporter")
```

---

## 3. Niveau 3 : Diagramme de Composant (Component)

Ce diagramme détaille les composants internes de l'application Spring Boot.

```mermaid
C4Component
    title Diagramme de Composant pour l'Application Spring Boot

    Container_Boundary(app, "Application Spring Boot") {
        Component(controllers, "REST Controllers", "Spring MVC", "Gère les requêtes du dashboard et les actions DLT.")
        Component(batchConsumer, "EventBatchConsumer", "Spring Kafka", "Consommation batch d'événements.")
        Component(abstractConsumer, "AbstractBatchConsumer", "Base Class", "Gère le cycle de vie, les métriques et l'enrichissement MDC.")
        Component(tuningService, "KafkaTuningService", "PID Controller", "Auto-tune des paramètres Kafka.")
        Component(persistenceService, "EventPersistenceService", "Spring Data JPA", "Persistance résiliente.")
        Component(cbStateListener, "CircuitBreakerStateListener", "Resilience4j", "Gestion du cycle de vie du consommateur.")
        Component(dltService, "DltService", "Service", "Gestion du routage DLT.")
        Component(dashboardService, "DashboardService", "Service", "Agrégation des métriques.")
        Component(wsService, "WebSocketService", "Spring WebSocket", "Diffusion temps réel.")
        Component(mdcFilter, "LoggingMdcFilter", "Servlet Filter", "Enrichissement des logs HTTP et corrélation.")
    }

    ContainerDb_Ext(database, "Base de données", "JDBC", "Stockage.")
    Container_Ext(kafka, "Kafka Broker", "Kafka Protocol", "Source de données.")

    Rel(batchConsumer, abstractConsumer, "Hérite de")
    Rel(abstractConsumer, persistenceService, "Appelle pour persistance")
    Rel(abstractConsumer, dltService, "Route les erreurs")
    Rel(tuningService, batchConsumer, "Ajuste le polling")
    Rel(cbStateListener, batchConsumer, "Contrôle le container")
    Rel(mdcFilter, controllers, "Prépare le contexte")
    Rel(controllers, dashboardService, "Consulte les métriques")
    Rel(dashboardService, wsService, "Diffuse les données")
    Rel(persistenceService, database, "JDBC")
    Rel(batchConsumer, kafka, "Kafka Protocol")
```
