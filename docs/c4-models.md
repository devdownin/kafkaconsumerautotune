# Modèles C4 - Consotopic

Ce document présente l'architecture de l'application Consotopic en suivant le modèle C4 (Context, Container, Component).

## 1. Niveau 1 : Diagramme de Contexte (System Context)

Ce diagramme montre le système Consotopic dans son environnement global, ses interactions avec les utilisateurs et les systèmes externes.

```puml
@startuml C4_Elements
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Context.puml

LAYOUT_WITH_LEGEND()

Person(user, "Utilisateur / Opérateur", "Surveille les performances et gère les erreurs via le dashboard.")
System(consotopic, "Consotopic", "Application de consommation Kafka haute performance avec auto-tuning et résilience.")

System_Ext(kafka, "Kafka", "Flux de messages d'entrée (Message Broker).")
System_Ext(database, "Base de données", "Stockage persistant des événements et erreurs DLT (Oracle/H2).")

Rel(user, consotopic, "Surveille et gère", "HTTPS/WebSocket")
Rel(consotopic, kafka, "Consomme des messages", "Kafka Protocol")
Rel(consotopic, database, "Persiste les données", "JDBC")
@enduml
```

---

## 2. Niveau 2 : Diagramme de Conteneur (Container)

Ce diagramme décompose le système Consotopic en conteneurs logiques (applications, bases de données, etc.).

```puml
@startuml C4_Containers
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Container.puml

LAYOUT_WITH_LEGEND()

Person(user, "Utilisateur / Opérateur", "Surveille les performances et gère les erreurs.")

System_Boundary(consotopic_boundary, "Système Consotopic") {
    Container(ui, "Interface Web", "Thymeleaf, Tailwind CSS, ApexCharts", "Fournit le dashboard et les outils de gestion DLT.")
    Container(app, "Application Spring Boot", "Java 21, Spring Boot 3.5", "Gère la logique de consommation, l'auto-tuning, la résilience et la persistance.")
}

ContainerDb_Ext(database, "Base de données", "Relational (Oracle/H2)", "Stocke les événements persistés et les messages en erreur (DLT).")
Container_Ext(kafka, "Kafka Broker", "Message Broker", "Source des flux d'événements à haute fréquence.")

Rel(user, ui, "Interagit avec", "HTTPS")
Rel(ui, app, "Appels API & Flux temps réel", "HTTPS/WebSocket")
Rel(app, database, "Lit/Écrit dans", "JDBC")
Rel(app, kafka, "Consomme de", "Kafka Protocol")
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
    Component(batchConsumer, "EventBatchConsumer", "Spring Kafka", "Implémentation concrète de AbstractBatchConsumer pour les événements.")
    Component(abstractConsumer, "AbstractBatchConsumer", "Base Class", "Gère le cycle de vie du batch, les métriques et le fallback de persistance.")
    Component(tuningService, "KafkaTuningService", "PID Controller", "Ajuste dynamiquement max.poll.records et d'autres paramètres.")
    Component(persistenceService, "EventPersistenceService", "Spring Data JPA", "Gère la persistance sécurisée par Circuit Breaker et Retry.")
    Component(cbStateListener, "CircuitBreakerStateListener", "Resilience4j Listener", "Pilote le cycle de vie du consommateur Kafka selon l'état du Circuit Breaker.")
    Component(dltService, "DltService", "Service", "Gère le routage vers la Dead Letter Topic et la base DltEvent.")
    Component(dashboardService, "DashboardService", "Service", "Agrège les métriques de la JVM, Kafka et de l'application.")
    Component(wsService, "WebSocketService", "Spring WebSocket", "Diffuse les mises à jour temps réel (métriques, événements, état CB).")
    Component(repos, "JPA Repositories", "Spring Data", "Couche d'accès aux données.")
}

ContainerDb_Ext(database, "Base de données", "JDBC", "Stockage.")
Container_Ext(kafka, "Kafka Broker", "Kafka Protocol", "Source de données.")

Rel(batchConsumer, abstractConsumer, "Hérite de")
Rel(abstractConsumer, persistenceService, "Appelle pour persistance batch/individuelle")
Rel(abstractConsumer, dltService, "Route les messages en erreur")
Rel(persistenceService, repos, "Utilise pour accès DB")
Rel(repos, database, "JDBC")

Rel(tuningService, batchConsumer, "Monitore et ajuste les paramètres de polling")
Rel(cbStateListener, batchConsumer, "Arrête/Démarre le container (via KafkaListenerEndpointRegistry)")
Rel(persistenceService, cbStateListener, "Déclenche des changements d'état (Circuit OPEN/CLOSED)")

Rel(controllers, dashboardService, "Récupère les données de monitoring")
Rel(dashboardService, repos, "Agrège les données métier")
Rel(dashboardService, wsService, "Envoie les mises à jour")
Rel(abstractConsumer, wsService, "Notifie les nouveaux événements")
Rel(cbStateListener, wsService, "Notifie les changements d'état système")

Rel(batchConsumer, kafka, "Consomme", "Kafka Protocol")
@enduml
```
