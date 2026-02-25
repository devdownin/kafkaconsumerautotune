# Documentation Technique - Kafka Consumer Auto-tune

## 1. Résumé Exécutif
Consotopic est une application Spring Boot de haute performance conçue pour consommer des messages Kafka en mode batch, les traiter, et les persister dans une base de données (Oracle/H2). L'application se distingue par son moteur d'**auto-tuning** intelligent basé sur un contrôleur PID qui ajuste dynamiquement les paramètres du consommateur Kafka pour optimiser le débit et la latence en temps réel. Elle intègre également un système robuste de gestion des erreurs via une Dead Letter Topic (DLT), un mécanisme de résilience par repli (fallback), une protection par **Circuit Breaker**, et un tableau de bord complet de monitoring.

**Documents complémentaires :**
- [Modèles C4 (Architecture - PlantUML)](docs/c4-models.md)
- [Modèles C4 (Architecture - Mermaid)](docs/c4-mermaid.md)
- [Gestion des Erreurs et Résilience](docs/error-management.md)
- [Observabilité (Logging & Tracing)](docs/observability.md)

---

## 2. Architecture Globale
L'application suit une architecture orientée services et modulaire avec les couches suivantes :
- **Couche de Consommation Générique** : Utilisation d'une classe abstraite `AbstractBatchConsumer` pour standardiser le traitement des flux Kafka.
- **Couche de Traitement (Moteur d'Auto-tune)** : Analyseur de performance basé sur un contrôleur PID qui réajuste les paramètres Kafka.
- **Couche de Persistance Résiliente** : Utilisation de Spring Data JPA avec protection par Circuit Breaker (Resilience4j) et support de batches JDBC.
- **Couche de Gestion DLT** : Système de récupération, stockage en base (DltEvent) et re-traitement des messages en échec.
- **Couche d'Observabilité** : Logging JSON ELK, Tracing distribué via OpenTelemetry, et Dashboard temps réel (WebSocket).

---

## 3. Composants Clés

### 3.1 Consommation Kafka (Mode Batch et Généricité)
L'architecture de consommation repose sur `AbstractBatchConsumer<T>`, une classe de base générique qui implémente le cycle de vie du traitement batch.
- **Standardisation** : Gère uniformément les métriques, le routage DLT, et la logique de persistance avec fallback.
- **Mode Batch** : Activé via `factory.setBatchListener(true)`. Permet de traiter une liste de `ConsumerRecord` en une seule transaction logique.
- **Acquittement Manuel** : L'offset n'est commité qu'une fois le traitement et la persistance du batch terminés avec succès (`acknowledgment.acknowledge()`).
- **Isolation** : Configuré en `read_committed` pour garantir la lecture de messages stables.

### 3.2 Moteur d'Auto-Tune (KafkaTuningService)
C'est le composant le plus innovant de l'application. Il surveille le débit (`msg/s`) et la durée moyenne de traitement d'un batch.
- **Contrôleur PID** : Utilise les coefficients Proportional (KP=150), Integral (KI=20) et Derivative (KD=50).
- **Cible (Setpoint)** : Vise une durée de traitement de batch de **1,2 seconde**.
- **Paramètres Ajustés** :
    - `max.poll.records` : Augmenté si le traitement est trop rapide, diminué s'il dépasse la cible.
    - `fetch.max.wait.ms` : Ajusté selon le débit détecté.
    - `concurrency` : Aligné automatiquement sur le nombre de partitions du topic Kafka.
- **Sécurité** :
    - Seuil de changement minimal de 10% pour éviter les micro-ajustements.
    - Temps de pause (cooldown) de 5 minutes entre deux redémarrages de consommateur pour éviter les tempêtes de rebalance.

### 3.3 Résilience et Gestion des Erreurs (Circuit Breaker & Fallback)
L'application implémente une stratégie de résilience à plusieurs niveaux pour garantir la continuité du service.

#### 3.3.1 Protection de la Persistance (Circuit Breaker)
Le service `EventPersistenceService` est protégé par un **Circuit Breaker** Resilience4j (nommé `persistence`).
- **Retry** : En cas d'erreur transitoire, une politique de retry est appliquée avant l'ouverture du circuit.
- **Gestion d'État** : Si la base de données devient indisponible, le circuit passe à l'état `OPEN`.
- **Pilotage du Consommateur** : Le `CircuitBreakerStateListener` écoute les changements d'état. Si le circuit est `OPEN`, il arrête automatiquement le container Kafka (`KafkaListenerEndpointRegistry`) pour éviter d'accumuler des erreurs. Le consommateur est redémarré dès que le circuit repasse en `HALF_OPEN` ou `CLOSED`.

#### 3.3.2 Mécanisme de Repli (Fallback)
Si la persistence d'un batch complet échoue (ex: erreur de contrainte sur un seul message), l'application bascule automatiquement en mode individuel :
1. Chaque message du batch est tenté séparément.
2. Les messages qui réussissent sont persistés.
3. Les messages provoquant toujours une erreur (ex: "Poison Message") sont dirigés vers la DLT.
4. Le reste du batch est ainsi validé, permettant au consommateur de progresser sans blocage.

#### 3.3.3 Dead Letter Topic (DLT)
- **Kafka DLT Topic** : Pour une traçabilité technique avec headers (`DLT_EXCEPTION_MESSAGE`, etc.).
- **Base de données (DltEvent)** : Pour une gestion via l'interface utilisateur.
- **Actions possibles** : Retry (re-jeu), Discard (abandon), Modification du payload avant retry.

---

## 4. Configuration

### 4.1 Variables d'Environnement Principales
| Variable | Description | Défaut |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Liste des brokers Kafka | `kafkadev:9092` |
| `DB_HOST` | Host de la base Oracle | `localhost` |
| `DB_USER` / `DB_PASSWORD` | Identifiants DB | - |
| `KAFKA_TOPIC_NAME` | Topic source | `asf.peage.backoffice.sortie.recouvrable` |
| `MANAGEMENT_OTLP_TRACING_ENDPOINT` | Endpoint OTLP pour les traces | `http://otel-collector:4318/v1/traces` |
| `MANAGEMENT_OTLP_METRICS_EXPORT_URL` | Endpoint OTLP pour les métriques | `http://otel-collector:4318/v1/metrics` |

### 4.2 Paramètres de Seuil (Circuit Breaker)
Configurables dans `application.yml` :
- `failureRateThreshold`: 50%
- `waitDurationInOpenState`: 10000ms
- `permittedNumberOfCallsInHalfOpenState`: 3

---

## 5. Guide Opérationnel

### 5.1 Monitoring en Temps Réel
Accédez au dashboard via `/dashboard`. Il affiche :
- Les courbes de débit (Throughput).
- Le lag Kafka en temps réel.
- L'état de santé du système et les paramètres actuels d'auto-tune.
- Les notifications système via des "Toasts" (ex: changement d'état du Circuit Breaker).

L'onglet **Kafka Optimizer** (`/optimizer`) permet de suivre l'historique des changements appliqués par le moteur d'auto-tune et affiche en temps réel les valeurs actuelles de tous les paramètres optimisables (`max.poll.records`, `fetch.min.bytes`, `fetch.max.wait.ms`, `fetch.max.bytes`, `max.poll.interval.ms` et `concurrency`).

---

## 6. Spécifications Techniques et Qualité
- **Java** : 21 (Utilisation des `record` pour les DTOs).
- **Spring Boot** : 3.5.9
- **Resilience** : Resilience4j (Circuit Breaker, Retry).
- **Tests** : Suite de tests d'intégration complète avec `@EmbeddedKafka` et tests de Circuit Breaker.
- **Monitoring** : Micrometer + Prometheus + WebSocket (STOMP).
- **UI** : Thymeleaf + Tailwind CSS + Prism.js (Syntax highlighting).
