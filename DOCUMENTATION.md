# Documentation Technique - Kafka Consumer Auto-tune

## 1. Résumé Exécutif
Consotopic est une application Spring Boot de haute performance conçue pour consommer des messages Kafka en mode batch, les traiter, et les persister. L'application se distingue par son moteur d'**auto-tuning** intelligent basé sur un contrôleur PID qui ajuste dynamiquement les paramètres pour optimiser le débit. Elle intègre une gestion avancée des erreurs (DLT, Fallback), une protection par **Circuit Breaker**, et une pile d'observabilité complète (Loki, Prometheus, Jaeger, Grafana).

**Documents complémentaires :**
- [Gestion des Erreurs et Résilience](docs/error-management.md)
- [Observabilité (Logging, Tracing, Métriques)](docs/observability.md)

---

## 2. Architecture Globale

L'application suit une architecture modulaire. Les données circulent comme suit :

```mermaid
graph TD
    K[Kafka Topic] -->|1. Batch de Records| C[AbstractBatchConsumer]
    C -->|2. Transformation| P[EventProcessingService]
    P -->|3. Validation| C
    C -->|4. Persistance Batch| S[EventPersistenceService]
    S -->|5. Circuit Breaker| DB[(Base de données)]

    subgraph "Boucle d'Auto-Tuning"
        M[Micrometer Metrics] --- T[KafkaTuningService]
        T -->|Ajustement| C
    end

    subgraph "Gestion des Erreurs"
        C -->|Fallback / DLT| DLT[DltService]
        DLT -->|Stockage| DB
    end
```

---

## 3. Composants Clés en Détail

### 3.1 Consommation Kafka (AbstractBatchConsumer)
Gère le cycle de vie du traitement batch avec une généricité totale (`<T>`).
-   **Standardisation** : Routage DLT, métriques, et logs structurés automatiques.
-   **Classification d'Erreurs** : Distingue les erreurs permanentes (envoi direct en DLT) des erreurs transitoires (déclenchement du retry Kafka).

### 3.2 Moteur d'Auto-Tune (KafkaTuningService)
Surveille le débit et ajuste les paramètres pour atteindre une cible de **1,2 seconde** par batch.
-   **Contrôleur PID** : Ajuste `max.poll.records`, `fetch.*` et la `concurrency`.
-   **Lissage (EMA)** : Utilise une moyenne mobile exponentielle pour ignorer les pics de latence isolés et stabiliser les redémarrages.
-   **Throttling Santé** : Réduit préventivement la charge si le CPU ou la Mémoire dépassent 90% d'utilisation.

### 3.3 Résilience et Circuit Breaker
Protège la persistance via Resilience4j.
-   **Circuit Breaker** : Bascule en `OPEN` en cas de défaillance DB.
-   **Auto-Pause/Reprise** : Le consommateur Kafka s'arrête et redémarre automatiquement selon l'état du circuit.
-   **Fallback Chirurgical** : En cas d'échec de batch, l'application tente chaque message individuellement pour isoler les "poison messages" vers la DLT tout en sauvant le reste du lot.

---

## 4. Observabilité et Supervision

### 4.1 Pile de Centralisation
-   **Logs** : Centralisés dans **Loki** via Promtail. Format JSON ELK/ECS nativement supporté.
-   **Traces** : Exportées vers **Jaeger** via OTLP.
-   **Corrélation** : Support des **Exemplars** Prometheus permettant de passer d'un graphique de métriques à une trace Jaeger d'un seul clic.

### 4.2 Monitoring et Alerting
-   **Tableaux de bord** : Dashboard Technique et Dashboard Business KPI provisionnés dans Grafana.
-   **Alertes** : Définies dans `prometheus-rules.yml` sur le lag Kafka, le taux d'erreur et la performance des batchs.

---

## 5. Spécifications Techniques
-   **Langage** : Java 21 (Utilisation des `record`).
-   **Framework** : Spring Boot 3.5.9.
-   **Base de données** : Oracle (ou H2 en profil dev).
-   **Infrastructure** : Docker Compose prêt pour la production (Prometheus, Loki, Jaeger, Grafana).
