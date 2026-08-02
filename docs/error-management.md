# Gestion des Erreurs et Résilience - Consotopic

Ce document détaille la stratégie de gestion des erreurs et les mécanismes de résilience mis en œuvre dans l'application Consotopic.

## 1. Philosophie de Gestion des Erreurs

L'application adopte une approche de "fail-safe" et de "graceful degradation" :
- **Isolation** : Un message corrompu ne doit pas bloquer l'ensemble du flux.
- **Transparence** : Chaque erreur est tracée, mesurée et notifiée en temps réel.
- **Récupération** : Les erreurs transitoires sont gérées par des retries, tandis que les erreurs permanentes sont isolées.

---

## 2. Classification et Traitement des Erreurs

### 2.1 Erreurs Transitoires (`TransientException`)
Ce sont des erreurs temporaires (ex: Timeout DB, indisponibilité réseau).
-   **Traitement** : L'exception est levée jusqu'au container Kafka, déclenchant un re-jeu du batch.
-   **Cadence** : le `DefaultErrorHandler` du listener rejoue le batch `spring.kafka.listener.retry.max-attempts` fois (défaut 3) avec un délai de `retry.back-off-ms` (défaut 2 s) entre chaque tentative. Ce délai est délibéré : rejouer immédiatement ne laisse aucune chance à une base en difficulté de se rétablir.

### 2.2 Erreurs Permanentes (`PermanentException`)
Ce sont des erreurs liées au contenu (ex: JSON invalide, violation de contrainte). Le re-traitement échouera systématiquement.
-   **Traitement** : Le message est immédiatement routé vers la **Dead Letter Topic (DLT)** pour libérer le reste du batch.
-   **Classification** : `PermanentException` et `JsonProcessingException` sont déclarées non rejouables auprès du `DefaultErrorHandler`. Le consumer les ayant déjà routées en DLT, un re-jeu ne ferait que rejouer un message qui ne peut pas aboutir.

---

## 3. Stratégie de Résilience Multi-niveaux

### 3.1 Persistance avec Repli (Fallback)
Implémenté dans `AbstractBatchConsumer` :
1.  **Tentative Batch** : On essaie de persister tous les messages valides d'un coup (performance).
2.  **Bascule Individuelle** : Si la persistance batch échoue, on tente chaque message un par un. Les messages sains sont sauvés, les messages "poison" sont envoyés en DLT. Cela évite de bloquer la consommation à cause d'un seul mauvais message.

### 3.2 Lissage de l'Auto-Tune (EMA)
Pour éviter que le moteur d'Auto-Tune ne redémarre le consommateur trop souvent à cause de pics de latence isolés :
-   Un **Filtre Passe-Bas (Exponential Moving Average)** est appliqué sur la durée des batchs.
-   Le contrôleur PID réagit à cette valeur lissée, garantissant une stabilité du système même en cas de charge irrégulière.

### 3.3 Protection Infrastructurelle (Circuit Breaker)
Pour protéger la base de données :
-   **Circuit Breaker** (Resilience4j) sur le service de persistance.
-   **Auto-Pause** : Si le circuit s'ouvre, le consommateur Kafka est arrêté (`container.stop()`).
-   **Auto-Reprise** : Redémarrage automatique dès que le circuit repasse en `HALF_OPEN` ou `CLOSED`.
-   **Exécution asynchrone** : l'arrêt et le redémarrage sont dispatchés sur un thread dédié. Resilience4j publie les transitions d'état de façon synchrone sur le thread qui les a provoquées — pour ce circuit, le thread listener Kafka lui-même — et `container.stop()` y attendrait le thread en train d'exécuter l'appel.

> **Note sur la reprise** : le circuit ne se referme qu'après `permittedNumberOfCallsInHalfOpenState` appels réussis (défaut 3). Un appel correspond à **un batch**, pas à un message : publier trois messages ne garantit pas trois appels, le consumer restant libre de tous les retourner dans un même poll.

### 3.4 Throttling d'Urgence
Si le système détecte une saturation critique (CPU > 90% ou Mémoire > 90%), le moteur d'Auto-Tune réduit le `max.poll.records` de 30% pour soulager l'infrastructure avant même que des erreurs ne surviennent.

> **Limite connue** : cette réduction reste soumise au cooldown de redémarrage (`min-restart-interval-ms`, défaut 5 minutes). Sous saturation prolongée, la protection peut donc être différée d'autant.

---

## 4. Cycle de Vie de la DLT

1.  **Enregistrement** : Les erreurs sont stockées dans la table `DLT_EVENTS` et publiées sur le topic Kafka défini par `kafka.topic.dlt`.
2.  **Gestion Opérateur** : Via l'interface utilisateur (`/dlt-management`), il est possible de :
    -   **Retry** : Re-jouer le message.
    -   **Discard** : Abandonner le message.
    -   **Edit & Retry** : Corriger le payload (ex: fixer un JSON) puis re-jouer.
    -   Ces actions existent aussi en masse (`retry-all`, `discard-all`, sélection multiple).

### 4.1 Garanties du re-jeu

-   **Clé préservée** : la clé Kafka d'origine est stockée (`ORIGINAL_KEY`) et restituée au re-jeu. Sans elle, le message repartirait sur une autre partition et l'ordre par clé serait rompu.
-   **Accusé de réception attendu** : l'événement n'est marqué `RESOLVED` qu'après acquittement du broker. Le marquer sur un envoi asynchrone non confirmé perdrait le message si l'envoi échouait ensuite.
-   **En-têtes restitués** : les en-têtes d'origine sont sérialisés en JSON à la mise en DLT et réappliqués au re-jeu.
