# Gestion des Erreurs et Résilience - Consotopic

Ce document détaille la stratégie de gestion des erreurs et les mécanismes de résilience mis en œuvre dans l'application Consotopic pour garantir un traitement robuste des messages Kafka.

## 1. Philosophie de Gestion des Erreurs

L'application adopte une approche de "fail-safe" et de "graceful degradation" :
- **Isolation** : Un message corrompu (Poison Message) ne doit pas bloquer l'ensemble du flux.
- **Transparence** : Chaque erreur est tracée, mesurée par des métriques et notifiée en temps réel.
- **Récupération** : Les erreurs transitoires sont gérées par des retries automatiques, tandis que les erreurs permanentes sont isolées pour une intervention manuelle.

---

## 2. Classification des Erreurs

### 2.1 Erreurs Transitoires
Ce sont des erreurs temporaires qui peuvent être résolues par une simple répétition de l'action.
- **Exemples** : Indisponibilité temporaire de la base de données, timeout réseau, verrouillage de ligne (locking).
- **Traitement** : Utilisation de `@Retry` (Resilience4j) et mécanisme de retry au niveau de Kafka.

### 2.2 Erreurs Permanentes (Poison Messages)
Ce sont des erreurs liées au contenu du message lui-même. Le re-traitement automatique échouera systématiquement.
- **Exemples** : JSON invalide, absence d'identifiant obligatoire, violation de contrainte d'intégrité (unique key).
- **Traitement** : Routage immédiat vers la **Dead Letter Topic (DLT)**.

---

## 3. Stratégie de Résilience Multi-niveaux

### 3.1 Niveau 1 : Filtrage au Traitement (Mapping)
Lors de la réception d'un batch, chaque message est transformé par le `EventProcessingService`.
- Si l'extraction de l'ID via JsonPath échoue ou si le format est invalide, le message est marqué pour la DLT.
- Le reste du batch continue son processus.

### 3.2 Niveau 2 : Persistance avec Repli (Fallback)
C'est le mécanisme le plus critique, implémenté dans `AbstractBatchConsumer`.
1. **Tentative Batch** : On essaie de persister l'ensemble des messages valides en une seule opération JDBC.
2. **Détection d'Échec** : Si la persistance du batch échoue (ex: une `DataIntegrityViolationException`), l'application passe en **mode individuel**.
3. **Traitement Individuel** : Chaque message du batch est tenté séparément.
   - Si un message réussit, il est validé.
   - Si un message échoue (le "poison"), il est envoyé à la DLT avec le détail de l'erreur.
4. **Progression** : Le batch Kafka est acquitté (`ack`), permettant d'avancer l'offset tout en ayant isolé les erreurs.

### 3.3 Niveau 3 : Protection Infrastructurelle (Circuit Breaker)
Pour protéger la base de données Oracle contre la surcharge en cas d'instabilité :
- **Circuit Breaker** : Appliqué sur `EventPersistenceService`.
- **État OPEN** : Si le taux d'échec dépasse le seuil (50%), le circuit s'ouvre.
- **Auto-Pause** : Le `CircuitBreakerStateListener` détecte l'ouverture et arrête immédiatement le consommateur Kafka (`container.stop()`). Cela évite de consommer des messages qui ne pourront pas être persistés.
- **Auto-Reprise** : Dès que le circuit repasse en `HALF_OPEN` ou `CLOSED` (après un délai de repos), le consommateur est redémarré automatiquement.

---

## 4. Cycle de Vie de la DLT

Lorsqu'un message est envoyé en DLT :
1. **Kafka DLT** : Le message est publié sur un topic dédié (`.dlt`) avec des headers techniques (`DLT_EXCEPTION_MESSAGE`, `DLT_ORIGINAL_OFFSET`).
2. **Persistance DltEvent** : Une copie est stockée dans la table `DLT_EVENT` pour permettre une gestion via l'UI.
3. **Actions Opérateur** :
   - **Retry** : Le message est renvoyé dans le topic principal.
   - **Discard** : Le message est marqué comme abandonné.
   - **Edit & Retry** : Le payload est corrigé manuellement (ex: correction d'un champ JSON) puis renvoyé en traitement.

---

## 5. Observabilité

- **Dashboard** : Visualisation en temps réel du taux d'erreur et de l'état du Circuit Breaker.
- **Toasts (WebSockets)** : Notifications immédiates en cas de bascule en mode individuel ou de changement d'état du circuit.
- **Métriques (Micrometer)** :
  - `myconsumer.kafka.events.errors` : Compteur global des erreurs par type.
  - `resilience4j.circuitbreaker.state` : État actuel du circuit.
