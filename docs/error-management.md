# Gestion des Erreurs et Résilience - KafkaConsumerAutoTune

Ce document détaille la stratégie de gestion des erreurs et les mécanismes de résilience mis en œuvre dans l'application KafkaConsumerAutoTune.

## 1. Philosophie de Gestion des Erreurs

L'application adopte une approche de "fail-safe" et de "graceful degradation" :
- **Isolation** : Un message corrompu ne doit pas bloquer l'ensemble du flux.
- **Transparence** : Chaque erreur est tracée, mesurée et notifiée en temps réel.
- **Récupération** : Les erreurs transitoires sont gérées par des retries, tandis que les erreurs permanentes sont isolées.

---

## 2. Classification et Traitement des Erreurs

### 2.1 Erreurs Transitoires (`TransientException`)
Ce sont des erreurs temporaires (ex: Timeout DB, indisponibilité réseau).
-   **Traitement** : L'exception est levée jusqu'au container Kafka, déclenchant un re-jeu immédiat au niveau de Kafka (Retry).

### 2.2 Erreurs Permanentes (`PermanentException`)
Ce sont des erreurs liées au contenu (ex: JSON invalide, violation de contrainte). Le re-traitement échouera systématiquement.
-   **Traitement** : Le message est immédiatement routé vers la **Dead Letter Topic (DLT)** pour libérer le reste du batch.

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

### 3.4 Throttling d'Urgence
Si le système détecte une saturation critique (CPU > 90% ou Mémoire > 90%), le moteur d'Auto-Tune réduit immédiatement le `max.poll.records` de 30% pour soulager l'infrastructure avant même que des erreurs ne surviennent.

---

## 4. Cycle de Vie de la DLT

1.  **Enregistrement** : Les erreurs sont stockées dans la table `DLT_EVENT` et publiées sur Kafka `.dlt`.
2.  **Gestion Opérateur** : Via l'interface utilisateur, il est possible de :
    -   **Retry** : Re-jouer le message.
    -   **Discard** : Abandonner le message.
    -   **Edit & Retry** : Corriger le payload (ex: fixer un JSON) puis re-jouer.

## 5. Erreurs HTTP (interface web et API)

`GlobalExceptionHandler` intercepte les exceptions non traitées et répond selon
le type d'appelant. Auparavant il renvoyait systématiquement une page HTML avec
un statut **200 OK** : les sondes de supervision voyaient une application saine
alors qu'elle servait des pages d'erreur, et le JavaScript du tableau de bord,
qui teste `response.ok`, considérait comme réussis des appels d'API en échec.

### 5.1 Négociation du format

| Appelant | Critère | Réponse |
|---|---|---|
| Navigateur | Tout le reste | Vue `error.html`, statut **500** |
| Client d'API | URI sous `/api/`, ou `Accept: application/json`, ou `X-Requested-With: XMLHttpRequest` | Corps JSON `ApiError`, statut **500** |

Le second critère existe pour les rares endpoints JSON situés hors du préfixe
`/api/` — aujourd'hui `POST /dlt-management/{id}/retry-with-payload`, dont
l'appel `fetch` envoie explicitement l'en-tête `Accept`.

### 5.2 Corps `ApiError`

```json
{
  "timestamp": "2026-08-14T18:42:11.732Z",
  "status": 400,
  "error": "Bad Request",
  "message": "The request parameters are invalid.",
  "path": "/api/simulation/start",
  "fieldErrors": { "totalMessages": "must be at least 1" }
}
```

`fieldErrors` n'est présent que sur un échec de validation. Les champs vides
sont omis de la sérialisation.

> **À revoir avec l'authentification.** `message` reprend le message de
> l'exception, qui peut exposer des détails d'infrastructure. C'est le
> comportement qu'avait déjà la page d'erreur HTML, et l'application n'a
> actuellement aucune authentification : l'exposition est donc la même par les
> deux surfaces. À restreindre le jour où un contrôle d'accès est introduit.

### 5.3 Validation des requêtes

Les corps de requête annotés `@Valid` renvoient **400** avec le détail par
champ. `SimulationRequestDTO` borne ainsi le volume, les pourcentages, le délai
et le débit visé : les garde-fous de l'interface sont côté client et ne
protègent donc pas l'endpoint, qui est appelable directement.
