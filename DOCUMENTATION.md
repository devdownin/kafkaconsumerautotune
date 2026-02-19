# Documentation Technique - Consotopic (Kafka Consumer Auto-tune)

## 1. Résumé Exécutif
Consotopic est une application Spring Boot de haute performance conçue pour consommer des messages Kafka en mode batch, les traiter, et les persister dans une base de données (Oracle/H2). L'application se distingue par son moteur d'**auto-tuning** intelligent basé sur un contrôleur PID qui ajuste dynamiquement les paramètres du consommateur Kafka pour optimiser le débit et la latence en temps réel. Elle intègre également un système robuste de gestion des erreurs via une Dead Letter Topic (DLT) et un tableau de bord complet de monitoring.

---

## 2. Architecture Globale
L'application suit une architecture orientée services avec les couches suivantes :
- **Couche de Consommation** : Moteur Kafka configuré en mode batch avec gestion manuelle des acquittements (Acks).
- **Couche de Traitement (Moteur d'Auto-tune)** : Analyseur de performance qui réajuste les paramètres Kafka.
- **Couche de Persistance** : Utilisation de Spring Data JPA avec support de batches JDBC pour une insertion efficace.
- **Couche de Gestion DLT** : Système de récupération et de re-traitement des messages en échec.
- **Couche de Monitoring** : Dashboard temps réel via WebSocket, Thymeleaf et Micrometer/Prometheus.

---

## 3. Composants Clés

### 3.1 Consommation Kafka (Batch Mode)
Le service `EventBatchConsumer` est le point d'entrée des messages.
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

### 3.3 Gestion des Erreurs et DLT (DltService)
Tout message dont le traitement échoue est dirigé vers deux destinations :
1. **Kafka DLT Topic** : Pour une traçabilité technique avec headers (`DLT_EXCEPTION_MESSAGE`, etc.).
2. **Base de données (DltEvent)** : Pour une gestion via l'interface utilisateur.
- **Actions possibles** : Retry (re-jeu), Discard (abandon), Modification du payload avant retry.

### 3.4 Dashboard et Observabilité
Le `DashboardService` agrège des métriques provenant de :
- **Kafka AdminClient** : Lag par partition, état des Consumer Groups.
- **JVM** : Utilisation CPU, mémoire Heap, threads.
- **Database** : État du pool Hikari (connexions actives/inactives).
- **Application** : Débit (sliding window de 5s et 24h), taux de succès.

---

## 4. Configuration

### 4.1 Variables d'Environnement Principales
| Variable | Description | Défaut |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Liste des brokers Kafka | `kafkadev:9092` |
| `DB_HOST` | Host de la base Oracle | `localhost` |
| `DB_USER` / `DB_PASSWORD` | Identifiants DB | - |
| `KAFKA_TOPIC_NAME` | Topic source | `asf.peage.backoffice.sortie.recouvrable` |

### 4.2 Profils Spring
- **dev** : Utilise une base H2 en mémoire et Kafka en clair.
- **rec** : Active la configuration SSL pour Kafka.
- **local-h2** : Configuration pour tests locaux sans infrastructure complexe.

---

## 5. Guide Opérationnel

### 5.1 Monitoring en Temps Réel
Accédez au dashboard via `/dashboard`. Il affiche :
- Les courbes de débit (Throughput).
- Le lag Kafka en temps réel.
- L'état de santé du système et les paramètres actuels d'auto-tune.

### 5.2 Gestion des Incidents (DLT)
Via l'onglet "DLT Management" :
1. Identifier les messages en erreur (status `UNRESOLVED`).
2. Analyser le message d'erreur et le payload.
3. Choisir de "Retry" après correction du système source ou "Discard" si le message est corrompu.

---

## 6. Spécifications Techniques
- **Java** : 21
- **Spring Boot** : 3.5.9
- **Kafka Client** : Inclus dans Spring Kafka
- **Base de données** : Oracle 23c (Runtime) / H2 (Dev/Test)
- **Monitoring** : Micrometer + Prometheus
- **UI** : Thymeleaf + Tailwind CSS + WebSockets (STOMP)
