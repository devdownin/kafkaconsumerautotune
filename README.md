# 🚀 Consotopic : Le Consommateur Kafka Intelligent

Bienvenue dans **Consotopic**, une implémentation de référence d'un consommateur Kafka haute performance pour Spring Boot. Plus qu'un simple outil de consommation, Consotopic est conçu comme un système auto-adaptatif capable d'optimiser ses propres performances en temps réel.

---

## 🎯 Pourquoi Consotopic ?

Traditionnellement, configurer un consommateur Kafka est un jeu de devinettes :
- *Combien de messages dois-je prendre par lot (`max.poll.records`) ?*
- *Combien de temps dois-je attendre le serveur (`fetch.max.wait.ms`) ?*
- *De combien de threads ai-je besoin ?*

Si vous fixez ces valeurs trop bas, vous sous-utilisez vos ressources. Si vous les fixez trop haut, vous risquez de saturer votre base de données ou de déclencher des rebalances Kafka incessants (le fameux "rebalance storm").

**Consotopic résout cela en automatisant ces réglages.**

---

## 🧠 L'Innovation : Le "Régulateur de Vitesse" (Contrôleur PID)

Imaginez que vous conduisez une voiture. Pour maintenir une vitesse constante, vous n'appuyez pas sur l'accélérateur à une position fixe. Vous ajustez votre pression en fonction de la pente, du vent et de la vitesse actuelle.

Consotopic fait exactement la même chose pour Kafka grâce à un **Contrôleur PID** (Proportionnel, Intégral, Dérivé) :
1. **Mesure** : Il observe le temps réel mis pour traiter un lot de messages.
2. **Cible** : Il a un objectif (ex: traiter chaque lot en exactement 1,2 seconde).
3. **Action** : Si le traitement est trop rapide, il augmente le nombre de messages par lot. S'il ralentit (ex: la base de données est fatiguée), il réduit la charge instantanément.

> **Résultat** : Un débit optimal constant, peu importe la charge ou la santé du réseau.

---

## 🛡️ Résilience de "Qualité Industrielle"

Le traitement de données réelles est chaotique. Consotopic est bâti pour survivre aux pannes les plus courantes :

### 1. Le Disjoncteur (Circuit Breaker)
Si votre base de données Oracle tombe, Consotopic ne s'acharne pas. Il "coupe le courant" :
- Le Circuit Breaker passe à l'état **OPEN**.
- Le consommateur Kafka est **mis en pause** automatiquement pour éviter de perdre des messages ou de saturer les logs d'erreurs.
- Il reprend tout seul dès que la base est de nouveau saine.

### 2. Le Mode Repli Individuel (Individual Fallback)
C'est le mode "chirurgical". Si un lot de 100 messages échoue à cause d'un seul message corrompu (un "Poison Message") :
- Consotopic ne rejette pas tout le lot.
- Il re-tente chaque message **un par un**.
- Les 99 messages sains sont sauvegardés.
- Le message fautif est isolé et envoyé en **DLT** (Dead Letter Topic).

---

## 📊 Observabilité Totale

On ne peut pas améliorer ce qu'on ne mesure pas. Consotopic offre :
- **Dashboard Temps Réel** : Visualisez le débit (msg/s), le lag Kafka et les interventions de l'Optimizer.
- **Tracing Distribué** : Suivez chaque message de Kafka jusqu'à la base de données via OpenTelemetry et Jaeger.
- **Logs Structurés** : Format JSON (ECS) prêt pour ELK, avec injection automatique des IDs de trace.

---

## 🚀 Démarrage Rapide

### Pré-requis
- Docker et Docker Compose
- Java 21 (si vous voulez compiler localement)

### Lancer la stack complète
```bash
docker-compose up -d
```
Ceci lance : Kafka, Oracle XE, Prometheus, Jaeger, et l'application Consotopic.

### Accéder aux outils
- **Dashboard Consotopic** : [http://localhost:8080/dashboard](http://localhost:8080/dashboard)
- **Kafka Optimizer** : [http://localhost:8080/optimizer](http://localhost:8080/optimizer)
- **Jaeger (Traces)** : [http://localhost:8080/jaeger](http://localhost:16686)

---

## 📚 En savoir plus
- [Documentation Technique détaillée](DOCUMENTATION.md)
- [Gestion des Erreurs](docs/error-management.md)
- [Observabilité](docs/observability.md)
