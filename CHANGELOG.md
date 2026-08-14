# Changelog

Toutes les modifications notables de ce projet sont consignées dans ce fichier.

Le format s'appuie sur [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/),
et le projet suit le [versionnage sémantique](https://semver.org/lang/fr/).

## [Non publié]

### Modifié

- Les tests d'intégration s'exécutent contre un vrai broker Kafka fourni par
  Testcontainers (image `apache/kafka` en mode KRaft) au lieu d'un broker
  embarqué sur port figé, et sont pris en charge par Failsafe en phase
  `integration-test` sous le suffixe `*IT`. `mvn test` ne conserve que les
  tests unitaires et passe d'environ 90 à 20 secondes ; `mvn verify` exécute
  l'ensemble et requiert désormais un démon Docker.
- JaCoCo instrumente les deux exécuteurs : les relevés de Surefire et de
  Failsafe sont fusionnés avant le calcul du rapport et le contrôle de seuil.

### Ajouté

- Workflow d'intégration continue exécutant `mvn verify` sur chaque pull
  request, avec publication des rapports de test et de couverture.
- Analyse statique CodeQL sur les pushes, les pull requests et selon une
  planification hebdomadaire.
- Suivi des dépendances par Dependabot (Maven, GitHub Actions, Docker).
- `SECURITY.md`, `CODE_OF_CONDUCT.md`, `CHANGELOG.md`, `CODEOWNERS`, gabarits
  d'issue et de pull request.
- Métadonnées Maven `url`, `scm`, `developers`, `issueManagement` et
  `distributionManagement`.

### Corrigé

- La couverture JaCoCo n'était jamais produite : la configuration `argLine` de
  Surefire écrasait l'agent injecté par `prepare-agent`.
- Le workflow de publication Maven utilisait le JDK 11 alors que le projet
  requiert Java 21, et effectuait un `deploy` sans `distributionManagement`.
- Le déploiement GitHub Pages publiait la racine complète du dépôt au lieu du
  seul site vitrine.
- Le `Dockerfile` figeait le numéro de version dans le chemin du JAR.

### Modifié

- Le site vitrine est désormais servi à la racine des GitHub Pages plutôt que
  sous `/static-site/`.
- Journaux d'exécution et `node_modules` retirés du suivi Git et ajoutés au
  `.gitignore`.
- Workflow `deploy-pages.yml` supprimé, redondant avec `static.yml`.

## [1.0.1]

Première version publiée du consommateur Kafka auto-adaptatif : régulation PID
du débit de consommation, disjoncteur sur la persistance, repli unitaire,
gestion de la DLT, tableau de bord et observabilité Prometheus / Micrometer /
OpenTelemetry.
