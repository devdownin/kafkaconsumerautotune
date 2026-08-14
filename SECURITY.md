# Politique de sécurité

## Versions supportées

Les correctifs de sécurité sont appliqués à la dernière version publiée.

| Version | Supportée          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Signaler une vulnérabilité

**N'ouvrez pas d'issue publique pour une faille de sécurité.** Une issue est
visible de tous et exposerait le problème avant qu'un correctif existe.

Utilisez le signalement privé de GitHub :

1. Rendez-vous sur l'onglet **Security** du dépôt.
2. Cliquez sur **Report a vulnerability**.
3. Décrivez la faille, sa portée et les étapes pour la reproduire.

Ce canal est privé entre vous et les mainteneurs.

## Ce à quoi vous attendre

- **Accusé de réception** sous 7 jours.
- **Première évaluation** (gravité, périmètre affecté) sous 14 jours.
- **Correctif ou plan d'action communiqué** sous 30 jours pour les failles
  confirmées de gravité haute ou critique.

Ce projet est maintenu bénévolement : ces délais sont un engagement de bonne
foi, pas un contrat de service.

## Portée

Relèvent de cette politique les vulnérabilités affectant le code de ce dépôt :
consommation Kafka, persistance, exposition des endpoints Actuator et du
tableau de bord, gestion de la DLT.

En sont exclus :

- Les vulnérabilités des dépendances tierces — signalez-les en amont, chez
  l'éditeur concerné. Dependabot suit ces mises à jour ici.
- La pile Docker Compose fournie à titre de démonstration, dont les
  identifiants sont volontairement triviaux et documentés comme tels
  (voir le README). Elle n'est pas destinée à un usage en production.
