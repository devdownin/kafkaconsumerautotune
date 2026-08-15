# Captures d'écran

Utilisées par `README.md` et par la page Docker Hub (`.docker/hub-description.md`).

| Fichier | Page | Où il apparaît |
|---|---|---|
| `dashboard.png` | `/` | README (accroche), Docker Hub (accroche) |
| `optimizer.png` | `/optimizer` | README, Docker Hub |
| `dlt-management.png` | `/dlt-management`, panneau d'inspection ouvert | README, Docker Hub |
| `consumer-groups.png` | `/consumer-groups` | README |

> **Les chiffres qui y figurent sont illustratifs.** Ce sont des données de
> démonstration, pas des mesures relevées sur une installation réelle. Les deux
> pages qui les affichent le disent explicitement ; garder cette mention si les
> captures sont refaites.

## La page Docker Hub a besoin d'URL absolues

Docker Hub rend le markdown sur son propre domaine : un chemin relatif n'y
résout pas. `.docker/hub-description.md` pointe donc vers
`raw.githubusercontent.com/.../main/docs/images/`. Une capture ajoutée n'apparaît
sur Docker Hub qu'**une fois fusionnée dans `main`** — avant, le lien renvoie 404.

Le workflow `dockerhub-description.yml` resynchronise la page à chaque push sur
`main` touchant `.docker/hub-description.md`. Remplacer une image sans modifier
ce fichier ne déclenche donc rien côté Docker Hub, mais ce n'est pas gênant :
c'est le contenu du PNG qui change, pas son URL.

## Refaire les captures

### Depuis la pile de démonstration

C'est le chemin le plus direct si vous l'avez sous la main :

```sh
docker compose up -d
```

Puis générer du trafic depuis `/simulation` (préréglage « Nominal Flow » pour
des chiffres crédibles, « Degraded Data » pour peupler la page DLT), et
photographier les pages en 1920×1040, thème sombre.

### Sans infrastructure

Les captures actuelles ont été produites ainsi, l'environnement ne disposant ni
de Kafka ni d'Oracle. Le principe : rendre les templates via MockMvc avec un jeu
de données représentatif, servir le HTML obtenu à côté de `static/`, puis
photographier avec Chromium.

1. Un test temporaire annoté `@WebMvcTest({DashboardController.class,
   DltManagementController.class})` bouchonne `DashboardService`,
   `KafkaOptimizerService` et le dépôt, puis écrit le HTML rendu de chaque page.
   Les séries temporelles doivent être fournies (`successThroughput`,
   `timestamps`, `maxPollRecordsHistory`, `concurrencyHistory`), sans quoi les
   graphiques sont vides.
2. Copier ce HTML et le contenu de `src/main/resources/static/` dans un même
   répertoire, puis le servir : `python3 -m http.server 8099`. Les chemins
   `/vendor/...` et `/js/...` se résolvent alors normalement.
3. Photographier avec Playwright, `colorScheme: 'dark'`, viewport 1920×1040,
   `deviceScaleFactor: 1.5`.

Deux détails qui font la différence :

- **Couper la route `/ws`.** Sans backend, la barre d'état afficherait
  « WS: Disconnected » en rouge. Après chargement, appeler
  `updateWsStatus(true)` pour montrer l'état nominal.
- **Laisser ~2 s** avant la capture : les graphiques ApexCharts s'animent à
  l'apparition.

Pour `dlt-management.png`, cliquer sur `.inspect-btn` avant de photographier :
c'est le panneau d'inspection qui montre la correction de payload et le rejeu.

L'application doit avoir été construite au moins une fois (`./mvnw package`)
pour que `static/vendor/css/app.css` existe — il est généré, pas versionné. Voir
`docs/frontend-build.md`.
