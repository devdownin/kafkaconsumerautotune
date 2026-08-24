# Build du frontend

Le tableau de bord chargeait Tailwind depuis `cdn.tailwindcss.com`. Cette version
embarque le moteur complet et **recompile les classes dans le navigateur à chaque
chargement de page** ; l'amont la documente comme inadaptée à la production. La
feuille est désormais compilée à la construction.

## Ce que ça change

| | Avant (CDN) | Après (compilé) |
|---|---|---|
| Transféré | moteur Tailwind complet | 40 Ko minifiés, mis en cache |
| Coût par chargement | analyse du DOM + génération du CSS | aucun |
| Hors ligne | interface sans aucun style | fonctionne |
| Origines externes | 1 | 0 |

## Fichiers

| Fichier | Rôle |
|---|---|
| `package.json` | Dépendances et scripts du build. |
| `package-lock.json` | Versions figées ; `npm ci` s'y fie. |
| `tailwind.config.js` | Thème et chemins scannés. Reprend la configuration qui était en ligne dans `fragments/head.html`. |
| `src/main/frontend/app.css` | Source : directives Tailwind puis les styles propres au tableau de bord. |
| `src/main/resources/static/vendor/css/app.css` | **Généré.** Non versionné. |
| `src/main/resources/static/js/` | Comportement de la coquille (`app-shell.js`, `notifications.js`) et des pages (`pages/<page>.js`). |
| `src/test/js/` | Tests de ce JavaScript, exécutés par `node --test`. |

## Comment ça s'exécute

`frontend-maven-plugin` est branché sur la phase `generate-resources`, avant que
`process-resources` ne recopie `src/main/resources` vers `target/classes` :

1. `install-node-and-npm` — télécharge Node dans `target/`. Rien à installer sur
   le poste de travail ni dans l'image de build.
2. `npm ci` — installe les dépendances figées.
3. `npm run build:css` — compile et minifie.

Une quatrième exécution, `npm run test:js`, est branchée sur la phase `test` et
lance `node --test` sur `src/test/js/`. Node y est déjà installé par la première
exécution, donc rien de plus à télécharger. `-DskipTests` la saute, comme
surefire — c'est ce que fait le `Dockerfile`.

Un `./mvnw package` suffit donc, y compris au premier clone. Le `Dockerfile`
copie `package.json`, `package-lock.json` et `tailwind.config.js` avant `src/`,
pour que le cache de couches survive à une modification des templates.

### Développement

```sh
npm run watch:css   # recompile à chaque modification de template
```

### Construire sans Node

```sh
./mvnw package -Dfrontend.skip=true
```

Réutilise l'`app.css` déjà présent. Utile hors ligne ou sur un agent sans accès
à `nodejs.org` — mais si le fichier n'a jamais été généré, l'interface s'affichera
sans style.

## Le piège à connaître : la purge

Tailwind ne conserve que les classes **trouvées dans les fichiers listés sous
`content`**. Une classe assemblée dynamiquement est invisible pour le scanner :

```js
element.className = `bg-${color}-500`;   // purgée : n'apparaît nulle part telle quelle
element.className = colors[type];        // conservée si colors contient des littéraux
```

Le code respecte aujourd'hui la seconde forme partout — c'est vérifié, aucune
classe n'est assemblée par concaténation. **En ajouter une casserait le style
silencieusement**, sans erreur au build ni en test. Si c'est inévitable, ajouter
la classe à `safelist` dans `tailwind.config.js`.

Le comportement des pages vit dans `static/js/pages/`, hors des templates. Ces
fichiers composent du balisage à coups de classes littérales, d'où le glob
`static/js/**` dans `content` : sans lui, les éléments construits en JavaScript
s'afficheraient sans style.

### Vérifier après une modification importante

```sh
npm run build:css
grep -c 'translate-x-\\\[150%\\\]' src/main/resources/static/vendor/css/app.css
```

Plus généralement, extraire les classes des templates et contrôler leur présence
dans le CSS généré ; c'est ce qui a été fait lors de la bascule, sur 464 classes.

## Tester le JavaScript

```sh
npm run test:js
```

`src/test/js/app-shell.test.js` couvre `connectWs`, la connexion temps réel :
reconnexion après coupure, doublement puis plafonnement du délai, filtrage des
rappels d'un client remplacé. Le code y est chargé dans un contexte `node:vm`
muni d'un faux DOM, d'un faux SockJS/Stomp et d'une horloge simulée — l'isolation
évite d'écraser `setTimeout` dans le processus du lanceur de tests.

Une coupure de WebSocket ne se produit dans aucun test MockMvc : sans ce fichier,
cette logique ne serait couverte nulle part.

## Ce qui reste inline

Deux choses seulement :

- Le script `#theme-preference` de `fragments/head.html`, synchrone : il doit
  poser la classe `dark` **avant le premier rendu**, sinon un utilisateur en
  thème sombre voit un flash blanc. Le thème lui-même est passé dans
  `tailwind.config.js`.
- Les données que Thymeleaf injecte dans les pages (`initialStats`, `dltEvents`,
  `lastEvent`…), une à trois lignes par template. Le script en ligne s'exécute à
  l'analyse, donc avant le fichier différé qui les consomme.
