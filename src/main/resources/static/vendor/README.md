# Ressources tierces embarquées

Ces fichiers étaient auparavant chargés depuis `cdn.jsdelivr.net`,
`cdnjs.cloudflare.com` et `fonts.googleapis.com`. Ils sont désormais servis par
l'application elle-même : le tableau de bord fonctionne en environnement
cloisonné, les versions ne peuvent plus bouger sous nos pieds, et aucun tiers ne
voit le trafic des utilisateurs.

Ils sont référencés depuis `templates/fragments/head.html`, et pour Prism depuis
`message-viewer.html`.

## Inventaire

| Fichier | Version | Origine |
|---|---|---|
| `js/sockjs-1.6.1.min.js` | 1.6.1 | npm `sockjs-client`, `dist/sockjs.min.js` |
| `js/stomp-2.3.3.min.js` | 2.3.3 | npm `stompjs`, `lib/stomp.min.js` |
| `js/apexcharts-6.8.0.min.js` | 6.8.0 | npm `apexcharts`, `dist/apexcharts.min.js` |
| `js/prism-core-1.29.0.min.js` | 1.29.0 | npm `prismjs`, `components/prism-core.min.js` |
| `js/prism-json-1.29.0.min.js` | 1.29.0 | npm `prismjs`, `components/prism-json.min.js` |
| `css/prism-okaidia-1.29.0.min.css` | 1.29.0 | npm `prismjs`, `themes/` |
| `css/inter.css` + `fonts/inter-*.woff2` | — | Google Fonts, Inter |
| `css/material-symbols.css` + `fonts/material-symbols-outlined-subset.woff2` | — | Google Fonts, Material Symbols Outlined |

La version figure dans le nom de fichier pour les bibliothèques : une montée de
version se voit dans le diff et ne peut pas être servie depuis un cache
navigateur périmé.

Prism n'est pas repris tel quel : le bundle des CDN embarque aussi les grammaires
markup, CSS et JavaScript, dont l'UI ne se sert pas. Seuls le cœur et la grammaire
réellement utilisée (JSON) sont embarqués.

## Mettre à jour une bibliothèque

```sh
npm pack sockjs-client@1.6.1        # ou stompjs, apexcharts, prismjs
tar xzf sockjs-client-1.6.1.tgz
cp package/dist/sockjs.min.js src/main/resources/static/vendor/js/sockjs-1.6.1.min.js
```

Renommer le fichier avec la nouvelle version, puis mettre à jour la référence
dans `fragments/head.html` et le tableau ci-dessus.

## Régénérer les polices

Inter et Material Symbols sont des polices **variables** : Google sert un seul
fichier par sous-ensemble, quel que soit le nombre de graisses demandées. C'est
pourquoi `inter.css` ne déclare que deux `@font-face` (`latin` et `latin-ext`)
couvrant `font-weight: 100 900`, et non un par graisse.

Seuls les sous-ensembles `latin` et `latin-ext` sont embarqués. Les alphabets
cyrillique, grec et vietnamien retombent sur la police système — sans effet sur
l'interface, dont les libellés sont latins, et sans effet sur les charges utiles
affichées, rendues en police à chasse fixe.

**Material Symbols est sous-ensemblée aux seules icônes utilisées** (54 glyphes
au lieu des ~3 500 du jeu complet, soit 19 Ko au lieu de ~3,5 Mo). **Toute icône
ajoutée à un template ou injectée depuis JavaScript doit être ajoutée à la liste
`icon_names`, sinon elle s'affichera sous forme de texte littéral.**

Pour régénérer, en remplaçant `<icônes>` par la liste triée et séparée par des
virgules :

```sh
UA='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
curl -A "$UA" 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap'
curl -A "$UA" 'https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:wght,FILL@100..700,0..1&icon_names=<icônes>&display=swap'
```

L'en-tête `User-Agent` est nécessaire : sans lui, Google renvoie du TTF au lieu
du WOFF2. Télécharger ensuite les fichiers pointés par les `url(...)`, les
placer dans `fonts/`, et réécrire les `src:` vers `/vendor/fonts/...`.

Pour vérifier qu'aucune icône ne manque, comparer la liste `icon_names` aux
icônes présentes dans le HTML **rendu** — pas seulement dans les templates, afin
de couvrir celles injectées depuis JavaScript.

## Ce qui reste hors de ce répertoire

Tailwind est toujours chargé depuis `cdn.tailwindcss.com`. Sa version CDN
embarque le moteur complet et compile les classes dans le navigateur à chaque
chargement ; la remplacer suppose de compiler une feuille de style à la
construction, ce qui relève de la chaîne de build. Voir `docs/ui-audit.md` §4.
C'est donc la seule dépendance externe restante, et la seule ressource qui
manquera à une installation déconnectée.
