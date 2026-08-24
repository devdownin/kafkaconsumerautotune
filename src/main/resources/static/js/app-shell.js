/*
 * Comportements communs à la coquille : bascule de thème, barre latérale
 * rétractable, indicateur WebSocket et horloge du pied de page. Servi en
 * fichier statique et mis en cache, au lieu d'être réémis dans chaque page.
 */
function initThemeToggle() {
    var themeToggleDarkIcon = document.getElementById('theme-toggle-dark-icon');
    var themeToggleLightIcon = document.getElementById('theme-toggle-light-icon');
    if (!themeToggleDarkIcon || !themeToggleLightIcon) return;

    var themeToggleBtn = document.getElementById('theme-toggle');
    if (!themeToggleBtn) return;

    function syncToggle() {
        var isDark = document.documentElement.classList.contains('dark');
        themeToggleLightIcon.classList.toggle('hidden', !isDark);
        themeToggleDarkIcon.classList.toggle('hidden', isDark);
        themeToggleBtn.setAttribute('aria-pressed', String(isDark));
        themeToggleBtn.setAttribute('aria-label', isDark ? 'Switch to light theme' : 'Switch to dark theme');
        themeToggleBtn.setAttribute('data-tooltip', isDark ? 'Light theme' : 'Dark theme');
    }

    syncToggle();

    themeToggleBtn.addEventListener('click', function () {
        var nowDark = !document.documentElement.classList.contains('dark');
        document.documentElement.classList.toggle('dark', nowDark);
        localStorage.setItem('color-theme', nowDark ? 'dark' : 'light');
        syncToggle();
    });
}

/*
 * Off-canvas navigation (viewports below lg only). The sidebar stays in
 * normal flow on large screens, so these classes are inert there.
 */
function toggleSidebar(forceClose) {
    var sidebar = document.getElementById('app-sidebar');
    var overlay = document.getElementById('sidebar-overlay');
    var toggle = document.getElementById('sidebar-toggle');
    if (!sidebar || !overlay || !toggle) return;

    var open = forceClose === true ? false : !sidebar.classList.contains('is-open');
    sidebar.classList.toggle('is-open', open);
    overlay.classList.toggle('is-open', open);
    toggle.setAttribute('aria-expanded', String(open));
    toggle.setAttribute('aria-label', open ? 'Close navigation menu' : 'Open navigation menu');
}

/*
 * Live-status indicator. Pages that open a WebSocket call updateWsStatus();
 * pages that render static server-side data never do, and used to leave the
 * status bar stuck on "Connecting" forever. If nothing reports in, the
 * indicator falls back to an explicit "off" state.
 */
var wsStatusReported = false;

function updateWsStatus(connected) {
    wsStatusReported = true;

    // Bandeau d'alerte, sur les pages qui en portent un.
    const banner = document.getElementById('ws-alert');
    if (banner) banner.classList.toggle('hidden', connected);

    const dot = document.getElementById('ws-status-dot');
    const text = document.getElementById('ws-status-text');
    if (!dot || !text) return;
    dot.classList.remove('bg-slate-400', 'bg-rose-500', 'bg-emerald-500');
    if (connected) {
        dot.classList.add('bg-emerald-500');
        dot.classList.remove('animate-pulse');
        text.innerText = 'WS: Connected';
    } else {
        dot.classList.add('bg-rose-500');
        dot.classList.add('animate-pulse');
        text.innerText = 'WS: Reconnecting';
    }
}

/*
 * Connexion temps réel (SockJS + STOMP).
 *
 * Sept pages recopiaient les quatre mêmes lignes d'ouverture, et aucune ne
 * se reconnectait : le rappel d'erreur repeignait l'indicateur en rouge et
 * s'arrêtait là. À la première coupure — redémarrage du serveur, portail
 * captif, veille de la machine — la page restait muette jusqu'à ce que
 * quelqu'un pense à la recharger. metrics.html affichait pourtant
 * « Attempting to reconnect... », que rien n'exécutait.
 *
 *   connectWs({
 *       '/topic/stats': updateStats,   // le corps est déjà désérialisé
 *       '/topic/dlt': onDltEvent
 *   });
 *
 * /topic/system-events est abonné d'office : il alimente l'indicateur de
 * disjoncteur du pied de page, présent sur toutes les pages. Quatre d'entre
 * elles ne s'y abonnaient pas, et leur indicateur restait donc figé sur la
 * valeur rendue au chargement.
 */
var WS_RETRY_MIN_MS = 1000;
var WS_RETRY_MAX_MS = 30000;

function connectWs(topics) {
    var handlers = topics || {};
    var retryDelay = WS_RETRY_MIN_MS;
    var retryTimer = null;
    var generation = 0;
    var client = null;

    function open() {
        // Chaque tentative porte son numéro : les rappels d'un client
        // remplacé arrivent encore après coup et ne doivent pas déclencher
        // une seconde reconnexion pour une seule coupure.
        var attempt = ++generation;
        if (client) {
            try { client.disconnect(); } catch (e) { /* déjà fermé */ }
        }

        var opened = Stomp.over(new SockJS('/ws'));
        opened.debug = null;
        client = opened;

        opened.connect({}, function () {
            if (attempt !== generation) return;
            retryDelay = WS_RETRY_MIN_MS;
            updateWsStatus(true);
            initSystemNotifications(opened);
            Object.keys(handlers).forEach(function (topic) {
                opened.subscribe(topic, function (message) {
                    handlers[topic](JSON.parse(message.body));
                });
            });
        }, function () {
            // Stomp appelle ce rappel aussi bien sur échec d'ouverture que
            // sur fermeture d'une connexion établie.
            if (attempt !== generation) return;
            updateWsStatus(false);
            // Comparaison explicite : un identifiant de minuteur peut valoir
            // zéro, et « if (retryTimer) » laisserait alors passer un second
            // essai pour la même coupure.
            if (retryTimer !== null) return;
            retryTimer = setTimeout(function () {
                retryTimer = null;
                open();
            }, retryDelay);
            // Plafonné : une panne longue ne doit pas marteler le serveur.
            retryDelay = Math.min(retryDelay * 2, WS_RETRY_MAX_MS);
        });
    }

    /*
     * Les navigateurs brident les minuteurs des onglets en arrière-plan.
     * Sans ce rappel, revenir sur un onglet resté ouvert pendant une coupure
     * peut demander encore une demi-minute avant la tentative suivante.
     */
    document.addEventListener('visibilitychange', function () {
        if (document.visibilityState !== 'visible' || retryTimer === null) return;
        clearTimeout(retryTimer);
        retryTimer = null;
        retryDelay = WS_RETRY_MIN_MS;
        open();
    });

    open();
}

function initStatusBar() {
    const timeElem = document.getElementById('footer-time');
    if (timeElem) {
        const pad = (n) => n.toString().padStart(2, '0');
        const render = () => {
            const now = new Date();
            timeElem.innerText = pad(now.getHours()) + ':' + pad(now.getMinutes()) + ':' + pad(now.getSeconds());
        };
        let ticker = null;
        // Stop ticking while the tab is hidden: no timer wake-ups in background tabs.
        const sync = () => {
            if (ticker) { clearInterval(ticker); ticker = null; }
            if (document.visibilityState === 'visible') {
                render();
                ticker = setInterval(render, 1000);
            }
        };
        document.addEventListener('visibilitychange', sync);
        sync();
    }

    setTimeout(function () {
        if (wsStatusReported) return;
        const dot = document.getElementById('ws-status-dot');
        const text = document.getElementById('ws-status-text');
        if (!dot || !text) return;
        dot.classList.remove('animate-pulse');
        text.innerText = 'WS: Not used on this page';
    }, 5000);
}

document.addEventListener('DOMContentLoaded', initStatusBar);
document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') toggleSidebar(true);
});
