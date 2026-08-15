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
        text.innerText = 'WS: Disconnected';
    }
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
