/*
 * Toasts du tableau de bord : la primitive showToast et l'abonnement aux
 * événements système. Servi en fichier statique et mis en cache, au lieu
 * d'être réémis dans chaque réponse HTML.
 */
var TOAST_MAX_VISIBLE = 4;

var TOAST_STYLES = {
    SUCCESS: { icon: 'check_circle', color: 'text-emerald-500' },
    ERROR:   { icon: 'error',        color: 'text-rose-500' },
    WARNING: { icon: 'warning',      color: 'text-amber-500' },
    INFO:    { icon: 'info',         color: 'text-primary' }
};

function toastStyle(type) {
    return TOAST_STYLES[String(type || 'INFO').toUpperCase()] || TOAST_STYLES.INFO;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text == null ? '' : text;
    return div.innerHTML;
}

/* Drops the oldest toasts so a burst of system events cannot fill the screen. */
function trimToasts(container) {
    while (container.children.length > TOAST_MAX_VISIBLE) {
        container.removeChild(container.firstChild);
    }
}

function dismissToast(toast) {
    if (!toast || toast.dataset.dismissed) return;
    toast.dataset.dismissed = 'true';
    toast.classList.add('opacity-0', 'translate-x-[150%]');
    setTimeout(() => toast.remove(), 400);
}

/*
 * Single toast primitive for the whole UI.
 *   showToast('Payload copied', 'success')
 * `type` is one of success | error | warning | info.
 */
function showToast(message, type, options) {
    const container = document.getElementById('toast-container');
    if (!container) return null;
    const opts = options || {};
    const style = toastStyle(type);

    const toast = document.createElement('div');
    toast.className = 'pointer-events-auto flex items-start gap-4 p-4 rounded-xl shadow-2xl border ' +
        'border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 min-w-[280px] max-w-[400px] ' +
        'translate-x-[150%] opacity-0 transition-all duration-300 ease-out';

    toast.innerHTML = `
        <div class="p-2 rounded-lg bg-slate-50 dark:bg-slate-800 ${style.color} shrink-0">
            <span class="material-symbols-outlined text-xl" aria-hidden="true">${style.icon}</span>
        </div>
        <div class="flex-1 min-w-0">
            <div class="flex items-center gap-2 flex-wrap">
                <h5 class="text-sm font-bold">${escapeHtml(opts.title || message)}</h5>
                ${opts.category ? `<span class="text-[8px] font-black uppercase px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-500 border border-slate-200 dark:border-slate-700">${escapeHtml(opts.category)}</span>` : ''}
            </div>
            ${opts.title ? `<p class="text-xs text-slate-500 mt-1 break-words">${escapeHtml(message)}</p>` : ''}
            ${opts.timestamp ? `<p class="text-[10px] text-slate-400 mt-2">${escapeHtml(new Date(opts.timestamp).toLocaleTimeString())}</p>` : ''}
        </div>
        <button type="button" class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 shrink-0" aria-label="Dismiss notification">
            <span class="material-symbols-outlined text-sm" aria-hidden="true">close</span>
        </button>
    `;
    toast.querySelector('button').addEventListener('click', () => dismissToast(toast));

    container.appendChild(toast);
    trimToasts(container);

    requestAnimationFrame(() => toast.classList.remove('translate-x-[150%]', 'opacity-0'));
    setTimeout(() => dismissToast(toast), opts.duration || 6000);
    return toast;
}

/* Rich toast for SystemEventDTO messages pushed over /topic/system-events. */
function showEventToast(event) {
    return showToast(event.message, event.type, {
        title: event.title,
        category: event.category,
        timestamp: event.timestamp,
        duration: 8000
    });
}

// Global WebSocket Listener for System Events
function initSystemNotifications(stompClient) {
    stompClient.subscribe('/topic/system-events', function (message) {
        const event = JSON.parse(message.body);
        showEventToast(event);
        updateSystemStatus(event);
    });
}

function updateSystemStatus(event) {
    if (event.category === 'CIRCUIT_BREAKER') {
        const dot = document.getElementById('cb-status-dot');
        const text = document.getElementById('cb-status-text');
        const alert = document.getElementById('cb-alert');

        const status = event.message.replace('State changed to ', '');

        if (dot && text) {
            text.innerText = status;
            dot.className = 'size-2 rounded-full ' +
                (status === 'CLOSED' ? 'bg-emerald-500' : (status === 'HALF_OPEN' ? 'bg-amber-500' : 'bg-rose-500'));
        }

        if (alert) {
            if (status === 'OPEN') alert.classList.remove('hidden');
            else alert.classList.add('hidden');
        }
    }

    if (event.category === 'BATCH' && event.type === 'WARNING') {
        const fallbackAlert = document.getElementById('fallback-alert');
        if (fallbackAlert) {
            fallbackAlert.classList.remove('hidden');
            // Auto-hide after 30 seconds of no fallback events
            if (window.fallbackTimeout) clearTimeout(window.fallbackTimeout);
            window.fallbackTimeout = setTimeout(() => {
                fallbackAlert.classList.add('hidden');
            }, 30000);
        }
    }
}
