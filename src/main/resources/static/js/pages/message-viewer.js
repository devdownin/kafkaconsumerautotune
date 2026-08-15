/*
 * Comportement de la page Message Viewer : rendu JSON coloré,
 * comparaison d'événements et recherche dans la charge utile.
 */
var currentEventId = null;
var compareMode = false;
var compareSelection = [];

document.addEventListener('DOMContentLoaded', function() {
    initThemeToggle();
    initHistoryList();
    initSearch();

    if (lastEvent && lastEvent.payload) {
        selectEvent(lastEvent.id);
    } else {
        document.getElementById('main-content-panel').classList.add('hidden');
        showNoMessage(true);
    }

    initWebSocket();
});

function showNoMessage(visible) {
    const warning = document.getElementById('no-message-warning');
    warning.classList.toggle('hidden', !visible);
    warning.classList.toggle('flex', visible);
}

/* Delegated listeners instead of one inline handler per history entry. */
function initHistoryList() {
    const list = document.getElementById('history-list');
    list.addEventListener('click', function (e) {
        const input = e.target.closest('.compare-input');
        if (input) {
            e.stopPropagation();
            updateCompareSelection(Number(input.dataset.id));
            return;
        }
        const item = e.target.closest('.history-item');
        if (item) selectEvent(Number(item.dataset.id));
    });
}

function initWebSocket() {
    connectWs({
        '/topic/events': function (events) {
            if (events.length === 0) return;

            const list = document.getElementById('history-list');
            events.forEach(event => {
                recentEvents.unshift(event);
                if (recentEvents.length > 20) recentEvents.pop();
                list.insertBefore(buildHistoryItem(event), list.firstChild);
                while (list.children.length > 20) list.removeChild(list.lastChild);
            });

            if (!compareMode) {
                const latest = events[0];
                if (latest.payload) selectEvent(latest.id);
            }
        }
    });
}

function buildHistoryItem(event) {
    const date = new Date(event.createdAt);
    const pad = (n) => String(n).padStart(2, '0');
    const formattedTime = pad(date.getHours()) + ':' + pad(date.getMinutes()) + ':' + pad(date.getSeconds());

    const item = document.createElement('button');
    item.type = 'button';
    item.id = 'history-' + event.id;
    item.dataset.id = event.id;
    item.className = 'w-full text-left px-3 py-2.5 rounded-xl text-xs cursor-pointer hover:bg-slate-100 dark:hover:bg-slate-800 flex justify-between items-center group history-item border border-transparent transition-all animate-pulse';
    item.innerHTML = `
        <span class="flex items-center gap-3">
            <span class="compare-checkbox ${compareMode ? '' : 'hidden'}">
                <input type="checkbox" id="check-${event.id}" data-id="${event.id}" aria-label="Select for comparison" class="compare-input rounded border-slate-300 text-primary focus:ring-primary size-3">
            </span>
            <span class="flex flex-col overflow-hidden">
                <span class="font-bold text-slate-700 dark:text-slate-300 truncate">${escapeHtml(event.eventId)}</span>
                <span class="text-[9px] text-slate-500">${formattedTime}</span>
            </span>
        </span>
        <span class="material-symbols-outlined text-sm opacity-0 group-hover:opacity-100 text-primary transition-opacity" aria-hidden="true">chevron_right</span>
    `;
    setTimeout(() => item.classList.remove('animate-pulse'), 2000);
    return item;
}

function selectEvent(id) {
    const event = recentEvents.find(e => e.id === id);
    if (!event || !event.payload) return;

    if (compareMode) {
        const checkbox = document.getElementById('check-' + id);
        if (checkbox) {
            checkbox.checked = !checkbox.checked;
            updateCompareSelection(id);
        }
        return;
    }

    currentEventId = id;
    document.querySelectorAll('.history-item').forEach(item => {
        item.classList.remove('bg-primary/5', 'border-primary/20', 'ring-1', 'ring-primary/20');
    });
    const activeItem = document.getElementById('history-' + id);
    if (activeItem) activeItem.classList.add('bg-primary/5', 'border-primary/20', 'ring-1', 'ring-primary/20');

    visualizeMessage(event);
}

function visualizeMessage(event) {
    let data;
    try {
        data = JSON.parse(event.payload);
    } catch (e) {
        showToast('This message is not valid JSON and cannot be visualized', 'error');
        return;
    }

    document.getElementById('main-content-panel').classList.remove('hidden');
    showNoMessage(false);
    document.getElementById('last-update-time').innerText = new Date().toLocaleTimeString('en-US');

    const container = document.getElementById('generic-blocks-container');
    container.innerHTML = '';
    blockConfigs.forEach((config, index) => {
        const val = getValueByPath(data, config.jsonPath);
        const card = document.createElement('div');
        card.className = index === 0
            ? 'bg-gradient-to-br from-primary to-blue-600 p-6 rounded-2xl text-white shadow-lg shadow-blue-500/20'
            : 'bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 p-6 rounded-2xl shadow-sm';
        // Payload values are untrusted: escape before injecting.
        card.innerHTML =
            `<p class="${index === 0 ? 'text-blue-100' : 'text-slate-500 dark:text-slate-400'} text-xs font-bold uppercase tracking-wider mb-1">${escapeHtml(config.title)}</p>` +
            `<h4 class="text-2xl font-black break-words">${escapeHtml(val)}</h4>`;
        container.appendChild(card);
    });

    // Headers
    const headersSection = document.getElementById('section-headers');
    const headersContent = document.getElementById('headers-content');
    if (event.headers) {
        headersSection.classList.remove('hidden');
        let parsedHeaders = {};
        try { parsedHeaders = JSON.parse(event.headers); } catch (e) { /* ignore malformed headers */ }
        headersContent.innerHTML = Object.entries(parsedHeaders).map(([k, v]) => `
            <div class="flex flex-col p-3 bg-slate-50 dark:bg-slate-800/50 rounded-lg">
                <span class="text-[10px] font-bold text-slate-400 uppercase">${escapeHtml(k)}</span>
                <span class="text-xs break-all">${escapeHtml(v)}</span>
            </div>
        `).join('');
    } else {
        headersSection.classList.add('hidden');
    }

    const rawContentElem = document.getElementById('raw-json-content');
    const jsonString = JSON.stringify(data, null, 2);
    rawContentElem.textContent = jsonString;

    document.getElementById('json-meta').innerText = `Size: ${new Blob([jsonString]).size} bytes`;

    if (window.Prism) Prism.highlightElement(rawContentElem);
    applySearchHighlight();
}

function getValueByPath(obj, path) {
    if (!path) return "N/A";
    try {
        let current = obj;
        for (const part of path.split('.')) {
            if (current == null || current[part] === undefined) return "N/A";
            current = current[part];
        }
        return (typeof current === 'object') ? JSON.stringify(current) : String(current);
    } catch (e) { return "N/A"; }
}

/* Was referenced by the two nav buttons but never defined (ReferenceError). */
function scrollToSection(sectionId) {
    const section = document.getElementById(sectionId);
    if (!section) return;
    section.scrollIntoView({ behavior: 'smooth', block: 'start' });
    document.querySelectorAll('.category-btn').forEach(btn => {
        btn.classList.toggle('category-active', btn.dataset.target === sectionId);
    });
}

function toggleCompareMode() {
    compareMode = !compareMode;
    const btn = document.getElementById('btn-compare-mode');
    btn.setAttribute('aria-pressed', String(compareMode));
    if (compareMode) {
        btn.innerText = 'Cancel';
        btn.classList.add('text-rose-500');
        document.querySelectorAll('.compare-checkbox').forEach(c => c.classList.remove('hidden'));
    } else {
        exitCompareMode();
    }
}

function exitCompareMode() {
    compareMode = false;
    compareSelection = [];
    const btn = document.getElementById('btn-compare-mode');
    btn.innerText = 'Compare';
    btn.classList.remove('text-rose-500');
    btn.setAttribute('aria-pressed', 'false');
    document.querySelectorAll('.compare-checkbox').forEach(c => c.classList.add('hidden'));
    document.querySelectorAll('.compare-input').forEach(i => i.checked = false);
    document.getElementById('section-diff').classList.add('hidden');
    document.getElementById('section-overview').classList.remove('hidden');
    document.getElementById('section-raw').classList.remove('hidden');
    if (currentEventId) selectEvent(currentEventId);
}

function updateCompareSelection(id) {
    const checkbox = document.getElementById('check-' + id);
    if (!checkbox) return;
    if (checkbox.checked) {
        if (compareSelection.length >= 2) {
            const firstId = compareSelection.shift();
            const firstCheck = document.getElementById('check-' + firstId);
            if (firstCheck) firstCheck.checked = false;
        }
        compareSelection.push(id);
    } else {
        compareSelection = compareSelection.filter(sid => sid !== id);
    }
    if (compareSelection.length === 2) showDiff();
}

function showDiff() {
    const eventA = recentEvents.find(e => e.id === compareSelection[0]);
    const eventB = recentEvents.find(e => e.id === compareSelection[1]);
    if (!eventA || !eventB) return;
    ['section-overview', 'section-raw', 'section-headers'].forEach(s => document.getElementById(s).classList.add('hidden'));
    document.getElementById('section-diff').classList.remove('hidden');
    document.getElementById('diff-label-1').innerText = 'Event ID: ' + eventA.eventId;
    document.getElementById('diff-label-2').innerText = 'Event ID: ' + eventB.eventId;
    const c1 = document.getElementById('diff-content-1');
    const c2 = document.getElementById('diff-content-2');
    c1.textContent = prettyPayload(eventA.payload);
    c2.textContent = prettyPayload(eventB.payload);
    if (window.Prism) { Prism.highlightElement(c1); Prism.highlightElement(c2); }
}

function prettyPayload(payload) {
    try { return JSON.stringify(JSON.parse(payload), null, 2); } catch (e) { return payload; }
}

function initSearch() {
    const input = document.getElementById('json-search');
    let debounce;
    input.addEventListener('input', function () {
        clearTimeout(debounce);
        debounce = setTimeout(applySearchHighlight, 150);
    });
    input.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') { input.value = ''; applySearchHighlight(); }
    });
}

/*
 * Highlights matching Prism tokens in place. Cheaper and far simpler than
 * re-rendering the payload: highlighting only toggles a class per token.
 */
function applySearchHighlight() {
    const query = document.getElementById('json-search').value.trim().toLowerCase();
    const contentElem = document.getElementById('raw-json-content');
    const counter = document.getElementById('search-match-count');
    if (!contentElem) return;

    const tokens = contentElem.querySelectorAll('.token');
    let matchCount = 0;
    let firstMatch = null;

    tokens.forEach(token => {
        const isMatch = query.length > 0 && token.textContent.toLowerCase().includes(query);
        token.classList.toggle('search-match', isMatch);
        if (isMatch) {
            matchCount++;
            if (!firstMatch) firstMatch = token;
        }
    });

    counter.innerText = query.length === 0 ? '' : `${matchCount} match${matchCount === 1 ? '' : 'es'}`;
    if (firstMatch) firstMatch.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function copyRawJson() {
    navigator.clipboard.writeText(document.getElementById('raw-json-content').textContent)
        .then(() => showToast('JSON copied to clipboard', 'success'))
        .catch(() => showToast('Clipboard access denied', 'error'));
}
