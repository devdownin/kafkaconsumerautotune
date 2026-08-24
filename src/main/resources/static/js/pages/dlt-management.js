/*
 * Comportement de la page DLT Management : tableau paginé, sélection
 * multiple, rejeu et inspection des charges utiles.
 */
var currentInspectedId = null;
/* Survives re-renders: renderTable() rebuilds the rows from scratch. */
var selectedIds = new Set();

document.addEventListener('DOMContentLoaded', function () {
    initThemeToggle();
    initFilters();
    initWebSocket();

    document.getElementById('dlt-table-body').addEventListener('click', onTableClick);

    const editor = document.getElementById('inspect-payload-editor');
    if (editor) editor.addEventListener('input', validateJson);

    // Auto-hide the post-redirect confirmation banner.
    const successMsg = document.getElementById('success-banner');
    if (successMsg) {
        setTimeout(() => {
            successMsg.style.transition = 'opacity 0.5s ease';
            successMsg.style.opacity = '0';
            setTimeout(() => successMsg.remove(), 500);
        }, 4000);
    }

    document.addEventListener('click', function (e) {
        const wrapper = document.getElementById('export-menu-wrapper');
        if (wrapper && !wrapper.contains(e.target)) closeExportMenu();
    });
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') { closeExportMenu(); hideInspection(); }
    });
});

// SockJS/Stomp are deferred, so the connection is opened after parsing.
function initWebSocket() {
    connectWs({
        '/topic/stats': updateStats,
        '/topic/dlt': function (event) {
            dltEvents.unshift(event);
            if (dltEvents.length > 50) dltEvents.pop();
            filterEvents();
        }
    });
}

function updateStats(stats) {
    const totalDltElem = document.getElementById('total-dlt-count-24h');
    if (totalDltElem && stats.totalDlt24h != null) totalDltElem.innerText = stats.totalDlt24h.toLocaleString('en-US');
    const unresolvedElem = document.getElementById('unresolved-errors');
    if (unresolvedElem && stats.unresolvedErrors != null) unresolvedElem.innerText = stats.unresolvedErrors.toLocaleString('en-US');
    const avgResolutionElem = document.getElementById('avg-resolution-time');
    if (avgResolutionElem) avgResolutionElem.innerText = stats.avgResolutionTime;
}

/* ---------------------------------------------------------------- Filters */

function initFilters() {
    let debounce;
    const rerun = () => { clearTimeout(debounce); debounce = setTimeout(filterEvents, 150); };
    document.getElementById('filter-error-type').addEventListener('change', filterEvents);
    document.getElementById('filter-topic').addEventListener('input', rerun);
    document.getElementById('filter-partition').addEventListener('input', rerun);
}

function matchesFilters(event) {
    const errorType = document.getElementById('filter-error-type').value;
    const topic = document.getElementById('filter-topic').value.trim().toLowerCase();
    const partition = document.getElementById('filter-partition').value.trim();

    const isValidation = (event.errorMessage || '').includes('Validation');
    const matchesType = errorType === 'all' ||
        (errorType === 'parsing' && !isValidation) ||
        (errorType === 'validation' && isValidation);
    const matchesTopic = !topic || (event.originalTopic || '').toLowerCase().includes(topic);
    const matchesPartition = !partition || String(event.originalPartition) === partition;

    return matchesType && matchesTopic && matchesPartition;
}

function filterEvents() {
    renderTable(dltEvents.filter(matchesFilters));
}

/* ----------------------------------------------------------------- Table */

function formatTimestamp(value) {
    const date = new Date(value);
    if (isNaN(date.getTime())) return '-';
    const pad = (n) => String(n).padStart(2, '0');
    return date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate()) + ' ' +
           pad(date.getHours()) + ':' + pad(date.getMinutes()) + ':' + pad(date.getSeconds());
}

function severityClass(severity) {
    return severity === 'HIGH'
        ? 'bg-rose-100 dark:bg-rose-500/10 text-rose-600 dark:text-rose-400'
        : 'bg-amber-100 dark:bg-amber-500/10 text-amber-600 dark:text-amber-400';
}

/*
 * Rebuilds the body from the given events. The previous implementation
 * emitted six cells for a seven-column table (dropping the checkbox), which
 * shifted every value one column left and silently disabled bulk selection.
 */
function renderTable(events) {
    const tbody = document.getElementById('dlt-table-body');
    tbody.innerHTML = '';
    document.getElementById('showing-count').innerText = events.length;

    if (events.length === 0) {
        const empty = document.createElement('tr');
        empty.id = 'dlt-empty-row';
        empty.innerHTML = '<td colspan="7" class="px-6 py-10 text-center text-slate-500">No DLT events found.</td>';
        tbody.appendChild(empty);
        updateSelection();
        return;
    }

    const fragment = document.createDocumentFragment();
    events.forEach(event => {
        const row = document.createElement('tr');
        row.className = 'hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors group dlt-row';
        row.dataset.id = event.id;
        const checked = selectedIds.has(String(event.id)) ? ' checked' : '';
        row.innerHTML = `
            <td class="px-6 py-4">
                <input type="checkbox" value="${event.id}" name="event-selection" onchange="updateSelection()"${checked}
                       aria-label="Select event at offset ${escapeHtml(event.originalOffset)}"
                       class="event-checkbox rounded border-slate-300 dark:border-slate-700 text-primary focus:ring-primary size-4">
            </td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-500">${escapeHtml(formatTimestamp(event.dhm))}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">${escapeHtml(event.originalTopic)}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-500">${escapeHtml(event.originalPartition)}</td>
            <td class="px-6 py-4 whitespace-nowrap text-sm font-mono text-slate-500">${escapeHtml(event.originalOffset)}</td>
            <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 rounded text-[10px] font-bold uppercase ${severityClass(event.severity)}">${escapeHtml(event.severity || 'MEDIUM')}</span>
            </td>
            <td class="px-6 py-4 whitespace-nowrap text-right text-sm">
                <button type="button" class="inspect-btn text-primary font-bold hover:underline" data-id="${event.id}">Inspect</button>
            </td>
        `;
        fragment.appendChild(row);
    });
    tbody.appendChild(fragment);
    updateSelection();
}

/* One delegated listener instead of an inline handler per row. */
function onTableClick(e) {
    if (e.target.closest('input[type="checkbox"]')) return;
    const row = e.target.closest('.dlt-row');
    if (!row) return;
    inspectEvent(Number(row.dataset.id), row);
}

/* ------------------------------------------------------------ Inspection */

function inspectEvent(id, row) {
    const event = dltEvents.find(e => e.id === id);
    if (!event) return;

    currentInspectedId = id;
    document.getElementById('retry-form').action = '/dlt-management/' + event.id + '/retry';
    document.getElementById('discard-form').action = '/dlt-management/' + event.id + '/discard';

    document.getElementById('inspect-offset').innerText = event.originalOffset;
    document.getElementById('inspect-partition').innerText = event.originalPartition;
    document.getElementById('inspect-error').innerText = event.errorMessage;

    const severityElem = document.getElementById('inspect-severity');
    severityElem.innerText = event.severity || 'MEDIUM';
    severityElem.className = 'text-sm font-medium ' + (event.severity === 'HIGH' ? 'text-rose-500' : 'text-amber-500');

    let payload = event.payload;
    try {
        payload = JSON.stringify(JSON.parse(payload), null, 2);
    } catch (err) { /* leave the raw payload as-is when it is not JSON */ }

    document.getElementById('inspect-payload-editor').value = payload;
    validateJson();

    const headersContainer = document.getElementById('inspect-headers');
    headersContainer.innerHTML = '';
    if (event.headers) {
        try {
            const headersMap = JSON.parse(event.headers);
            Object.entries(headersMap).forEach(([k, v]) => {
                const badge = document.createElement('div');
                badge.className = 'flex flex-col px-2 py-1 bg-slate-100 dark:bg-slate-800 rounded border border-slate-200 dark:border-slate-700 min-w-[120px] max-w-full';
                badge.innerHTML = `
                    <span class="text-[8px] font-bold text-slate-400 uppercase truncate" title="${escapeHtml(k)}">${escapeHtml(k)}</span>
                    <span class="text-[10px] font-medium text-slate-700 dark:text-slate-300 truncate" title="${escapeHtml(v)}">${escapeHtml(v)}</span>
                `;
                headersContainer.appendChild(badge);
            });
        } catch (e) {
            headersContainer.innerHTML = '<span class="text-xs text-slate-500 italic">Error parsing headers</span>';
        }
    } else {
        headersContainer.innerHTML = '<span class="text-xs text-slate-500 italic">No headers present</span>';
    }

    const panel = document.getElementById('inspection-panel');
    panel.classList.remove('hidden');
    panel.classList.add('flex');

    document.querySelectorAll('.dlt-row').forEach(tr => tr.classList.remove('bg-primary/5', 'ring-1', 'ring-primary/20'));
    if (row) row.classList.add('bg-primary/5', 'ring-1', 'ring-primary/20');
}

function hideInspection() {
    const panel = document.getElementById('inspection-panel');
    panel.classList.add('hidden');
    panel.classList.remove('flex');
    document.querySelectorAll('.dlt-row').forEach(tr => tr.classList.remove('bg-primary/5', 'ring-1', 'ring-primary/20'));
}

function validateJson() {
    const editor = document.getElementById('inspect-payload-editor');
    const validBadge = document.getElementById('json-valid-badge');
    const invalidBadge = document.getElementById('json-invalid-badge');
    const saveBtn = document.getElementById('save-retry-btn');

    let valid = true;
    try { JSON.parse(editor.value); } catch (e) { valid = false; }

    validBadge.classList.toggle('hidden', !valid);
    invalidBadge.classList.toggle('hidden', valid);
    saveBtn.disabled = !valid;
    editor.classList.toggle('border-rose-500', !valid);
    editor.classList.toggle('text-emerald-500', valid);
    editor.classList.toggle('text-rose-500', !valid);
}

function beautifyJson() {
    const editor = document.getElementById('inspect-payload-editor');
    try {
        editor.value = JSON.stringify(JSON.parse(editor.value), null, 4);
        validateJson();
    } catch (e) {
        showToast('Cannot format: invalid JSON', 'error');
    }
}

async function saveAndRetry() {
    if (!currentInspectedId) return;
    const payload = document.getElementById('inspect-payload-editor').value;

    const saveBtn = document.getElementById('save-retry-btn');
    const originalText = saveBtn.innerHTML;
    saveBtn.disabled = true;
    saveBtn.innerHTML = '<span class="material-symbols-outlined animate-spin mr-2" aria-hidden="true">sync</span> Processing...';

    try {
        // Accept: this endpoint lives outside /api/, so the header is what
        // tells the error handler to answer with JSON rather than a page.
        const response = await fetch(`/dlt-management/${currentInspectedId}/retry-with-payload`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify({ payload: payload })
        });

        if (response.ok) {
            window.location.href = '/dlt-management?success=retried';
            return;
        }
        showToast('Error retrying event', 'error');
    } catch (e) {
        showToast('Network error while retrying event', 'error');
    }
    saveBtn.disabled = false;
    saveBtn.innerHTML = originalText;
}

function copyPayloadToClipboard() {
    const payload = document.getElementById('inspect-payload-editor').value;
    navigator.clipboard.writeText(payload)
        .then(() => showToast('Payload copied to clipboard', 'success'))
        .catch(() => showToast('Clipboard access denied', 'error'));
}

/* ------------------------------------------------------------- Selection */

function toggleSelectAll() {
    const selectAll = document.getElementById('select-all');
    document.querySelectorAll('.event-checkbox').forEach(cb => cb.checked = selectAll.checked);
    updateSelection();
}

function updateSelection() {
    selectedIds = new Set(
        Array.from(document.querySelectorAll('.event-checkbox:checked')).map(cb => cb.value)
    );
    const toolbar = document.getElementById('bulk-toolbar');
    document.getElementById('selected-count').innerText = selectedIds.size;
    toolbar.classList.toggle('hidden', selectedIds.size === 0);

    const boxes = document.querySelectorAll('.event-checkbox');
    const selectAll = document.getElementById('select-all');
    selectAll.checked = boxes.length > 0 && selectedIds.size === boxes.length;
    selectAll.indeterminate = selectedIds.size > 0 && selectedIds.size < boxes.length;
}

function clearSelection() {
    document.querySelectorAll('.event-checkbox').forEach(cb => cb.checked = false);
    updateSelection();
}

function submitBulk(action) {
    const ids = Array.from(selectedIds);
    if (ids.length === 0) return;
    if (action === 'discard' && !confirm(`Discard ${ids.length} selected event(s)? This cannot be undone.`)) return;
    if (action === 'retry' && !confirm(`Send ${ids.length} selected event(s) back to their original topics?`)) return;

    const form = document.createElement('form');
    form.method = 'POST';
    form.action = action === 'retry' ? '/dlt-management/bulk-retry' : '/dlt-management/bulk-discard';

    ids.forEach(id => {
        const input = document.createElement('input');
        input.type = 'hidden';
        input.name = 'ids';
        input.value = id;
        form.appendChild(input);
    });

    document.body.appendChild(form);
    form.submit();
}

/* ---------------------------------------------------------------- Export */

function toggleExportMenu() {
    const menu = document.getElementById('export-menu');
    const button = document.getElementById('export-menu-button');
    const open = menu.classList.toggle('hidden') === false;
    button.setAttribute('aria-expanded', String(open));
}

function closeExportMenu() {
    const menu = document.getElementById('export-menu');
    if (!menu || menu.classList.contains('hidden')) return;
    menu.classList.add('hidden');
    document.getElementById('export-menu-button').setAttribute('aria-expanded', 'false');
}

function exportDlt(format) {
    closeExportMenu();
    const dataToExport = dltEvents.filter(matchesFilters);
    if (dataToExport.length === 0) {
        showToast('No data to export', 'error');
        return;
    }

    if (format === 'csv') {
        const headers = ["ID", "Timestamp", "Topic", "Partition", "Offset", "Status", "Error Message"];
        const csvContent = [
            headers.join(","),
            ...dataToExport.map(e => [
                e.id,
                e.dhm,
                e.originalTopic,
                e.originalPartition,
                e.originalOffset,
                e.status,
                `"${(e.errorMessage || '').replace(/"/g, '""')}"`
            ].join(","))
        ].join("\n");
        downloadFile(csvContent, `dlt_export_${Date.now()}.csv`, 'text/csv');
    } else {
        downloadFile(JSON.stringify(dataToExport, null, 4), `dlt_export_${Date.now()}.json`, 'application/json');
    }
    showToast(`Exported ${dataToExport.length} items to ${format.toUpperCase()}`, 'success');
}

function downloadFile(content, fileName, contentType) {
    const blob = new Blob([content], { type: contentType });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = fileName;
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    // Release the blob instead of leaking it for the lifetime of the page.
    setTimeout(() => URL.revokeObjectURL(url), 0);
}
