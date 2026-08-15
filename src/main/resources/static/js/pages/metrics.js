/*
 * Comportement de la page Application Metrics : graphiques temps réel
 * et filtrage de la liste des métriques.
 */
function initWebSocket() {
    // Le bandeau #ws-alert est piloté par updateWsStatus, comme l'indicateur
    // du pied de page : une seule chose à tenir à jour.
    connectWs({
        '/topic/metrics': updateJvmUI,
        '/topic/metrics-live': updateLiveMetrics
    });
}

var sparklineCharts = {};

function updateLiveMetrics(metrics) {
    metrics.forEach(metric => {
        const row = document.getElementById('metric-' + metric.name);
        if (!row) return;

        // Update Value
        const valueElem = row.querySelector('.metric-value');
        if (valueElem) {
            valueElem.innerText = metric.value;
            valueElem.className = 'metric-value text-xs font-bold ' +
                (metric.status === 'CRITICAL' ? 'text-rose-500' : (metric.status === 'WARNING' ? 'text-amber-500' : 'text-primary'));
        }

        // Update Trend
        const trendElem = row.querySelector('.metric-trend');
        if (trendElem) {
            let icon = 'remove';
            let color = 'text-slate-400';
            if (metric.trend === 'UP') { icon = 'trending_up'; color = 'text-rose-500'; }
            else if (metric.trend === 'DOWN') { icon = 'trending_down'; color = 'text-emerald-500'; }
            trendElem.innerHTML = `<span class="material-symbols-outlined ${color} text-sm">${icon}</span>`;
        }

        // Update Sparkline
        if (sparklineCharts[metric.name]) {
            sparklineCharts[metric.name].updateSeries([{ data: metric.history }]);
        }
    });
}

function updateJvmUI(stats) {
    const usedMb = Math.round((stats.heapUsed || 0) / 1024 / 1024);
    const maxMb = Math.round((stats.heapMax || 0) / 1024 / 1024);
    const commMb = Math.round((stats.heapCommitted || 0) / 1024 / 1024);
    const pct = maxMb > 0 ? (usedMb * 100 / maxMb) : 0;
    const processCpu = stats.processCpuLoad || 0;
    const systemCpu = stats.systemCpuLoad || 0;

    const heapSum = document.getElementById('heap-summary');
    if (heapSum) heapSum.innerText = usedMb + ' MB';
    const heapUsedVal = document.getElementById('heap-used-val');
    if (heapUsedVal) heapUsedVal.innerText = usedMb + ' MB / ' + maxMb + ' MB';
    const heapCommVal = document.getElementById('heap-committed-val');
    if (heapCommVal) heapCommVal.innerText = commMb + ' MB';
    const heapMaxVal = document.getElementById('heap-max-val');
    if (heapMaxVal) heapMaxVal.innerText = maxMb + ' MB';
    const heapProg = document.getElementById('heap-progress');
    if (heapProg) heapProg.style.width = pct + '%';

    const threadsSum = document.getElementById('threads-summary');
    if (threadsSum) threadsSum.innerText = stats.threadCount;
    const threadCountVal = document.getElementById('thread-count-val');
    if (threadCountVal) threadCountVal.innerText = stats.threadCount;
    const peakThreadVal = document.getElementById('peak-thread-val');
    if (peakThreadVal) peakThreadVal.innerText = stats.peakThreadCount;

    const cpuSum = document.getElementById('cpu-summary');
    if (cpuSum) cpuSum.innerText = processCpu.toFixed(1) + '%';
    const procCpuVal = document.getElementById('process-cpu-val');
    if (procCpuVal) procCpuVal.innerText = processCpu.toFixed(1) + '%';
    const procCpuProg = document.getElementById('process-cpu-progress');
    if (procCpuProg) procCpuProg.style.width = processCpu + '%';
    const sysCpuVal = document.getElementById('system-cpu-val');
    if (sysCpuVal) sysCpuVal.innerText = systemCpu.toFixed(1) + '%';
    const sysCpuProg = document.getElementById('system-cpu-progress');
    if (sysCpuProg) sysCpuProg.style.width = systemCpu + '%';
}

document.addEventListener('DOMContentLoaded', function() {
    initThemeToggle();
    initMetricFilters();
    initSparklines();
    initWebSocket();
});

function initSparklines() {
    document.querySelectorAll('.metric-sparkline').forEach(el => {
        const row = el.closest('.metric-row');
        const metricName = row ? row.getAttribute('data-name') : null;
        const historyStr = el.getAttribute('data-history');
        if (!historyStr) return;

        try {
            const history = JSON.parse(historyStr);
            if (!history || history.length < 2) {
                el.innerHTML = '<span class="text-[8px] text-slate-400 italic">No trend data</span>';
                return;
            }

            const options = {
                series: [{ data: history }],
                chart: {
                    id: metricName,
                    type: 'area',
                    height: 32,
                    width: 100,
                    sparkline: { enabled: true },
                    animations: { enabled: false }
                },
                stroke: { curve: 'smooth', width: 2 },
                fill: {
                    type: 'gradient',
                    gradient: { shadeIntensity: 1, opacityFrom: 0.45, opacityTo: 0.1 }
                },
                colors: ['#135bec'],
                tooltip: { enabled: false }
            };

            const chart = new ApexCharts(el, options);
            chart.render();
            if (metricName) {
                sparklineCharts[metricName] = chart;
            }
        } catch (e) {
            console.error("Error parsing history for sparkline", e);
        }
    });
}

function initMetricFilters() {
    const searchInput = document.getElementById('metric-search');
    const filterSelect = document.getElementById('metric-filter');

    function filterTable() {
        const rows = document.querySelectorAll('#metrics-table-body tr:not(.no-results-row)');
        const searchTerm = searchInput.value.toLowerCase();
        const filterValue = filterSelect.value;
        let visibleCount = 0;

        rows.forEach(row => {
            const name = (row.getAttribute('data-name') || "").toLowerCase();
            const isApp = row.getAttribute('data-app-specific') === 'true';

            const matchesSearch = name.includes(searchTerm);
            let matchesFilter = true;

            if (filterValue === 'app') matchesFilter = isApp;
            if (filterValue === 'jvm') matchesFilter = !isApp;
            if (filterValue === 'kafka') matchesFilter = name.includes('kafka');

            if (matchesSearch && matchesFilter) {
                row.style.display = 'table-row';
                visibleCount++;
            } else {
                row.style.display = 'none';
            }
        });

        const noResultsRow = document.querySelector('.no-results-row');
        if (noResultsRow) {
            noResultsRow.style.display = visibleCount === 0 ? 'table-row' : 'none';
        }
    }

    if (searchInput && filterSelect) {
        // Debounced: the table can hold hundreds of meter rows.
        let debounce;
        searchInput.addEventListener('input', function () {
            clearTimeout(debounce);
            debounce = setTimeout(filterTable, 150);
        });
        filterSelect.addEventListener('change', filterTable);
        filterTable(); // Initial filter
    }
}

let sortDirections = [true, true, true, true]; // true for asc
function sortMetricsTable(colIndex) {
    const tbody = document.getElementById('metrics-table-body');
    const rows = Array.from(tbody.querySelectorAll('.metric-row'));
    const noResultsRow = tbody.querySelector('.no-results-row');
    const asc = sortDirections[colIndex];

    const sortedRows = rows.sort((a, b) => {
        let valA, valB;

        if (colIndex === 2) { // Value column (numeric)
            valA = parseFloat(a.cells[colIndex].textContent.trim().replace(',', ''));
            valB = parseFloat(b.cells[colIndex].textContent.trim().replace(',', ''));
            if (isNaN(valA)) valA = 0;
            if (isNaN(valB)) valB = 0;
            return asc ? valA - valB : valB - valA;
        } else {
            valA = a.cells[colIndex].textContent.trim().toLowerCase();
            valB = b.cells[colIndex].textContent.trim().toLowerCase();
            return asc ? valA.localeCompare(valB) : valB.localeCompare(valA);
        }
    });

    // Clear and re-append
    rows.forEach(row => row.remove());
    sortedRows.forEach(row => {
        if (noResultsRow) {
            tbody.insertBefore(row, noResultsRow);
        } else {
            tbody.appendChild(row);
        }
    });

    sortDirections[colIndex] = !asc;

    // Update the sort affordance and expose the order via aria-sort.
    document.querySelectorAll('.sortable-th').forEach(th => {
        const icon = th.querySelector('.sort-icon');
        if (Number(th.dataset.col) === colIndex) {
            th.setAttribute('aria-sort', asc ? 'ascending' : 'descending');
            icon.innerText = asc ? 'arrow_drop_up' : 'arrow_drop_down';
            icon.classList.remove('opacity-0');
            icon.classList.add('opacity-100', 'text-primary');
        } else {
            th.setAttribute('aria-sort', 'none');
            icon.innerText = 'unfold_more';
            icon.classList.add('opacity-0');
            icon.classList.remove('opacity-100', 'text-primary');
        }
    });
}
