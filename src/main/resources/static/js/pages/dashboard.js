/*
 * Comportement de la page Tableau de bord : graphiques ApexCharts,
 * abonnements temps réel et rendu du flux d'événements.
 */
// WebSocket setup. Runs on DOMContentLoaded because SockJS/Stomp are deferred.
function initWebSocket() {
    var socket = new SockJS('/ws');
    var stompClient = Stomp.over(socket);
    stompClient.debug = null;

    stompClient.connect({}, function (frame) {
        updateWsStatus(true);
        initSystemNotifications(stompClient);

        stompClient.subscribe('/topic/events', function (eventMessage) {
            var events = JSON.parse(eventMessage.body);
            updateDashboard(events);
        });
        stompClient.subscribe('/topic/stats', function (statsMessage) {
            var stats = JSON.parse(statsMessage.body);
            updateStats(stats);
        });
        stompClient.subscribe('/topic/dlt', function (dltMessage) {
            var dltEvent = JSON.parse(dltMessage.body);
            updateDashboardWithDlt(dltEvent);
            pulseDltCounter();
        });
    }, function(error) {
        updateWsStatus(false);
    });
}

var throughputChart, donutChart;
var currentDuration = '10m';

function initCharts() {
    const is24h = currentDuration === '24h';
    const timestamps = is24h ? [] : (initialStats.timestamps || []);

    var throughputOptions = {
        series: [
            {
                name: 'Success (Msg/s)',
                type: 'bar',
                data: is24h ? (initialStats.throughput24h || []) : (initialStats.successThroughput || [])
            },
            {
                name: 'DLT (Err/s)',
                type: 'bar',
                data: is24h ? [] : (initialStats.errorThroughput || [])
            },
            {
                name: 'Retries (Msg/s)',
                type: 'bar',
                data: is24h ? [] : (initialStats.retryThroughput || [])
            }
        ],
        chart: {
            height: 300,
            type: 'bar',
            stacked: true,
            toolbar: { show: false },
            animations: { enabled: true, easing: 'linear', dynamicAnimation: { speed: 1000 } }
        },
        plotOptions: {
            bar: {
                columnWidth: '95%',
                borderRadius: 2
            }
        },
        dataLabels: { enabled: false },
        stroke: {
            curve: 'smooth',
            width: [0, 0, 0],
            dashArray: [0, 0, 0, 0]
        },
        colors: ['#135bec', '#f43f5e', '#f59e0b'],
        fill: {
            opacity: [0.85, 0.85, 0.85]
        },
        grid: { borderColor: '#1e293b', strokeDashArray: 4 },
        xaxis: {
            type: 'datetime',
            categories: timestamps,
            labels: {
                show: !is24h,
                style: { colors: '#94a3b8', fontSize: '10px' },
                datetimeUTC: false,
                format: 'HH:mm:ss'
            },
            axisBorder: { show: false },
            axisTicks: { show: false }
        },
        yaxis: [
            {
                seriesName: 'Success (Msg/s)',
                title: { text: 'Throughput (Msg/s)', style: { color: '#135bec', fontWeight: 600 } },
                labels: {
                    style: { colors: '#94a3b8' },
                    formatter: function(val) { return val.toFixed(1); }
                }
            },
            {
                seriesName: 'Success (Msg/s)',
                show: false
            },
            {
                seriesName: 'Success (Msg/s)',
                show: false
            }
        ],
        tooltip: {
            theme: 'dark',
            x: { format: 'HH:mm:ss' }
        },
        legend: {
            show: true,
            position: 'top',
            horizontalAlign: 'right',
            labels: { colors: '#94a3b8' },
            itemMargin: { horizontal: 10, vertical: 5 }
        }
    };

    throughputChart = new ApexCharts(document.querySelector("#throughput-chart"), throughputOptions);
    throughputChart.render();

    var donutOptions = {
        series: [initialStats.successCount || 0, initialStats.errorCount || 0, initialStats.retryCount || 0],
        chart: { type: 'donut', height: 250 },
        labels: ['Success', 'DLT / Errors', 'Retries'],
        colors: ['#135bec', '#f43f5e', '#f59e0b'],
        plotOptions: {
            pie: {
                donut: {
                    size: '75%',
                    labels: {
                        show: true,
                        name: { show: true, color: '#94a3b8' },
                        value: { show: true, color: '#f8fafc' },
                        total: { show: true, label: 'Success Rate', formatter: function() { return (initialStats.successRate || 0).toFixed(2) + '%' } }
                    }
                }
            }
        },
        dataLabels: { enabled: false },
        legend: { show: false },
        stroke: { show: false },
        tooltip: { theme: 'dark' }
    };

    donutChart = new ApexCharts(document.querySelector("#status-donut-chart"), donutOptions);
    donutChart.render();
}

document.addEventListener('DOMContentLoaded', function() {
    initCharts();
    initThemeToggle();
    initSearch();
    initWebSocket();
});

function updateThroughputDuration(duration) {
    currentDuration = duration;
    if (throughputChart) {
        const is24h = duration === '24h';
        throughputChart.updateOptions({
            xaxis: { labels: { show: !is24h } }
        });
        throughputChart.updateSeries([
            { data: is24h ? initialStats.throughput24h : initialStats.successThroughput },
            { data: is24h ? [] : initialStats.errorThroughput },
            { data: is24h ? [] : initialStats.retryThroughput }
        ]);
    }
}


function setText(id, value) {
    const elem = document.getElementById(id);
    if (elem) elem.innerText = value;
}

function formatCount(value) {
    return (value == null ? 0 : value).toLocaleString('en-US');
}

function updateStats(stats) {
    initialStats = stats;

    // Update Circuit Breaker Alert visibility
    const cbAlert = document.getElementById('cb-alert');
    if (cbAlert) {
        if (stats.circuitBreakerStatus === 'OPEN') {
            cbAlert.classList.remove('hidden');
        } else {
            cbAlert.classList.add('hidden');
        }
    }

    const successRate = stats.successRate || 0;
    setText('total-processed-count', formatCount(stats.totalProcessed));
    setText('success-rate', successRate.toFixed(2) + '%');
    const rateBar = document.getElementById('success-rate-bar');
    if (rateBar) rateBar.style.width = successRate + '%';
    setText('dlt-count', formatCount(stats.dltCount));
    setText('consumer-lag', formatCount(stats.consumerLag));
    // The PID-tuned values are shown on the lag card but were never refreshed.
    if (stats.maxPollRecords != null) setText('max-poll-val', stats.maxPollRecords);
    if (stats.concurrency != null) setText('concurrency-val', stats.concurrency);
    if (stats.fetchMaxWaitMs != null) setText('fetch-wait-val', stats.fetchMaxWaitMs + 'ms');

    if (throughputChart) {
        const is24h = currentDuration === '24h';
        // Single updateOptions call: passing the series along with the axis
        // avoids the second full re-render updateSeries would trigger.
        throughputChart.updateOptions({
            xaxis: { categories: is24h ? [] : stats.timestamps },
            series: [
                { name: 'Success (Msg/s)', data: is24h ? stats.throughput24h : stats.successThroughput },
                { name: 'DLT (Err/s)', data: is24h ? [] : stats.errorThroughput },
                { name: 'Retries (Msg/s)', data: is24h ? [] : stats.retryThroughput }
            ]
        }, false, false);
    }
    if (donutChart) {
        donutChart.updateSeries([stats.successCount, stats.errorCount, stats.retryCount]);
    }
    setText('donut-success-count', formatCount(stats.successCount));
    setText('donut-error-count', formatCount(stats.errorCount));
    setText('donut-retry-count', formatCount(stats.retryCount));
}

function updateDashboard(events) {
    var tableBody = document.getElementById('events-table-body');
    var emptyState = document.getElementById('empty-state');
    if (emptyState && events.length > 0) emptyState.style.display = 'none';

    events.forEach(function(event) {
        var row = document.createElement('tr');
        row.className = 'hover:bg-slate-50/50 dark:hover:bg-slate-800/50 transition-colors animate-pulse';
        var date = new Date(event.createdAt);
        var formattedDate = date.toISOString().replace('T', ' ').substring(0, 19);

        const cells = [
            { html: '<span class="px-2 py-0.5 rounded text-[10px] font-bold uppercase bg-emerald-500/10 text-emerald-500 border border-emerald-500/20">Success</span>', class: 'px-6 py-4' },
            { text: event.kafkaTopic || '-', class: 'px-6 py-4 text-sm font-medium' },
            { text: event.eventId, class: 'px-6 py-4 text-sm font-bold text-primary' },
            { text: event.kafkaOffset != null ? event.kafkaOffset : '-', class: 'px-6 py-4 text-sm font-mono text-slate-500' },
            { text: event.kafkaPartition != null ? event.kafkaPartition : '-', class: 'px-6 py-4 text-sm font-medium' },
            { text: event.id, class: 'px-6 py-4 text-sm font-mono text-slate-500' },
            { text: formattedDate, class: 'px-6 py-4 text-sm text-slate-500 text-right' }
        ];

        cells.forEach(c => {
            const td = document.createElement('td');
            td.className = c.class;
            if (c.html) td.innerHTML = c.html;
            else td.textContent = c.text;
            row.appendChild(td);
        });

        if (tableBody.firstChild) tableBody.insertBefore(row, tableBody.firstChild);
        else tableBody.appendChild(row);
        applyFilterToRow(row);
        setTimeout(function() { row.classList.remove('animate-pulse'); }, 2000);
        if (tableBody.children.length > 15) tableBody.removeChild(tableBody.lastChild);
    });
}

function updateDashboardWithDlt(event) {
    var tableBody = document.getElementById('events-table-body');
    var emptyState = document.getElementById('empty-state');
    if (emptyState) emptyState.style.display = 'none';

    var row = document.createElement('tr');
    row.className = 'bg-rose-500/5 hover:bg-rose-500/10 transition-colors animate-pulse';
    var date = new Date(event.dhm);
    var formattedDate = date.toISOString().replace('T', ' ').substring(0, 19);

    const cells = [
        { html: '<span class="px-2 py-0.5 rounded text-[10px] font-bold uppercase bg-rose-500/10 text-rose-500 border border-rose-500/20">DLT</span>', class: 'px-6 py-4' },
        { text: event.originalTopic || '-', class: 'px-6 py-4 text-sm font-medium' },
        { text: event.errorMessage, class: 'px-6 py-4 text-sm font-bold text-rose-500 truncate max-w-[200px]', title: event.errorMessage },
        { text: event.originalOffset != null ? event.originalOffset : '-', class: 'px-6 py-4 text-sm font-mono text-slate-500' },
        { text: event.originalPartition != null ? event.originalPartition : '-', class: 'px-6 py-4 text-sm font-medium' },
        { text: event.id, class: 'px-6 py-4 text-sm font-mono text-slate-500' },
        { text: formattedDate, class: 'px-6 py-4 text-sm text-slate-500 text-right' }
    ];

    cells.forEach(c => {
        const td = document.createElement('td');
        td.className = c.class;
        if (c.html) td.innerHTML = c.html;
        else td.textContent = c.text;
        if (c.title) td.title = c.title;
        row.appendChild(td);
    });

    if (tableBody.firstChild) tableBody.insertBefore(row, tableBody.firstChild);
    else tableBody.appendChild(row);
    applyFilterToRow(row);

    setTimeout(function() { row.classList.remove('animate-pulse'); }, 2000);
    if (tableBody.children.length > 15) tableBody.removeChild(tableBody.lastChild);
}

function pulseDltCounter() {
    const counter = document.getElementById('dlt-count');
    const dot = document.getElementById('dlt-pulse-dot');
    if (counter) {
        counter.classList.add('text-rose-500', 'scale-110', 'transition-transform');
        if (dot) dot.classList.remove('hidden');
        setTimeout(() => {
            counter.classList.remove('scale-110');
            if (dot) setTimeout(() => dot.classList.add('hidden'), 5000);
        }, 200);
    }
}

var currentQuery = '';

/*
 * Rows cache their searchable text in a data attribute. The previous version
 * read row.innerText on every keystroke, which forces a synchronous layout
 * per row; textContent + caching keeps filtering off the layout path.
 */
function rowSearchText(row) {
    if (!row.dataset.search) {
        row.dataset.search = row.textContent.toLowerCase().replace(/\s+/g, ' ');
    }
    return row.dataset.search;
}

function applyFilterToRow(row) {
    row.style.display = !currentQuery || rowSearchText(row).includes(currentQuery) ? '' : 'none';
}

function filterDashboardTable() {
    const input = document.getElementById('dashboard-search');
    currentQuery = input ? input.value.trim().toLowerCase() : '';
    document.querySelectorAll('#events-table-body tr:not(#empty-state)').forEach(applyFilterToRow);
}

function clearSearch() {
    const input = document.getElementById('dashboard-search');
    if (input) input.value = '';
    toggleSearchClear();
    filterDashboardTable();
    if (input) input.focus();
}

function toggleSearchClear() {
    const input = document.getElementById('dashboard-search');
    const btn = document.getElementById('search-clear');
    if (!input || !btn) return;
    btn.classList.toggle('hidden', input.value.length === 0);
}

function initSearch() {
    const input = document.getElementById('dashboard-search');
    if (!input) return;
    let debounce;
    input.addEventListener('input', function () {
        toggleSearchClear();
        clearTimeout(debounce);
        debounce = setTimeout(filterDashboardTable, 150);
    });
    input.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') clearSearch();
    });
}
