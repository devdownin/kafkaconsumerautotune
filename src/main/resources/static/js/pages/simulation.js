/*
 * Comportement de la page Traffic Simulator : formulaire de scénario,
 * préréglages et suivi de progression.
 */
let stompClient;
let donutChart;

const presets = {
    nominal: { total: 5000, tput: 200, delay: 0, err: 0, malf: 0, dup: 0 },
    stress: { total: 20000, tput: 1000, delay: 0, err: 2, malf: 1, dup: 0 },
    degraded: { total: 2000, tput: 50, delay: 0, err: 20, malf: 10, dup: 5 },
    idempotency: { total: 1000, tput: 100, delay: 0, err: 0, malf: 0, dup: 80 }
};

function applyPreset(id) {
    const p = presets[id];
    document.getElementById('totalMessages').value = p.total;
    document.getElementById('targetThroughput').value = p.tput;
    document.getElementById('delayBetweenMessagesMs').value = p.delay;

    document.getElementById('errorPercentage').value = p.err;
    document.getElementById('val-error').innerText = p.err + '%';

    document.getElementById('malformedJsonPercentage').value = p.malf;
    document.getElementById('val-malformed').innerText = p.malf + '%';

    document.getElementById('duplicatePercentage').value = p.dup;
    document.getElementById('val-duplicate').innerText = p.dup + '%';
}

function initWs() {
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    stompClient.debug = null;
    stompClient.connect({}, () => {
        updateWsStatus(true);
        stompClient.subscribe('/topic/simulation', (msg) => {
            updateUI(JSON.parse(msg.body));
        });
    }, () => updateWsStatus(false));
}

function intValue(id, fallback) {
    const parsed = parseInt(document.getElementById(id).value, 10);
    return Number.isFinite(parsed) ? parsed : fallback;
}

/*
 * Both calls used to be fire-and-forget: a rejected request or a server error
 * left the UI showing "Idle" with no explanation.
 */
async function startSimulation() {
    const total = intValue('totalMessages', 0);
    if (total <= 0) {
        showToast('Total messages must be greater than zero', 'error');
        document.getElementById('totalMessages').focus();
        return;
    }

    const data = {
        totalMessages: total,
        errorPercentage: intValue('errorPercentage', 0),
        malformedJsonPercentage: intValue('malformedJsonPercentage', 0),
        delayBetweenMessagesMs: intValue('delayBetweenMessagesMs', 0),
        duplicatePercentage: intValue('duplicatePercentage', 0),
        targetThroughputMsgPerSec: intValue('targetThroughput', 0)
    };

    const startBtn = document.getElementById('start-btn');
    startBtn.disabled = true;
    try {
        const response = await fetch('/api/simulation/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify(data)
        });
        if (!response.ok) {
            showToast(await serverMessage(response, 'The simulation could not be started'), 'error');
            startBtn.disabled = false;
            return;
        }
        showToast(`Simulation started: ${total.toLocaleString()} messages`, 'success');
    } catch (e) {
        showToast('Network error while starting the simulation', 'error');
        startBtn.disabled = false;
    }
}

async function stopSimulation() {
    try {
        const response = await fetch('/api/simulation/stop', {
            method: 'POST',
            headers: { 'Accept': 'application/json' }
        });
        if (!response.ok) {
            showToast(await serverMessage(response, 'The simulation could not be stopped'), 'error');
            return;
        }
        showToast('Simulation stopped', 'info');
    } catch (e) {
        showToast('Network error while stopping the simulation', 'error');
    }
}

/*
 * Turns the ApiError body into something worth reading. Server-side
 * validation reports the offending fields, which is more useful than a
 * generic failure notice.
 */
async function serverMessage(response, fallback) {
    try {
        const body = await response.json();
        if (body.fieldErrors) {
            const details = Object.entries(body.fieldErrors)
                .map(([field, problem]) => `${field} ${problem}`)
                .join(', ');
            if (details) return `Invalid parameters: ${details}`;
        }
        return body.message || fallback;
    } catch (e) {
        return fallback;
    }
}

function updateUI(status) {
    if (!status) return;
    document.getElementById('stat-processed').textContent = status.processedMessages.toLocaleString();
    document.getElementById('stat-total').textContent = status.totalMessages.toLocaleString();
    document.getElementById('stat-valid').textContent = status.sentValid.toLocaleString();
    document.getElementById('stat-error').textContent = status.sentError.toLocaleString();
    document.getElementById('stat-malformed').textContent = status.sentMalformed.toLocaleString();
    document.getElementById('stat-duplicate').textContent = status.sentDuplicate.toLocaleString();

    const progress = status.totalMessages > 0 ? (status.processedMessages / status.totalMessages * 100) : 0;
    document.getElementById('progress-bar').style.width = progress + '%';
    document.getElementById('progress-text').textContent = Math.round(progress) + '%';

    if (status.running) {
        document.getElementById('sim-badge').className = 'flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-500 text-[10px] font-bold uppercase tracking-wider';
        document.getElementById('sim-dot').className = 'size-1.5 rounded-full bg-emerald-500 animate-pulse';
        document.getElementById('sim-status-text').textContent = 'Running';
        document.getElementById('start-btn').disabled = true;
        document.getElementById('stop-btn').disabled = false;
        document.getElementById('simulation-chart-card').classList.remove('hidden');

        const elapsed = Math.round((Date.now() - status.startTime) / 1000);
        document.getElementById('stat-elapsed').textContent = elapsed + 's';

        updateDonut(status);
    } else {
        document.getElementById('sim-badge').className = 'flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-slate-500/10 text-slate-500 text-[10px] font-bold uppercase tracking-wider';
        // was bg-slate-50: a near-white dot, invisible on the light background
        document.getElementById('sim-dot').className = 'size-1.5 rounded-full bg-slate-500';
        document.getElementById('sim-status-text').textContent = 'Idle';
        document.getElementById('start-btn').disabled = false;
        document.getElementById('stop-btn').disabled = true;
    }
}

function updateDonut(status) {
    const data = [status.sentValid, status.sentError, status.sentMalformed, status.sentDuplicate];
    if (!donutChart) {
        const options = {
            series: data,
            chart: { type: 'donut', height: '100%' },
            labels: ['Valid', 'Business Error', 'Malformed', 'Duplicate'],
            colors: ['#10b981', '#f59e0b', '#f43f5e', '#3b82f6'],
            legend: { position: 'bottom', labels: { colors: '#94a3b8' } },
            dataLabels: { enabled: false },
            stroke: { show: false },
            tooltip: { theme: 'dark' }
        };
        donutChart = new ApexCharts(document.querySelector("#simulation-donut"), options);
        donutChart.render();
    } else {
        donutChart.updateSeries(data);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    initThemeToggle();
    initWs();
    // Initial status fetch
    fetch('/api/simulation/status')
        .then(res => res.ok ? res.json() : null)
        .then(updateUI)
        .catch(() => showToast('Could not read the current simulation status', 'error'));
});
