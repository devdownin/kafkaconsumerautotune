/*
 * Comportement de la page Kafka Optimizer : chronologie des réglages
 * appliqués par l'auto-tuning.
 */
var timelineChart;

function initTimelineChart() {
    var options = {
        series: [
            {
                name: 'Throughput (Msg/s)',
                type: 'area',
                data: initialStats.successThroughput || []
            },
            {
                name: 'max.poll.records',
                type: 'line',
                data: initialStats.maxPollRecordsHistory || []
            },
            {
                name: 'concurrency',
                type: 'line',
                data: initialStats.concurrencyHistory || []
            }
        ],
        chart: {
            height: 350,
            type: 'line',
            stacked: false,
            toolbar: { show: false },
            animations: { enabled: true, easing: 'linear', dynamicAnimation: { speed: 1000 } }
        },
        stroke: {
            width: [0, 3, 3],
            curve: 'smooth'
        },
        plotOptions: {
            bar: { columnWidth: '50%' }
        },
        fill: {
            type: 'gradient',
            gradient: {
                inverseColors: false,
                shade: 'light',
                type: "vertical",
                opacityFrom: [0.35, 1, 1],
                opacityTo: [0.1, 1, 1],
                stops: [0, 90, 100]
            }
        },
        colors: ['#135bec', '#10b981', '#f59e0b'],
        labels: initialStats.timestamps || [],
        xaxis: {
            type: 'datetime',
            labels: {
                style: { colors: '#94a3b8', fontSize: '10px' },
                datetimeUTC: false,
                format: 'HH:mm:ss'
            }
        },
        yaxis: [
            {
                title: { text: 'Throughput (Msg/s)', style: { color: '#135bec' } },
                labels: {
                    style: { colors: '#94a3b8' },
                    formatter: function(val) { return val.toFixed(1); }
                }
            },
            {
                opposite: true,
                title: { text: 'max.poll.records', style: { color: '#10b981' } },
                labels: {
                    style: { colors: '#94a3b8' }
                }
            },
            {
                opposite: true,
                offsetY: 0,
                title: { text: 'concurrency', style: { color: '#f59e0b' } },
                labels: {
                    style: { colors: '#94a3b8' },
                    offsetX: -10
                }
            }
        ],
        grid: { borderColor: '#1e293b', strokeDashArray: 4 },
        tooltip: {
            theme: 'dark',
            x: { format: 'HH:mm:ss' }
        },
        legend: {
            show: true,
            position: 'top',
            horizontalAlign: 'right',
            labels: { colors: '#94a3b8' }
        }
    };

    timelineChart = new ApexCharts(document.querySelector("#optimizer-timeline-chart"), options);
    timelineChart.render();
}

// WebSocket setup. Runs on DOMContentLoaded because SockJS/Stomp are deferred.
function initWebSocket() {
    var socket = new SockJS('/ws');
    var stompClient = Stomp.over(socket);
    stompClient.debug = null;

    stompClient.connect({}, function (frame) {
        updateWsStatus(true);
        stompClient.subscribe('/topic/stats', function (statsMessage) {
            updateTimeline(JSON.parse(statsMessage.body));
        });
    }, function(error) {
        updateWsStatus(false);
    });
}

function updateTimeline(stats) {
    if (!timelineChart) return;
    // Axis and series in one call: two calls meant two full chart re-renders.
    timelineChart.updateOptions({
        xaxis: { categories: stats.timestamps },
        series: [
            { name: 'Throughput (Msg/s)', data: stats.successThroughput },
            { name: 'max.poll.records', data: stats.maxPollRecordsHistory },
            { name: 'concurrency', data: stats.concurrencyHistory }
        ]
    }, false, false);
}

document.addEventListener('DOMContentLoaded', function() {
    initThemeToggle();
    initTimelineChart();
    initWebSocket();
});
