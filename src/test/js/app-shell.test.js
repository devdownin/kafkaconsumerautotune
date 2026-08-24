/*
 * Tests de connectWs — la reconnexion temps réel.
 *
 * Le code du tableau de bord tourne dans un navigateur ; il est ici chargé
 * dans un contexte `vm` muni d'un faux DOM, d'un faux SockJS/Stomp et d'une
 * horloge simulée. L'isolation par `vm` évite de remplacer setTimeout dans
 * le processus du lanceur de tests, qui s'en sert lui-même.
 *
 * Ce qui est vérifié ici ne l'est nulle part ailleurs : une coupure de
 * WebSocket ne se produit pas dans un test MockMvc.
 */
const { test, beforeEach } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const STATIC_JS = path.join(__dirname, '..', '..', 'main', 'resources', 'static', 'js');

/** Monte un contexte de page neuf et y charge la coquille. */
function loadShell() {
    let now = 0;
    const timers = [];
    const elements = {};
    const listeners = {};
    const clients = [];

    function element(id) {
        const classes = new Set();
        return {
            id, classes, innerText: '',
            classList: {
                add: (...c) => c.forEach((x) => classes.add(x)),
                remove: (...c) => c.forEach((x) => classes.delete(x)),
                toggle: (c, on) => (on ? classes.add(c) : classes.delete(c)),
                contains: (c) => classes.has(c)
            },
            setAttribute() {},
            addEventListener() {}
        };
    }
    for (const id of ['ws-status-dot', 'ws-status-text', 'ws-alert', 'toast-container']) {
        elements[id] = element(id);
    }

    const sandbox = {
        // Un identifiant de minuteur démarré à zéro : un navigateur n'en
        // renvoie jamais, mais le code ne doit pas dépendre de sa véracité.
        setTimeout: (fn, ms) => {
            const timer = { at: now + ms, fn, id: timers.length };
            timers.push(timer);
            return timer.id;
        },
        clearTimeout: (id) => {
            const timer = timers.find((t) => t.id === id);
            if (timer) timer.cancelled = true;
        },
        setInterval: () => 0,
        clearInterval: () => {},
        requestAnimationFrame: (fn) => fn(),
        document: {
            visibilityState: 'visible',
            getElementById: (id) => elements[id] || null,
            addEventListener: (event, fn) => {
                (listeners[event] = listeners[event] || []).push(fn);
            },
            createElement: () => ({ textContent: '', innerHTML: '' })
        },
        SockJS: function SockJS(url) {
            this.url = url;
        },
        Stomp: {
            over(socket) {
                const client = {
                    socket,
                    topics: [],
                    disconnected: false,
                    connect(headers, onOk, onErr) {
                        this.succeed = () => onOk({});
                        this.fail = () => onErr({});
                    },
                    subscribe(topic, fn) {
                        this.topics.push(topic);
                        this.handlers = this.handlers || {};
                        this.handlers[topic] = fn;
                        return { unsubscribe() {} };
                    },
                    disconnect() {
                        this.disconnected = true;
                    }
                };
                clients.push(client);
                return client;
            }
        }
    };

    vm.createContext(sandbox);
    for (const file of ['notifications.js', 'app-shell.js']) {
        vm.runInContext(fs.readFileSync(path.join(STATIC_JS, file), 'utf8'), sandbox, { filename: file });
    }

    return {
        sandbox,
        clients,
        latest: () => clients[clients.length - 1],
        text: () => elements['ws-status-text'].innerText,
        bannerHidden: () => elements['ws-alert'].classes.has('hidden'),
        becomeVisible: () => (listeners['visibilitychange'] || []).forEach((fn) => fn()),
        /** Avance l'horloge et déclenche les minuteurs échus. */
        advance(ms) {
            now += ms;
            for (const timer of [...timers]) {
                if (!timer.cancelled && !timer.fired && timer.at <= now) {
                    timer.fired = true;
                    timer.fn();
                }
            }
        },
        /** Avance jusqu'à l'ouverture d'une nouvelle connexion. Renvoie le délai écoulé. */
        waitForReconnect(limit = 120000) {
            const before = clients.length;
            let waited = 0;
            while (clients.length === before && waited < limit) {
                this.advance(250);
                waited += 250;
            }
            return clients.length === before ? null : waited;
        }
    };
}

let page;
beforeEach(() => {
    page = loadShell();
});

test('une connexion réussie abonne les sujets demandés et ceux du système', () => {
    const seen = [];
    page.sandbox.connectWs({ '/topic/stats': (body) => seen.push(body) });

    assert.strictEqual(page.clients.length, 1);
    page.latest().succeed();

    assert.deepStrictEqual(page.latest().topics.sort(), ['/topic/stats', '/topic/system-events']);
    assert.strictEqual(page.text(), 'WS: Connected');
    assert.ok(page.bannerHidden(), 'le bandeau d\'alerte est caché tant que la connexion tient');

    // Le gestionnaire reçoit un corps déjà désérialisé. Comparaison champ à
    // champ : l'objet est construit dans le contexte vm, donc son prototype
    // n'est pas celui de ce fichier et deepStrictEqual le refuserait.
    page.latest().handlers['/topic/stats']({ body: '{"totalDlt24h":7}' });
    assert.strictEqual(seen.length, 1);
    assert.strictEqual(seen[0].totalDlt24h, 7);
});

test('une coupure déclenche une reconnexion, pas seulement un voyant rouge', () => {
    page.sandbox.connectWs({});
    page.latest().succeed();
    page.latest().fail();

    assert.strictEqual(page.text(), 'WS: Reconnecting');
    assert.ok(!page.bannerHidden(), 'le bandeau d\'alerte réapparaît');
    assert.strictEqual(page.clients.length, 1, 'la reconnexion est différée, pas synchrone');

    assert.strictEqual(page.waitForReconnect(), 1000, 'premier essai après une seconde');
});

test('deux rappels d\'erreur pour une seule coupure ne programment qu\'un essai', () => {
    page.sandbox.connectWs({});
    page.latest().succeed();

    page.latest().fail();
    page.latest().fail();

    page.advance(5000);
    assert.strictEqual(page.clients.length, 2, 'une seule tentative malgré deux rappels');
});

test('le délai double puis plafonne à trente secondes', () => {
    page.sandbox.connectWs({});
    page.latest().succeed();

    const delays = [];
    for (let i = 0; i < 7; i++) {
        page.latest().fail();
        delays.push(page.waitForReconnect());
    }

    assert.deepStrictEqual(delays, [1000, 2000, 4000, 8000, 16000, 30000, 30000]);
});

test('une reconnexion réussie remet le délai à sa valeur initiale', () => {
    page.sandbox.connectWs({});
    page.latest().succeed();

    page.latest().fail();
    page.waitForReconnect();
    page.latest().fail();
    assert.strictEqual(page.waitForReconnect(), 2000, 'le délai a doublé');

    page.latest().succeed();
    page.latest().fail();
    assert.strictEqual(page.waitForReconnect(), 1000, 'puis reparti de zéro');
});

test('le rappel d\'un client remplacé ne relance rien', () => {
    page.sandbox.connectWs({});
    const first = page.latest();
    first.succeed();
    first.fail();
    page.waitForReconnect();

    const opened = page.clients.length;
    first.fail();
    page.advance(120000);

    assert.strictEqual(page.clients.length, opened, 'le client périmé est ignoré');
});

test('le client remplacé est fermé', () => {
    page.sandbox.connectWs({});
    const first = page.latest();
    first.succeed();
    first.fail();
    page.waitForReconnect();

    assert.ok(first.disconnected, 'la connexion abandonnée ne reste pas ouverte');
});

test('revenir sur l\'onglet retente sans attendre le minuteur bridé', () => {
    page.sandbox.connectWs({});
    page.latest().succeed();
    page.latest().fail();

    const opened = page.clients.length;
    page.becomeVisible();

    assert.strictEqual(page.clients.length, opened + 1, 'essai immédiat au retour au premier plan');
});
