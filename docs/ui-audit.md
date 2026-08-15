# UI audit — ergonomics and performance

Scope: the eleven Thymeleaf views under `src/main/resources/templates` and the four
shared fragments they build on. The audit covers what was found, what was fixed in
this change, and what deliberately stays open.

---

> **Note (after the audit).** The Flink Metrics page referenced throughout this
> document has since been removed: the project has no Flink integration, and the
> page told operators their metric definitions were "automatically exported to
> Prometheus" when nothing ever read them. The findings below are kept as the
> record of what the audit examined at the time.

## 1. Defects found (functionality)

These were not cosmetic — each one broke a feature that the UI advertises.

| # | Where | Symptom | Root cause |
|---|-------|---------|------------|
| 1 | `error.html` (via `fragments/head`) | The error page itself threw while rendering, so a handled exception produced a 500 with no page | The head fragment built the title from `stats.appName`, but `GlobalExceptionHandler` never puts `stats` in the model |
| 2 | `dlt-management.html` | Every filter change threw, **and** live DLT events never reached the table (the `/topic/dlt` subscription calls the same function) | `filterEvents()` read `document.getElementById('filter-partition')`, an element that did not exist |
| 3 | `dlt-management.html` | After any filter or live update, every column shifted one place left and bulk selection stopped working | `renderTable()` emitted 6 `<td>` for a 7-column table, dropping the checkbox cell |
| 4 | `dlt-management.html` | Toasts ("Payload copied", "Error retrying event"…) never appeared | The page called `showToast()` but never included the notifications fragment that owns `#toast-container`, so the function returned early |
| 5 | `dlt-management.html` | The bulk-actions bar rendered at the top-left of the viewport instead of over the toolbar | `absolute` with no positioned ancestor |
| 6 | `message-viewer.html` | Both left-hand navigation buttons threw `ReferenceError` on click | `scrollToSection()` was called but never defined |
| 7 | `settings.html` | With `SSL` as the Kafka protocol and no `?success` parameter, the protocol badge silently faded out after 3 s | The auto-hide selector `.bg-emerald-500\/10` matched the badge, not just the banner |
| 8 | `simulation.html` | The idle status dot was invisible | `bg-slate-50` (near-white) instead of `bg-slate-500` |
| 9 | `simulation.html` | Start/Stop buttons hovered with no visual change | `hover:bg-primary-dark` — a colour absent from the Tailwind theme |
| 10 | `simulation.html` | A rejected or failing start/stop request left the UI showing "Idle" with no explanation | `fetch()` fire-and-forget, no `.catch`, no status check |
| 11 | all views | Icon-only tooltips never rendered anywhere | `data-tooltip` was used on 5 elements but had no CSS implementation |
| 12 | `consumer-groups`, `db-status`, `settings`, `flink-metrics` | The footer said "WS: Connecting" forever | These pages open no WebSocket, and nothing ever resolved the indicator |
| 13 | `dashboard.html` | A notification bell with a permanent red "unread" dot did nothing when clicked | Dead control |

All thirteen are fixed. Details of the approach are in the code comments at each site.

**Found later, in the simplification pass:** no page reconnected its WebSocket.
The error callback repainted the footer indicator red and stopped there, so the
first outage — a server restart, a laptop waking up — left the page silently
stale until someone reloaded it. `metrics.html` displayed "Real-time updates
disconnected. Attempting to reconnect…" while nothing attempted anything; the
same shape of untrue claim as the Flink page above. Fixed in `connectWs`
(`static/js/app-shell.js`), which now backs off from 1 s to 30 s and retries
immediately when a background tab comes back to the foreground.

That defect survived the audit because the bootstrap was copied into seven
pages: reading any one of them, the missing reconnection looks like a local
omission rather than a systematic one.

## 2. Performance

### Network

| Change | Effect |
|--------|--------|
| Removed duplicate CDN includes (`metrics`, `architecture`, `message-viewer` re-loaded SockJS/Stomp/ApexCharts already present in the head) | 3, 2 and 2 fewer script executions on those pages |
| Removed the dead page-specific `<head>` block in `message-viewer` | It was silently dropped by `th:replace` on `<head>`, so the whole Prism theme + inline CSS had been duplicated into the body to compensate |
| Removed Prism from `dlt-management` | Two scripts were downloaded and never used — the payload editor is a plain `<textarea>` |
| Replaced the `ui-avatars.com` avatar with locally rendered initials | One fewer third-party request **per page load**, and the OS user name is no longer sent to an external service |
| `preconnect` for the one remaining third-party origin (Tailwind) | Its TLS handshake starts alongside HTML parsing instead of after it. The other three origins are gone entirely |
| `defer` on SockJS / Stomp / ApexCharts / Prism | They no longer block parsing. Pages that bootstrapped a WebSocket from an inline script were moved into `DOMContentLoaded`, which is what makes `defer` safe here |
| Subset the Material Symbols font with `icon_names=` | The full icon font is ~3.5 MB; the UI uses 57 icons and the subset weighs 20 KB. Verified glyph by glyph against the font's own ligature table, so nothing falls back to literal text |
| Vendored every third-party asset except Tailwind (see §4) | Fonts, SockJS, Stomp, ApexCharts and Prism are served by the application. Removes three external origins, pins versions that could previously shift under us, and makes the UI work air-gapped apart from Tailwind |

Net effect on request count: 7 → 6 on a typical page, 12 → 9 on `message-viewer`,
9 → 6 on `dlt-management` and `metrics`. More significant than the count is what
those requests now cost: four third-party origins became one, so a cold page load
performs a single external DNS + TLS handshake instead of four, and every other
asset rides the connection the HTML already arrived on. The largest asset also
shrank by two orders of magnitude — the icon font went from ~3.5 MB to 20 KB.

Self-serving the assets means they no longer arrive with a CDN's cache headers, so
`spring.web.resources.cache` sets a one-day `Cache-Control` on the static tree.
Without it, vendoring would have *cost* a conditional request per asset per
navigation and been a net regression on repeat visits.

### Runtime

- **Table filtering** (`dashboard`, `dlt-management`, `metrics`, `flink-metrics`) is
  debounced at 150 ms. The dashboard filter also stopped reading `row.innerText`,
  which forces a synchronous layout for every row on every keystroke — it now caches
  `textContent` on the row.
- **Chart updates** merged `updateOptions` + `updateSeries` into one
  `updateOptions(..., false, false)` call on `dashboard` and `optimizer`. Each stats
  push was previously triggering two full ApexCharts re-renders.
- **Footer clock** no longer runs a 1 Hz `setInterval` in background tabs; it stops
  on `visibilitychange` and resumes when the tab is visible.
- **Toast flooding**: system-event toasts are capped at 4 on screen. A burst of
  circuit-breaker events used to stack unbounded toasts, each holding a 8 s timer.
- **Blob URLs** from the DLT export are revoked after download instead of being held
  for the lifetime of the page.

### Correctness/safety touched along the way

`message-viewer` interpolated Kafka payload values and header names into `innerHTML`
without escaping; so did the DLT header badges. Both now go through `escapeHtml`.
`Retry All`, `Discard All`, per-event `Discard` and both bulk actions — all
irreversible — now ask for confirmation. Only the Flink metric delete had one before.

## 3. Ergonomics and accessibility

- **Skip link** as the first tab stop on every page, targeting `<main>`.
- **Accessible names** on every icon-only button (theme, refresh, export, close,
  dismiss, expand); decorative icon glyphs marked `aria-hidden` so screen readers stop
  announcing "dark_mode".
- **`aria-current="page"`** on the active navigation entry and a labelled `<nav>`
  landmark. The ten near-identical nav blocks are now one parameterised fragment.
- **Form labels**: every search box, select and read-only configuration field is now
  associated with a `<label>` (visible or `sr-only`).
- **Sortable columns** in the metrics explorer moved from `<th onclick>` to a real
  `<button>` inside the header, with `aria-sort` reflecting the current order — they
  were unreachable by keyboard.
- **The Flink metric dialog** gained `role="dialog"`/`aria-modal`, Escape and
  backdrop-click dismissal, and focus that moves into the dialog and returns to the
  opener on close.
- **The DLT export menu** was hover-only; it is now a click-toggled menu with
  `aria-expanded`, closing on Escape or outside click.
- **Responsive**: below `lg` the 16 rem sidebar becomes an off-canvas drawer with an
  overlay and a floating toggle. Previously it consumed the viewport permanently,
  leaving the content unusable on narrow screens.
- **`prefers-reduced-motion`** is honoured — the UI uses a lot of `animate-pulse`,
  `animate-ping` and `animate-spin`.
- **Consistent chrome**: the theme toggle and refresh button were missing from
  `simulation` and `flink-metrics` only; both now match every other page.
- **Visible focus** (`:focus-visible`) on all custom controls.

## 4. Not addressed (deliberate)

1. **Tailwind is loaded from the CDN.** `cdn.tailwindcss.com` ships the full engine
   and compiles classes in the browser on every page load; upstream documents it as
   unsuitable for production. Replacing it means introducing a Node build step
   (`tailwindcss` CLI → a single small `app.css` served from `static/`), which is a
   build-pipeline change rather than a UI change. This is the largest remaining
   performance item by a wide margin, and it now also blocks item 2: Tailwind is
   the last asset keeping the UI from working fully offline.
2. ~~**The UI cannot run offline / air-gapped.**~~ **Mostly resolved.** Fonts,
   SockJS, Stomp, ApexCharts and Prism now live under
   `src/main/resources/static/vendor/` and are served by the application; see the
   README there for how to refresh them. Tailwind is the sole remaining external
   asset, and it cannot be vendored meaningfully without item 1 — its CDN build
   *is* the compiler. So a disconnected install now renders and functions, but
   comes up unstyled.
3. **Server-side pagination.** `dlt-management` renders 25 rows and `message-viewer`
   20, both capped in the controller; filtering and sorting happen entirely client
   side. Fine at that size, but the "Showing N recent errors" label is easy to
   misread as a total.
4. **No CSRF tokens on the POST forms**, because Spring Security is not on the
   classpath. Worth revisiting if authentication is ever added.
5. ~~**Duplicated page shell.**~~ **Resolved.** The eleven views now render through
   `fragments/layout :: page`, which owns the head, skip link, sidebar, header
   container, footer and shared scripts. `TemplateAssetsTest` fails if a page
   rebuilds the shell by hand, which is how `simulation` and `flink-metrics` lost
   their theme toggles in the first place.

   The shared behaviour scripts moved out of the templates while doing it: the
   toast logic and the shell behaviours were inlined into **every HTML response**
   (~9 KB each time). They are now `static/js/notifications.js` and
   `static/js/app-shell.js`, fetched once and cached. Pages shrank by 19% on
   average — between 3.5 KB and 10.5 KB of HTML per response.

   A later pass did the same for the per-page behaviour: 1 627 lines that lived
   in the `page-trailing` blocks of six templates now live in
   `static/js/pages/<page>.js`. Only the values Thymeleaf injects stay inline.

6. **Static assets can be served stale for up to a day.** `max-age: 1d` with no
   cache busting: after a deployment a browser can hold yesterday's `app.css` or
   `js/pages/*.js` against today's HTML. The usual answer is content-hashed
   filenames (`spring.web.resources.chain.strategy.content`, which Thymeleaf's
   `@{...}` picks up through `ResourceUrlEncodingFilter`). Not enabled yet: it
   could not be exercised end to end here, since the application needs Oracle and
   Kafka to start.

## 5. How this was verified

All eleven views were rendered through `MockMvc` in a temporary `@WebMvcTest` — every
one returns 200, including the error page with no `stats` in the model, which is the
regression from item 1. The icon subset was cross-checked against the icons present in
the rendered HTML of every page, not just against the templates, so icons injected
from JavaScript are covered.

For the vendoring pass, a second temporary test re-rendered the pages, asserted that
no page still names `jsdelivr`, `cdnjs`, `fonts.googleapis.com` or `fonts.gstatic.com`,
and then requested every `/vendor/...` URL those pages reference — 26 assets, all
served with 200, plus the three font binaries the stylesheets point at. The icon
subset was additionally checked against the ligature table inside the generated
`woff2`: all 57 icons the templates use resolve to a real glyph.
