# Living Workspace foundation

Local fixture-backed prototype on `feat/living-workspace-foundation`. No production application startup, API access, AI calls, credentials, database, or execution paths.

## Launch

From the repository root, with Node.js 20 or newer:

```sh
node prototypes/living-workspace/server.cjs
```

Open **http://127.0.0.1:4173**. Set `PORT` to use a different local port. The server binds only to loopback. No dependency installation is required to run the prototype. Do not launch `server.js` for this preview.

Choose a domain to open its details, then **Investigate** to open the second registered tool. Drag panel titles or use arrow keys on a focused title. Drag a corner handle or use its arrow keys to resize. Use **Dock left**, **Dock right**, **Float**, **Focus/Restore**, and the open-panel buttons. On narrow screens, the selected panel is shown at full available width; the panel buttons switch between tools. Close panels to return to the field.

The translucent golden Max sphere opens an explicitly simulated conversation. Try **investigate acquisition**, **investigate content**, **arrange**, or **close panels**. Commands only change presentation. There is no natural-language model or production Max connection.

## Architecture and scope

- `fixtures.mjs`: two fictional tenants with disjoint record IDs and the existing four-domain overview shape.
- `store.mjs`: shared state, registered tool metadata, and a validated command dispatcher. Human controls and simulated Max use the same dispatcher.
- `app.mjs`: tool renderers, pointer/keyboard panel controls, spatial adapter, and deterministic simulated conversation.
- `index.html` / `workspace.css`: isolated desktop shell, touch fallback, golden Max presence, and reduced-motion treatment.
- `server.cjs`: dependency-free static server with an explicit asset allowlist, no application routes, and CSP `connect-src 'none'`.

The prototype loads the existing `public/command-deck/spatial-deck.js`, `command-deck.css`, and shared tokens without modifying them. It preserves the domain order, slots, priority bands, and four connections. A neutral **Business field** origin retains the original central geometry; Max's conversational presence lives separately in the upper-right. Domain clicks are adapted to registered panels instead of the production drawer. No production Command Deck controller or shared API client is loaded.

The command envelope is `{ type, source: 'human' | 'max', tenantId, generation, ...parameters }`. Supported commands: `select_entity`, `open_tool`, `dock_panel`, `resize_panel`, `move_panel`, `focus_panel`, `maximize_panel`, `close_panel`, `toggle_max`, `message`. Unknown commands/tools/records and stale scopes are rejected. This is a presentation protocol, not an authorization system.

Tenant switching always **resets** selection, trail, panels, focus, history, conversation, and composer text. A monotonically increasing generation rejects late commands, including an A → B → A switch. State is in memory and snapshots are copied; no tenant records are persisted in browser storage. The reused renderer may persist only its existing list/spatial preference. Switching never calls the production tenant coordinator.

History is a bounded event list (60 entries, latest 12 displayed), not undo/redo. Investigation context is shared through record selection and trail; tool panels retain the record they were opened for. Messages are capped at 40. Browser refresh starts a fresh session. Multiple panels can occupy the same dock; open-panel buttons select the foreground panel. There are no docking tabs/split trees, saved layouts, real collaboration, durable conversation restoration, or business mutations.

## Tests

Pure store and existing spatial contracts:

```sh
node --test prototypes/living-workspace/store.test.mjs test/livingCommandDeck.test.js
```

Browser tests use the repository's Puppeteer version and Chromium. With dependencies already installed:

```sh
CHROMIUM_PATH=/usr/bin/chromium node --test --test-concurrency=1 prototypes/living-workspace/browser.test.cjs test/browser/livingCommandUi.test.js
```

Alternatively, install only browser tooling outside the checkout, using system Chromium:

```sh
npm install --prefix /tmp/pulseforge-browser --no-package-lock --ignore-scripts puppeteer@24.42.0
NODE_PATH=/tmp/pulseforge-browser/node_modules CHROMIUM_PATH=/usr/bin/chromium node --test --test-concurrency=1 prototypes/living-workspace/store.test.mjs prototypes/living-workspace/browser.test.cjs test/livingCommandDeck.test.js test/browser/livingCommandUi.test.js
```

Adjust `CHROMIUM_PATH` to your installation, or omit it when Puppeteer has a downloaded browser. Tests launch headless Chromium with `--no-sandbox`, following the existing local browser harness convention. Only loopback fixtures/intercepted requests are used. Tests cover tenant reset and stale scopes, command validation, shared context, mouse move/resize, dock/focus/restore, simulated commands, touch resize, keyboard controls, reduced motion, no API requests, and server isolation.
