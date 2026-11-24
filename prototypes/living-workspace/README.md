# Living Workspace foundation

Local fixture-backed prototype on `feat/living-workspace-foundation`. No production application startup, API access, AI calls, credentials, database, or execution paths.

## Launch

From the repository root, with Node.js 20 or newer:

```sh
node prototypes/living-workspace/server.cjs
```

Open **http://127.0.0.1:4173**. Set `PORT` to use a different local port. The server binds only to loopback. No dependency installation is required to run the prototype. Do not launch `server.js` for this preview.

The opening demonstration shows the spatial field beside domain details and investigation panels. **Expand field**, **Dock field**, and **Minimize field** preserve selected records, open tools, and investigation context. Drag the field background (or focus it and use arrow keys) to pan; use +/− to zoom and **Reset view** to recover the overview. Record satellites open evidence directly. The persistent workspace dock restores access to open panels.

Choose a domain to open its details, then **Investigate** to open the second registered tool. Drag panel titles or use arrow keys on a focused title. Drag a corner handle or use its arrow keys to resize; resizing detaches a panel into a floating window. Use **Primary**, **Secondary**, **Float**, **Focus/Restore**, and the open-panel buttons. The primary/secondary slots retain the existing `left`/`right` command values for compatibility. The two dock slots stack alongside the field and become side-by-side when the field is minimized. On narrow screens, the selected panel is shown at full available width; the panel buttons switch between tools. Close panels to return to the field.

The translucent golden Max sphere opens an explicitly simulated conversation. Try **investigate acquisition**, **investigate content**, **arrange**, or **close panels**. Commands only change presentation. There is no natural-language model or production Max connection.

## Architecture and scope

- `fixtures.mjs`: two fictional tenants with disjoint record IDs and the existing four-domain overview shape.
- `store.mjs`: shared state, registered tool metadata, and a validated command dispatcher. Human controls and simulated Max use the same dispatcher.
- `app.mjs`: retained tool renderers, pointer/keyboard panel controls, responsive docking, and deterministic simulated conversation.
- `spatial-field.mjs`: prototype-only asymmetric exploration renderer, domain/record connections, pan/zoom, accessible list view, and shared selection.
- `index.html` / `workspace.css`: isolated desktop shell, touch fallback, golden Max presence, and reduced-motion treatment.
- `server.cjs`: dependency-free static server with an explicit asset allowlist, no application routes, and CSP `connect-src 'none'`.

The corrected presentation is a full-viewport dark application shell with restrained purple accents, bounded internal scrolling, and a translucent champagne sphere with SVG filaments. The sphere has no visible resting label; its accessible name and reduced-motion behavior remain intact.

The prototype-only renderer replaces the old four-slot presentation, preserving the four domain identities and supplied priority semantics: elevated/urgent domains sit closer to workspace context, with stronger emphasis. Curved connections show the shared workspace relationship; subordinate fixture-record buttons open investigations. It never computes business priority or invents cross-domain evidence. Production spatial code is untouched and no production scripts or styles are loaded.

The command envelope is `{ type, source: 'human' | 'max', tenantId, generation, ...parameters }`. Supported commands: `select_entity`, `open_tool`, `dock_panel`, `resize_panel`, `move_panel`, `focus_panel`, `maximize_panel`, `close_panel`, `toggle_max`, `message`, `set_field_mode`, `set_field_view`. Unknown commands/tools/records and stale scopes are rejected. This is a presentation protocol, not an authorization system.

Tenant switching always **resets** selection, trail, panels, focus, history, conversation, and composer text. A monotonically increasing generation rejects late commands, including an A → B → A switch. State is in memory and snapshots are copied; no tenant records are persisted in browser storage. Layout mode and pan/zoom also reset on tenant switch. Switching never calls the production tenant coordinator.

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

## Screenshot capture and verification

Set `WORKSPACE_SCREENSHOTS` to an output directory when running `browser.test.cjs` to capture the opening field-plus-two-panels view and expanded field at 1440×900, 1180×820, 1024×768, and 1280×800. The test verifies page bounds, non-overlap, selection/trail/panel preservation across all three field modes, pan/zoom, and an unlabeled accessible Max sphere. No screenshots are served by the fixture server.

This is an interpretation of the supplied visual requirements, not a pixel match to the prior conversation's V0.3 artifact. Remaining limits: fictional records, scripted Max responses, in-memory sessions, fixed two-slot docking (additional tools in a slot stack), no undo/redo or live collaboration.
