# Living Workspace foundation

Local fixture-backed prototype on `feat/living-workspace-foundation`. No production application startup, API access, AI calls, credentials, database, or execution paths.

## Launch

From the repository root, with Node.js 20 or newer:

```sh
node prototypes/living-workspace/server.cjs
```

Open **http://127.0.0.1:4173**. Set `PORT` to use a different local port. The server binds only to loopback. No dependency installation is required to run the prototype. Do not launch `server.js` for this preview.

The opening demonstration shows the spatial field on top, with domain details and investigation panels side by side underneath. **Expand field**, **Dock field**, and **Minimize field** preserve selected records, open tools, and investigation context. Drag the field background (or focus it and use arrow keys) to pan; use +/− to zoom and **Reset view** to recover the overview. Record satellites open evidence directly. The persistent workspace dock restores access to open panels.

Choose a domain to open its details, then **Investigate** to open the second registered tool. Drag panel titles or use arrow keys on a focused title. Drag a corner handle or use its arrow keys to resize; resizing detaches a panel into a floating window. Use the panel's **Dock region** selector for left, right, top, bottom, or floating placement. **Move / resize** offers touch-friendly step buttons; focused title and resize handles support arrow keys. Minimize a panel with its minus button and restore it from the persistent workspace dock. **Focus** fills the workspace; **Restore** returns its prior geometry.

The **Layouts** menu offers Spatial first, Research, and Review presets, field docking on any edge, a field-size slider, and named layout save/restore/delete. These are defaults and commands, not locked compositions. Docked panels tile in real regions of the remaining workspace. On narrow screens, the focused panel is shown at full width and the dock switches between tools.

The translucent golden Max sphere opens an explicitly simulated conversation. Try **investigate acquisition**, **investigate content**, **arrange research**, **arrange spatial-first**, **arrange review**, **undo**, **redo**, **save layout My research**, **load layout My research**, **minimize panels**, or **close panels**. Commands only change presentation. There is no natural-language model or production Max connection.

## Architecture and scope

- `fixtures.mjs`: two fictional tenants with disjoint record IDs and the existing four-domain overview shape.
- `store.mjs`: shared state, registered tool metadata, and a validated command dispatcher. Human controls and simulated Max use the same dispatcher.
- `app.mjs`: retained tool renderers, pointer/keyboard panel controls, responsive docking, and deterministic simulated conversation.
- `layout.mjs`: pure responsive geometry for field anchoring, four panel docking regions, floating windows, and focus.
- `spatial-field.mjs`: prototype-only asymmetric exploration renderer, domain/record connections, pan/zoom, accessible list view, and shared selection.
- `index.html` / `workspace.css`: isolated desktop shell, touch fallback, golden Max presence, and reduced-motion treatment.
- `server.cjs`: dependency-free static server with an explicit asset allowlist, no application routes, and CSP `connect-src 'none'`.

The corrected presentation is a full-viewport dark application shell with deep navy surfaces, champagne-gold highlights, warm off-white text and blue-gray secondary text, bounded internal scrolling, and a translucent champagne sphere with SVG filaments. The sphere has no visible resting label; its accessible name and reduced-motion behavior remain intact.

The prototype-only renderer replaces the old four-slot presentation, preserving the four domain identities and supplied priority semantics: elevated/urgent domains sit closer to workspace context, with stronger emphasis. Curved connections show the shared workspace relationship; subordinate fixture-record buttons open investigations. It never computes business priority or invents cross-domain evidence. Production spatial code is untouched and no production scripts or styles are loaded.

The command envelope is `{ type, source: 'human' | 'max', tenantId, generation, ...parameters }`. Supported commands: `select_entity`, `open_tool`, `dock_panel`, `resize_panel`, `move_panel`, `focus_panel`, `maximize_panel`, `close_panel`, `toggle_max`, `message`, `set_field_mode`, `set_field_view`, `set_field_dock`, `minimize_panel`, `restore_panel`, `arrange_layout`, `save_layout`, `load_layout`, `delete_layout`, `undo_layout`, `redo_layout`. Unknown commands/tools/records and stale scopes are rejected. This is a presentation protocol, not an authorization system.

Tenant switching always **resets** selection, trail, panels, focus, history, conversation, and composer text. A monotonically increasing generation rejects late commands, including an A → B → A switch. Active state is in memory and snapshots are copied. Named layouts persist under `pulseforge.fixture.layouts.v1.<tenant>` in localStorage, containing only validated tool placement and geometry, never selected records, conversation, or investigation data. Applying a saved layout maps its slots onto currently open tools and preserves their records and the shared context. At most 12 named layouts per tenant are supported; storage failures fall back to session memory with a visible notice. Layout mode and pan/zoom also reset on tenant switch. Switching never calls the production tenant coordinator.

Layout changes from both actors use the same bounded undo/redo stack (40 steps), including move, resize, dock, minimize, focus, close, field modes, presets, and saved layout edits. **Undo/Redo** buttons or Ctrl/Cmd+Z / Shift+Z operate on layout, leaving current selection, investigation trail, and conversation intact. Max rearrangement is one atomic command and one undo step. A new layout change clears redo; tenant switching clears both stacks. A separate event list retains 60 events (latest 12 displayed). Messages are capped at 40.

Browser refresh starts a fresh active session with the default composition; named templates remain available for that tenant. There is no cross-device synchronization, real-time collaboration, durable conversation restoration, or business mutation.

## Tests

Pure store and existing spatial contracts:

```sh
node --test prototypes/living-workspace/store.test.mjs prototypes/living-workspace/layout.test.mjs test/livingCommandDeck.test.js
```

Browser tests use the repository's Puppeteer version and Chromium. With dependencies already installed:

```sh
CHROMIUM_PATH=/usr/bin/chromium node --test --test-concurrency=1 prototypes/living-workspace/browser.test.cjs test/browser/livingCommandUi.test.js
```

Alternatively, install only browser tooling outside the checkout, using system Chromium:

```sh
npm install --prefix /tmp/pulseforge-browser --no-package-lock --ignore-scripts puppeteer@24.42.0
NODE_PATH=/tmp/pulseforge-browser/node_modules CHROMIUM_PATH=/usr/bin/chromium node --test --test-concurrency=1 prototypes/living-workspace/store.test.mjs prototypes/living-workspace/layout.test.mjs prototypes/living-workspace/browser.test.cjs test/livingCommandDeck.test.js test/browser/livingCommandUi.test.js
```

Adjust `CHROMIUM_PATH` to your installation, or omit it when Puppeteer has a downloaded browser. Tests launch headless Chromium with `--no-sandbox`, following the existing local browser harness convention. Only loopback fixtures/intercepted requests are used. Tests cover tenant reset and stale scopes, command validation, shared context, mouse move/resize, dock/focus/restore, simulated commands, touch resize, keyboard controls, reduced motion, no API requests, and server isolation.

## Screenshot capture and verification

Set `WORKSPACE_SCREENSHOTS` to an output directory when running `browser.test.cjs` to capture the opening field-plus-two-panels view and expanded field at 1440×900, 1180×820, 1024×768, and 1280×800. It also captures `default-1180x820.png`, `customized-1180x820.png`, and `max-rearranged-1180x820.png` through real UI interactions and a simulated Max command. The test verifies page bounds, non-overlap, selection/trail/panel preservation across all three field modes, pan/zoom, and an unlabeled accessible Max sphere. No screenshots are served by the fixture server.

This is an interpretation of the supplied visual requirements, not a pixel match to the prior conversation's V0.3 artifact. Remaining limits: fictional records, scripted Max responses, local-device named layouts, in-memory active sessions, and no live collaboration. Docking uses four regions rather than arbitrary nested split trees.
