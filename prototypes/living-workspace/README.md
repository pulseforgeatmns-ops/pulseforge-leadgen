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

## V0.6 — Living Business Field

The solar-system visuals (sun, planets, moons, rings, orbital tracks) are gone. The field is now a quiet spatial environment:

- **Context core.** The selected tenant is a small glass card at the centre of the overview: name, domain count and record count. It is not a light source. In a focused domain it recedes to a compact "‹ Tenant" chip that returns to the overview.
- **Domain nodes.** Acquisition, Content, Clients and Campaigns are glass cards with fine borders and a layered edge for depth. Each shows its record count, a status dot (needs attention / on track / new) and a one-line operational note. Prominence comes from fixture priority (elevated gets a champagne hairline and a slightly larger card), status, relevance and selection.
- **Connections that mean something.** Core ↔ domain links, plus the domain relationships the fixtures declare (prospects become campaign audiences; client stories feed content; campaigns reactivate clients). Selecting an object draws its declared relationships, labelled: to sibling objects as an arc beside the cluster, and to receded domains, which brighten as related.
- **Gravitational navigation.** Focusing a domain brings it forward, gathers its business objects into a cluster on a gentle arc, and lets the other domains recede to an edge column without disappearing. Targets settle and stay put. Transitions run only when the context changes; nothing animates while idle. Reduced motion removes the transitions as well.
- Progressive exploration, breadcrumbs, zoom (now in the field header), pan, list fallback, Escape-to-overview and tenant isolation are preserved.

**Composition.** The default is still the field above two panels.
- Field height is adjustable with the splitter.
- The current composition is remembered per tenant (`pulseforge.fixture.workspace.v1.<tenant>`, record-free) and re-applied on return. Layouts → **Reset to default** forgets it.
- **Field focus** is a preset, and the expanded field can be saved and restored like any composition. Selecting records in the expanded field keeps it expanded and shows a compact selection card with **Show in panels**.
- The bottom field bar is gone (zoom moved into the header) and the workspace bar is slimmer.
- Panels keep their 300×220 minimum. Overflow is stacked into the dock.
- The ⋯ title-bar menu now also offers **Pin to this record / Follow the selection**.

**Max presence.**
- Closed, Max is a translucent golden energy sphere: glass body, warm internal glow, fine organic filaments, a soft boundary. The only ambient motion is an 18-second filament brightness drift.
- **Engaged** (open) shows a steady ring.
- **Attention** shows a small static gold notch. It appears when the selected record has open questions that Max hasn't been opened on yet, and the accessible name says how many.
- In the conversation, the technical context ("Max sees") is a collapsed summary line, and the messages dominate. Suggestions, the composer and verified receipts remain. On wide screens the workspace reflows beside the conversation instead of sitting under it.

**Working surfaces.** `tools.mjs` registers a renderer per store tool. Renderers compose reusable pieces from `surfaces.mjs` (header, section, status chip, stage track, signal meter, fact grid, record rows, next actions, evidence with provenance, findings, timeline, questions, metric row). Renderers never dispatch: they call a narrow api (`select`, `investigate`, `runAction`).
- **Details** for a prospect shows stage track, qualification signals, profile, relationships (clickable), suggested next actions and siblings. Proposals are labelled "Proposal · not executed".
- **Investigation** shows evidence (source, observed date, confidence), research findings, outstanding questions and the activity timeline.
- Domain records get a summary surface.
- Future agent tools register the same way.

**History consistency.**
- *Layout history* (Undo/Redo) holds geometry, docking, field mode/size and saved layouts. Focus, pan/zoom and domain focus are no longer undo steps.
- *Navigation history* (‹ › in the field header, Alt+←/→) holds selection and domain focus.
- Panels **follow** the shared selection or are explicitly **pinned** (labelled "Pinned"). After every command, batch, undo, redo and navigation step, the store reconciles following panels to the authoritative selection. Undo can therefore never leave a panel showing a different record, and restoring a saved composition never changes the selection.
- The first composition of a session is a system step (no undo entry).
- Max's arrangement replies now report actual on-screen placement. If minimum sizes prevent stacking, Max says so. It shrinks the field first when that is enough room.

## V0.5 — Max collaborative workspace

**Context.** Max reads the shared store only: tenant, focused domain, selected record, open panels with their dock and record, and field state (`describeContext` in `max-sim.mjs`). The conversation shows that context ("What Max sees") and offers context-aware suggestion chips that submit the same text as typing.

**Requests** (simulated phrase interpreter, no model): “Max, show me Northgate Bakery.” · “Open the investigation.” · “Put the evidence beside / below the prospect details.” · “Expand / dock / minimize the spatial field.” · “Make the field bigger / smaller.” · “Restore my previous layout.” · “Take me back to Acquisition.” · “Overview.” · “What am I looking at?” · “Close the investigation.” plus the earlier undo/redo, save/load layout, arrange, minimize/close panels. Names are matched in the **current tenant only**.

**How a turn works.** `planMaxTurn(text, state, fixture)` returns registered dispatcher commands. The app dispatches them as one `batch` from `source: 'max'`, which means all or nothing and one undo step. The reply is then composed by `plan.report(afterState, ok)` from the resulting state, never from intent. A rejected batch says nothing moved; a state that disagrees with the plan is reported as such. Each reply carries a receipt of the commands applied. Panels Max moved get a brief static outline. On screens ≥ 1000 px the workspace reflows beside the open conversation, so Max's changes are never hidden under it.

**Previous layout.** Before any layout-changing plan, Max records a `checkpoint_layout` (a record-free template, the same format as saved layouts). “Restore my previous layout” applies it with `restore_checkpoint`, keeps selection and records, and tells you what changed, including any later manual changes it replaced. Undo still steps back through every change. Checkpoints reset on tenant switch.

**Presentation.** A splitter between the field and the panels resizes the field by drag (mouse or touch) or as a keyboard ARIA separator (arrows ±2%, PageUp/PageDown ±6%, Home/End). It previews live, commits one undo step, and is capped so open panels keep 300×220. Panel docking, floating, sizing, focus, minimize and close live in one title-bar **⋯** menu (`role="menu"`: arrow keys, Home/End, Escape returns focus, outside click closes). The Max sphere is static translucent glass with one slow halo breath. When open, it shows a steady ring.

New store commands: `batch { commands }`, `checkpoint_layout` (Max only), `restore_checkpoint`; `message` accepts an optional `receipt` array.

## Architecture and scope

- `fixtures.mjs`: two fictional tenants with disjoint record IDs, the four-domain overview shape, and fictional business objects (`kind: 'object'`) orbiting each domain.
- `store.mjs`: shared state, registered tool metadata, and a validated command dispatcher. Human controls and simulated Max use the same dispatcher.
- `app.mjs`: retained tool renderers, pointer/keyboard panel controls, responsive docking, and deterministic simulated conversation.
- `layout.mjs`: pure responsive geometry for field anchoring, four panel docking regions, floating windows, and focus.
- `spatial-field.mjs`: prototype-only orbital renderer (sun, planets, moons, orbit paths), domain focus camera, pan/zoom, accessible list view, and shared selection.
- `index.html` / `workspace.css`: isolated desktop shell, touch fallback, golden Max presence, and reduced-motion treatment.
- `tools.mjs` / `surfaces.mjs`: registered tool renderers and the reusable surface components they compose.
- `max-sim.mjs`: simulated Max — context description, phrase → command planning, verified reporting, suggestions.
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

`v06.browser.test.cjs` runs the V0.6 acceptance scenario on desktop (mouse and keyboard) and tablet landscape 1180×820 / 1024×768 (touch): overview → focus Acquisition → select Northgate Bakery → Details/Investigation/breadcrumbs/Max context agree → Max rearranges → manual field resize → save and restore a composition → layout undo stays consistent → expanded field → per-tenant persistence → tenant isolation. A keyboard and reduced-motion test covers field navigation, pinning and the list fallback. With `WORKSPACE_SCREENSHOTS` set, it writes `v06-<desktop|tablet|tablet-small>-<1..5>-*.png`. `acceptance.browser.test.cjs` runs the earlier V0.5 scenario on desktop (1440×900, mouse and keyboard) and tablet landscape (1180×820 and 1024×768, touch). The steps are overview → open Max → show Northgate Bakery → verify orbit, breadcrumbs and both panels → evidence beside details → manual field resize → restore previous layout → selection still Northgate → tenant switch isolation. It also covers the remaining Max commands, honest replies, and keyboard operation of the title-bar menu. `max-sim.test.mjs` covers planning, verification and isolation without a browser. With `WORKSPACE_SCREENSHOTS` set, it writes `v05-<desktop|tablet|tablet-small>-<1..6>-*.png`.

## Screenshot capture and verification

Set `WORKSPACE_SCREENSHOTS` to an output directory when running `browser.test.cjs` to capture `orbital-overview-1440x900.png`, `orbital-acquisition-1440x900.png` and `orbital-selected-prospect-1440x900.png` (at 2× scale), plus the opening field-plus-two-panels view and expanded field at 1440×900, 1180×820, 1024×768, and 1280×800. It also captures `default-1180x820.png`, `customized-1180x820.png`, and `max-rearranged-1180x820.png` through real UI interactions and a simulated Max command. The test verifies page bounds, non-overlap, selection/trail/panel preservation across all three field modes, pan/zoom, and an unlabeled accessible Max sphere. No screenshots are served by the fixture server.

This is an interpretation of the supplied visual requirements, not a pixel match to the prior conversation's V0.3 artifact. Remaining limits: fictional records, scripted Max responses, local-device named layouts, in-memory active sessions, and no live collaboration. Docking uses four regions rather than arbitrary nested split trees.
