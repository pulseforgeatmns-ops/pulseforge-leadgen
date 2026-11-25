import { workspaceGeometry, MIN_PANEL } from './layout.mjs';
import { panelSteps, planMaxTurn, describeContext, suggestions } from './max-sim.mjs';
import { createSpatialField } from './spatial-field.mjs';
import { fixtures } from './fixtures.mjs';
import { createWorkspace, tools } from './store.mjs';
import { renderTool } from './tools.mjs';
import { statusChip, chip } from './surfaces.mjs';

let layoutStorage = null;
try { layoutStorage = window.localStorage; } catch { /* in-memory fallback */ }
const workspace = createWorkspace(fixtures, 'harbor', { storage: layoutStorage });
const $ = id => document.getElementById(id);
// V0.7 controls are created here so the fixture HTML stays compatible with V0.6 embeds.
const lockControl = document.createElement('button');
lockControl.id = 'layout-lock'; lockControl.type = 'button'; lockControl.setAttribute('aria-pressed', 'false');
document.querySelector('.layout-actions').insertBefore(lockControl, $('layout-menu'));
const lensControl = document.createElement('nav');
lensControl.className = 'lens-control'; lensControl.setAttribute('aria-label', 'Workspace lens');
for (const lens of ['operations', 'entities', 'objectives']) { const b = document.createElement('button'); b.type = 'button'; b.dataset.lens = lens; b.textContent = lens[0].toUpperCase() + lens.slice(1); lensControl.append(b); }
$('field').querySelector('.field-header').append(lensControl);
const entitySearch = document.createElement('input'); entitySearch.id = 'entity-search'; entitySearch.type = 'search'; entitySearch.placeholder = 'Search entities'; entitySearch.setAttribute('aria-label', 'Search entities'); entitySearch.hidden = true;
const entityDomain = document.createElement('select'); entityDomain.id = 'entity-domain'; entityDomain.setAttribute('aria-label', 'Filter entities by domain'); entityDomain.hidden = true;
entityDomain.innerHTML = '<option value="all">All domains</option><option value="acquisition">Acquisition</option><option value="clients">Clients</option><option value="campaigns">Campaigns</option><option value="content">Content</option>';
$('field').querySelector('.field-header').append(entitySearch, entityDomain);
const panelNodes = new Map();
const send = (type, data = {}, source = 'human', scope = workspace.scope()) =>
  workspace.dispatch({ ...data, type, source, ...scope });
const recordFor = id => fixtures[workspace.getState().tenantId].records.find(r => r.id === id);
function element(tag, text, className) {
  const el = document.createElement(tag);
  if (text) el.textContent = text;
  if (className) el.className = className;
  return el;
}
function button(text, label, action) {
  const el = element('button', text); el.type = 'button'; el.setAttribute('aria-label', label);
  el.addEventListener('click', action); return el;
}
const domainRecord = (domainId, tenantId = workspace.getState().tenantId) => fixtures[tenantId].records.find(r => r.kind === 'domain' && r.domain === domainId);
const objectsOf = domainId => fixtures[workspace.getState().tenantId].records.filter(r => r.kind === 'object' && r.domain === domainId);
const domainLabel = domainId => fixtures[workspace.getState().tenantId].overview.domains.find(d => d.id === domainId)?.label || domainId;
// Human navigation runs as one batch per gesture: one undo step, never a half-applied view.
// Existing panels of a tool are retargeted in place (geometry kept) — the same helper Max plans with.
const batch = (commands, source = 'human', scope = workspace.scope()) => send('batch', { commands }, source, scope);
// Bring a domain planet into focus, select its record and show its details.
function focusDomain(domainId, source = 'human', scope = workspace.scope()) {
  const record = domainRecord(domainId, scope.tenantId);
  return batch([{ type: 'focus_domain', domainId }, { type: 'select_entity', recordId: record.id },
    ...panelSteps(workspace.getState(), 'domain', record.id, 'left')], source, scope);
}
function openRecord(record, source = 'human', scope = workspace.scope()) {
  if (record.kind === 'object') return selectObject(record, source, scope);
  focusDomain(record.domain, source, scope);
  send('set_field_mode', { mode: 'docked' }, source, scope);
}
// Selecting a business object in the field updates shared context and both working panels.
function selectObject(record, source = 'human', scope = workspace.scope()) {
  const state = workspace.getState();
  return batch([{ type: 'focus_domain', domainId: record.domain }, { type: 'select_entity', recordId: record.id },
    ...panelSteps(state, 'domain', record.id, 'left'), ...panelSteps(state, 'investigation', record.id, 'right'),
    ...(state.fieldMode === 'minimized' ? [{ type: 'set_field_mode', mode: 'docked' }] : [])], source, scope);
}
function returnToOverview(source = 'human', scope = workspace.scope()) {
  send('focus_domain', { domainId: null }, source, scope);
}
// Narrow api handed to registered tool renderers.
const toolApi = () => ({
  fixture: fixtures[workspace.getState().tenantId], recordFor,
  select: record => (record.kind === 'object' ? selectObject(record) : focusDomain(record.domain)),
  investigate: record => openInvestigation(record),
  runAction(record, a) {
    if (a.kind === 'max') { if (!workspace.getState().maxOpen) toggleMax(); return askMax('What is still outstanding?'); }
    const compare = a.label.match(/^Compare with (.+)$/);
    const other = compare && fixtures[workspace.getState().tenantId].records.find(r => r.name === compare[1]);
    if (other) return selectObject(other);
    return openInvestigation(record);
  },
});

function gesture(handle, node, panelId, resize) {
  handle.addEventListener('pointerdown', event => {
    if (workspace.getState().layoutLocked) { event.preventDefault(); $('status').textContent = 'Layout is locked. Unlock this tenant composition before moving panels.'; return; }
    if (event.button !== 0) return;
    handle.focus();
    const scope = workspace.scope();
    send('focus_panel', { panelId });
    const area = $('work-area').getBoundingClientRect();
    const rect = node.getBoundingClientRect();
    const start = { x: event.clientX, y: event.clientY, left: rect.left - area.left, top: rect.top - area.top };
    let final = null;
    handle.setPointerCapture(event.pointerId);
    function move(e) {
      if (e.pointerId !== event.pointerId) return;
      const dx = e.clientX - start.x, dy = e.clientY - start.y;
      if (resize) {
        final = { width: Math.max(280, Math.min(area.width - start.left, rect.width + dx)), height: Math.max(240, Math.min(area.height - start.top, rect.height + dy)) };
        node.style.width = `${final.width}px`; node.style.height = `${final.height}px`;
      } else {
        final = { x: Math.max(0, Math.min(area.width - rect.width, start.left + dx)), y: Math.max(0, Math.min(area.height - rect.height, start.top + dy)) };
        node.style.left = `${final.x}px`; node.style.top = `${final.y}px`; node.style.right = 'auto';
      }
    }
    function finish(e) {
      if (e.pointerId !== event.pointerId) return;
      handle.removeEventListener('pointermove', move); handle.removeEventListener('pointerup', finish); handle.removeEventListener('pointercancel', finish);
      if (handle.hasPointerCapture(event.pointerId)) handle.releasePointerCapture(event.pointerId);
      if (final && e.type !== 'pointercancel') {
        send(resize ? 'resize_panel' : 'move_panel', { panelId, ...(resize ? { x: start.left, y: start.top } : {}), ...final }, 'human', scope);
      }
      render(workspace.getState());
    }
    handle.addEventListener('pointermove', move); handle.addEventListener('pointerup', finish); handle.addEventListener('pointercancel', finish);
  });
}
function makePanel(panel) {
  const tool = tools[panel.toolId], record = recordFor(panel.recordId);
  const node = element('section', '', 'panel'); node.dataset.panelId = panel.id;
  node.tabIndex = -1; node.setAttribute('aria-label', `${tool.title}: ${record.name}`);
  const header = element('header');
  const grip = button('', `Move ${tool.title}; drag or use arrow keys`, () => send('focus_panel', { panelId: panel.id }));
  grip.className = 'grip'; grip.append(element('span', tool.title, 'grip-title'), element('span', record.name, 'grip-record'), element('span', 'Pinned', 'pin-badge'));
  grip.addEventListener('keydown', event => {
    const delta = { ArrowLeft: [-20, 0], ArrowRight: [20, 0], ArrowUp: [0, -20], ArrowDown: [0, 20] }[event.key];
    if (!delta) return; event.preventDefault();
    const rect = node.getBoundingClientRect(), area = $('work-area').getBoundingClientRect();
    send('move_panel', { panelId: panel.id, x: Math.max(0, Math.min(area.width - rect.width, rect.left - area.left + delta[0])), y: Math.max(0, Math.min(area.height - rect.height, rect.top - area.top + delta[1])) });
  });
  // One compact, accessible title-bar menu holds docking, floating, sizing and focus.
  const menuButton = button('⋯', `${tool.title} panel options`, () => openPanelMenu(panel.id, menuButton));
  menuButton.className = 'panel-menu-button'; menuButton.setAttribute('aria-haspopup', 'menu'); menuButton.setAttribute('aria-expanded', 'false');
  menuButton.addEventListener('keydown', e => { if (e.key === 'ArrowDown' || e.key === 'ArrowUp') { e.preventDefault(); openPanelMenu(panel.id, menuButton, e.key === 'ArrowUp'); } });
  header.append(grip, menuButton, button('−', 'Minimize panel', () => send('minimize_panel', { panelId: panel.id })), button('×', 'Close panel', () => closePanel(panel.id)));
  const body = element('div', '', 'panel-body'); renderTool(panel.toolId, body, record, toolApi());
  const trail = element('p', '', 'trail'); body.append(trail);
  const resize = button('◢', 'Resize panel; arrow keys resize', () => {}); resize.className = 'resize';
  resize.addEventListener('keydown', event => {
    const delta = { ArrowLeft: [-20, 0], ArrowRight: [20, 0], ArrowUp: [0, -20], ArrowDown: [0, 20] }[event.key];
    if (!delta) return; event.preventDefault(); resizeBy(panel.id, node, ...delta);
  });
  node.append(header, body, resize);
  node.addEventListener('focusin', () => { if (workspace.getState().focused !== panel.id) send('focus_panel', { panelId: panel.id }); });
  node.addEventListener('pointerdown', () => { if (workspace.getState().focused !== panel.id) send('focus_panel', { panelId: panel.id }); });
  gesture(grip, node, panel.id, false); gesture(resize, node, panel.id, true);
  return node;
}
function closePanel(panelId) {
  send('close_panel', { panelId });
  const focused = workspace.getState().focused;
  if (focused) panelNodes.get(focused)?.focus(); else $('cdViewToggle').focus();
}
function resizeBy(panelId, node, dw, dh) {
  const rect = node.getBoundingClientRect(), area = $('work-area').getBoundingClientRect();
  return send('resize_panel', { panelId, x: rect.left - area.left, y: rect.top - area.top, width: Math.max(MIN_PANEL.width, rect.width + dw), height: Math.max(MIN_PANEL.height, rect.height + dh) });
}
let openMenu = null;
function closePanelMenu(restoreFocus = false) {
  if (!openMenu) return;
  const { menu, anchor, onOutside } = openMenu; openMenu = null;
  menu.remove(); anchor.setAttribute('aria-expanded', 'false');
  document.removeEventListener('pointerdown', onOutside, true);
  if (restoreFocus && anchor.isConnected) anchor.focus({ preventScroll: true });
}
function openPanelMenu(panelId, anchor, focusLast = false) {
  if (openMenu?.anchor === anchor) return closePanelMenu(true);
  closePanelMenu();
  const p = workspace.getState().panels.find(x => x.id === panelId); if (!p) return;
  const node = panelNodes.get(panelId);
  const menu = element('div', '', 'panel-menu'); menu.id = 'panel-menu'; menu.setAttribute('role', 'menu');
  menu.setAttribute('aria-label', `${tools[p.toolId].title} panel options`);
  const items = [];
  const heading = text => { const h = element('div', text, 'menu-heading'); h.setAttribute('role', 'presentation'); menu.append(h); };
  const item = (text, action, opts = {}) => {
    const b = element('button', text); b.type = 'button'; b.tabIndex = -1; b.dataset.action = opts.id;
    b.setAttribute('role', opts.checked === undefined ? 'menuitem' : 'menuitemradio');
    if (opts.checked !== undefined) b.setAttribute('aria-checked', String(opts.checked));
    b.addEventListener('click', () => { closePanelMenu(); action(); (anchor.isConnected ? anchor : panelNodes.get(panelId))?.focus({ preventScroll: true }); });
    menu.append(b); items.push(b);
  };
  heading('Place');
  for (const dock of ['left', 'right', 'top', 'bottom']) item(`Dock ${dock}`, () => send('dock_panel', { panelId, dock }), { id: `dock-${dock}`, checked: p.dock === dock });
  item('Float', () => send('dock_panel', { panelId, dock: 'floating' }), { id: 'dock-floating', checked: p.dock === 'floating' });
  heading('Size');
  item(p.maximized ? 'Restore size' : 'Focus (fill workspace)', () => send('maximize_panel', { panelId }), { id: 'maximize' });
  for (const [label, dw, dh, id] of [['Wider', 40, 0, 'wider'], ['Narrower', -40, 0, 'narrower'], ['Taller', 0, 40, 'taller'], ['Shorter', 0, -40, 'shorter']]) item(`${label} (floats)`, () => resizeBy(panelId, node, dw, dh), { id });
  heading('Context');
  item(p.follow ? 'Pin to this record' : 'Follow the selection', () => send('pin_panel', { panelId, pinned: !!p.follow }), { id: 'pin' });
  heading('Panel');
  item('Minimize', () => send('minimize_panel', { panelId }), { id: 'minimize' });
  item('Close', () => closePanel(panelId), { id: 'close' });
  menu.addEventListener('keydown', e => {
    const i = items.indexOf(document.activeElement);
    const go = n => { e.preventDefault(); items[(n + items.length) % items.length].focus(); };
    if (e.key === 'ArrowDown') go(i + 1); else if (e.key === 'ArrowUp') go(i - 1);
    else if (e.key === 'Home') go(0); else if (e.key === 'End') go(items.length - 1);
    else if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); closePanelMenu(true); }
    else if (e.key === 'Tab') closePanelMenu();
  });
  document.body.append(menu);
  const r = anchor.getBoundingClientRect(), m = menu.getBoundingClientRect();
  const top = r.bottom + 4 + m.height > innerHeight - 8 ? Math.max(8, r.top - 4 - m.height) : r.bottom + 4;
  Object.assign(menu.style, { left: `${Math.max(8, Math.min(innerWidth - m.width - 8, r.right - m.width))}px`, top: `${top}px` });
  const onOutside = e => { if (!menu.contains(e.target) && e.target !== anchor) closePanelMenu(); };
  document.addEventListener('pointerdown', onOutside, true);
  openMenu = { menu, anchor, onOutside }; anchor.setAttribute('aria-expanded', 'true');
  (focusLast ? items.at(-1) : items.find(b => b.getAttribute('aria-checked') === 'true') || items[0]).focus({ preventScroll: true });
}
function openInvestigation(record, source = 'human', scope = workspace.scope()) {
  return batch([{ type: 'select_entity', recordId: record.id }, ...panelSteps(workspace.getState(), 'investigation', record.id, 'right'), { type: 'set_field_mode', mode: 'docked' }], source, scope);
}
function renderCrumbs(state, fixture) {
  const selected = recordFor(state.selected);
  const signature = `${state.tenantId}|${state.fieldFocus}|${state.selected}`;
  const nav = $('field-crumbs');
  if (nav.dataset.signature === signature) return;
  nav.dataset.signature = signature;
  const items = [];
  const crumb = (text, label, action) => { const li = element('li'); li.append(action ? button(text, label, action) : element('span', text)); items.push(li); return li; };
  crumb(fixture.name, `${fixture.name} overview`, state.fieldFocus ? () => returnToOverview() : null);
  if (state.fieldFocus) {
    const objectInFocus = selected?.kind === 'object' && selected.domain === state.fieldFocus;
    crumb(domainLabel(state.fieldFocus), `${domainLabel(state.fieldFocus)} domain`, objectInFocus ? () => focusDomain(state.fieldFocus) : null);
    if (objectInFocus) crumb(selected.name);
  }
  items.at(-1).firstChild.setAttribute('aria-current', 'location');
  nav.querySelector('ol').replaceChildren(...items);
}
const tabs = element('nav', '', 'panel-tabs'); tabs.setAttribute('aria-label', 'Open panels'); $('work-area').after(tabs);
function render(state) {
  const fixture = fixtures[state.tenantId];
  $('work-area').dataset.fieldMode = state.fieldMode;
  $('empty-tools').hidden = state.panels.length > 0 || state.fieldMode !== 'docked';
  document.querySelectorAll('[data-field-mode]').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.fieldMode === state.fieldMode)));
  spatial.render(fixture, state);
  renderCrumbs(state, fixture);
  $('return-overview').hidden = !state.fieldFocus;
  $('nav-back').disabled = !state.canBack; $('nav-forward').disabled = !state.canForward;
  renderSelectionCard(state, fixture);
  renderMaxPresence(state, fixture);
  $('pref-status').textContent = state.hasPreference ? 'Your current composition is remembered for this tenant.' : 'Using the default composition.';
  $('field-summary').textContent = state.fieldFocus
    ? `${domainLabel(state.fieldFocus)} · ${objectsOf(state.fieldFocus).length} ${fixture.overview.domains.find(d => d.id === state.fieldFocus).objectPlural}`
    : `${fixture.overview.domains.length} domains · ${fixture.records.filter(r => r.kind === 'object').length} records`;
  $('zoom-level').textContent = `${Math.round(state.fieldView.zoom * 100)}%`;
  for (const [id, node] of panelNodes) if (!state.panels.some(p => p.id === id)) { node.remove(); panelNodes.delete(id); }
  const area = $('work-area').getBoundingClientRect();
  const layout = workspaceGeometry(state, area.width, area.height);
  const field = innerWidth <= 700 && state.fieldMode === 'docked' ? { x: 0, y: 0, width: area.width, height: area.height } : layout.field;
  Object.assign($('field').style, { left: `${field.x}px`, top: `${field.y}px`, width: `${field.width}px`, height: `${field.height}px` });
  placeSplitter(state, field, area);
  $('undo-layout').disabled = !state.canUndo; $('redo-layout').disabled = !state.canRedo;
  lockControl.textContent = state.layoutLocked ? 'Unlock layout' : 'Lock layout';
  lockControl.setAttribute('aria-pressed', String(state.layoutLocked));
  lockControl.setAttribute('aria-label', state.layoutLocked ? 'Unlock workspace layout' : 'Lock workspace layout');
  document.querySelectorAll('[data-lens]').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.lens === state.lens)));
  entitySearch.hidden = entityDomain.hidden = state.lens !== 'entities'; entitySearch.value = state.entityQuery; entityDomain.value = state.entityDomain;
  $('field-dock').value = state.fieldDock; $('field-ratio').value = Math.round(state.fieldRatio * 100); $('field-size').textContent = `${Math.round(state.fieldRatio * 100)}%`;
  const savedSignature = `${state.tenantId}:${Object.keys(state.savedLayouts).join('|')}`;
  if ($('saved-layouts').dataset.signature !== savedSignature) {
    $('saved-layouts').dataset.signature = savedSignature;
    $('saved-layouts').replaceChildren(...Object.keys(state.savedLayouts).map(name => { const o = element('option', name); o.value = name; return o; }));
  }
  $('load-layout').disabled = $('delete-layout').disabled = !Object.keys(state.savedLayouts).length;
  if (state.storageWarning) $('layout-status').textContent = 'Device storage unavailable. Layout kept for this session only.';
  for (const panel of state.panels) {
    let node = panelNodes.get(panel.id);
    if (!node) { node = makePanel(panel); panelNodes.set(panel.id, node); $('panels').append(node); }
    node.dataset.focused = String(state.focused === panel.id);
    node.dataset.pinned = String(!panel.follow);
    node.dataset.dock = panel.dock; node.dataset.maximized = String(panel.maximized);
    const compact = innerWidth <= 700;
    let { x, y, width, height } = layout.panels[panel.id] || { x: 0, y: 0, width: panel.width, height: panel.height };
    if (compact) { width = area.width; height = Math.min(panel.height, area.height); x = 0; y = 0; }
    if (panel.maximized) { width = area.width; height = area.height; x = 0; y = 0; }
    const stacked = !compact && layout.stacked.includes(panel.id);
    node.dataset.stacked = String(stacked);
    node.hidden = panel.minimized || stacked || state.fieldMode === 'expanded' || (compact && state.focused !== panel.id);
    Object.assign(node.style, { width: `${width}px`, height: `${height}px`, left: `${x}px`, top: `${y}px`, right: 'auto', zIndex: state.focused === panel.id ? '20' : '10' });
    node.querySelector('.trail').textContent = `Shared trail: ${state.trail.map(id => fixture.records.find(r => r.id === id).name).join(' → ') || 'No selection'}`;
  }
  // Keep buttons stable during focus and pointer interactions.
  const signature = state.panels.map(p => `${p.id}:${p.minimized}:${layout.stacked.includes(p.id)}`).join('|');
  if (tabs.dataset.signature !== signature) {
    tabs.dataset.signature = signature;
    tabs.replaceChildren(...state.panels.map(p => {
      const selectPanel = () => {
        if (workspace.getState().fieldMode === 'expanded') send('set_field_mode', { mode: 'docked' });
        if (workspace.getState().panels.find(panel => panel.id === p.id)?.minimized) send('restore_panel', { panelId: p.id });
        if (workspace.getState().focused !== p.id) send('focus_panel', { panelId: p.id });
        panelNodes.get(p.id)?.focus({ preventScroll: true });
      };
      const stacked = layout.stacked.includes(p.id);
      const tab = button(`${p.minimized ? '▱ ' : stacked ? '⧉ ' : ''}${tools[p.toolId].title} · ${recordFor(p.recordId).name}`, `Show ${p.id}`, selectPanel);
      if (stacked) tab.title = 'Stacked to keep open panels at a usable size. Select to bring forward.';
      // A completed tap selects immediately, even when the browser suppresses
      // a compatibility click after a resize gesture. Scrolling is not a tap.
      let start = null;
      tab.addEventListener('pointerdown', e => { start = e.pointerType === 'touch' ? { id: e.pointerId, x: e.clientX, y: e.clientY } : null; });
      tab.addEventListener('pointercancel', () => { start = null; });
      tab.addEventListener('pointerup', e => {
        if (start && start.id === e.pointerId && Math.hypot(e.clientX - start.x, e.clientY - start.y) < 10) selectPanel();
        start = null;
      });
      return tab;
    }));
  }
  tabs.querySelectorAll('button').forEach((b, i) => b.setAttribute('aria-pressed', String(state.panels[i].id === state.focused)));
  $('selection').textContent = state.selected ? `Shared selection · ${recordFor(state.selected).name}` : 'No selection · choose a domain';
  const stackedCount = innerWidth > 700 ? layout.stacked.length : 0;
  tabs.dataset.stackedNote = stackedCount ? `${stackedCount} stacked to keep panels usable` : '';
  document.querySelectorAll('[data-domain-id]').forEach(n => n.classList.toggle('selected', n.dataset.domainId === recordFor(state.selected)?.domain));
  $('conversation').hidden = !state.maxOpen; $('max-orb').setAttribute('aria-expanded', String(state.maxOpen));
  // On wide screens the workspace reflows beside Max instead of hiding under it, so Max's changes stay visible.
  document.body.dataset.maxOpen = String(state.maxOpen);
  renderMaxContext(state, fixture); // always: hidden DOM must never hold another tenant's context
  const messageSignature = JSON.stringify(state.messages);
  if ($('messages').dataset.signature !== messageSignature) {
    $('messages').dataset.signature = messageSignature;
    $('messages').replaceChildren(...state.messages.map(m => {
      const el = element('div', '', `message ${m.source}`);
      el.append(element('span', m.source === 'max' ? 'Max · simulated' : 'You', 'speaker'), element('p', m.text));
      if (m.receipt?.length) {
        const d = element('details', '', 'receipt'); d.append(element('summary', `✓ ${m.receipt.length} workspace command${m.receipt.length > 1 ? 's' : ''} applied and verified`));
        const ol = element('ol'); m.receipt.forEach(r => ol.append(element('li', r))); d.append(ol); el.append(d);
      }
      return el;
    }));
    $('messages').scrollTop = $('messages').scrollHeight;
  }
  $('history').replaceChildren(...state.history.slice(-12).reverse().map(h => element('li', `${h.source === 'max' ? 'Max' : 'You'} · ${h.type.replaceAll('_', ' ')}${h.recordId ? ' · ' + recordFor(h.recordId).name : ''}`)));
}
const spatial = createSpatialField({ host: $('cdSpatialCanvas'), orbit: $('cdSpatialOrbit'), connections: $('cdIntelConnections'), list: $('cdSpatialList'), dispatch: workspace.dispatch, scope: workspace.scope,
  onFocusDomain: domainId => focusDomain(domainId), onSelectObject: record => selectObject(record), onOverview: () => returnToOverview() });
$('return-overview').addEventListener('click', () => { returnToOverview(); $('cdSpatialCanvas').focus({ preventScroll: true }); });
$('cdViewToggle').addEventListener('click', () => {
  const list = !$('cdSpatialCanvas').hidden;
  $('cdSpatialCanvas').hidden = list; $('cdSpatialList').hidden = !list;
  $('cdViewToggle').textContent = list ? 'Spatial view' : 'List view';
  $('cdViewToggle').setAttribute('aria-pressed', String(list));
});
document.querySelectorAll('[data-field-mode]').forEach(b => b.addEventListener('click', () => send('set_field_mode', { mode: b.dataset.fieldMode })));
document.querySelectorAll('[data-lens]').forEach(b => b.addEventListener('click', () => send('set_lens', { lens: b.dataset.lens })));
let entitySearchTimer;
entitySearch.addEventListener('input', () => { clearTimeout(entitySearchTimer); entitySearchTimer = setTimeout(() => send('set_entity_filter', { query: entitySearch.value, domain: entityDomain.value }), 120); });
entityDomain.addEventListener('change', () => send('set_entity_filter', { query: entitySearch.value, domain: entityDomain.value }));
lockControl.addEventListener('click', () => {
  const state = workspace.getState();
  if (!state.layoutLocked) send('lock_layout');
  else if (confirm(`Unlock the ${fixtures[state.tenantId].name} layout at revision ${state.compositionRevision}?`)) send('unlock_layout', { revision: state.compositionRevision, confirm: true });
  const result = workspace.getLastResult(); if (!result.ok) $('status').textContent = `Layout change blocked: ${result.reason.replaceAll('_', ' ')}.`;
});
for (const [id, delta] of [['zoom-in', .1], ['zoom-out', -.1]]) $(id).addEventListener('click', () => send('set_field_view', { ...workspace.getState().fieldView, zoom: workspace.getState().fieldView.zoom + delta }));
$('reset-field').addEventListener('click', () => send('set_field_view', { x: 0, y: 0, zoom: 1 }));
$('open-context').addEventListener('click', () => openDemonstration());
$('tenant').addEventListener('change', () => {
  $('question').value = ''; $('layout-name').value = ''; $('layout-menu').open = false; $('layout-status').textContent = 'Saved locally on this device. Records stay in context.';
  workspace.switchTenant($('tenant').value);
  render(workspace.getState());
  $('status').textContent = `Switched to ${fixtures[$('tenant').value].name}. Previous context cleared.`;
});
function toggleMax() { closePanelMenu(); send('toggle_max'); if (workspace.getState().maxOpen) $('question').focus(); else $('max-orb').focus(); }
$('max-orb').addEventListener('click', toggleMax); $('close-max').addEventListener('click', toggleMax);
document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && workspace.getState().maxOpen) { event.preventDefault(); toggleMax(); }
});
// Expanded field is a first-class layout: the selection stays visible in the field itself.
function renderSelectionCard(state, fixture) {
  const card = $('field-selection'), record = recordFor(state.selected);
  const show = state.fieldMode === 'expanded' && !!record;
  card.hidden = !show; if (!show) return;
  const signature = `${state.tenantId}|${record.id}`;
  if (card.dataset.signature === signature) return;
  card.dataset.signature = signature;
  const title = element('div', '', 'sel-title');
  title.append(element('span', record.kind === 'object' ? `${record.type} · ${domainLabel(record.domain)}` : 'Domain', 'eyebrow'), element('strong', record.name));
  const row = element('div', '', 'chip-row');
  if (record.kind === 'object') row.append(chip(record.stage, 'stage'), statusChip(record.status));
  card.replaceChildren(title, row, element('p', record.summary), button('Show in panels', `Show ${record.name} in the working panels`, () => send('set_field_mode', { mode: 'docked' })));
}
// Max presence: engaged while open; attention when the selection carries open questions Max hasn't been shown.
const maxSeen = new Set();
function renderMaxPresence(state) {
  const record = recordFor(state.selected);
  const key = `${state.tenantId}|${state.selected}`;
  if (state.maxOpen) maxSeen.add(key);
  const questions = record?.questions?.length || 0;
  const attention = !state.maxOpen && questions > 0 && !maxSeen.has(key);
  $('max-orb').dataset.attention = String(attention);
  $('max-orb').setAttribute('aria-label', attention ? `Talk to Max (simulation) — ${questions} open question${questions > 1 ? 's' : ''} on ${record.name}` : 'Talk to Max (simulation)');
}
$('nav-back').addEventListener('click', () => send('navigate_back'));
$('nav-forward').addEventListener('click', () => send('navigate_forward'));
document.addEventListener('keydown', e => {
  if (!e.altKey || e.target.closest('input, textarea, select')) return;
  if (e.key === 'ArrowLeft') { e.preventDefault(); send('navigate_back'); }
  if (e.key === 'ArrowRight') { e.preventDefault(); send('navigate_forward'); }
});
$('reset-composition').addEventListener('click', () => {
  workspace.forgetPreference(); send('arrange_layout', { preset: 'spatial-first' });
  $('layout-status').textContent = 'Default composition restored for this tenant.';
});
// What Max sees, shown to the operator so the shared context is never hidden.
function renderMaxContext(state, fixture) {
  const ctx = describeContext(state, fixture);
  const signature = `${ctx.summary}|${state.layoutCheckpoint ? 1 : 0}`;
  if ($('max-context').dataset.signature === signature) return;
  $('max-context').dataset.signature = signature;
  const rows = [['Workspace', ctx.path.join(' › ')], ['Selected', ctx.selected ? `${ctx.selected.name}${ctx.selected.kind === 'object' ? ` · ${ctx.selected.type}` : ''}` : 'Nothing'],
    ['Panels', ctx.visible.length ? ctx.visible.map(p => `${p.title} (${p.dock}${p.pinned ? ', pinned' : ''}) · ${p.record.name}`).join(' / ') : 'None open'], ['Field', ctx.field]];
  const dl = element('dl'); rows.forEach(([k, v]) => dl.append(element('dt', k), element('dd', v)));
  $('max-context').replaceChildren(dl);
  $('max-context-line').textContent = `${ctx.path.join(' › ')} · ${ctx.visible.length} panel${ctx.visible.length === 1 ? '' : 's'}`;
  $('max-suggestions').replaceChildren(...suggestions(state, fixture).map(text => button(text, `Ask Max: ${text}`, () => askMax(text))));
}
const maxFieldRatio = (dock = workspace.getState().fieldDock) => {
  const area = $('work-area').getBoundingClientRect(), horizontal = ['top', 'bottom'].includes(dock);
  const size = horizontal ? area.height : area.width, min = horizontal ? MIN_PANEL.height : MIN_PANEL.width;
  return workspace.getState().panels.some(p => !p.minimized) && size ? Math.max(0.3, Math.min(0.65, (size - 8 - min) / size)) : 0.65;
};
// Geometry facts for Max, from the same pure layout function the workspace renders with.
function fitRatioForRows(rows) {
  const h = $('work-area').getBoundingClientRect().height;
  return h ? (h - 8 - rows * MIN_PANEL.height - 8 * (rows - 1)) / h : 0;
}
function panelRelation(st, a, b) {
  const area = $('work-area').getBoundingClientRect();
  const g = workspaceGeometry(st, area.width, area.height);
  if (st.fieldMode === 'expanded') return 'hidden';
  if (g.stacked.includes(a) || g.stacked.includes(b)) return 'stacked';
  const A = g.panels[a], B = g.panels[b]; if (!A || !B) return 'hidden';
  if (B.y >= A.y + A.height - 1) return 'below';
  if (A.y >= B.y + B.height - 1) return 'above';
  return 'beside';
}
// One Max turn: plan from current context → dispatch as one batch → reply from the verified result.
function askMax(text) {
  const scope = workspace.scope();
  if (!send('message', { text })) return;
  const before = workspace.getState();
  const plan = planMaxTurn(text, before, fixtures[before.tenantId], { maxFieldRatio: maxFieldRatio(), fitRatioForRows, relation: panelRelation });
  let ok = true;
  if (plan.undo) ok = send(plan.undo, {}, 'max', scope);
  else if (plan.steps.length) ok = send('batch', { commands: plan.steps }, 'max', scope);
  const after = workspace.getState();
  const { text: reply, receipt } = plan.report(after, ok);
  if (!send('message', { text: reply, receipt }, 'max', scope)) return; // tenant changed underneath: discard
  if (ok) markMaxChanges(before, after);
}
// Briefly outline panels Max moved or retargeted so the operator sees what changed.
function markMaxChanges(before, after) {
  const sig = p => p && `${p.dock}|${p.minimized}|${p.maximized}|${p.x}|${p.y}|${p.width}|${p.height}`;
  for (const p of after.panels) {
    const prior = before.panels.find(q => q.id === p.id);
    if (prior && sig(prior) === sig(p) && before.fieldRatio === after.fieldRatio && before.fieldMode === after.fieldMode) continue;
    const node = panelNodes.get(p.id); if (!node) continue;
    node.dataset.maxTouched = 'true'; clearTimeout(node.maxTimer);
    node.maxTimer = setTimeout(() => { delete node.dataset.maxTouched; }, 1800);
  }
}
$('ask').addEventListener('submit', event => {
  event.preventDefault();
  const text = $('question').value.trim(); if (!text) return;
  $('question').value = ''; askMax(text);
});
// Field splitter: drag (mouse/touch), or keyboard as an ARIA separator. Previews live; commits one undo step.
function placeSplitter(state, field, area) {
  const splitter = $('field-splitter');
  const show = state.fieldMode === 'docked' && innerWidth > 700 && !state.layoutLocked;
  splitter.hidden = !show; if (!show) return;
  const horizontal = ['top', 'bottom'].includes(state.fieldDock);
  splitter.dataset.orientation = horizontal ? 'horizontal' : 'vertical';
  splitter.setAttribute('aria-orientation', horizontal ? 'horizontal' : 'vertical');
  const g = 8;
  const box = { top: { x: field.x, y: field.y + field.height, w: field.width, h: g }, bottom: { x: field.x, y: field.y - g, w: field.width, h: g },
    left: { x: field.x + field.width, y: field.y, w: g, h: field.height }, right: { x: field.x - g, y: field.y, w: g, h: field.height } }[state.fieldDock];
  Object.assign(splitter.style, { left: `${box.x}px`, top: `${box.y}px`, width: `${box.w}px`, height: `${box.h}px` });
  const max = Math.round(maxFieldRatio(state.fieldDock) * 100);
  splitter.setAttribute('aria-valuemin', '30'); splitter.setAttribute('aria-valuemax', String(max));
  splitter.setAttribute('aria-valuenow', String(Math.round(state.fieldRatio * 100)));
  splitter.setAttribute('aria-valuetext', `Spatial field ${Math.round(state.fieldRatio * 100)}% of the workspace`);
}
function setFieldRatio(ratio, scope = workspace.scope()) {
  const state = workspace.getState();
  const next = Math.max(0.3, Math.min(maxFieldRatio(state.fieldDock), ratio));
  if (Math.abs(next - state.fieldRatio) < 0.001) return render(state);
  send('set_field_dock', { dock: state.fieldDock, ratio: Math.round(next * 1000) / 1000 }, 'human', scope);
}
{
  const splitter = $('field-splitter');
  const ratioAt = e => {
    const a = $('work-area').getBoundingClientRect(), dock = workspace.getState().fieldDock;
    return { top: (e.clientY - a.top) / a.height, bottom: (a.bottom - e.clientY) / a.height, left: (e.clientX - a.left) / a.width, right: (a.right - e.clientX) / a.width }[dock];
  };
  let drag = null;
  splitter.addEventListener('pointerdown', e => {
    if (workspace.getState().layoutLocked) return;
    if (e.button !== 0) return;
    e.preventDefault(); splitter.focus({ preventScroll: true });
    drag = { id: e.pointerId, scope: workspace.scope(), ratio: workspace.getState().fieldRatio };
    splitter.setPointerCapture(e.pointerId); splitter.dataset.dragging = 'true';
  });
  splitter.addEventListener('pointermove', e => {
    if (!drag || drag.id !== e.pointerId) return;
    drag.ratio = Math.max(0.3, Math.min(maxFieldRatio(), ratioAt(e)));
    render({ ...workspace.getState(), fieldRatio: drag.ratio }); // preview only; not dispatched
  });
  const end = e => {
    if (!drag || drag.id !== e.pointerId) return;
    const d = drag; drag = null; delete splitter.dataset.dragging;
    if (splitter.hasPointerCapture(e.pointerId)) splitter.releasePointerCapture(e.pointerId);
    if (e.type === 'pointercancel') return render(workspace.getState());
    setFieldRatio(d.ratio, d.scope);
  };
  splitter.addEventListener('pointerup', end); splitter.addEventListener('pointercancel', end);
  splitter.addEventListener('keydown', e => {
    const dock = workspace.getState().fieldDock, r = workspace.getState().fieldRatio;
    const grow = { top: 'ArrowDown', bottom: 'ArrowUp', left: 'ArrowRight', right: 'ArrowLeft' }[dock];
    const shrink = { top: 'ArrowUp', bottom: 'ArrowDown', left: 'ArrowLeft', right: 'ArrowRight' }[dock];
    const step = { [grow]: 0.02, [shrink]: -0.02, PageUp: 0.06, PageDown: -0.06 }[e.key];
    if (step !== undefined) { e.preventDefault(); setFieldRatio(r + step); }
    else if (e.key === 'Home') { e.preventDefault(); setFieldRatio(0.3); }
    else if (e.key === 'End') { e.preventDefault(); setFieldRatio(0.65); }
  });
}
// Opening composition: the business field overview on top, acquisition details and evidence underneath.
function openDemonstration() {
  const record = domainRecord('acquisition'), state = workspace.getState();
  const commands = [{ type: 'select_entity', recordId: record.id }, ...panelSteps(state, 'domain', record.id, 'left'),
    ...panelSteps(state, 'investigation', record.id, 'right'), { type: 'set_field_mode', mode: 'docked' }];
  // First composition of a fresh session is a system step (no undo entry); later requests are operator actions.
  if (!workspace.getState().panels.length && !workspace.getState().history.length) workspace.initialize(commands); else batch(commands);
}
$('undo-layout').addEventListener('click', () => send('undo_layout'));
$('redo-layout').addEventListener('click', () => send('redo_layout'));
document.querySelectorAll('[data-preset]').forEach(b => b.addEventListener('click', () => { send('arrange_layout', { preset: b.dataset.preset }); $('layout-menu').open = false; }));
$('field-dock').addEventListener('change', () => send('set_field_dock', { dock: $('field-dock').value, ratio: workspace.getState().fieldRatio }));
$('field-ratio').addEventListener('change', () => send('set_field_dock', { dock: $('field-dock').value, ratio: Number($('field-ratio').value) / 100 }));
$('save-layout-form').addEventListener('submit', e => {
  e.preventDefault(); const name = $('layout-name').value.trim();
  const ok = send('save_layout', { name });
  $('layout-status').textContent = ok ? (workspace.getState().storageWarning ? 'Saved for this session; device storage unavailable.' : `Saved “${name}” for this tenant.`) : 'Use a name up to 40 characters; at most 12 layouts per tenant.';
  if (ok) { $('saved-layouts').value = name; $('layout-name').value = ''; }
});
$('load-layout').addEventListener('click', () => { send('load_layout', { name: $('saved-layouts').value }); $('layout-menu').open = false; });
$('delete-layout').addEventListener('click', () => send('delete_layout', { name: $('saved-layouts').value }));
document.addEventListener('keydown', e => {
  if (!(e.ctrlKey || e.metaKey) || e.target.closest('input, textarea, select, [contenteditable]')) return;
  if (e.key.toLowerCase() === 'z') { e.preventDefault(); send(e.shiftKey ? 'redo_layout' : 'undo_layout'); }
});
workspace.subscribe(render); render(workspace.getState());
openDemonstration();
new ResizeObserver(() => render(workspace.getState())).observe($('work-area'));
// Explicit prototype inspection seam for deterministic tests, not a production API.
window.livingWorkspace = workspace;
