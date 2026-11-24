import { createSpatialField } from './spatial-field.mjs';
import { fixtures } from './fixtures.mjs';
import { createWorkspace, tools } from './store.mjs';

const workspace = createWorkspace(fixtures);
const $ = id => document.getElementById(id);
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
function openRecord(record, source = 'human', scope = workspace.scope()) {
  send('select_entity', { recordId: record.id }, source, scope);
  send('open_tool', { toolId: 'domain', recordId: record.id }, source, scope);
  send('dock_panel', { panelId: `domain:${record.id}`, dock: 'left' }, source, scope);
  send('set_field_mode', { mode: 'docked' }, source, scope);
}
const renderers = {
  domain(body, record) {
    body.append(element('p', record.domain.toUpperCase(), 'eyebrow'), element('h2', record.name), element('p', record.summary));
    const facts = element('dl', '', 'record-facts');
    for (const [label, value] of [['Record', record.id], ['Status', 'Awaiting review'], ['Source', 'Local fixture']]) { facts.append(element('dt', label), element('dd', value)); }
    body.append(facts);
    body.append(button('Investigate →', `Investigate ${record.name}`, () => {
      send('select_entity', { recordId: record.id });
      openInvestigation(record);
    }));
  },
  investigation(body, record) {
    body.append(element('p', 'EVIDENCE · FIXTURE', 'eyebrow'), element('h2', record.name));
    const list = element('ol', '', 'evidence-list'); record.evidence.forEach(text => list.append(element('li', text)));
    body.append(list, element('p', 'This investigation is shared with the simulated Max conversation.'));
    body.append(button('Select this record', `Select ${record.name}`, () => send('select_entity', { recordId: record.id })));
  },
};

function gesture(handle, node, panelId, resize) {
  handle.addEventListener('pointerdown', event => {
    if (event.button !== 0) return;
    handle.focus();
    const scope = workspace.scope();
    send('focus_panel', { panelId });
    const area = $('work-area').getBoundingClientRect();
    const rect = node.getBoundingClientRect();
    const fromRight = false;
    const start = { x: event.clientX, y: event.clientY, left: rect.left - area.left, top: rect.top - area.top };
    let final = null;
    handle.setPointerCapture(event.pointerId);
    function move(e) {
      if (e.pointerId !== event.pointerId) return;
      const dx = e.clientX - start.x, dy = e.clientY - start.y;
      if (resize) {
        final = { width: Math.max(280, Math.min(fromRight ? area.width : area.width - start.left, rect.width + (fromRight ? -dx : dx))), height: Math.max(240, Math.min(area.height - start.top, rect.height + dy)) };
        node.style.width = `${final.width}px`; node.style.height = `${final.height}px`;
        if (fromRight) node.style.left = `${area.width - final.width}px`;
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
        if (resize) send('move_panel', { panelId, x: start.left, y: start.top }, 'human', scope);
        send(resize ? 'resize_panel' : 'move_panel', { panelId, ...final }, 'human', scope);
      }
      render(workspace.getState());
    }
    handle.addEventListener('pointermove', move); handle.addEventListener('pointerup', finish); handle.addEventListener('pointercancel', finish);
  });
}
function makePanel(panel) {
  const node = element('section', '', 'panel'); node.dataset.panelId = panel.id;
  node.tabIndex = -1; node.setAttribute('aria-label', `${tools[panel.toolId].title}: ${recordFor(panel.recordId).name}`);
  const header = element('header');
  const grip = button(tools[panel.toolId].title, `Move ${tools[panel.toolId].title}; arrow keys move`, () => send('focus_panel', { panelId: panel.id }));
  grip.className = 'grip';
  grip.addEventListener('keydown', event => {
    const delta = { ArrowLeft: [-20, 0], ArrowRight: [20, 0], ArrowUp: [0, -20], ArrowDown: [0, 20] }[event.key];
    if (!delta) return; event.preventDefault();
    const rect = node.getBoundingClientRect(), area = $('work-area').getBoundingClientRect();
    send('move_panel', { panelId: panel.id, x: Math.max(0, Math.min(area.width - rect.width, rect.left - area.left + delta[0])), y: Math.max(0, Math.min(area.height - rect.height, rect.top - area.top + delta[1])) });
  });
  header.append(grip, button('×', 'Close panel', () => {
    send('close_panel', { panelId: panel.id });
    const focused = workspace.getState().focused;
    if (focused) panelNodes.get(focused)?.focus(); else $('cdViewToggle').focus();
  }));
  const controls = element('nav', '', 'panel-controls'); controls.setAttribute('aria-label', 'Panel layout');
  for (const dock of ['left', 'right', 'floating']) controls.append(button(dock === 'floating' ? 'Float' : dock === 'left' ? 'Primary' : 'Secondary', dock === 'floating' ? 'Dock floating' : `Dock ${dock === 'left' ? 'primary' : 'secondary'} slot`, () => send('dock_panel', { panelId: panel.id, dock })));
  const maximize = button('Focus', 'Focus or restore panel', () => send('maximize_panel', { panelId: panel.id }));
  maximize.className = 'maximize'; controls.append(maximize);
  const body = element('div', '', 'panel-body'); renderers[panel.toolId](body, recordFor(panel.recordId));
  const trail = element('p', '', 'trail'); body.append(trail);
  const resize = button('◢', 'Resize panel; arrow keys resize', () => {}); resize.className = 'resize';
  resize.addEventListener('keydown', event => {
    const delta = { ArrowLeft: [-20, 0], ArrowRight: [20, 0], ArrowUp: [0, -20], ArrowDown: [0, 20] }[event.key];
    if (!delta) return; event.preventDefault();
    const rect = node.getBoundingClientRect();
    const area = $('work-area').getBoundingClientRect();
    send('move_panel', { panelId: panel.id, x: rect.left - area.left, y: rect.top - area.top });
    send('resize_panel', { panelId: panel.id, width: rect.width + delta[0], height: rect.height + delta[1] });
  });
  node.append(header, controls, body, resize);
  node.addEventListener('focusin', () => { if (workspace.getState().focused !== panel.id) send('focus_panel', { panelId: panel.id }); });
  node.addEventListener('pointerdown', () => { if (workspace.getState().focused !== panel.id) send('focus_panel', { panelId: panel.id }); });
  gesture(grip, node, panel.id, false); gesture(resize, node, panel.id, true);
  return node;
}
function openInvestigation(record) {
  send('select_entity', { recordId: record.id });
  send('open_tool', { toolId: 'investigation', recordId: record.id });
  send('dock_panel', { panelId: `investigation:${record.id}`, dock: 'right' });
  send('set_field_mode', { mode: 'docked' });
}
const tabs = element('nav', '', 'panel-tabs'); tabs.setAttribute('aria-label', 'Open panels'); $('work-area').after(tabs);
function render(state) {
  const fixture = fixtures[state.tenantId];
  $('work-area').dataset.fieldMode = state.fieldMode;
  $('empty-tools').hidden = state.panels.length > 0 || state.fieldMode !== 'docked';
  document.querySelectorAll('[data-field-mode]').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.fieldMode === state.fieldMode)));
  spatial.render(fixture, state);
  $('zoom-level').textContent = `${Math.round(state.fieldView.zoom * 100)}%`;
  for (const [id, node] of panelNodes) if (!state.panels.some(p => p.id === id)) { node.remove(); panelNodes.delete(id); }
  const area = $('work-area').getBoundingClientRect();
  for (const panel of state.panels) {
    let node = panelNodes.get(panel.id);
    if (!node) { node = makePanel(panel); panelNodes.set(panel.id, node); $('panels').append(node); }
    node.dataset.focused = String(state.focused === panel.id);
    node.dataset.dock = panel.dock; node.dataset.maximized = String(panel.maximized);
    const compact = innerWidth <= 700;
    const split = state.fieldMode === 'minimized' ? 0 : Math.round(area.width * .57) + 10;
    const docked = panel.dock !== 'floating' && !panel.maximized;
    const slots = ['left', 'right'].filter(dock => state.panels.some(p => p.dock === dock));
    const index = slots.indexOf(panel.dock);
    const two = slots.length > 1;
    let width = Math.min(panel.width, area.width), height = Math.min(panel.height, area.height);
    let x = Math.min(panel.x, area.width - width), y = Math.min(panel.y, area.height - height);
    if (docked && !compact) {
      if (state.fieldMode === 'minimized') {
        width = two ? (area.width - 10) / 2 : area.width; height = area.height - 48;
        x = index * (width + 10); y = 48;
      } else {
        width = area.width - split; height = two ? (area.height - 10) / 2 : area.height;
        x = split; y = index * (height + 10);
      }
    }
    if (compact) { width = area.width; x = 0; y = 0; }
    if (panel.maximized) { width = area.width; height = area.height; x = 0; y = 0; }
    node.hidden = state.fieldMode === 'expanded' || (compact && state.focused !== panel.id);
    Object.assign(node.style, { width: `${width}px`, height: `${height}px`, left: `${x}px`, top: `${y}px`, right: 'auto', zIndex: state.focused === panel.id ? '20' : '10' });
    node.querySelector('.maximize').textContent = panel.maximized ? 'Restore' : 'Focus';
    node.querySelector('.maximize').setAttribute('aria-pressed', String(panel.maximized));
    node.querySelector('.trail').textContent = `Shared trail: ${state.trail.map(id => fixture.records.find(r => r.id === id).name).join(' → ') || 'No selection'}`;
  }
  // Keep buttons stable during focus and pointer interactions.
  const signature = state.panels.map(p => p.id).join('|');
  if (tabs.dataset.signature !== signature) {
    tabs.dataset.signature = signature;
    tabs.replaceChildren(...state.panels.map(p => {
      const selectPanel = () => {
        if (workspace.getState().fieldMode === 'expanded') send('set_field_mode', { mode: 'docked' });
        if (workspace.getState().focused !== p.id) send('focus_panel', { panelId: p.id });
        panelNodes.get(p.id)?.focus({ preventScroll: true });
      };
      const tab = button(`${tools[p.toolId].title} · ${recordFor(p.recordId).domain}`, `Show ${p.id}`, selectPanel);
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
  document.querySelectorAll('[data-domain-id]').forEach(n => n.classList.toggle('selected', n.dataset.domainId === recordFor(state.selected)?.domain));
  $('conversation').hidden = !state.maxOpen; $('max-orb').setAttribute('aria-expanded', String(state.maxOpen));
  const messageSignature = JSON.stringify(state.messages);
  if ($('messages').dataset.signature !== messageSignature) {
    $('messages').dataset.signature = messageSignature;
    $('messages').replaceChildren(...state.messages.map(m => element('p', `${m.source === 'max' ? 'Max (simulated)' : 'You'}: ${m.text}`, m.source)));
    $('messages').scrollTop = $('messages').scrollHeight;
  }
  $('history').replaceChildren(...state.history.slice(-12).reverse().map(h => element('li', `${h.source === 'max' ? 'Max' : 'You'} · ${h.type.replaceAll('_', ' ')}${h.recordId ? ' · ' + recordFor(h.recordId).name : ''}`)));
}
const spatial = createSpatialField({ host: $('cdSpatialCanvas'), orbit: $('cdSpatialOrbit'), connections: $('cdIntelConnections'), list: $('cdSpatialList'), dispatch: workspace.dispatch, scope: workspace.scope, onOpen: openRecord, onInvestigate: openInvestigation });
$('cdViewToggle').addEventListener('click', () => {
  const list = !$('cdSpatialCanvas').hidden;
  $('cdSpatialCanvas').hidden = list; $('cdSpatialList').hidden = !list;
  $('cdViewToggle').textContent = list ? 'Spatial view' : 'List view';
  $('cdViewToggle').setAttribute('aria-pressed', String(list));
});
document.querySelectorAll('[data-field-mode]').forEach(b => b.addEventListener('click', () => send('set_field_mode', { mode: b.dataset.fieldMode })));
for (const [id, delta] of [['zoom-in', .1], ['zoom-out', -.1]]) $(id).addEventListener('click', () => send('set_field_view', { ...workspace.getState().fieldView, zoom: workspace.getState().fieldView.zoom + delta }));
$('reset-field').addEventListener('click', () => send('set_field_view', { x: 0, y: 0, zoom: 1 }));
$('open-context').addEventListener('click', () => openDemonstration());
$('tenant').addEventListener('change', () => {
  $('question').value = '';
  workspace.switchTenant($('tenant').value);
  render(workspace.getState());
  $('status').textContent = `Switched to ${fixtures[$('tenant').value].name}. Previous context cleared.`;
});
function toggleMax() { send('toggle_max'); if (workspace.getState().maxOpen) $('question').focus(); else $('max-orb').focus(); }
$('max-orb').addEventListener('click', toggleMax); $('close-max').addEventListener('click', toggleMax);
document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && workspace.getState().maxOpen) { event.preventDefault(); toggleMax(); }
});
$('ask').addEventListener('submit', event => {
  event.preventDefault();
  const text = $('question').value.trim(); if (!text) return;
  const scope = workspace.scope();
  send('message', { text }); $('question').value = '';
  const state = workspace.getState();
  const record = fixtures[state.tenantId].records.find(r => text.toLowerCase().includes(r.domain)) || recordFor(state.selected);
  let reply;
  if (/close panels/i.test(text)) {
    state.panels.forEach(p => send('close_panel', { panelId: p.id }, 'max', scope)); reply = 'I closed the working panels. Your selection is still available.';
  } else if (/arrange/i.test(text)) {
    state.panels.forEach((p, i) => send('dock_panel', { panelId: p.id, dock: i % 2 ? 'right' : 'left' }, 'max', scope));
    reply = state.panels.length ? 'I arranged your panels at the sides of the field.' : 'Choose a domain first, then ask me to arrange the panels.';
  } else if (record) {
    openRecord(record, 'max', scope);
    send('dock_panel', { panelId: `domain:${record.id}`, dock: 'left' }, 'max', scope);
    send('open_tool', { toolId: 'investigation', recordId: record.id }, 'max', scope);
    send('dock_panel', { panelId: `investigation:${record.id}`, dock: 'right' }, 'max', scope);
    reply = `We’re investigating ${record.name}. I opened its details and evidence, using our shared selection.`;
  } else reply = 'I can simulate opening a domain and its investigation. Try “investigate acquisition”, “arrange”, or “close panels”.';
  send('message', { text: reply }, 'max', scope);
});
function openDemonstration() {
  const record = fixtures[workspace.getState().tenantId].records.find(r => r.domain === 'acquisition');
  openRecord(record); openInvestigation(record);
}
workspace.subscribe(render); render(workspace.getState());
openDemonstration();
new ResizeObserver(() => render(workspace.getState())).observe($('work-area'));
// Explicit prototype inspection seam for deterministic tests, not a production API.
window.livingWorkspace = workspace;
