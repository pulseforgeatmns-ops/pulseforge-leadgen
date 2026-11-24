// Presentation state only. Neither this registry nor commands grant business authority.
// History model (V0.6):
// - Layout history (undo/redo) holds geometry, docking, field mode/size and saved layouts.
// - Navigation history (back/forward) holds the shared selection and domain focus.
// - Panels either follow the shared selection (default for tools opened on it) or are
//   explicitly pinned. After every command, undo, redo or navigation step, following
//   panels are reconciled to the authoritative selection, so no history operation can
//   leave a panel silently showing a different record.
export const tools = Object.freeze({
  domain: Object.freeze({ title: 'Details' }),
  investigation: Object.freeze({ title: 'Investigation' }),
});
const clone = value => structuredClone(value);
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
export function createWorkspace(fixtures, initialTenant = Object.keys(fixtures)[0], { storage = null } = {}) {
  let generation = 0;
  let state;
  let past = [], future = [];
  const layoutKeys = ['panels', 'focused', 'fieldMode', 'fieldDock', 'fieldRatio', 'savedLayouts'];
  const navKeys = ['selected', 'fieldFocus'];
  let navPast = [], navFuture = [], preference = null;
  const capture = () => Object.fromEntries(layoutKeys.map(key => [key, clone(state[key])]));
  const layoutCommands = new Set(['open_tool', 'dock_panel', 'resize_panel', 'move_panel', 'maximize_panel', 'close_panel', 'minimize_panel', 'restore_panel', 'set_field_mode', 'restore_checkpoint', 'pin_panel', 'set_field_dock', 'arrange_layout', 'save_layout', 'load_layout', 'delete_layout']);
  const storageKey = id => `pulseforge.fixture.layouts.v1.${id}`;
  const preferenceKey = id => `pulseforge.fixture.workspace.v1.${id}`;
  function validTemplate(t) {
    return t && ['docked', 'expanded', 'minimized'].includes(t.fieldMode)
      && ['top', 'bottom', 'left', 'right'].includes(t.fieldDock)
      && Number.isFinite(t.fieldRatio) && t.fieldRatio >= .3 && t.fieldRatio <= .65
      && Array.isArray(t.panels) && t.panels.length <= 20 && t.panels.every(p =>
        Object.hasOwn(tools, p.toolId) && ['left', 'right', 'top', 'bottom', 'floating'].includes(p.dock)
        && ['x', 'y', 'width', 'height'].every(k => Number.isFinite(p[k]) && p[k] >= 0 && p[k] <= 4000)
        && p.width >= 280 && p.width <= 1200 && p.height >= 200 && p.height <= 900
        && typeof p.minimized === 'boolean' && typeof p.maximized === 'boolean');
  }
  function readLayouts(id) {
    try {
      const saved = JSON.parse(storage?.getItem(storageKey(id)) || '{}');
      return Object.fromEntries(Object.entries(saved).filter(([name, t]) => name.trim() && name.length <= 40 && validTemplate(t)).slice(0, 12));
    } catch { return {}; }
  }
  function readPreference(id) {
    try { const t = JSON.parse(storage?.getItem(preferenceKey(id)) || 'null'); return validTemplate(t) ? t : null; } catch { return null; }
  }
  // The operator's current composition is remembered per tenant (record-free) and re-applied on return.
  function persistPreference() {
    preference = templateOf(state); state.hasPreference = true;
    try { storage?.setItem(preferenceKey(state.tenantId), JSON.stringify(preference)); } catch { state.storageWarning = true; }
  }
  function reconcile(s) {
    if (!s.selected) return;
    for (const p of s.panels) {
      if (!p.follow || p.recordId === s.selected) continue;
      const id = `${p.toolId}:${s.selected}`;
      if (s.panels.some(q => q !== p && q.id === id)) { p.follow = false; continue; } // another panel already shows it: keep this one, explicitly pinned
      if (s.focused === p.id) s.focused = id;
      p.id = id; p.recordId = s.selected;
    }
  }
  const prefSignature = s => JSON.stringify([s.panels.map(({ toolId, dock, x, y, width, height, minimized, maximized }) => [toolId, dock, x, y, width, height, minimized, maximized]), s.fieldMode, s.fieldDock, s.fieldRatio]);
  function persist() {
    try { storage?.setItem(storageKey(state.tenantId), JSON.stringify(state.savedLayouts)); state.storageWarning = !storage; }
    catch { state.storageWarning = true; }
  }
  const listeners = new Set();
  const emit = () => listeners.forEach(fn => fn(clone(state)));
  function switchTenant(tenantId) {
    if (!Object.hasOwn(fixtures, tenantId)) throw new Error('Unknown fixture tenant');
    generation++; past = []; future = []; navPast = []; navFuture = []; preference = readPreference(tenantId);
    state = { tenantId, generation, selected: null, trail: [], panels: [], focused: null,
      history: [], messages: [], maxOpen: false, fieldMode: 'docked', fieldDock: 'top', fieldRatio: .48, savedLayouts: readLayouts(tenantId), canUndo: false, canRedo: false, storageWarning: false, fieldView: { x: 0, y: 0, zoom: 1 }, fieldFocus: null, layoutCheckpoint: null, canBack: false, canForward: false, hasPreference: !!preference };
    emit();
  }
  switchTenant(initialTenant);
  function dispatch(command) {
    if (!command || command.tenantId !== state.tenantId || command.generation !== generation) return false;
    if (!['human', 'max'].includes(command.source)) return false;
    if (command.type === 'undo_layout' || command.type === 'redo_layout') {
      const from = command.type === 'undo_layout' ? past : future;
      const to = command.type === 'undo_layout' ? future : past;
      if (!from.length) return false;
      const prefBefore = prefSignature(state);
      to.push(capture()); Object.assign(state, from.pop()); reconcile(state);
      state.canUndo = past.length > 0; state.canRedo = future.length > 0;
      state.history = [...state.history, { type: command.type, source: command.source, recordId: null }].slice(-60);
      persist(); if (prefSignature(state) !== prefBefore) persistPreference(); emit(); return true;
    }
    if (command.type === 'navigate_back' || command.type === 'navigate_forward') {
      const from = command.type === 'navigate_back' ? navPast : navFuture;
      const to = command.type === 'navigate_back' ? navFuture : navPast;
      if (!from.length) return false;
      to.push(Object.fromEntries(navKeys.map(k => [k, state[k]])));
      const target = from.pop();
      state.selected = target.selected; state.fieldFocus = target.fieldFocus; state.fieldView = { x: 0, y: 0, zoom: 1 };
      if (target.selected) state.trail = [...state.trail.filter(id => id !== target.selected), target.selected].slice(-20);
      reconcile(state);
      state.canBack = navPast.length > 0; state.canForward = navFuture.length > 0;
      state.history = [...state.history, { type: command.type, source: command.source, recordId: state.selected }].slice(-60);
      emit(); return true;
    }
    const before = capture();
    const next = clone(state);
    let changedLayout = false, touchedSaved = false;
    // A batch applies registered commands in order to one draft: all or nothing,
    // one undo step. Max plans run this way so the operator never sees half a plan.
    const list = command.type === 'batch' ? command.commands : [command];
    if (!Array.isArray(list) || !list.length || list.length > 24) return false;
    for (const step of list) {
      if (!step || typeof step !== 'object' || ['batch', 'undo_layout', 'redo_layout', 'navigate_back', 'navigate_forward'].includes(step.type)) return false;
      if (!applyCommand(next, { ...step, source: command.source })) return false;
      next.history.push({ type: step.type, source: command.source, recordId: step.recordId || next.panels.find(p => p.id === step.panelId)?.recordId || null });
      if (layoutCommands.has(step.type)) changedLayout = true;
      if (['save_layout', 'delete_layout'].includes(step.type)) touchedSaved = true;
    }
    next.history = next.history.slice(-60);
    reconcile(next);
    if (navKeys.some(k => next[k] !== state[k])) {
      navPast.push(Object.fromEntries(navKeys.map(k => [k, state[k]]))); navPast = navPast.slice(-40); navFuture = [];
    }
    next.canBack = navPast.length > 0; next.canForward = navFuture.length > 0;
    if (changedLayout && JSON.stringify(before) !== JSON.stringify(Object.fromEntries(layoutKeys.map(k => [k, next[k]])))) {
      past.push(before); past = past.slice(-40); future = [];
    }
    next.canUndo = past.length > 0; next.canRedo = future.length > 0;
    const prefChanged = prefSignature(next) !== prefSignature(state);
    state = next;
    if (touchedSaved) persist();
    if (prefChanged) persistPreference();
    emit(); return true;
  }
  // Startup composition (system, not an operator action): no undo, navigation or history
  // entries. The tenant's remembered composition, if any, is then applied to the opened tools.
  function initialize(commands) {
    const next = clone(state);
    for (const step of commands) if (!applyCommand(next, { ...step, source: 'system' })) return false;
    if (preference) applyTemplate(next, preference);
    reconcile(next);
    state = next; emit(); return true;
  }
  function forgetPreference() {
    preference = null; state.hasPreference = false;
    try { storage?.removeItem?.(preferenceKey(state.tenantId)); } catch { /* ignore */ }
    emit();
  }
  const templateOf = s => ({ fieldDock: s.fieldDock, fieldRatio: s.fieldRatio, fieldMode: s.fieldMode,
    panels: s.panels.map(({ toolId, dock, x, y, width, height, minimized, maximized }) => ({ toolId, dock, x, y, width, height, minimized, maximized })) });
  // Applies a record-free template to the open tools, matching slots by tool; records and selection are untouched.
  function applyTemplate(next, template) {
    if (!validTemplate(template)) return false;
    next.fieldDock = template.fieldDock; next.fieldRatio = template.fieldRatio; next.fieldMode = template.fieldMode;
    const used = new Set();
    next.panels.forEach(p => {
      const index = template.panels.findIndex((slot, i) => !used.has(i) && slot.toolId === p.toolId);
      if (index >= 0) { used.add(index); for (const key of ['dock', 'x', 'y', 'width', 'height', 'minimized', 'maximized']) p[key] = template.panels[index][key]; }
    });
    return true;
  }
  function applyCommand(next, command) {
    const record = fixtures[next.tenantId].records.find(r => r.id === command.recordId);
    const panel = next.panels.find(p => p.id === command.panelId);
    switch (command.type) {
      case 'set_field_dock':
        if (!['top', 'bottom', 'left', 'right'].includes(command.dock) || !Number.isFinite(command.ratio)) return false;
        next.fieldDock = command.dock; next.fieldRatio = clamp(command.ratio, .3, .65); next.fieldMode = 'docked';
        break;
      case 'arrange_layout': {
        if (!['spatial-first', 'research', 'review', 'field'].includes(command.preset)) return false;
        next.fieldDock = command.preset === 'research' ? 'left' : 'top';
        next.fieldRatio = command.preset === 'research' ? .55 : .48;
        next.fieldMode = { review: 'minimized', field: 'expanded' }[command.preset] || 'docked';
        next.panels.forEach((p, i) => { p.dock = command.preset === 'research' ? (i % 2 ? 'bottom' : 'top') : (i % 2 ? 'right' : 'left'); p.minimized = false; p.maximized = false; });
        break;
      }
      case 'save_layout': {
        const name = typeof command.name === 'string' ? command.name.trim() : '';
        if (!name || name.length > 40 || (!Object.hasOwn(next.savedLayouts, name) && Object.keys(next.savedLayouts).length >= 12)) return false;
        next.savedLayouts = { ...next.savedLayouts, [name]: templateOf(next) };
        break;
      }
      case 'load_layout': {
        if (!Object.hasOwn(next.savedLayouts, command.name)) return false;
        if (!applyTemplate(next, next.savedLayouts[command.name])) return false;
        break;
      }
      case 'checkpoint_layout':
        // Max records the operator's layout before changing it, so "restore my previous layout" is exact.
        if (command.source !== 'max' && command.source !== 'system') return false;
        next.layoutCheckpoint = templateOf(next); break;
      case 'restore_checkpoint':
        if (!next.layoutCheckpoint || !applyTemplate(next, next.layoutCheckpoint)) return false;
        next.layoutCheckpoint = null; break;
      case 'delete_layout':
        if (!Object.hasOwn(next.savedLayouts, command.name)) return false;
        delete next.savedLayouts[command.name]; break;
      case 'minimize_panel':
        if (!panel) return false;
        panel.minimized = true;
        if (next.focused === panel.id) next.focused = next.panels.find(p => !p.minimized)?.id || null;
        break;
      case 'restore_panel':
        if (!panel) return false;
        panel.minimized = false; next.focused = panel.id; break;
      case 'set_field_mode':
        if (!['expanded', 'docked', 'minimized'].includes(command.mode)) return false;
        next.fieldMode = command.mode;
        break;
      case 'set_field_view':
        if (![command.x, command.y, command.zoom].every(Number.isFinite)) return false;
        next.fieldView = { x: clamp(command.x, -500, 500), y: clamp(command.y, -500, 500), zoom: clamp(command.zoom, .7, 1.6) };
        break;
      case 'focus_domain': {
        // Orbital navigation: null returns to the overview. Focus changes recentre the camera.
        const domainIds = fixtures[state.tenantId].overview.domains.map(d => d.id);
        if (command.domainId !== null && !domainIds.includes(command.domainId)) return false;
        if (next.fieldFocus !== command.domainId) next.fieldView = { x: 0, y: 0, zoom: 1 };
        next.fieldFocus = command.domainId;
        break;
      }
      case 'retarget_panel': {
        // Point an open tool at another record in place, keeping its geometry and dock.
        if (!panel || !record) return false;
        const id = `${panel.toolId}:${record.id}`;
        if (id === panel.id) { next.focused = id; break; }
        if (next.panels.some(p => p.id === id)) return false;
        panel.id = id; panel.recordId = record.id; panel.minimized = false; panel.follow = record.id === next.selected;
        if (next.focused === command.panelId) next.focused = id;
        break;
      }
      case 'pin_panel':
        // Pinned panels keep their record; following panels track the shared selection.
        if (!panel || typeof command.pinned !== 'boolean') return false;
        panel.follow = !command.pinned; break;
      case 'select_entity':
        if (!record) return false;
        next.selected = record.id;
        next.trail = [...next.trail.filter(id => id !== record.id), record.id].slice(-20);
        break;
      case 'open_tool': {
        if (!Object.hasOwn(tools, command.toolId) || !record) return false;
        const id = `${command.toolId}:${record.id}`;
        if (!next.panels.some(p => p.id === id)) {
          next.panels.push({ id, toolId: command.toolId, recordId: record.id, follow: record.id === next.selected, dock: 'floating',
            x: 30 + next.panels.length * 24, y: 40 + next.panels.length * 24, width: 360, height: 420, maximized: false, minimized: false });
        }
        next.panels.find(p => p.id === id).minimized = false;
        next.focused = id;
        break;
      }
      case 'dock_panel':
        if (!panel || !['left', 'right', 'top', 'bottom', 'floating'].includes(command.dock)) return false;
        panel.dock = command.dock; panel.minimized = false; panel.maximized = false; next.focused = panel.id;
        break;
      case 'resize_panel':
        if (!panel || !Number.isFinite(command.width) || !Number.isFinite(command.height)) return false;
        panel.width = clamp(command.width, 280, 1200); panel.height = clamp(command.height, 200, 900);
        if (Number.isFinite(command.x) && Number.isFinite(command.y)) { panel.x = clamp(command.x, 0, 4000); panel.y = clamp(command.y, 0, 4000); panel.dock = 'floating'; panel.maximized = false; }
        break;
      case 'move_panel':
        if (!panel || !Number.isFinite(command.x) || !Number.isFinite(command.y)) return false;
        panel.x = clamp(command.x, 0, 4000); panel.y = clamp(command.y, 0, 4000);
        panel.dock = 'floating'; panel.maximized = false;
        break;
      case 'focus_panel':
        if (!panel) return false;
        next.focused = panel.id;
        break;
      case 'maximize_panel':
        if (!panel) return false;
        panel.maximized = !panel.maximized; next.focused = panel.id;
        break;
      case 'close_panel':
        if (!panel) return false;
        next.panels = next.panels.filter(p => p.id !== panel.id);
        if (next.focused === panel.id) next.focused = next.panels.at(-1)?.id || null;
        break;
      case 'toggle_max': next.maxOpen = !next.maxOpen; break;
      case 'message': {
        if (typeof command.text !== 'string' || !command.text.trim() || command.text.length > 1000) return false;
        const receipt = command.receipt ?? [];
        if (!Array.isArray(receipt) || receipt.length > 12 || !receipt.every(r => typeof r === 'string' && r.length <= 120)) return false;
        next.messages.push(receipt.length ? { source: command.source, text: command.text, receipt: [...receipt] } : { source: command.source, text: command.text });
        next.messages = next.messages.slice(-40);
        break;
      }
      default: return false;
    }
    return true;
  }
  return { getState: () => clone(state), scope: () => ({ tenantId: state.tenantId, generation }),
    dispatch, switchTenant, initialize, forgetPreference, subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); } };
}
