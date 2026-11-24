// Presentation state only. Neither this registry nor commands grant business authority.
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
  const layoutKeys = ['panels', 'focused', 'fieldMode', 'fieldView', 'fieldFocus', 'fieldDock', 'fieldRatio', 'savedLayouts'];
  const capture = () => Object.fromEntries(layoutKeys.map(key => [key, clone(state[key])]));
  const layoutCommands = new Set(['open_tool', 'dock_panel', 'resize_panel', 'move_panel', 'focus_panel', 'maximize_panel', 'close_panel', 'minimize_panel', 'restore_panel', 'set_field_mode', 'set_field_view', 'focus_domain', 'retarget_panel', 'restore_checkpoint', 'set_field_dock', 'arrange_layout', 'save_layout', 'load_layout', 'delete_layout']);
  const storageKey = id => `pulseforge.fixture.layouts.v1.${id}`;
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
  function persist() {
    try { storage?.setItem(storageKey(state.tenantId), JSON.stringify(state.savedLayouts)); state.storageWarning = !storage; }
    catch { state.storageWarning = true; }
  }
  const listeners = new Set();
  const emit = () => listeners.forEach(fn => fn(clone(state)));
  function switchTenant(tenantId) {
    if (!Object.hasOwn(fixtures, tenantId)) throw new Error('Unknown fixture tenant');
    generation++; past = []; future = [];
    state = { tenantId, generation, selected: null, trail: [], panels: [], focused: null,
      history: [], messages: [], maxOpen: false, fieldMode: 'docked', fieldDock: 'top', fieldRatio: .48, savedLayouts: readLayouts(tenantId), canUndo: false, canRedo: false, storageWarning: false, fieldView: { x: 0, y: 0, zoom: 1 }, fieldFocus: null, layoutCheckpoint: null };
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
      to.push(capture()); Object.assign(state, from.pop());
      state.canUndo = past.length > 0; state.canRedo = future.length > 0;
      state.history = [...state.history, { type: command.type, source: command.source, recordId: null }].slice(-60);
      persist(); emit(); return true;
    }
    const before = capture();
    const next = clone(state);
    let changedLayout = false, touchedSaved = false;
    // A batch applies registered commands in order to one draft: all or nothing,
    // one undo step. Max plans run this way so the operator never sees half a plan.
    const list = command.type === 'batch' ? command.commands : [command];
    if (!Array.isArray(list) || !list.length || list.length > 24) return false;
    for (const step of list) {
      if (!step || typeof step !== 'object' || ['batch', 'undo_layout', 'redo_layout'].includes(step.type)) return false;
      if (!applyCommand(next, { ...step, source: command.source })) return false;
      next.history.push({ type: step.type, source: command.source, recordId: step.recordId || next.panels.find(p => p.id === step.panelId)?.recordId || null });
      if (layoutCommands.has(step.type)) changedLayout = true;
      if (['save_layout', 'delete_layout'].includes(step.type)) touchedSaved = true;
    }
    next.history = next.history.slice(-60);
    if (changedLayout && JSON.stringify(before) !== JSON.stringify(Object.fromEntries(layoutKeys.map(k => [k, next[k]])))) {
      past.push(before); past = past.slice(-40); future = [];
    }
    next.canUndo = past.length > 0; next.canRedo = future.length > 0;
    state = next;
    if (touchedSaved) persist();
    emit(); return true;
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
        if (!['spatial-first', 'research', 'review'].includes(command.preset)) return false;
        next.fieldDock = command.preset === 'research' ? 'left' : 'top';
        next.fieldRatio = command.preset === 'research' ? .55 : .48;
        next.fieldMode = command.preset === 'review' ? 'minimized' : 'docked';
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
        if (command.source !== 'max') return false;
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
        panel.id = id; panel.recordId = record.id; panel.minimized = false;
        if (next.focused === command.panelId) next.focused = id;
        break;
      }
      case 'select_entity':
        if (!record) return false;
        next.selected = record.id;
        next.trail = [...next.trail.filter(id => id !== record.id), record.id].slice(-20);
        break;
      case 'open_tool': {
        if (!Object.hasOwn(tools, command.toolId) || !record) return false;
        const id = `${command.toolId}:${record.id}`;
        if (!next.panels.some(p => p.id === id)) {
          next.panels.push({ id, toolId: command.toolId, recordId: record.id, dock: 'floating',
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
    dispatch, switchTenant, subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); } };
}
