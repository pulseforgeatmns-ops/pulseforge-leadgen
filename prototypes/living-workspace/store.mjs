// Presentation state only. Neither this registry nor commands grant business authority.
export const tools = Object.freeze({
  domain: Object.freeze({ title: 'Domain details' }),
  investigation: Object.freeze({ title: 'Investigation' }),
});
const clone = value => structuredClone(value);
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
export function createWorkspace(fixtures, initialTenant = Object.keys(fixtures)[0], { storage = null } = {}) {
  let generation = 0;
  let state;
  let past = [], future = [];
  const layoutKeys = ['panels', 'focused', 'fieldMode', 'fieldView', 'fieldDock', 'fieldRatio', 'savedLayouts'];
  const capture = () => Object.fromEntries(layoutKeys.map(key => [key, clone(state[key])]));
  const layoutCommands = new Set(['open_tool', 'dock_panel', 'resize_panel', 'move_panel', 'focus_panel', 'maximize_panel', 'close_panel', 'minimize_panel', 'restore_panel', 'set_field_mode', 'set_field_view', 'set_field_dock', 'arrange_layout', 'save_layout', 'load_layout', 'delete_layout']);
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
      history: [], messages: [], maxOpen: false, fieldMode: 'docked', fieldDock: 'top', fieldRatio: .48, savedLayouts: readLayouts(tenantId), canUndo: false, canRedo: false, storageWarning: false, fieldView: { x: 0, y: 0, zoom: 1 } };
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
    const record = fixtures[state.tenantId].records.find(r => r.id === command.recordId);
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
        const template = { fieldDock: next.fieldDock, fieldRatio: next.fieldRatio, fieldMode: next.fieldMode,
          panels: next.panels.map(({ toolId, dock, x, y, width, height, minimized, maximized }) => ({ toolId, dock, x, y, width, height, minimized, maximized })) };
        next.savedLayouts = { ...next.savedLayouts, [name]: template };
        break;
      }
      case 'load_layout': {
        if (!Object.hasOwn(next.savedLayouts, command.name)) return false;
        const template = next.savedLayouts[command.name];
        if (!validTemplate(template)) return false;
        next.fieldDock = template.fieldDock; next.fieldRatio = template.fieldRatio; next.fieldMode = template.fieldMode;
        const used = new Set();
        next.panels.forEach(p => {
          const index = template.panels.findIndex((slot, i) => !used.has(i) && slot.toolId === p.toolId);
          if (index >= 0) { used.add(index); for (const key of ['dock', 'x', 'y', 'width', 'height', 'minimized', 'maximized']) p[key] = template.panels[index][key]; }
        });
        break;
      }
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
      case 'message':
        if (typeof command.text !== 'string' || !command.text.trim() || command.text.length > 1000) return false;
        next.messages.push({ source: command.source, text: command.text });
        next.messages = next.messages.slice(-40);
        break;
      default: return false;
    }
    next.history.push({ type: command.type, source: command.source, recordId: command.recordId || panel?.recordId || null });
    next.history = next.history.slice(-60);
    if (layoutCommands.has(command.type) && JSON.stringify(before) !== JSON.stringify(Object.fromEntries(layoutKeys.map(k => [k, next[k]])))) {
      past.push(before); past = past.slice(-40); future = [];
    }
    next.canUndo = past.length > 0; next.canRedo = future.length > 0;
    state = next;
    if (['save_layout', 'delete_layout'].includes(command.type)) persist();
    emit(); return true;
  }
  return { getState: () => clone(state), scope: () => ({ tenantId: state.tenantId, generation }),
    dispatch, switchTenant, subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); } };
}
