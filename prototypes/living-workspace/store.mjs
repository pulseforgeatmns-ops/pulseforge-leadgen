// Presentation state only. Neither this registry nor commands grant business authority.
export const tools = Object.freeze({
  domain: Object.freeze({ title: 'Domain details' }),
  investigation: Object.freeze({ title: 'Investigation' }),
});
const clone = value => structuredClone(value);
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
export function createWorkspace(fixtures, initialTenant = Object.keys(fixtures)[0]) {
  let generation = 0;
  let state;
  const listeners = new Set();
  const emit = () => listeners.forEach(fn => fn(clone(state)));
  function switchTenant(tenantId) {
    if (!Object.hasOwn(fixtures, tenantId)) throw new Error('Unknown fixture tenant');
    generation++;
    state = { tenantId, generation, selected: null, trail: [], panels: [], focused: null,
      history: [], messages: [], maxOpen: false, fieldMode: 'docked', fieldView: { x: 0, y: 0, zoom: 1 } };
    emit();
  }
  switchTenant(initialTenant);
  function dispatch(command) {
    if (!command || command.tenantId !== state.tenantId || command.generation !== generation) return false;
    if (!['human', 'max'].includes(command.source)) return false;
    const next = clone(state);
    const record = fixtures[state.tenantId].records.find(r => r.id === command.recordId);
    const panel = next.panels.find(p => p.id === command.panelId);
    switch (command.type) {
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
            x: 30 + next.panels.length * 24, y: 40 + next.panels.length * 24, width: 360, height: 420, maximized: false });
        }
        next.focused = id;
        break;
      }
      case 'dock_panel':
        if (!panel || !['left', 'right', 'floating'].includes(command.dock)) return false;
        panel.dock = command.dock; panel.maximized = false; next.focused = panel.id;
        break;
      case 'resize_panel':
        if (!panel || !Number.isFinite(command.width) || !Number.isFinite(command.height)) return false;
        panel.width = clamp(command.width, 280, 1200); panel.height = clamp(command.height, 240, 900);
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
    state = next; emit(); return true;
  }
  return { getState: () => clone(state), scope: () => ({ tenantId: state.tenantId, generation }),
    dispatch, switchTenant, subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); } };
}
