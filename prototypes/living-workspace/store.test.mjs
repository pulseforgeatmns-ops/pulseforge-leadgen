import test from 'node:test';
import assert from 'node:assert/strict';
import { createWorkspace } from './store.mjs';
import { fixtures } from './fixtures.mjs';
const make = () => createWorkspace(fixtures);
const command = (store, type, fields = {}, source = 'human') => store.dispatch({ ...fields, type, source, ...store.scope() });
test('tenant switch clears all records, investigations, panels, history and conversations', () => {
  const store = make();
  command(store, 'select_entity', { recordId: 'harbor-content' });
  command(store, 'open_tool', { recordId: 'harbor-content', toolId: 'investigation' });
  command(store, 'message', { text: 'Harbor private context' });
  command(store, 'toggle_max');
  const oldScope = store.scope();
  store.switchTenant('cedar');
  assert.deepEqual(store.getState(), { tenantId: 'cedar', generation: 2, selected: null, trail: [], panels: [], focused: null, history: [], messages: [], maxOpen: false, fieldMode: 'docked', fieldView: { x: 0, y: 0, zoom: 1 } });
  assert.equal(store.dispatch({ ...oldScope, type: 'open_tool', source: 'max', recordId: 'harbor-content', toolId: 'domain' }), false);
  assert.equal(command(store, 'select_entity', { recordId: 'harbor-content' }), false);
  store.switchTenant('harbor');
  assert.equal(store.dispatch({ ...oldScope, type: 'message', source: 'max', text: 'Delayed reply' }), false);
  assert.equal(store.getState().selected, null);
});
test('registered commands validate tools, records, finite geometry and scope without mutation', () => {
  const store = make(), before = store.getState();
  for (const fields of [
    { type: 'send_email' }, { type: 'open_tool', toolId: '__proto__', recordId: 'harbor-content' },
    { type: 'open_tool', toolId: 'domain', recordId: 'cedar-content' }, { type: 'dock_panel', panelId: 'missing', dock: 'right' },
  ]) assert.equal(store.dispatch({ ...store.scope(), source: 'max', ...fields }), false);
  assert.deepEqual(store.getState(), before);
  command(store, 'open_tool', { toolId: 'domain', recordId: 'harbor-content' });
  const panelId = store.getState().focused;
  assert.equal(command(store, 'resize_panel', { panelId, width: NaN, height: 300 }), false);
  command(store, 'resize_panel', { panelId, width: 1, height: 50000 });
  assert.equal(store.getState().panels[0].width, 280);
  assert.equal(store.getState().panels[0].height, 900);
});
test('human and Max share selection, panel identity, layout and bounded history', () => {
  const store = make();
  command(store, 'select_entity', { recordId: 'harbor-acquisition' });
  command(store, 'open_tool', { toolId: 'investigation', recordId: 'harbor-acquisition' }, 'max');
  command(store, 'open_tool', { toolId: 'investigation', recordId: 'harbor-acquisition' });
  const panelId = store.getState().focused;
  command(store, 'dock_panel', { panelId, dock: 'right' }, 'max');
  command(store, 'maximize_panel', { panelId });
  assert.equal(store.getState().panels.length, 1);
  assert.equal(store.getState().panels[0].dock, 'right');
  assert.equal(store.getState().panels[0].maximized, true);
  for (let n = 0; n < 100; n++) command(store, 'focus_panel', { panelId });
  assert.equal(store.getState().history.length, 60);
  const snapshot = store.getState(); snapshot.trail.push('cedar-content');
  assert.deepEqual(store.getState().trail, ['harbor-acquisition']);
  command(store, 'close_panel', { panelId });
  assert.equal(store.getState().focused, null);
});

test('field layout and pan/zoom preserve investigation context and reject stale tenant commands', () => {
  const store = make();
  command(store, 'select_entity', { recordId: 'harbor-content' });
  command(store, 'open_tool', { recordId: 'harbor-content', toolId: 'investigation' });
  const before = store.getState(), oldScope = store.scope();
  for (const mode of ['expanded', 'minimized', 'docked']) {
    assert.equal(command(store, 'set_field_mode', { mode }), true);
    assert.deepEqual(store.getState().panels, before.panels);
    assert.deepEqual(store.getState().trail, before.trail);
    assert.equal(store.getState().selected, before.selected);
  }
  assert.equal(command(store, 'set_field_mode', { mode: 'invalid' }), false);
  command(store, 'set_field_view', { x: 20, y: -40, zoom: 1.2 });
  assert.deepEqual(store.getState().fieldView, { x: 20, y: -40, zoom: 1.2 });
  assert.equal(command(store, 'set_field_view', { x: NaN, y: 0, zoom: 1 }), false);
  store.switchTenant('cedar');
  assert.equal(store.dispatch({ ...oldScope, source: 'max', type: 'set_field_mode', mode: 'expanded' }), false);
  assert.deepEqual(store.getState().fieldView, { x: 0, y: 0, zoom: 1 });
});
