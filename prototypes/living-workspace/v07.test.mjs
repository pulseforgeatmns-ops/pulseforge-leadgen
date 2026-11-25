import test from 'node:test';
import assert from 'node:assert/strict';
import { createWorkspace } from './store.mjs';
import { fixtures } from './fixtures.mjs';

const makeStorage = () => { const values = new Map(); return { getItem: k => values.get(k) || null, setItem: (k, v) => values.set(k, v), removeItem: k => values.delete(k) }; };
const command = (store, type, fields = {}, source = 'human') => store.dispatch({ ...store.scope(), source, type, ...fields });

test('layout lock rejects an entire Max batch while navigation and lenses remain available', () => {
  const store = createWorkspace(fixtures, 'harbor', { storage: makeStorage() });
  assert.equal(command(store, 'lock_layout'), true);
  const before = store.getState();
  assert.equal(command(store, 'batch', { commands: [{ type: 'select_entity', recordId: 'harbor-acquisition-1' }, { type: 'set_field_mode', mode: 'expanded' }] }, 'max'), false);
  assert.equal(store.getLastResult().reason, 'layout_locked');
  assert.equal(store.getState().selected, before.selected, 'batch is atomic');
  assert.equal(command(store, 'select_entity', { recordId: 'harbor-acquisition-1' }), true);
  assert.equal(command(store, 'set_lens', { lens: 'entities' }), true);
  assert.equal(store.getState().lens, 'entities');
  assert.equal(command(store, 'undo_layout'), false);
});

test('locked composition persists per tenant and a confirmed copy preserves the original', () => {
  const storage = makeStorage();
  const store = createWorkspace(fixtures, 'harbor', { storage });
  command(store, 'set_field_mode', { mode: 'expanded' }); command(store, 'lock_layout');
  const revision = store.getState().compositionRevision;
  store.switchTenant('cedar'); assert.equal(store.getState().layoutLocked, false);
  store.switchTenant('harbor');
  assert.equal(store.getState().layoutLocked, true); assert.equal(store.getState().fieldMode, 'expanded');
  assert.equal(command(store, 'copy_locked_composition', { revision, confirm: true }), true);
  assert.equal(store.getState().layoutLocked, false);
  assert.ok(Object.keys(store.getState().savedLayouts).some(n => n.startsWith('Temporary copy')));
});

test('entity filtering is bounded state, preserves selection, and scales fixture counts without nodes', () => {
  const store = createWorkspace(fixtures);
  command(store, 'select_entity', { recordId: 'harbor-acquisition-1' });
  command(store, 'set_lens', { lens: 'entities' });
  command(store, 'set_entity_filter', { query: 'bakery', domain: 'acquisition' });
  const state = store.getState();
  assert.equal(state.selected, 'harbor-acquisition-1');
  assert.deepEqual([state.lens, state.entityQuery, state.entityDomain, state.nodeBudget], ['entities', 'bakery', 'acquisition', 18]);
  command(store, 'set_lens', { lens: 'objectives' });
  assert.equal(store.getState().selected, 'harbor-acquisition-1');
});
