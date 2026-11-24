import test from 'node:test';
import assert from 'node:assert/strict';
import { createWorkspace } from './store.mjs';
import { fixtures } from './fixtures.mjs';
import { planMaxTurn, describeContext } from './max-sim.mjs';
function turn(w, text, env) {
  const plan = planMaxTurn(text, w.getState(), fixtures[w.getState().tenantId], env);
  const ok = plan.undo ? w.dispatch({ ...w.scope(), type: plan.undo, source: 'max' }) : plan.steps.length ? w.dispatch({ ...w.scope(), type: 'batch', source: 'max', commands: plan.steps }) : true;
  return { plan, ok, ...plan.report(w.getState(), ok) };
}
test('the six example requests plan dispatcher commands and report the verified result', () => {
  const w = createWorkspace(fixtures, 'harbor');
  let r = turn(w, 'Max, show me Northgate Bakery.');
  assert.equal(r.plan.kind, 'select'); assert.equal(w.getState().selected, 'harbor-acquisition-1');
  assert.deepEqual(w.getState().panels.map(p => p.id), ['domain:harbor-acquisition-1', 'investigation:harbor-acquisition-1']);
  r = turn(w, 'Open the investigation.'); assert.equal(r.plan.kind, 'investigate'); assert.match(r.text, /already open/);
  w.dispatch({ ...w.scope(), type: 'dock_panel', source: 'human', panelId: 'investigation:harbor-acquisition-1', dock: 'bottom' });
  r = turn(w, 'Put the evidence beside the prospect details.');
  assert.equal(r.plan.kind, 'arrange'); assert.equal(w.getState().panels[1].dock, 'right');
  r = turn(w, 'Expand the spatial field.'); assert.equal(w.getState().fieldMode, 'expanded');
  r = turn(w, 'Restore my previous layout.'); assert.equal(w.getState().fieldMode, 'docked'); assert.match(r.text, /^Restored/);
  r = turn(w, 'Take me back to Acquisition.'); assert.equal(w.getState().fieldFocus, 'acquisition'); assert.equal(w.getState().selected, 'harbor-acquisition');
  assert.ok(w.getState().history.every(h => ['max', 'human'].includes(h.source)));
});
test('replies never claim success the dispatcher did not confirm', () => {
  const w = createWorkspace(fixtures, 'harbor');
  const plan = planMaxTurn('show me Northgate', w.getState(), fixtures.harbor);
  assert.match(plan.report(w.getState(), false).text, /rejected the change, so nothing moved/);
  assert.deepEqual(plan.report(w.getState(), false).receipt, []);
  // Dispatcher accepted but state disagrees with the plan: Max says so instead of confirming.
  assert.match(plan.report(w.getState(), true).text, /I tried to select Northgate Bakery, but the selection is empty/);
  // A stale scope (tenant switched mid-turn) is rejected by the dispatcher.
  const stale = w.scope(); w.switchTenant('cedar');
  assert.equal(w.dispatch({ ...stale, type: 'batch', source: 'max', commands: plan.steps }), false);
});
test('Max only sees the current tenant and caps field growth to keep panels usable', () => {
  const w = createWorkspace(fixtures, 'cedar');
  const r = turn(w, 'show me Northgate Bakery');
  assert.equal(r.plan.steps.length, 0); assert.match(r.text, /didn’t find that in Cedar Studio/); assert.equal(w.getState().selected, null);
  assert.doesNotMatch(describeContext(w.getState(), fixtures.cedar).summary, /Harbor/);
  turn(w, 'show me Willowmere');
  const grow = turn(w, 'make the field bigger', { maxFieldRatio: 0.52 });
  assert.equal(w.getState().fieldRatio, 0.52); assert.match(grow.text, /52% \(was 48%\)/);
  assert.match(turn(w, 'make the field bigger', { maxFieldRatio: 0.52 }).text, /already as large/);
});
