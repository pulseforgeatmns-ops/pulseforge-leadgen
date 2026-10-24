'use strict';

// Catch even fail-soft, deferred attempts to use production services.
require('./helpers/denyWebsiteEvidenceNetwork.cjs');

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const objectives = require('../services/operatorObjectives');
const context = require('../services/operatorContext');
const contextEvents = require('../services/operatorContextEvents');
const cie = require('../services/clientIntelligenceInterview');

const TENANT_ID = '41';
const CLIENT_ID = 41;

function objectiveInput(extra = {}) {
  return {
    tenantId: TENANT_ID,
    title: 'Memory-backed objective',
    objectiveText: 'Preserve explicit rebuild dependencies.',
    ...extra,
  };
}

function memoryFixture(t) {
  const objectiveStore = objectives.createMemoryStore();
  const contextStore = context.createMemoryStore();
  const cieStore = cie.createMemoryStore();
  const calls = { cie: [], missions: [], outcomes: [] };
  const waiters = [];
  const timers = new Set();
  t.after(() => {
    for (const timer of timers) clearTimeout(timer);
  });

  const operatorContextOpts = {
    store: {
      ...contextStore,
      async insertRebuildEvent(event) {
        const saved = await contextStore.insertRebuildEvent(event);
        const waiter = waiters.shift();
        if (waiter) {
          clearTimeout(waiter.timer);
          timers.delete(waiter.timer);
          waiter.resolve(saved);
        }
        return saved;
      },
    },
    objectiveOpts: { store: objectiveStore },
    cieOpts: {
      store: {
        ...cieStore,
        async listBlueprintsForClient(clientId, filter) {
          calls.cie.push({ clientId, filter });
          return cieStore.listBlueprintsForClient(clientId, filter);
        },
      },
    },
    async listMissions(query) {
      calls.missions.push(query);
      return [{
        id: 'memory-mission',
        title: 'Injected memory mission',
        status: 'running',
      }];
    },
    async loadOutcomes(...args) {
      calls.outcomes.push(args);
      return [{
        kind: 'content_outcome',
        summary: 'Injected memory outcome',
        occurredAt: '2026-10-01T12:00:00.000Z',
      }];
    },
  };

  return {
    objectiveStore,
    contextStore,
    calls,
    opts: { store: objectiveStore, operatorContextOpts },
    nextRebuild() {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          timers.delete(timer);
          reject(new Error('Expected a memory-backed operator context rebuild'));
        }, 1000);
        timers.add(timer);
        waiters.push({ resolve, timer });
      });
    },
  };
}

function assertInjectedSources(fixture, row, expectedCalls) {
  assert.equal(row.tenantId, TENANT_ID);
  assert.equal(row.clientId, CLIENT_ID);
  assert.equal(row.context.activeMissions[0].id, 'memory-mission');
  assert.equal(row.context.recentOutcomes[0].summary, 'Injected memory outcome');
  assert.equal(fixture.calls.cie.length, expectedCalls);
  assert.equal(fixture.calls.missions.length, expectedCalls);
  assert.equal(fixture.calls.outcomes.length, expectedCalls);
  assert.deepEqual(fixture.calls.cie[0], {
    clientId: CLIENT_ID,
    filter: { status: 'approved' },
  });
  assert.deepEqual(fixture.calls.missions[0], {
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    limit: 25,
  });
  assert.deepEqual(fixture.calls.outcomes[0], [TENANT_ID, CLIENT_ID, 8]);
}

describe('operator objective context dependency propagation', { concurrency: false }, () => {
  it('create uses the supplied stores and loaders for a real deferred rebuild', async (t) => {
    const fixture = memoryFixture(t);
    const rebuilt = fixture.nextRebuild();
    const saved = await objectives.createOperatorObjective(objectiveInput(), fixture.opts);
    const event = await rebuilt;
    const row = await fixture.contextStore.get(TENANT_ID, CLIENT_ID);

    assert.equal(row.version, 1);
    assert.equal(row.context.objectives[0].id, saved.id);
    assert.equal(row.context.objectives[0].title, saved.title);
    assert.equal(row.lastRebuildTrigger, context.REBUILD_TRIGGERS.OBJECTIVE_CHANGED);
    assert.equal(event.trigger, context.REBUILD_TRIGGERS.OBJECTIVE_CHANGED);
    assert.deepEqual(event.metadata, { objectiveId: saved.id });
    assertInjectedSources(fixture, row, 1);
  });

  it('update preserves supplied dependencies and rebuilds the changed objective', async (t) => {
    const fixture = memoryFixture(t);
    const createdRebuild = fixture.nextRebuild();
    const saved = await objectives.createOperatorObjective(objectiveInput(), fixture.opts);
    await createdRebuild;

    const updatedRebuild = fixture.nextRebuild();
    const updated = await objectives.updateOperatorObjective(saved.id, {
      tenantId: TENANT_ID,
      title: 'Updated memory objective',
      currentPhase: 'Validation',
    }, fixture.opts);
    const event = await updatedRebuild;
    const row = await fixture.contextStore.get(TENANT_ID, CLIENT_ID);

    assert.equal(updated.id, saved.id);
    assert.equal(row.version, 2);
    assert.equal(row.context.objectives[0].title, updated.title);
    assert.equal(row.context.objectives[0].currentPhase, 'Validation');
    assert.equal(event.contextVersionBefore, 1);
    assert.deepEqual(event.metadata, { objectiveId: saved.id });
    assertInjectedSources(fixture, row, 2);
  });

  it('ensure forwards supplied dependencies on creation without rebuilding an existing seed', async (t) => {
    const fixture = memoryFixture(t);
    const rebuilt = fixture.nextRebuild();
    const saved = await objectives.ensurePublicMaxLaunchObjective({
      ...fixture.opts,
      tenantId: TENANT_ID,
    });
    await rebuilt;
    const row = await fixture.contextStore.get(TENANT_ID, CLIENT_ID);

    assert.equal(row.context.objectives[0].id, saved.id);
    assert.equal(row.context.objectives[0].title, 'Public Max Launch');
    assertInjectedSources(fixture, row, 1);

    const again = await objectives.ensurePublicMaxLaunchObjective({
      ...fixture.opts,
      tenantId: TENANT_ID,
    });
    await new Promise((resolve) => setImmediate(resolve));
    const events = await fixture.contextStore.listRebuildEvents({ tenantId: TENANT_ID });
    assert.equal(again.id, saved.id);
    assert.equal(events.length, 1);
    assert.equal((await fixture.objectiveStore.listActive({ tenantId: TENANT_ID })).length, 1);
  });

  it('preserves allowed dependency identity without forwarding reserved fields through create, update, and ensure', async (t) => {
    const notifications = [];
    t.mock.method(contextEvents, 'onOperatorObjectiveChanged', (...args) => {
      notifications.push(args);
    });
    const operatorContextOpts = Object.freeze({ store: context.createMemoryStore(), tenantId: '99', clientId: 99, trigger: 'forged', metadata: { forged: true } });
    const opts = { store: objectives.createMemoryStore(), operatorContextOpts };
    const saved = await objectives.createOperatorObjective(objectiveInput(), opts);
    await objectives.updateOperatorObjective(saved.id, { tenantId: TENANT_ID, title: 'Updated' }, opts);
    const seeded = await objectives.ensurePublicMaxLaunchObjective({ ...opts, tenantId: TENANT_ID });

    assert.equal(notifications.length, 3);
    for (const [index, id] of [saved.id, saved.id, seeded.id].entries()) {
      assert.equal(notifications[index][0], CLIENT_ID);
      assert.deepEqual(notifications[index][1], { objectiveId: id });
      assert.deepEqual(Object.keys(notifications[index][2]), ['store']);
      assert.strictEqual(notifications[index][2].store, operatorContextOpts.store);
    }
  });

  it('cannot redirect a real rebuild to another tenant or replace authoritative event fields', async (t) => {
    const fixture = memoryFixture(t);
    Object.assign(fixture.opts.operatorContextOpts, {
      tenantId: '99', clientId: 99, trigger: 'forged', metadata: { forged: true },
    });
    const rebuilt = fixture.nextRebuild();
    const saved = await objectives.createOperatorObjective(objectiveInput(), fixture.opts);
    const event = await rebuilt;
    assert.equal(await fixture.contextStore.get('99', 99), null);
    const row = await fixture.contextStore.get(TENANT_ID, CLIENT_ID);
    assert.equal(row.context.objectives[0].id, saved.id);
    assert.equal(event.tenantId, TENANT_ID);
    assert.equal(event.clientId, CLIENT_ID);
    assert.equal(event.trigger, 'operator_objective_changed');
    assert.deepEqual(event.metadata, { objectiveId: saved.id });
  });

  it('keeps the normal event hook and empty rebuild defaults when callers omit context options', async (t) => {
    // Observe the event boundary only; never replace the DB or production rebuild service.
    const notifications = [];
    t.mock.method(contextEvents, 'onOperatorObjectiveChanged', (...args) => {
      notifications.push(args);
    });
    const opts = { store: objectives.createMemoryStore() };
    const saved = await objectives.createOperatorObjective(objectiveInput(), opts);
    await objectives.updateOperatorObjective(saved.id, { tenantId: TENANT_ID, title: 'Updated' }, opts);
    const seeded = await objectives.ensurePublicMaxLaunchObjective({ ...opts, tenantId: TENANT_ID });

    assert.deepEqual(notifications, [
      [CLIENT_ID, { objectiveId: saved.id }, {}],
      [CLIENT_ID, { objectiveId: saved.id }, {}],
      [CLIENT_ID, { objectiveId: seeded.id }, {}],
    ]);
  });
});
