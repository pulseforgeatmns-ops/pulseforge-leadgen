'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');

test('zero verified inventory rolls Scout back before Max, Paige and Emmett; no second attempt', async () => {
  const amo = require('../packages/acquisition-mission');
  const steps = require('../packages/max/workspace/AmoOperatorApproval');
  const emmett = require('../packages/max/workspace/EmmettCapacityExecution');
  const { adapters } = require('../services/governedOutboundAdapters');
  const { missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
  const engine = amo.createAcquisitionMissionEngine();
  const source = engine.create({ tenantId: '10', objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.', targetSegment: 'Law Firms' });
  await steps.advancePlanAfterApproval({ engine, mission: source, tenantId: '10', question: 'Approved.' });
  const snapshot = engine.inspect(source.id, { tenantId: '10' });
  // Legacy approved missions predate the optional resolvedObjective property.
  // The daily mission factory materializes its absence as null.
  delete snapshot.mission.resolvedObjective;
  const sender = { ...emmett.FIXTURE_CANONICAL_SENDER, tenantId: '10', clientId: 10 };
  const program = { id: 'test-program', authorized_by: 'jake', scope_hash: hash(missionScope(snapshot.mission)),
    policy: { enrichmentLimit: 15, preparationAttemptsPerDay: 3, senderEmail: sender.senderEmail } };
  const contacts = Object.fromEntries(['co-harbor','co-granite'].map(id => [id, {
    prospect_id: id, company_id: id, email: `ops@${id}.example`, email_verified: false, email_status: 'invalid',
    email_provenance_source: 'website_email', do_not_contact: false,
  }]));
  let enriched = 0;
  const events = [];
  const queries = [];
  const adapter = adapters({ query: async sql => { queries.push(sql); return { rows: [] }; } }, {
    persist: false,
    runtime: { engine: () => engine, create: async input => engine.create(input) },
    loadMission: async id => engine.inspect(id, { tenantId: '10' }),
    contact: async id => contacts[id],
    tenant: async () => ({ sender }),
    infrastructure: async () => ({ snapshot: emmett.fixtureInfrastructureSnapshot('10') }),
    runScout: async mission => {
      const result = steps.fixtureScoutDiscoveryResult();
      result.payload = require('../packages/acquisition-mission/DiscoveryPayload').normalizeScoutDiscoveryPayload(
        result, { missionObjective: mission.objective, approvalConsumed: true });
      result.executionResult = amo.createExecutionResult({ specialist: 'scout', status: 'success',
        confidence: 0.72, evidence: result.payload.evidence, contributions: result.payload });
      // Production Scout exposes its SEC contributions as the same payload object.
      result.payload = result.executionResult.contributions;
      return result;
    },
    admission: { ensureMissionBoundCrmSchema: async () => {}, admitMissionBoundCandidate: async () => {} },
    enrich: async () => { enriched++; },
    runEmmett: async () => { assert.fail('Emmett must not run after Scout fails'); },
  });
  const store = { one: async () => ({ attempts: 0 }), suppression: async () => null,
    event: async (type, _key, data) => events.push({ type, ...data }) };
  store.ensurePreparation = async () => ({ created: false, progress: await store.one() });
  await assert.rejects(adapter.prepare(program, snapshot, '2026-09-18', store), { code: 'verified_inventory_shortfall' });
  const dailyId = `mission_daily_${hash([program.id, '2026-09-18']).slice(0, 24)}`;
  const daily = engine.inspect(dailyId, { tenantId: '10' });
  assert.equal(daily.mission.stage, 'discover');
  assert.equal(daily.mission.version, 0);
  assert.equal(daily.mission.pendingOperatorDecision.kind, 'discovery_approval');
  assert.deepEqual(daily.contributions, []);
  assert.equal(events.find(e => e.type === 'inventory_replenished').eligible, 0);
  assert.equal(enriched, 2);
  assert.equal(queries.filter(sql => sql.includes('SET attempts=attempts+1')).length, 1);
});
