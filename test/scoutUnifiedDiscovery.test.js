'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  Scout,
  discover,
  DISCOVERY_PHASES,
  DISCOVERY_STRATEGIES,
  DISCOVERY_OUTCOMES,
  SCOUT_DISCOVERY_EVENTS,
  listPhaseLog,
  clearPhaseLog,
  selectDiscoveryStrategy,
  buildDelegationFromMission,
} = require('../packages/scout');
const {
  createMissionEngine,
  executeCurrentStage,
  EXECUTOR_IDS,
  clearScoutDiscoveryAuditLog,
  listScoutDiscoveryAuditLog,
  DISCOVERY_STRATEGIES: AUDIT_STRATEGIES,
} = require('../packages/mission-engine');
const { createBuiltinRegistry } = require('../packages/capabilities');
const { assessExistingSufficiency } = require('../packages/max/scoutAcquisition/CandidateUniverse');
const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    resolverEnabled: true,
  });
}

describe('SPEC-123 unified Scout Discovery', () => {
  beforeEach(() => {
    clearPhaseLog();
    clearScoutDiscoveryAuditLog();
  });

  it('exports Scout.discover as the canonical contract', () => {
    assert.equal(typeof Scout.discover, 'function');
    assert.equal(Scout.discover, discover);
  });

  it('selectDiscoveryStrategy chooses External Heavy when no existing intelligence', () => {
    const gap = assessExistingSufficiency(
      { companies: [], people: [] },
      buildAcquisitionSearchDefinition({
        delegation: { tenantId: '10', targetContext: { geography: 'Manchester NH' } },
        tenantId: '10',
      })
    );
    const strategy = selectDiscoveryStrategy(gap, { companies: [] });
    assert.equal(strategy, DISCOVERY_STRATEGIES.EXTERNAL_HEAVY);
  });

  it('selectDiscoveryStrategy chooses Hybrid when existing + gap discovery needed', () => {
    const existing = {
      companies: [{ id: 'c1', name: 'Acme', lastEvaluatedAt: new Date().toISOString() }],
    };
    const searchDef = buildAcquisitionSearchDefinition({
      delegation: { tenantId: '10', targetContext: { geography: 'Manchester NH' } },
      tenantId: '10',
    });
    const gap = assessExistingSufficiency(existing, searchDef);
    const strategy = selectDiscoveryStrategy(gap, existing);
    assert.equal(strategy, DISCOVERY_STRATEGIES.HYBRID);
  });

  it('emits SCOUT_DISCOVERY_STARTED through SCOUT_DISCOVERY_COMPLETED phases', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      execute: false,
    });

    const result = await Scout.discover({
      mission,
      missionEngine: engine,
      scoutPayload: {},
    });

    assert.ok(result.discoveryReport);
    assert.ok(
      [
        DISCOVERY_OUTCOMES.COMPLETED,
        DISCOVERY_OUTCOMES.PARTIAL,
        DISCOVERY_OUTCOMES.BLOCKED,
      ].includes(result.outcome)
    );

    const phaseEvents = listPhaseLog();
    const eventNames = phaseEvents.map((e) => e.event);
    assert.ok(eventNames.includes(SCOUT_DISCOVERY_EVENTS.STARTED));
    assert.ok(eventNames.includes(SCOUT_DISCOVERY_EVENTS.GAP_ANALYSIS));
    assert.ok(eventNames.includes(SCOUT_DISCOVERY_EVENTS.COMPLETED));

    const phaseMarkers = phaseEvents.filter((e) => e.event === SCOUT_DISCOVERY_EVENTS.PHASE);
    const phaseNames = phaseMarkers.map((e) => e.phase);
    assert.ok(phaseNames.includes(DISCOVERY_PHASES.EXISTING_INTELLIGENCE));
    assert.ok(phaseNames.includes(DISCOVERY_PHASES.GAP_ANALYSIS));
    assert.ok(phaseNames.includes(DISCOVERY_PHASES.MISSION_UPDATE));
  });

  it('Mission Engine invokes Scout.discover via ScoutDiscoveryExecutor', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      execute: false,
    });

    const execution = await executeCurrentStage({
      mission,
      missionEngine: engine,
      operatorId: 'operator-1',
      message: 'Approved. Begin Scout discovery.',
    });

    assert.equal(execution.executorId, EXECUTOR_IDS.SCOUT_DISCOVERY);
    assert.ok(execution.result.discoveryReport);
    assert.ok(execution.result.discoveryResult);
    assert.equal(execution.result.discoveryReport.capabilityPath, 'scout.discover');

    const auditLogs = listScoutDiscoveryAuditLog();
    assert.ok(auditLogs.some((l) => l.event === 'SCOUT_DISCOVERY_STARTED'));
    assert.ok(auditLogs.some((l) => l.event === 'SCOUT_DISCOVERY_COMPLETED'));
  });

  it('existing intelligence is consulted before external discovery', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      execute: false,
    });

    const result = await Scout.discover({
      mission,
      missionEngine: engine,
      scoutPayload: {},
    });

    assert.ok(result.existingIntelligence);
    assert.equal(result.existingIntelligence.consulted, true);

    const gapEvent = listPhaseLog().find((e) => e.event === SCOUT_DISCOVERY_EVENTS.GAP_ANALYSIS);
    assert.ok(gapEvent);
    assert.ok(gapEvent.strategy);

    const report = result.discoveryReport;
    const companyStore = report.evidenceSources.find((s) => s.source === 'Company Store');
    assert.ok(companyStore);
    assert.equal(companyStore.attempted, true);
    assert.equal(companyStore.skipped, false);
  });

  it('discovery outcomes are explicit, never advisory prose', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      execute: false,
    });

    const result = await Scout.discover({
      mission,
      missionEngine: engine,
      scoutPayload: {},
    });

    assert.match(result.outcome, /^DISCOVERY_/);
    assert.ok(
      Object.values(DISCOVERY_OUTCOMES).includes(result.outcome),
      `Expected explicit outcome, got ${result.outcome}`
    );
  });

  it('operator-facing report uses Scout Discovery, not implementation paths', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      execute: false,
    });

    const result = await Scout.discover({
      mission,
      missionEngine: engine,
      scoutPayload: {},
    });

    const { formatDiscoveryOperatorResponse } = require('../packages/mission-engine/discoveryExecutionReport');
    const prose = formatDiscoveryOperatorResponse(result.discoveryReport);
    assert.match(prose, /Scout Discovery completed/i);
    assert.doesNotMatch(prose, /prospect_discovery/i);
    assert.doesNotMatch(prose, /runScoutAcquisitionIntelligence/i);
  });
});

describe('SPEC-123 strategy selection invariants', () => {
  it('Mission Engine never selects discovery strategy — Scout does internally', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      execute: false,
    });

    const result = await Scout.discover({
      mission,
      missionEngine: engine,
      scoutPayload: {},
    });

    assert.ok(result.strategy);
    // Strategy may be internal (Scout types) or audit-facing (mapped in discoveryReport)
    assert.ok(
      Object.values(DISCOVERY_STRATEGIES).includes(result.strategy) ||
        ['External Discovery', 'Hybrid', 'Stored Market Intelligence', 'No Strategy Selected'].includes(
          result.strategy
        )
    );
  });
});
