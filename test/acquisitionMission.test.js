'use strict';
const { createTestAmoRuntime } = require('../packages/max/workspace/tests/amoTestRuntime');

/**
 * SPEC-118 — Acquisition Mission Orchestration (service, routes, competency, Max Ask).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const training = require('../packages/max/training');
const amo = require('../packages/acquisition-mission');
const {
  maybeHandleAcquisitionMissionTurn,
  referencesMissionState,
  shouldInspectActiveMission,
} = require('../packages/max/workspace/AcquisitionMissionTurn');
const {
  resetEngine,
  getEngine,
  createMission,
  contribute,
  progressMission,
  attachScoutDiscovery,
  attachPaigeVariants,
  attachEmmettCapacity,
} = require('../services/acquisitionMission');

beforeEach(() => {
  resetEngine();
});

describe('SPEC-118 competency and docs', () => {
  it('registers acquisition_mission_orchestration as a graduated competency', () => {
    const competency = training.getCompetency('acquisition_mission_orchestration');
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-118'));
    assert.match(competency.exercises[0].generalLesson, /Max doesn't manage agents/i);
  });

  it('documents ADR-055 and specialist contracts', () => {
    const spec = fs.readFileSync(
      path.join(__dirname, '../docs/specs/SPEC-118_Acquisition_Mission_Orchestration.md'),
      'utf8'
    );
    const adr = fs.readFileSync(
      path.join(__dirname, '../docs/adr/ADR-055_Max_Manages_Missions.md'),
      'utf8'
    );
    assert.match(spec, /Max doesn't manage agents/);
    assert.match(spec, /never writes outbound copy/i);
    assert.match(spec, /Waiting for Domain Warm-up/);
    assert.match(adr, /Max doesn't manage agents/);
    assert.match(adr, /SPEC-022 remains the generic orchestrator/);
  });
});

describe('SPEC-118 service attach points', () => {
  it('attaches Scout, Paige, and Emmett outputs to the mission', async () => {
    const mission = await createMission({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
      targetSegment: 'Law Firms',
      campaign: 'Fall Outreach',
    }, { persist: false });

    const scout = await attachScoutDiscovery(
      { tenantId: '10', missionId: mission.id },
      {
        payload: {
          companies: [{ id: 1, name: 'Harbor Law' }],
          prospects: [{ id: 9, name: 'Alex' }],
          buyingSignals: ['Hiring'],
          evidence: ['places'],
          confidence: 0.7,
        },
      },
      { persist: false }
    );
    assert.ok(scout.contribution);
    assert.equal(scout.contribution.specialist, 'scout');

    const paige = await attachPaigeVariants(
      { tenantId: '10', missionId: mission.id },
      { variants: [{ label: 'Variant B' }], cta: 'reply', subjects: ['Walkthrough'] },
      { persist: false }
    );
    assert.equal(paige.contribution.specialist, 'paige');

    const emmett = await attachEmmettCapacity(
      { tenantId: '10', missionId: mission.id },
      {
        capacity: { recommended: 18, remaining: 18 },
        queue: { items: [{ id: 1 }] },
        recommendations: ['Tuesday morning'],
        health: { status: 'healthy' },
      },
      { persist: false }
    );
    assert.equal(emmett.contribution.specialist, 'emmett');
  });

  it('does not attach when no missionId is provided', async () => {
    const attached = await attachScoutDiscovery(
      { tenantId: '10' },
      { payload: { companies: [{ id: 1 }], evidence: ['x'] } },
      { persist: false }
    );
    assert.equal(attached, null);
  });
});

describe('SPEC-118 Max Ask', () => {
  it('answers why is this mission here from evidence through Max', async () => {
    const amoEngine = getEngine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
      targetSegment: 'Commercial Law Firms',
      campaign: 'Fall Outreach',
      confidence: 0.84,
    });
    amoEngine.contribute(mission.id, {
      specialist: 'scout',
      payload: {
        companies: Array.from({ length: 61 }, (_, i) => ({ id: i + 1 })),
        evidence: ['places'],
        qualifiedCount: 61,
      },
    });
    amoEngine.contribute(mission.id, {
      specialist: 'max',
      payload: {
        priorities: ['law firms'],
        objectiveReason: 'Commercial revenue remains primary objective.',
        recommendations: ['prioritize ops hires'],
      },
    });
    const turn = await maybeHandleAcquisitionMissionTurn({
      question: 'Why is this mission here?',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      persist: false,
    });
    assert.ok(turn);
    assert.match(turn.prose, /Mission exists because/);
    assert.match(turn.prose, /61 qualified/);
    assert.equal(turn.reason, 'mission_inspection');
    assert.equal(turn.answered.invented, false);
    assert.equal(turn.structured.metadata.missionInspection, true);
    assert.deepEqual(turn.structured.metadata.unavailable, []);
    assert.equal(turn.structured.metadata.sourcesUsed.knowledge, false);
    assert.equal(turn.structured.metadata.sourcesUsed.missionState, true);
  });

  it('routes progress inspection before durable retrieval when a mission is active', async () => {
    const amoEngine = getEngine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
      targetSegment: 'Commercial Law Firms',
      campaign: 'Fall Outreach',
      confidence: 0.84,
    });
    amoEngine.contribute(mission.id, {
      specialist: 'scout',
      payload: {
        companies: Array.from({ length: 61 }, (_, i) => ({ id: i + 1 })),
        evidence: ['places'],
        qualifiedCount: 61,
      },
    });
    amoEngine.contribute(mission.id, {
      specialist: 'max',
      payload: {
        priorities: ['law firms'],
        objectiveReason: 'Commercial revenue remains primary objective.',
        recommendations: ['prioritize ops hires'],
      },
    });
    amoEngine.progress(mission.id, { role: 'max' }, { stage: amo.STAGES.UNDERSTAND });
    amoEngine.progress(mission.id, { role: 'max' }, { stage: amo.STAGES.PLAN });
    amoEngine.progress(mission.id, { role: 'max' }, { stage: amo.STAGES.PREPARE });

    assert.ok(referencesMissionState('What is the 68% progress based on?'));
    assert.ok(
      shouldInspectActiveMission('What is the 68% progress based on?', true)
    );

    const turn = await maybeHandleAcquisitionMissionTurn({
      question: 'What is the 68% progress based on?',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      persist: false,
    });
    assert.ok(turn);
    assert.equal(turn.reason, 'mission_inspection');
    assert.equal(turn.answered.kind, 'inspection');
    assert.match(turn.prose, /Mission Progress/);
    assert.match(turn.prose, /Derived From/);
    assert.equal(turn.structured.metadata.missionInspection, true);
    assert.deepEqual(turn.structured.metadata.unavailable, []);
  });
});

describe('SPEC-118 routes and workspace wiring', () => {
  it('is mounted from server.js and serves the operator workspace', () => {
    const server = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    assert.match(server, /require\('\.\/routes\/acquisitionMissions'\)/);
    const routes = fs.readFileSync(path.join(__dirname, '../routes/acquisitionMissions.js'), 'utf8');
    assert.match(routes, /\/api\/v1\/amo\/missions/);
    assert.match(routes, /\/api\/v1\/amo\/ask/);
    const ui = fs.readFileSync(path.join(__dirname, '../public/acquisition-missions.html'), 'utf8');
    assert.match(ui, /Mission Health/);
    assert.match(ui, /Why is this mission here/);
    const shell = fs.readFileSync(path.join(__dirname, '../public/shared/shell.js'), 'utf8');
    assert.match(shell, /acquisition-missions/);
    const scout = fs.readFileSync(path.join(__dirname, '../services/scoutAcquisitionIntelligence.js'), 'utf8');
    assert.match(scout, /attachScoutDiscovery/);
    const paige = fs.readFileSync(path.join(__dirname, '../services/maxPaigeCampaignDelegation.js'), 'utf8');
    assert.match(paige, /attachPaigeVariants/);
    const emmett = fs.readFileSync(path.join(__dirname, '../services/emmettOutbound.js'), 'utf8');
    assert.match(emmett, /attachEmmettCapacity/);
  });
});

describe('SPEC-118 Max orchestrates', () => {
  it('lets Max progress after Scout discovery and rejects Paige advancing', async () => {
    const mission = await createMission({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
    }, { persist: false });
    await contribute(mission.id, {
      specialist: 'scout',
      payload: { prospects: [{ id: 1 }], evidence: ['web'] },
    }, { persist: false, tenantId: '10' });
    const advanced = await progressMission(mission.id, { role: 'max' }, { stage: amo.STAGES.UNDERSTAND }, { persist: false, tenantId: '10' });
    assert.equal(advanced.stage, 'understand');
    await assert.rejects(
      () => progressMission(mission.id, { role: 'emmett' }, {}, { persist: false, tenantId: '10' }),
      (err) => err.code === 'amo_max_orchestrates'
    );
  });
});
