'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { createMissionEngine } = require('../../mission-engine');
const { Scout } = require('../../scout');
const {
  advanceDiscoveryAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  RUNTIME_OWNERS,
  MISSION_RUNTIME_BOUNDARY_VIOLATION,
  isAmoMissionId,
  isAmoMissionRecord,
  resolveMissionRuntimeOwner,
  maySyncToMissionEngine,
  resolveScoutDiscoveryRuntimePolicy,
  assertMissionRuntimeBoundary,
} = require('../MissionRuntimeOwnership');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Hooksett and Auburn.';

describe('ADR-089 / SPEC-170 — Mission runtime ownership boundaries', () => {
  it('identifies AMO mission ids and records', () => {
    assert.equal(isAmoMissionId('mission_c1b6003f-0000-4000-8000-000000000001'), true);
    assert.equal(isAmoMissionId('msn_abc'), false);

    const amoMission = amo.createMission({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    assert.equal(isAmoMissionRecord(amoMission), true);
    assert.equal(resolveMissionRuntimeOwner(amoMission), RUNTIME_OWNERS.AMO);
  });

  it('forbids Mission Engine sync for AMO-owned Scout discovery', () => {
    const mission = {
      id: 'mission_c1b6003f-0000-4000-8000-000000000001',
      tenantId: '10',
      objective: STR_OBJECTIVE,
      structuredMissionApproved: true,
      stage: 'discover',
    };
    const missionEngine = createMissionEngine();

    const policy = resolveScoutDiscoveryRuntimePolicy({
      mission,
      missionEngine,
      opts: { amoMissionId: mission.id, runtimeOwner: RUNTIME_OWNERS.AMO },
    });
    assert.equal(policy.amoOwned, true);
    assert.equal(policy.syncToMissionEngine, false);
    assert.equal(policy.attachViaLegacyFacade, false);

    assert.throws(
      () =>
        assertMissionRuntimeBoundary({
          mission,
          missionEngine,
          expectedOwner: RUNTIME_OWNERS.AMO,
          operation: 'sync Scout discovery into Mission Engine',
        }),
      (err) => err.code === MISSION_RUNTIME_BOUNDARY_VIOLATION
    );
    assert.equal(maySyncToMissionEngine({ mission, missionEngine }), false);
  });

  it('Scout.discover does not throw Unknown mission for AMO id when Mission Engine is absent', async () => {
    const mission = {
      id: 'mission_c1b6003f-0000-4000-8000-000000000001',
      tenantId: '10',
      clientId: '10',
      objective: STR_OBJECTIVE,
      structuredMissionApproved: true,
      stage: 'discover',
    };

    const result = await Scout.discover({
      mission,
      missionEngine: null,
      scoutPayload: {},
      opts: {
        runtimeOwner: RUNTIME_OWNERS.AMO,
        amoMissionId: mission.id,
        attachScoutDiscovery: false,
        allowFixtureFallback: true,
        delegation: {
          tenantId: '10',
          businessContext: { operatorDirection: STR_OBJECTIVE, missionObjectiveImmutable: true },
          targetContext: { missionBound: true, segments: ['short_term_rental'] },
        },
      },
    });

    assert.ok(result);
    assert.ok(result.intelligenceResult || result.discoveryReport || result.phases);
  });

  it('Scout.discover rejects cross-runtime sync when Mission Engine is supplied for AMO mission', async () => {
    const mission = {
      id: 'mission_c1b6003f-0000-4000-8000-000000000001',
      tenantId: '10',
      objective: STR_OBJECTIVE,
      structuredMissionApproved: true,
      stage: 'discover',
    };
    const missionEngine = createMissionEngine();

    await assert.rejects(
      () =>
        Scout.discover({
          mission,
          missionEngine,
          scoutPayload: {},
          opts: {
            runtimeOwner: RUNTIME_OWNERS.AMO,
            amoMissionId: mission.id,
            attachScoutDiscovery: false,
            allowFixtureFallback: true,
            delegation: {
              tenantId: '10',
              businessContext: { operatorDirection: STR_OBJECTIVE },
              targetContext: { segments: ['short_term_rental'] },
            },
          },
        }),
      (err) => err.code === MISSION_RUNTIME_BOUNDARY_VIOLATION
    );
  });

  it('advanceDiscoveryAfterApproval executes Scout inside AMO without Mission Engine crossover', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
      planApproved: true,
    });

    const legacyEngine = createMissionEngine();

    const result = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: false,
      missionEngine: legacyEngine,
      runScout: async () => ({
        status: 'completed',
        confidence: 0.81,
        payload: {
          opportunities: [
            {
              companyId: 'co-str-1',
              name: 'Summit STR Management',
              fit: 0.84,
              signals: [{ type: 'hiring', label: 'Hiring coordinator', source: 'job_board' }],
              evidenceRefs: [{ label: 'Job board posting', snapshot: { source: 'job_board' } }],
            },
          ],
          qualifiedCount: 1,
        },
      }),
    });

    assert.equal(result.executionOutcome, 'completed');
    assert.ok(result.discovery);
    assert.equal(result.discovery.specialist, 'scout');
    assert.equal(engine.get(mission.id, '10').stage, 'discover');
  });
});
