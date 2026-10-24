'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  selectCanonicalContribution,
  isRolledBackContribution,
} = require('../CanonicalContributionSelection');
const { evaluateUpstreamArtifactCoherence } = require('../UpstreamArtifactCoherence');
const { canAdvertiseExecutionApproval } = require('../ExecutionApproval');
const { STAGES, OPERATOR_DECISION_KINDS } = require('../types');
const { evaluatePrioritizationReadiness } = require('../DecisionReadiness');
const { normalizeScoutDiscoveryPayload } = require('../DiscoveryPayload');
const { resolveMissionBoundIdentity } = require('../../max/workspace/MissionBoundIdentity');
const {
  chooseNextRecoveryIntent,
} = require('../../../scripts/lib/anchorCanonicalOutbound');

const MISSION_ID = 'mission_test_canonical';

function scoutRow(id, at, payloadExtras = {}, flags = {}) {
  return {
    id,
    missionId: MISSION_ID,
    specialist: 'scout',
    kind: 'discovery',
    at,
    payload: {
      ...payloadExtras,
      ...(flags.superseded ? { superseded: true } : {}),
      ...(flags.rolledBack ? { commitStatus: 'rolled_back' } : {}),
    },
  };
}

function strongScoutPayload(count = 15) {
  return normalizeScoutDiscoveryPayload({
    status: 'completed',
    summary: `${count} qualified STR operators found.`,
    discoveryStatus: 'complete',
    payload: {
      qualifiedCount: count,
      opportunities: Array.from({ length: count }, (_, index) => ({
        companyId: `ChIJ_test_${index}`,
        placeId: `ChIJ_test_${index}`,
        name: `STR Operator ${index + 1}`,
        fit: 0.8,
        timing: 0.7,
        confidence: 0.75,
        signals: [{ type: 'hiring', label: 'Hiring cleaning coordinator', source: 'job_board' }],
        evidenceRefs: [{ label: 'Google Places result', snapshot: { source: 'google_places' } }],
      })),
    },
  }, { missionObjective: 'Acquire STR operators in Manchester.' });
}

describe('Canonical contribution selection', () => {
  it('selects the newest valid Scout discovery over an older zero-candidate row regardless of array order', () => {
    const old = scoutRow('contrib_old_0', '2026-08-23T00:00:00.000Z', {
      qualifiedCount: 0,
      opportunities: [],
    });
    const fresh = scoutRow('contrib_new_15', '2026-09-15T00:00:00.000Z', strongScoutPayload(15));

    const reversed = selectCanonicalContribution([fresh, old], {
      missionId: MISSION_ID,
      specialist: 'scout',
      kind: 'discovery',
    });
    assert.equal(reversed.id, 'contrib_new_15');

    const forward = selectCanonicalContribution([old, fresh], {
      missionId: MISSION_ID,
      specialist: 'scout',
      kind: 'discovery',
    });
    assert.equal(forward.id, 'contrib_new_15');
  });

  it('skips superseded and rolled-back Scout rows', () => {
    const superseded = scoutRow('contrib_superseded', '2026-09-20T00:00:00.000Z', strongScoutPayload(15), {
      superseded: true,
    });
    const rolledBack = scoutRow('contrib_rolled', '2026-09-21T00:00:00.000Z', strongScoutPayload(15), {
      rolledBack: true,
    });
    const active = scoutRow('contrib_active', '2026-09-15T00:00:00.000Z', strongScoutPayload(15));

    const selected = selectCanonicalContribution([superseded, rolledBack, active], {
      missionId: MISSION_ID,
      specialist: 'scout',
      kind: 'discovery',
    });
    assert.equal(selected.id, 'contrib_active');
    assert.equal(isRolledBackContribution(rolledBack), true);
  });

  it('respects mission revision pointers for Emmett capacity', () => {
    const older = {
      id: 'cap_old',
      missionId: MISSION_ID,
      specialist: 'emmett',
      kind: 'capacity',
      at: '2026-09-10T00:00:00.000Z',
      payload: { queue: { items: [{ id: 'a' }] } },
    };
    const newer = {
      id: 'cap_new',
      missionId: MISSION_ID,
      specialist: 'emmett',
      kind: 'capacity',
      at: '2026-09-15T00:00:00.000Z',
      payload: { queue: { items: [{ id: 'b' }] } },
    };
    const mission = {
      id: MISSION_ID,
      revisionState: { emmettContributionId: 'cap_old' },
    };
    const selected = selectCanonicalContribution([newer, older], {
      missionId: MISSION_ID,
      specialist: 'emmett',
      kind: 'capacity',
      mission,
    });
    assert.equal(selected.id, 'cap_old');
  });

  it('produces coherent upstream readiness from the canonical Scout contribution', () => {
    const payload = strongScoutPayload(15);
    const scout = scoutRow('contrib_29fa7896', '2026-09-15T00:00:00.000Z', payload);
    const stale = scoutRow('contrib_081e51c3', '2026-08-23T00:00:00.000Z', { qualifiedCount: 0 });
    const max = {
      id: 'max_1',
      missionId: MISSION_ID,
      specialist: 'max',
      kind: 'prioritization',
      at: '2026-09-16T00:00:00.000Z',
      payload: {
        rankedTargets: Array.from({ length: 5 }, (_, index) => ({
          rank: index + 1,
          id: `ChIJ_test_${index}`,
          name: `STR Operator ${index + 1}`,
        })),
      },
    };
    const mission = { id: MISSION_ID, stage: STAGES.READY };
    const contributions = [stale, scout, max];
    const coherence = evaluateUpstreamArtifactCoherence(mission, contributions);
    assert.equal(coherence.scoutContributionId, 'contrib_29fa7896');
    assert.equal(coherence.scoutCandidateCount, 15);
    assert.equal(coherence.discoveryReadiness.sufficient, true);
    assert.equal(coherence.prioritizationReady, true);
    assert.equal(coherence.coherent, true);
    assert.equal(evaluatePrioritizationReadiness(payload).sufficient, true);
  });

  it('blocks execution approval advertisement when canonical upstream artifacts disagree', () => {
    const mission = { id: MISSION_ID, stage: STAGES.READY };
    const stale = scoutRow('contrib_stale', '2026-08-23T00:00:00.000Z', { qualifiedCount: 0 });
    const paige = {
      id: 'paige_1',
      missionId: MISSION_ID,
      specialist: 'paige',
      kind: 'variants',
      at: '2026-09-16T00:00:00.000Z',
      payload: { variants: [{ candidateId: 'ChIJ_1', subject: 'Hi', body: 'Body' }] },
    };
    const emmett = {
      id: 'emmett_1',
      missionId: MISSION_ID,
      specialist: 'emmett',
      kind: 'capacity',
      at: '2026-09-16T00:00:00.000Z',
      payload: { queue: { items: [{ id: 'ChIJ_1', paige: { candidateId: 'ChIJ_1', subject: 'Hi', body: 'Body', attributableIntelligence: {} } }] } },
    };
    assert.equal(
      canAdvertiseExecutionApproval(mission, [stale, paige, emmett]),
      false
    );
  });

  it('does not expose APPROVE_EXECUTION recovery stop when upstream artifacts are incoherent', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: STAGES.READY,
      pendingIntent: 'APPROVE_EXECUTION',
      scoutCandidateCount: 0,
      prioritizedCandidateCount: 5,
      prioritizationReady: false,
      upstreamCoherent: false,
      sendableCount: 0,
      discoveryReadiness: { sufficient: false, primaryBlocker: { reason: 'No ranked prospects.' } },
      contributions: { scout: { candidateCount: 0 }, max: { rankedCount: 5 }, paige: {}, emmett: {} },
    });
    assert.equal(chosen.intent, null);
    assert.equal(chosen.stop, true);
    assert.equal(chosen.reason, 'upstream_artifact_incoherent');
    assert.notEqual(chosen.reason, 'ready_awaiting_execution_approval');
  });

  it('keeps Place ID and CRM UUID in separate identity fields', () => {
    const identity = resolveMissionBoundIdentity({
      target: { id: 'ChIJabc123', name: 'Summit STR' },
      opp: {},
      prospect: { id: '550e8400-e29b-41d4-a716-446655440000', email: 'ops@example.com' },
    });
    assert.equal(identity.placeId, 'ChIJabc123');
    assert.equal(identity.crmCompanyId, null);
    assert.equal(identity.candidateId, 'ChIJabc123');
    assert.equal(identity.crmProspectId, '550e8400-e29b-41d4-a716-446655440000');
  });
});
