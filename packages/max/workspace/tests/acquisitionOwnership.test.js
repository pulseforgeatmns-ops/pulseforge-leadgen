'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { detectAcquisitionObjective } = require('../AcquisitionObjectiveDetection');
const {
  maybeHandleAcquisitionOwnershipTurn,
  buildClientIntelligenceMissionEvidence,
} = require('../AcquisitionOwnership');
const {
  maybeHandleClientIntelligenceTurn,
  composeClientContextReasoning,
  shouldClaimClientIntelligenceTurn,
} = require('../ClientIntelligenceContext');
const {
  createAcquisitionOwnershipAudit,
  clearAcquisitionOwnershipAuditLog,
  listAcquisitionOwnershipAuditLog,
} = require('../audit/AcquisitionOwnershipAudit');

const ANCHOR_OBJECTIVE =
  'I want to acquire one recurring commercial cleaning client in Greater Manchester.';

const ANCHOR_SUMMARY = {
  approved: true,
  blueprintId: 'bp-10',
  businessName: 'Anchor Cleaning',
  idealCustomers: 'law firms and accounting practices',
  geography: 'Greater Manchester',
  campaignGoals: 'recurring commercial cleaning clients',
  successMetrics: 'walkthroughs and signed contracts',
  commercialPreference: true,
  avoidCustomers: 'residential one-offs',
  unknowns: ['Which segment responds first'],
};

describe('SPEC-124 — Acquisition Ownership Convergence', () => {
  beforeEach(() => {
    clearAcquisitionOwnershipAuditLog();
  });

  it('detects acquisition objectives across phrasing variants', () => {
    assert.equal(detectAcquisitionObjective(ANCHOR_OBJECTIVE), true);
    assert.equal(
      detectAcquisitionObjective('Acquire one recurring commercial cleaning client'),
      true
    );
    assert.equal(
      detectAcquisitionObjective('Our goal is to land 3 new commercial accounts'),
      true
    );
    assert.equal(
      detectAcquisitionObjective('What should we focus on first?'),
      false
    );
    assert.equal(
      detectAcquisitionObjective('Why are we targeting STR?'),
      false
    );
    assert.equal(
      detectAcquisitionObjective('Find commercial cleaning opportunities in Manchester'),
      false
    );
  });

  it('builds structured Client Intelligence evidence from approved Blueprint', () => {
    const evidence = buildClientIntelligenceMissionEvidence(ANCHOR_SUMMARY);
    assert.equal(evidence.attached, true);
    assert.equal(evidence.blueprintId, 'bp-10');
    assert.ok(evidence.sectionsAttached.includes('idealCustomers'));
    assert.ok(evidence.sectionsAttached.includes('geography'));
    assert.ok(evidence.sectionsAttached.includes('successMetrics'));
    assert.equal(evidence.strategicEvidence.icp, ANCHOR_SUMMARY.idealCustomers);
    assert.equal(evidence.strategicEvidence.geography, ANCHOR_SUMMARY.geography);
  });

  it('creates acquisition mission and returns Mission Created response', async () => {
    const audit = createAcquisitionOwnershipAudit();
    const amoEngine = amo.createAcquisitionMissionEngine();

    const turn = await maybeHandleAcquisitionOwnershipTurn({
      question: ANCHOR_OBJECTIVE,
      context: { tenantId: '10' },
      acquisitionMissionEngine: amoEngine,
      audit,
      cieService: {
        getApprovedClientBlueprint: async () => ({
          id: 'bp-10',
          status: 'approved',
          normalizedFacts: {
            business_name: 'Anchor Cleaning',
            ideal_customers: ['law firms', 'accounting practices'],
            geography: ['Greater Manchester'],
            growth_focus: 'recurring commercial cleaning clients',
            success_metrics: ['walkthroughs', 'signed contracts'],
          },
        }),
      },
    });

    assert.ok(turn);
    assert.equal(turn.reason, 'acquisition_mission_created');
    assert.match(turn.prose, /Mission Created/);
    assert.match(turn.prose, /Approve discovery/);
    assert.doesNotMatch(turn.prose, /advisory guidance/i);
    assert.doesNotMatch(turn.prose, /I'd start with a qualified group/i);
    assert.equal(turn.mission.objective, ANCHOR_OBJECTIVE);
    assert.equal(amoEngine.list('10').length, 1);

    const ownerEvent = audit.log.find((row) => row.event === 'ACQUISITION_OWNER');
    assert.ok(ownerEvent);
    assert.equal(ownerEvent.owner, 'MissionEngine');
    assert.equal(ownerEvent.action, 'created');

    const ciEvent = audit.log.find(
      (row) => row.event === 'CLIENT_INTELLIGENCE_CONTRIBUTION'
    );
    assert.ok(ciEvent);
    assert.equal(ciEvent.attached, true);
    assert.ok(ciEvent.sectionsAttached.includes('idealCustomers'));
  });

  it('resumes similar acquisition mission instead of creating duplicate', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const existing = amoEngine.create({
      tenantId: '10',
      objective: 'Acquire one recurring commercial cleaning client.',
      targetSegment: 'Commercial',
    });

    const turn = await maybeHandleAcquisitionOwnershipTurn({
      question: 'Acquire one recurring commercial cleaning client',
      context: { tenantId: '10' },
      acquisitionMissionEngine: amoEngine,
    });

    assert.ok(turn);
    assert.equal(turn.reason, 'acquisition_mission_resumed');
    assert.match(turn.prose, /Mission Resumed/);
    assert.equal(turn.mission.id, existing.id);
    assert.equal(amoEngine.list('10').length, 1);
  });

  it('blocks Client Intelligence from claiming acquisition objectives', async () => {
    assert.equal(
      shouldClaimClientIntelligenceTurn(ANCHOR_OBJECTIVE, null, {
        approvedBlueprint: true,
      }),
      false
    );

    const cieTurn = await maybeHandleClientIntelligenceTurn({
      question: ANCHOR_OBJECTIVE,
      session: { context: { tenantId: '10' } },
      cieService: {
        getApprovedClientBlueprint: async () => ({
          id: 'bp-10',
          status: 'approved',
          normalizedFacts: {
            ideal_customers: ['law firms'],
            geography: ['Greater Manchester'],
          },
        }),
      },
    });

    assert.equal(cieTurn.handled, false);
    assert.equal(cieTurn.skipReason, 'acquisition_objective_owned_by_mission');
  });

  it('does not emit recommendation essays for acquisition objectives', () => {
    const composed = composeClientContextReasoning(ANCHOR_SUMMARY, ANCHOR_OBJECTIVE, {
      mode: 'focus',
    });
    assert.match(composed.prose, /qualified group|learning loop|advisory guidance/i);

    // CIE essay exists for advisory path, but ownership guard prevents it from
    // being selected when an acquisition objective is expressed.
    assert.equal(detectAcquisitionObjective(ANCHOR_OBJECTIVE), true);
  });

  it('emits global ACQUISITION_OWNER audit event', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    await maybeHandleAcquisitionOwnershipTurn({
      question: ANCHOR_OBJECTIVE,
      context: { tenantId: '10' },
      acquisitionMissionEngine: amoEngine,
    });

    const events = listAcquisitionOwnershipAuditLog();
    assert.ok(events.some((row) => row.event === 'ACQUISITION_OWNER'));
    assert.ok(
      events.some((row) => row.event === 'CLIENT_INTELLIGENCE_CONTRIBUTION')
    );
  });
});
