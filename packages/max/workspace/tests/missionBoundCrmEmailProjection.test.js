'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  MESSAGE_BINDING_SCOPES,
  BINDING_VALIDATION_RESULTS,
} = require('../../../acquisition-mission/types');
const { validateProspectMessageBindings } = require('../../../acquisition-mission/ExecutionApproval');
const {
  buildMissionBoundCandidates,
  listMissionBoundCompanyIds,
  listMissionBoundProspectIds,
} = require('../EmmettMissionCandidates');
const {
  isProjectableCrmProspect,
  projectableEmailFromCrmRecord,
  resolveMissionBoundRecipientEmail,
} = require('../MissionBoundCrmResolver');
const {
  mapAssessedToCapacityPayload,
  sanitizeQueueItem,
  fixtureInfrastructureSnapshot,
} = require('../EmmettCapacityExecution');
const { sendableQueueItems } = require('../../../../scripts/executeAnchorOneOutbound');
const eoi = require('../../../emmett-outbound');

const MISSION = {
  id: 'mission_test',
  tenantId: '10',
  clientId: 10,
  targetSegment: 'Law Firms',
  structuredMission: { market: { label: 'Law Firms', segment: 'law_firm' } },
};

function buildContributions() {
  return [
    {
      specialist: 'scout',
      kind: 'discovery',
      payload: {
        prospects: [
          { id: 101, companyId: 'co-harbor', company: 'Harbor Law', email: null },
          { id: 102, companyId: 'co-granite', company: 'Granite Legal', email: null },
        ],
      },
    },
    {
      specialist: 'max',
      kind: 'prioritization',
      payload: {
        rankedTargets: [
          {
            id: 'co-harbor',
            companyId: 'co-harbor',
            name: 'Harbor Law',
            rank: 1,
            fit: 0.9,
            timing: 0.8,
          },
          {
            id: 'co-granite',
            companyId: 'co-granite',
            name: 'Granite Legal',
            rank: 2,
            fit: 0.85,
            timing: 0.7,
          },
        ],
      },
    },
    {
      specialist: 'paige',
      kind: 'variants',
      payload: {
        variants: [
          {
            label: 'Primary',
            candidateId: 'co-harbor',
            subject: 'Harbor walkthrough',
            body: 'Harbor body',
            bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
            attributableIntelligence: { companyName: 'Harbor Law', rationale: 'intake' },
          },
          {
            label: 'Primary',
            candidateId: 'co-granite',
            subject: 'Granite walkthrough',
            body: 'Granite body',
            bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
            attributableIntelligence: { companyName: 'Granite Legal', rationale: 'expansion' },
          },
        ],
      },
    },
  ];
}

function verifiedCrm(id, email) {
  return {
    id,
    email,
    email_status: 'verified',
    email_verified: true,
    do_not_contact: false,
  };
}

function capacityFromCandidates(candidates) {
  const infrastructureSnapshot = fixtureInfrastructureSnapshot('10');
  const engine = eoi.createOutboundEngine();
  const assessed = engine.assess({
    tenantId: '10',
    clientId: 10,
    snapshot: infrastructureSnapshot,
    prospects: candidates,
  });
  return mapAssessedToCapacityPayload(assessed, { infrastructureSnapshot });
}

describe('mission-bound CRM email projection at PREPARE', () => {
  it('projects verified CRM email when discovery email is null', () => {
    const crmByProspectId = new Map([
      ['101', verifiedCrm(101, 'partner@harborlaw.com')],
    ]);
    const candidates = buildMissionBoundCandidates(MISSION, buildContributions(), { crmByProspectId });
    const harbor = candidates.find((row) => row.id === 'co-harbor');
    const granite = candidates.find((row) => row.id === 'co-granite');
    assert.equal(harbor.email, 'partner@harborlaw.com');
    assert.equal(granite.email, null);
  });

  it('does not project invalid or unverified CRM email', () => {
    const crmByProspectId = new Map([
      ['101', { id: 101, email: 'bad@example.com', email_status: 'invalid', email_verified: false, do_not_contact: false }],
      ['102', { id: 102, email: 'ops@granitelegal.com', email_status: 'risky', email_verified: true, do_not_contact: false }],
    ]);
    const candidates = buildMissionBoundCandidates(MISSION, buildContributions(), { crmByProspectId });
    assert.equal(candidates.find((row) => row.id === 'co-harbor').email, null);
    assert.equal(candidates.find((row) => row.id === 'co-granite').email, null);
  });

  it('does not project email from DNC CRM records', () => {
    const crmByProspectId = new Map([
      ['101', { ...verifiedCrm(101, 'partner@harborlaw.com'), do_not_contact: true }],
    ]);
    const candidates = buildMissionBoundCandidates(MISSION, buildContributions(), { crmByProspectId });
    assert.equal(candidates.find((row) => row.id === 'co-harbor').email, null);
  });

  it('never introduces CRM-only prospects outside the mission-bound set', () => {
    const crmByProspectId = new Map([
      ['101', verifiedCrm(101, 'partner@harborlaw.com')],
      ['999', verifiedCrm(999, 'stranger@otherfirm.com')],
    ]);
    const candidates = buildMissionBoundCandidates(MISSION, buildContributions(), { crmByProspectId });
    assert.equal(candidates.length, 2);
    assert.ok(!candidates.some((row) => row.prospectId === 999 || row.prospectId === '999'));
    assert.deepEqual(listMissionBoundProspectIds(MISSION, buildContributions()), ['101', '102']);
    assert.deepEqual(listMissionBoundCompanyIds(MISSION, buildContributions()), ['co-harbor', 'co-granite']);
  });

  it('resolveMissionBoundRecipientEmail reuses discovery email when present', () => {
    assert.equal(
      resolveMissionBoundRecipientEmail({
        discoveryEmail: 'legacy@discovery.com',
        prospectId: 101,
        crmByProspectId: new Map([['101', verifiedCrm(101, 'crm@harborlaw.com')]]),
      }),
      'legacy@discovery.com'
    );
  });

  it('isProjectableCrmProspect mirrors sending readiness email gates', () => {
    assert.equal(isProjectableCrmProspect(verifiedCrm(1, 'valid@lawfirm.com')), true);
    assert.equal(projectableEmailFromCrmRecord(verifiedCrm(1, 'valid@lawfirm.com')), 'valid@lawfirm.com');
    assert.equal(isProjectableCrmProspect({ ...verifiedCrm(1, 'info@lawfirm.com'), email_verified: false }), false);
  });

  it('SPEC-212 still passes after CAPACITY persist/reload with projected CRM email', () => {
    const crmByProspectId = new Map([
      ['101', verifiedCrm(101, 'partner@harborlaw.com')],
    ]);
    const candidates = buildMissionBoundCandidates(MISSION, buildContributions(), { crmByProspectId });
    const capacity = capacityFromCandidates(candidates);
    const reloaded = JSON.parse(JSON.stringify(capacity));
    const items = reloaded.queue?.items || [];
    assert.ok(items.length >= 1);
    for (const item of items) {
      const sanitized = sanitizeQueueItem(item);
      assert.ok(sanitized.paige?.candidateId);
      assert.equal(sanitized.paige.bindingScope, MESSAGE_BINDING_SCOPES.PROSPECT);
      assert.ok(sanitized.paige.attributableIntelligence);
    }
    const validation = validateProspectMessageBindings(reloaded);
    assert.equal(validation.valid, true);
    assert.equal(validation.result, BINDING_VALIDATION_RESULTS.VALID);
    const harborItem = items.find((row) => String(row.prospectId) === '101' || String(row.prospectId) === 'co-harbor');
    assert.ok(harborItem);
    assert.equal(harborItem.email, 'partner@harborlaw.com');
  });

  it('sendableQueueItems sees eligible item when verified CRM email was projected', () => {
    const crmByProspectId = new Map([
      ['101', verifiedCrm(101, 'partner@harborlaw.com')],
    ]);
    const candidates = buildMissionBoundCandidates(MISSION, buildContributions(), { crmByProspectId });
    const capacity = capacityFromCandidates(candidates);
    const sendable = sendableQueueItems({ payload: capacity });
    assert.ok(sendable.length >= 1);
    assert.ok(sendable.some((row) => String(row.email) === 'partner@harborlaw.com'));
  });
});
