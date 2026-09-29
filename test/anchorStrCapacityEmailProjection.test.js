'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  MESSAGE_BINDING_SCOPES,
  BINDING_VALIDATION_RESULTS,
} = require('../packages/acquisition-mission/types');
const { validateProspectMessageBindings } = require('../packages/acquisition-mission/ExecutionApproval');
const { buildMissionBoundCandidates } = require('../packages/max/workspace/EmmettMissionCandidates');
const {
  resolveMissionBoundRecipientEmail,
  isProjectableCrmProspect,
} = require('../packages/max/workspace/MissionBoundCrmResolver');
const { aliasCrmMapToIdentities } = require('../packages/max/workspace/CanonicalOutboundIdentity');
const {
  mapAssessedToCapacityPayload,
  fixtureInfrastructureSnapshot,
} = require('../packages/max/workspace/EmmettCapacityExecution');
const { resolveQueueSendability } = require('../packages/emmett-outbound/Queue');
const { sendableQueueItems } = require('../scripts/executeAnchorOneOutbound');
const eoi = require('../packages/emmett-outbound');

const MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';
const PLACE_BLUE = 'ChIJ43Z_V2dP4okRCRcDHefV8OU';
const PLACE_MILL = 'ChIJgyDf-cxO4okRSlEJCi27f94';
const PLACE_LOT = 'ChIJlot202example00000000000000001';
const CRM_BLUE = 'a1111111-3333-4333-8333-333333333333';
const CRM_MILL = 'b2222222-4444-4444-8444-444444444444';
const CRM_LOT = 'c3333333-5555-4555-9555-555555555555';

const MISSION = {
  id: MISSION_ID,
  tenantId: '10',
  clientId: 10,
  targetSegment: 'Short-term rental operators',
  structuredMission: { market: { label: 'Short-term rental operators', segment: 'str' } },
};

function verifiedCrm(id, email, extras = {}) {
  return {
    id,
    prospect_id: id,
    email,
    email_status: 'verified',
    email_verified: true,
    do_not_contact: false,
    ...extras,
  };
}

function strContributions() {
  return [
    {
      missionId: MISSION_ID,
      specialist: 'scout',
      kind: 'discovery',
      payload: {
        opportunities: [
          {
            id: PLACE_BLUE,
            companyId: PLACE_BLUE,
            placeId: PLACE_BLUE,
            name: 'Blue Door Living Property Management',
            website: 'https://bluedoorliving.com',
          },
          {
            id: PLACE_MILL,
            companyId: PLACE_MILL,
            placeId: PLACE_MILL,
            name: 'Mill City Property Management',
          },
          {
            id: PLACE_LOT,
            companyId: PLACE_LOT,
            placeId: PLACE_LOT,
            name: 'Lot 202 - Property Management Company',
          },
        ],
        prospects: [
          {
            id: CRM_BLUE,
            companyId: PLACE_BLUE,
            company: 'Blue Door Living Property Management',
            email: null,
          },
          {
            id: CRM_MILL,
            companyId: PLACE_MILL,
            company: 'Mill City Property Management',
            email: null,
          },
          {
            id: CRM_LOT,
            companyId: PLACE_LOT,
            company: 'Lot 202 - Property Management Company',
            email: null,
          },
        ],
      },
    },
    {
      missionId: MISSION_ID,
      specialist: 'max',
      kind: 'prioritization',
      payload: {
        rankedTargets: [
          {
            id: PLACE_BLUE,
            companyId: PLACE_BLUE,
            placeId: PLACE_BLUE,
            name: 'Blue Door Living Property Management',
            rank: 1,
            fit: 0.9,
            website: 'https://bluedoorliving.com',
          },
          {
            id: PLACE_MILL,
            companyId: PLACE_MILL,
            placeId: PLACE_MILL,
            name: 'Mill City Property Management',
            rank: 2,
            fit: 0.86,
          },
          {
            id: PLACE_LOT,
            companyId: PLACE_LOT,
            placeId: PLACE_LOT,
            name: 'Lot 202 - Property Management Company',
            rank: 3,
            fit: 0.84,
          },
        ],
      },
    },
    {
      missionId: MISSION_ID,
      specialist: 'paige',
      kind: 'variants',
      payload: {
        variants: [
          {
            candidateId: PLACE_BLUE,
            companyId: PLACE_BLUE,
            placeId: PLACE_BLUE,
            subject: 'Blue Door walkthrough',
            body: 'Blue Door body',
            bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
            attributableIntelligence: { companyName: 'Blue Door Living Property Management' },
          },
          {
            candidateId: PLACE_MILL,
            companyId: PLACE_MILL,
            placeId: PLACE_MILL,
            subject: 'Mill City walkthrough',
            body: 'Mill City body',
            bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
            attributableIntelligence: { companyName: 'Mill City Property Management' },
          },
          {
            candidateId: PLACE_LOT,
            companyId: PLACE_LOT,
            placeId: PLACE_LOT,
            subject: 'Lot 202 walkthrough',
            body: 'Lot 202 body',
            bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
            attributableIntelligence: { companyName: 'Lot 202 - Property Management Company' },
          },
        ],
      },
    },
  ];
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

describe('Anchor STR CAPACITY recipient email projection', () => {
  it('projects verified CRM email onto matching capacity item', () => {
    const baseCandidates = buildMissionBoundCandidates(MISSION, strContributions());
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([[CRM_BLUE, verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')]]),
    ], baseCandidates);
    const candidates = buildMissionBoundCandidates(MISSION, strContributions(), { crmByProspectId });
    const capacity = capacityFromCandidates(candidates);
    const blue = capacity.queue.items.find((row) => String(row.prospectId) === PLACE_BLUE);
    assert.equal(blue.email, 'ops@bluedoorliving.com');
    assert.equal(blue.sendable, true);
    assert.equal(blue.sendBlocker, null);
  });

  it('resolves Place ID alias to canonical CRM prospect when valid', () => {
    const email = resolveMissionBoundRecipientEmail({
      missionBoundKey: PLACE_BLUE,
      prospectId: CRM_BLUE,
      crmByProspectId: aliasCrmMapToIdentities([
        new Map([[PLACE_BLUE, verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com', { google_place_id: PLACE_BLUE })]]),
      ], buildMissionBoundCandidates(MISSION, strContributions())),
    });
    assert.equal(email, 'ops@bluedoorliving.com');
  });

  it('resolves crmProspectId alias correctly', () => {
    const email = resolveMissionBoundRecipientEmail({
      missionBoundKey: PLACE_MILL,
      prospectId: CRM_MILL,
      crmByProspectId: new Map([[CRM_MILL, verifiedCrm(CRM_MILL, 'hello@millcitypm.com')]]),
    });
    assert.equal(email, 'hello@millcitypm.com');
  });

  it('does not cross-bind company/domain alias to another tenant company', () => {
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([
        ['bluedoorliving.com', verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')],
      ]),
    ], buildMissionBoundCandidates(MISSION, strContributions()));
    const candidates = buildMissionBoundCandidates(MISSION, strContributions(), { crmByProspectId });
    const mill = candidates.find((row) => row.candidateId === PLACE_MILL);
    const lot = candidates.find((row) => row.candidateId === PLACE_LOT);
    assert.equal(mill.email, null);
    assert.equal(lot.email, null);
  });

  it('marks missing email as sendable false with missing_recipient_email blocker', () => {
    const candidates = buildMissionBoundCandidates(MISSION, strContributions());
    const mill = candidates.find((row) => row.candidateId === PLACE_MILL);
    const sendability = resolveQueueSendability({
      ...mill,
      paige: { subject: 'Hi', body: 'Body', author: 'paige', source: 'paige' },
      contentSource: 'paige',
      email: null,
    });
    assert.equal(sendability.sendable, false);
    assert.equal(sendability.sendBlocker, 'missing_recipient_email');
  });

  it('keeps verified email sendable when otherwise eligible', () => {
    const baseCandidates = buildMissionBoundCandidates(MISSION, strContributions());
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([
        [CRM_BLUE, verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')],
        [CRM_MILL, verifiedCrm(CRM_MILL, 'hello@millcitypm.com')],
      ]),
    ], baseCandidates);
    const capacity = capacityFromCandidates(
      buildMissionBoundCandidates(MISSION, strContributions(), { crmByProspectId })
    );
    const sendable = sendableQueueItems({ payload: capacity });
    assert.equal(sendable.length, 2);
    assert.ok(sendable.every((row) => String(row.email || '').trim()));
  });

  it('preserves Paige copy binding on projected queue items', () => {
    const baseCandidates = buildMissionBoundCandidates(MISSION, strContributions());
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([[CRM_BLUE, verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')]]),
    ], baseCandidates);
    const capacity = capacityFromCandidates(
      buildMissionBoundCandidates(MISSION, strContributions(), { crmByProspectId })
    );
    const blue = capacity.queue.items.find((row) => String(row.prospectId) === PLACE_BLUE);
    assert.equal(blue.paige.candidateId, PLACE_BLUE);
    assert.equal(blue.crmProspectId, CRM_BLUE);
  });

  it('passes SPEC-212 after projection', () => {
    const baseCandidates = buildMissionBoundCandidates(MISSION, strContributions());
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([[CRM_BLUE, verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')]]),
    ], baseCandidates);
    const capacity = capacityFromCandidates(
      buildMissionBoundCandidates(MISSION, strContributions(), { crmByProspectId })
    );
    const validation = validateProspectMessageBindings(capacity);
    assert.equal(validation.valid, true);
    assert.equal(validation.result, BINDING_VALIDATION_RESULTS.VALID);
  });

  it('does not project unverified CRM email', () => {
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([[CRM_BLUE, {
        id: CRM_BLUE,
        email: 'risky@bluedoorliving.com',
        email_status: 'risky',
        email_verified: true,
        do_not_contact: false,
      }]]),
    ], buildMissionBoundCandidates(MISSION, strContributions()));
    const blue = buildMissionBoundCandidates(MISSION, strContributions(), { crmByProspectId })
      .find((row) => row.candidateId === PLACE_BLUE);
    assert.equal(isProjectableCrmProspect(crmByProspectId.get(CRM_BLUE)), false);
    assert.equal(blue.email, null);
  });

  it('has no outbound side effects during capacity build', () => {
    const baseCandidates = buildMissionBoundCandidates(MISSION, strContributions());
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([[CRM_BLUE, verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')]]),
    ], baseCandidates);
    const capacity = capacityFromCandidates(
      buildMissionBoundCandidates(MISSION, strContributions(), { crmByProspectId })
    );
    assert.ok(capacity.queue.items.length >= 1);
    assert.equal(capacity.governor?.outcome, 'proceed');
    assert.ok(!capacity.sentToday);
  });
});
