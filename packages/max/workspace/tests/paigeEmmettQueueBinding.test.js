'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  MESSAGE_BINDING_SCOPES,
} = require('../../../acquisition-mission/types');
const { buildPerProspectVariants } = require('../PaigeVariantsExecutor');
const {
  buildMissionBoundCandidates,
  findBoundVariant,
} = require('../EmmettMissionCandidates');
const {
  mapAssessedToCapacityPayload,
  sanitizeQueueItem,
  fixtureInfrastructureSnapshot,
} = require('../EmmettCapacityExecution');
const {
  isGooglePlaceId,
  isUuid,
  prospectIdentity,
  canonicalOutboundIdentity,
  aliasCrmMapToIdentities,
} = require('../CanonicalOutboundIdentity');
const {
  resolveMissionBoundRecipientEmail,
  isProjectableCrmProspect,
} = require('../MissionBoundCrmResolver');
const {
  classifyQueueItems,
  chooseNextRecoveryIntent,
  FORBIDDEN_SEND_INTENTS,
} = require('../../../../scripts/lib/anchorCanonicalOutbound');
const eoi = require('../../../emmett-outbound');
const { resolveQueueSendability } = require('../../../emmett-outbound/Queue');
const { BLOCKER: COPY_SAFETY_BLOCKER } = require('../PaigeCopySafety');

const PLACE_BLUE = 'ChIJ43Z_V2dP4okRCRcDHefV8OU';
const PLACE_MILL = 'ChIJgyDf-cxO4okRSlEJCi27f94';
const CRM_BLUE = 'a1111111-3333-4333-8333-333333333333';
const CRM_MILL = 'b2222222-4444-4444-8444-444444444444';

const MISSION = {
  id: 'mission_str_binding',
  tenantId: '10',
  clientId: 10,
  targetSegment: 'Short-term rental operators',
  structuredMission: { market: { label: 'Short-term rental operators', segment: 'str' } },
};

function nested(specialist, kind, payload) {
  return {
    specialist,
    kind,
    payload: {
      specialist,
      kind,
      payload,
    },
  };
}

function maxPriorities() {
  return [
    { rank: 1, companyId: PLACE_BLUE, name: 'Blue Door Living Property Management', fit: 0.9, timing: 0.8 },
    { rank: 2, companyId: PLACE_MILL, name: 'Mill City Property Management', fit: 0.86, timing: 0.7 },
  ];
}

function paigeVariantsFromMax() {
  return buildPerProspectVariants({
    max: { priorities: maxPriorities(), rankedTargets: [] },
    plan: { market: { label: 'Greater Manchester offices' } },
  });
}

function contributions({ discoveryEmail = null, nestedPayload = true } = {}) {
  const scoutPayload = {
    opportunities: [
      { id: PLACE_BLUE, companyId: PLACE_BLUE, placeId: PLACE_BLUE, name: 'Blue Door Living Property Management', website: 'https://bluedoorliving.com' },
      { id: PLACE_MILL, companyId: PLACE_MILL, placeId: PLACE_MILL, name: 'Mill City Property Management' },
    ],
    prospects: [
      {
        id: CRM_BLUE,
        companyId: PLACE_BLUE,
        company: 'Blue Door Living Property Management',
        email: discoveryEmail,
      },
      {
        id: CRM_MILL,
        companyId: PLACE_MILL,
        company: 'Mill City Property Management',
        email: null,
      },
    ],
  };
  const maxPayload = { priorities: maxPriorities() };
  const variants = paigeVariantsFromMax();
  const paigePayload = { variants, subjects: variants.map((row) => row.subject) };
  if (!nestedPayload) {
    return [
      { missionId: MISSION.id, specialist: 'scout', kind: 'discovery', payload: scoutPayload },
      { missionId: MISSION.id, specialist: 'max', kind: 'prioritization', payload: maxPayload },
      { missionId: MISSION.id, specialist: 'paige', kind: 'variants', payload: paigePayload },
    ];
  }
  return [
    { ...nested('scout', 'discovery', scoutPayload), missionId: MISSION.id },
    { ...nested('max', 'prioritization', maxPayload), missionId: MISSION.id },
    { ...nested('paige', 'variants', paigePayload), missionId: MISSION.id },
  ];
}

function verifiedCrm(id, email) {
  return {
    id,
    prospect_id: id,
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

describe('Paige → Emmett sendable queue binding', () => {
  it('Max prioritized candidate identity binds to Paige variant identity', () => {
    const variants = paigeVariantsFromMax();
    assert.equal(variants.length, 2);
    assert.equal(variants[0].candidateId, PLACE_BLUE);
    assert.equal(variants[1].candidateId, PLACE_MILL);
    assert.equal(variants[0].companyId, PLACE_BLUE);
    assert.equal(variants[0].placeId, PLACE_BLUE);
    assert.ok(variants[0].subject);
    assert.ok(variants[0].body);
    assert.equal(findBoundVariant(variants, PLACE_BLUE).candidateId, PLACE_BLUE);
    assert.equal(findBoundVariant(variants, PLACE_MILL).candidateId, PLACE_MILL);
  });

  it('Paige variant copy projects subject/body/cta into persisted CAPACITY queue items', () => {
    const rows = contributions();
    const candidates = buildMissionBoundCandidates(MISSION, rows);
    assert.equal(candidates.length, 2);
    for (const candidate of candidates) {
      assert.ok(candidate.paige?.subject);
      assert.ok(candidate.paige?.body);
      assert.ok(candidate.paige?.cta);
      assert.equal(candidate.paige.candidateId, candidate.candidateId);
    }
    const capacity = capacityFromCandidates(candidates);
    const persisted = JSON.parse(JSON.stringify(capacity));
    const items = persisted.queue?.items || [];
    assert.equal(items.length, 2);
    for (const item of items) {
      assert.ok(item.paige?.subject);
      assert.ok(item.paige?.body);
      assert.ok(item.paige?.cta);
      assert.ok(item.candidateId || item.id);
      assert.equal(item.paige.candidateId, item.candidateId || item.id);
    }
    const classified = classifyQueueItems({ queue: { items } }, rows[2]);
    assert.equal(classified.blocked.filter((row) => row.reasons.includes('missing_paige_copy')).length, 0);
    assert.equal(items.filter((item) => item.paige?.candidateId).length, 2);
  });

  it('does not treat Google Place IDs as prospect UUIDs', () => {
    assert.equal(isGooglePlaceId(PLACE_BLUE), true);
    assert.equal(isUuid(PLACE_BLUE), false);
    assert.equal(prospectIdentity({ id: PLACE_BLUE }), null);
    assert.equal(prospectIdentity({ id: CRM_BLUE }), CRM_BLUE);
    const identity = canonicalOutboundIdentity({
      companyId: PLACE_BLUE,
      name: 'Blue Door Living Property Management',
    }, { prospect: { id: CRM_BLUE } });
    assert.equal(identity.candidateId, PLACE_BLUE);
    assert.equal(identity.placeId, PLACE_BLUE);
    assert.equal(identity.crmProspectId, CRM_BLUE);
  });

  it('preserves candidateId / companyId / crmProspectId lineage end-to-end', () => {
    const candidates = buildMissionBoundCandidates(MISSION, contributions());
    const blue = candidates.find((row) => row.company.includes('Blue Door'));
    assert.equal(blue.candidateId, PLACE_BLUE);
    assert.equal(blue.companyId, PLACE_BLUE);
    assert.equal(blue.placeId, PLACE_BLUE);
    assert.equal(blue.crmProspectId, CRM_BLUE);
    assert.equal(blue.prospectId, PLACE_BLUE);
    const items = capacityFromCandidates(candidates).queue.items.map(sanitizeQueueItem);
    const queued = items.find((row) => String(row.prospectId) === PLACE_BLUE);
    assert.equal(queued.candidateId, PLACE_BLUE);
    assert.equal(queued.companyId, PLACE_BLUE);
    assert.equal(queued.crmProspectId, CRM_BLUE);
    assert.equal(queued.paige.candidateId, PLACE_BLUE);
  });

  it('projects verified CRM email when frozen Scout snapshot has none', () => {
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([
        [CRM_BLUE, verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')],
      ]),
    ], buildMissionBoundCandidates(MISSION, contributions({ discoveryEmail: null })));
    const candidates = buildMissionBoundCandidates(MISSION, contributions({ discoveryEmail: null }), { crmByProspectId });
    const blue = candidates.find((row) => row.candidateId === PLACE_BLUE);
    const mill = candidates.find((row) => row.candidateId === PLACE_MILL);
    assert.equal(blue.email, 'ops@bluedoorliving.com');
    assert.equal(mill.email, null);
    const capacity = capacityFromCandidates(candidates);
    const queuedBlue = capacity.queue.items.find((row) => String(row.prospectId) === PLACE_BLUE);
    assert.equal(queuedBlue.email, 'ops@bluedoorliving.com');
    assert.equal(queuedBlue.sendable, true);
  });

  it('keeps missing recipient email as the only block when Paige copy exists', () => {
    const candidates = buildMissionBoundCandidates(MISSION, contributions());
    const mill = candidates.find((row) => row.candidateId === PLACE_MILL);
    assert.ok(mill.paige?.subject && mill.paige?.body);
    assert.equal(mill.email, null);
    const capacity = capacityFromCandidates(candidates);
    const classified = classifyQueueItems(capacity, contributions()[2]);
    const millBlocked = classified.blocked.find((row) => row.candidateId === PLACE_MILL || row.prospectId === PLACE_MILL);
    assert.ok(millBlocked);
    assert.ok(millBlocked.reasons.includes('missing_recipient_email_on_queue_item'));
    assert.ok(!millBlocked.reasons.includes('missing_paige_copy'));
  });

  it('becomes sendable when Paige copy and verified CRM email are both present', () => {
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([
        [CRM_BLUE, verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')],
        [CRM_MILL, verifiedCrm(CRM_MILL, 'hello@millcitypm.com')],
      ]),
    ], buildMissionBoundCandidates(MISSION, contributions()));
    const candidates = buildMissionBoundCandidates(MISSION, contributions(), { crmByProspectId });
    const capacity = capacityFromCandidates(candidates);
    const sendable = capacity.queue.items.filter((item) => item.sendable && String(item.email || '').trim());
    assert.equal(sendable.length, 2);
    assert.equal(isProjectableCrmProspect(verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')), true);
  });

  it('does not prompt execution approval when sendableCount is 0', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'ready',
      pendingIntent: 'APPROVE_EXECUTION',
      sendableCount: 0,
      capacityItemCount: 5,
      scoutCandidateCount: 15,
      contributions: { scout: {}, max: {}, paige: {}, emmett: {} },
    });
    assert.notEqual(chosen.reason, 'ready_awaiting_execution_approval');
    assert.notEqual(chosen.reason, 'execution_approval_without_sendable_queue');
    assert.equal(chosen.reason, 'capacity_queue_blocked');
    assert.match(chosen.operatorAction, /Resolve blocked recipient\/copy requirements/);
    assert.notEqual(chosen.intent, 'APPROVE_EXECUTION');
    assert.notEqual(chosen.intent, 'EXECUTE_OUTBOUND');
  });

  it('may reach READY and stop for operator execution approval when sendableCount > 0', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'ready',
      pendingIntent: 'APPROVE_EXECUTION',
      sendableCount: 2,
      scoutCandidateCount: 15,
      contributions: { scout: {}, max: {}, paige: {}, emmett: {} },
    });
    assert.equal(chosen.stop, true);
    assert.equal(chosen.intent, null);
    assert.equal(chosen.reason, 'ready_awaiting_execution_approval');
    assert.match(chosen.operatorAction, /APPROVE_EXECUTION/);
  });

  it('never selects APPROVE_EXECUTION or EXECUTE_OUTBOUND and leaves autosend false', () => {
    assert.deepEqual(FORBIDDEN_SEND_INTENTS, ['APPROVE_EXECUTION', 'EXECUTE_OUTBOUND']);
    const blocked = chooseNextRecoveryIntent({
      stage: 'prepare',
      sendableCount: 0,
      scoutCandidateCount: 15,
      discoveryApproved: true,
      contributions: { scout: {}, max: {}, approach: {}, paige: {}, emmett: {} },
    });
    assert.notEqual(blocked.intent, 'APPROVE_EXECUTION');
    assert.notEqual(blocked.intent, 'EXECUTE_OUTBOUND');
    assert.equal(blocked.reason, 'capacity_queue_blocked');
  });

  it('does not invent recipient emails during identity join', () => {
    const email = resolveMissionBoundRecipientEmail({
      discoveryEmail: null,
      missionBoundKey: PLACE_BLUE,
      prospectId: CRM_BLUE,
      crmByProspectId: new Map(),
    });
    assert.equal(email, null);
  });

  it('projects verified CRM email through exact domain identity without name matching', () => {
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([
        ['bluedoorliving.com', verifiedCrm(CRM_BLUE, 'ops@bluedoorliving.com')],
      ]),
    ], buildMissionBoundCandidates(MISSION, contributions({ discoveryEmail: null })));
    const candidates = buildMissionBoundCandidates(
      MISSION,
      contributions({ discoveryEmail: null }),
      { crmByProspectId }
    );
    const blue = candidates.find((row) => row.candidateId === PLACE_BLUE);
    assert.equal(blue.email, 'ops@bluedoorliving.com');
    assert.equal(blue.domain, 'bluedoorliving.com');
  });

  it('unsafe copy is blocked by internal_reasoning_leakage at sanitize time', () => {
    const unsafe = sanitizeQueueItem({
      email: 'sales@example.com',
      contentSource: 'paige',
      paige: {
        author: 'paige',
        source: 'paige',
        ready: true,
        candidateId: PLACE_BLUE,
        subject: 'Hello',
        body: 'Mission focus: Achieve 1 recurring_clients',
        cta: 'Reply',
        bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
        attributableIntelligence: { rationale: 'internal only' },
      },
    });
    assert.equal(unsafe.paige.subject, undefined);
    assert.equal(unsafe.paige.body, undefined);
    assert.equal(unsafe.sendable, false);
    assert.equal(unsafe.sendBlocker, COPY_SAFETY_BLOCKER);
  });

  it('missing subject/body still blocked after projection', () => {
    const blocked = sanitizeQueueItem({
      email: 'sales@example.com',
      contentSource: 'paige',
      paige: {
        author: 'paige',
        source: 'paige',
        ready: true,
        candidateId: PLACE_BLUE,
        bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
        attributableIntelligence: { companyName: 'Blue Door' },
      },
    });
    assert.equal(blocked.sendable, false);
    assert.equal(blocked.sendBlocker, 'missing_paige_copy');
  });

  it('no email still blocked when copy is present', () => {
    const variants = paigeVariantsFromMax();
    const blocked = resolveQueueSendability({
      email: null,
      contentSource: 'paige',
      paige: variants[0],
    });
    assert.equal(blocked.sendable, false);
    assert.equal(blocked.sendBlocker, 'missing_recipient_email');
  });

  it('DNC still blocked when copy and email are present', () => {
    const variants = paigeVariantsFromMax();
    const blocked = resolveQueueSendability({
      email: 'sales@bluedoorliving.org',
      dnc: true,
      contentSource: 'paige',
      paige: variants[0],
    });
    assert.equal(blocked.sendable, false);
    assert.equal(blocked.sendBlocker, 'dnc');
  });

  it('preserves attributableIntelligence without rendering into copy', () => {
    const candidates = buildMissionBoundCandidates(MISSION, contributions());
    const blue = candidates.find((row) => row.candidateId === PLACE_BLUE);
    assert.ok(blue.paige.attributableIntelligence);
    assert.equal(
      blue.paige.body.includes(blue.paige.attributableIntelligence.rationale || '___none___'),
      false
    );
    const capacity = capacityFromCandidates(candidates);
    const item = capacity.queue.items.find((row) => String(row.prospectId) === PLACE_BLUE);
    assert.ok(item.paige.attributableIntelligence);
    assert.ok(item.paige.subject);
    assert.ok(!item.paige.body.includes('fit 0.'));
  });

  it('candidate binding by Place ID preserved with projected copy', () => {
    const candidates = buildMissionBoundCandidates(MISSION, contributions());
    const blue = candidates.find((row) => row.candidateId === PLACE_BLUE);
    assert.equal(blue.placeId, PLACE_BLUE);
    assert.equal(blue.paige.candidateId, PLACE_BLUE);
    const capacity = capacityFromCandidates(candidates);
    const item = capacity.queue.items.find((row) => String(row.prospectId) === PLACE_BLUE);
    assert.equal(item.placeId, PLACE_BLUE);
    assert.equal(item.paige.candidateId, PLACE_BLUE);
    assert.ok(item.paige.subject);
  });
});
