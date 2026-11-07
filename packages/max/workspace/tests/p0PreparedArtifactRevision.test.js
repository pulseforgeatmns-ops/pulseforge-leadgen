'use strict';

/**
 * P0 — an execution approval binds the complete authorized content.
 *
 * The prepared-artifact revision (the package id stored by governed envelopes,
 * receipts and evidence — format unchanged) hashes only the first 25 queue
 * target ids and no recipient address, per-target flags, copy content or sender
 * name. A queue rewritten under the same contribution id beyond position 25
 * kept an existing approval. Approvals now also store a canonical digest of the
 * complete content; these tests pin it and its consequences at send time.
 * Deterministic: no network, no database, stub transport only.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const A = require('../AmoOperatorApproval');
const { executeOutboundBundle } = require('../OutboundExecutionAdapter');
const { liveScoutStub } = require('./amoTestRuntime');
const { canonicalJson } = require('../../../acquisition-mission/ExecutionApproval');

const TENANT = '10';
const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';
const SENDER = { tenantId: TENANT, clientId: 10, senderEmail: 'hello@example.com', senderName: 'Example Sender', sendingDomain: 'example.com' };
const QUEUE_SIZE = 32;

async function readyMission() {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: TENANT, objective: OBJECTIVE, targetSegment: 'Law Firms' });
  const g = () => engine.get(mission.id, TENANT);
  await A.advancePlanAfterApproval({ engine, mission, tenantId: TENANT, question: 'Approved.' });
  await A.advanceDiscoveryAfterApproval({ engine, mission, tenantId: TENANT, question: 'Approved. Begin Discovery.', runScout: liveScoutStub });
  await A.advancePrioritizationAfterApproval({ engine, mission: g(), tenantId: TENANT, question: 'Approved prioritization.' });
  await A.advanceMaxPrioritization({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true });
  await A.advanceAcquisitionApproach({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true });
  await A.advancePaigeVariants({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true, question: 'Generate variants.' });
  await A.advanceEmmettCapacity({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true, question: 'Plan capacity.' });
  return { engine, mission };
}

// Read the store directly: once an approval is voided, engine.inspect fails
// closed on the now-inconsistent mission (SPEC-136), which is itself correct.
const contributions = (engine, mission) => engine.store.listContributions(mission.id);
const revision = (engine, mission) => amo.computePreparedArtifactRevision(mission.id, contributions(engine, mission));
const contentDigest = (engine, mission) => amo.computePreparedContentDigest(mission.id, contributions(engine, mission));
const emmettRow = (engine, mission) => amo.findEmmettCapacity(contributions(engine, mission));
const paigeRow = (engine, mission) => amo.findPaigeVariants(contributions(engine, mission));

/** Rewrite a contribution payload IN PLACE (same id) — the case the old prefix hash missed. */
function rewrite(engine, row, mutatePayload) {
  engine.store.updateContribution(row.id, (current) => {
    const next = JSON.parse(JSON.stringify(current));
    const inner = next.payload && next.payload.payload ? next.payload.payload : next.payload;
    mutatePayload(inner);
    return next;
  });
}

function target(template, i) {
  return {
    ...JSON.parse(JSON.stringify(template)),
    id: `co-t${i}`, candidateId: `co-t${i}`, companyId: `co-t${i}`, prospectId: `co-t${i}`,
    email: `owner${i}@target${i}.example`, company: `Target ${i}`, position: i + 1,
    sendable: true, dnc: false,
  };
}

/** A READY mission whose queue holds QUEUE_SIZE targets (well past 25). */
async function largeQueueMission() {
  const { engine, mission } = await readyMission();
  rewrite(engine, emmettRow(engine, mission), (payload) => {
    const template = payload.queue.items[0];
    payload.queue.items = Array.from({ length: QUEUE_SIZE }, (_, i) => target(template, i));
  });
  return { engine, mission };
}

async function approve(engine, mission) {
  const routed = await amo.routeExecutionRequest(amo.createExecutionRequest({
    source: amo.EXECUTION_SOURCES.APPROVAL_BUTTON,
    intent: amo.EXECUTION_INTENTS.APPROVE_EXECUTION,
    missionId: mission.id,
    operatorId: 'operator-1',
    stage: amo.STAGES.READY,
    payload: {
      expectedPreparedArtifactRevision: revision(engine, mission),
      expectedPreparedContentDigest: contentDigest(engine, mission),
    },
  }), { engine, tenantId: TENANT });
  assert.equal(routed.action, 'execution_approved');
}

const approved = (engine, mission) => amo.isExecutionApproved(contributions(engine, mission), mission.id);

const MUTATIONS_BEYOND_25 = [
  ['changed target id at position 27', (p) => { p.queue.items[26].prospectId = 'co-intruder'; p.queue.items[26].id = 'co-intruder'; }],
  ['changed recipient address at position 28', (p) => { p.queue.items[27].email = 'someone-else@elsewhere.example'; }],
  ['added a 33rd target', (p) => { p.queue.items.push(target(p.queue.items[0], 99)); }],
  ['removed the last target', (p) => { p.queue.items.pop(); }],
  ['reordered positions 26 and 27', (p) => { const [a, b] = [p.queue.items[25], p.queue.items[26]]; p.queue.items[25] = b; p.queue.items[26] = a; }],
  ['duplicated target 26', (p) => { p.queue.items.push(JSON.parse(JSON.stringify(p.queue.items[25]))); }],
  ['flipped the sendable flag at position 30', (p) => { p.queue.items[29].sendable = !p.queue.items[29].sendable; }],
  ['flagged position 31 do-not-contact', (p) => { p.queue.items[30].dnc = !p.queue.items[30].dnc; }],
  ['rebound the copy for target 32', (p) => { p.queue.items[31].paige = { ...(p.queue.items[31].paige || {}), variantLabel: 'Other' }; }],
];

describe('P0 prepared content digest — complete target set', () => {
  for (const [label, mutate] of MUTATIONS_BEYOND_25) {
    it(`${label} changes the content digest and voids the approval`, async () => {
      const { engine, mission } = await largeQueueMission();
      await approve(engine, mission);
      const before = contentDigest(engine, mission);
      assert.equal(approved(engine, mission), true);

      rewrite(engine, emmettRow(engine, mission), mutate);

      assert.notEqual(contentDigest(engine, mission), before);
      assert.equal(approved(engine, mission), false, 'an approval cannot survive a change beyond position 25');
    });
  }

  it('the approval stores the complete-content digest alongside the unchanged package revision', async () => {
    const { engine, mission } = await largeQueueMission();
    await approve(engine, mission);
    const approval = amo.findValidExecutionApproval(contributions(engine, mission), mission.id);
    assert.equal(approval.payload.preparedArtifactRevision, revision(engine, mission));
    assert.equal(approval.payload.preparedContentDigest, contentDigest(engine, mission));
  });

  it('a legacy approval without a stored digest keeps its revision-only semantics (no mass invalidation at deploy)', async () => {
    const { engine, mission } = await largeQueueMission();
    await approve(engine, mission);
    const approval = amo.findValidExecutionApproval(contributions(engine, mission), mission.id);
    engine.store.updateContribution(approval.id, (row) => {
      const next = JSON.parse(JSON.stringify(row));
      delete next.payload.preparedContentDigest;
      return next;
    });
    assert.equal(approved(engine, mission), true);
  });

  it('is deterministic and survives a JSONB-style key reordering', async () => {
    const { engine, mission } = await largeQueueMission();
    const rows = contributions(engine, mission);
    const reverseKeys = (v) => (Array.isArray(v) ? v.map(reverseKeys)
      : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).reverse().map((k) => [k, reverseKeys(v[k])])) : v);
    const roundTripped = JSON.parse(JSON.stringify(rows)).map(reverseKeys);
    assert.equal(
      amo.computePreparedContentDigest(mission.id, roundTripped),
      amo.computePreparedContentDigest(mission.id, rows),
    );
    assert.equal(
      amo.computePreparedArtifactRevision(mission.id, roundTripped),
      amo.computePreparedArtifactRevision(mission.id, rows),
    );
    assert.equal(contentDigest(engine, mission), contentDigest(engine, mission));
    assert.equal(canonicalJson({ b: 1, a: [2, { d: 3, c: 4 }] }), '{"a":[2,{"c":4,"d":3}],"b":1}');
  });
});

describe('P0 prepared content digest — content, sender, scope and count', () => {
  it('approved copy edited in place (same Paige id) voids the approval', async () => {
    const { engine, mission } = await largeQueueMission();
    await approve(engine, mission);
    rewrite(engine, paigeRow(engine, mission), (p) => {
      const variants = p.variants || [];
      if (variants[0]) variants[0].body = `${variants[0].body || ''} — altered after approval`;
      else p.variants = [{ label: 'Primary', subject: 'x', body: 'altered after approval' }];
    });
    assert.equal(approved(engine, mission), false);
  });

  it('sender identity (email, domain or name) edited in place voids the approval', async () => {
    for (const mutate of [
      (p) => { p.senderIdentity.senderEmail = 'other@example.com'; p.senderIdentity.inboxId = 'other@example.com'; },
      (p) => { p.senderIdentity.sendingDomain = 'other.example'; p.senderIdentity.domain = 'other.example'; },
      (p) => { p.senderIdentity.senderName = 'Someone Else'; },
    ]) {
      const { engine, mission } = await largeQueueMission();
      await approve(engine, mission);
      rewrite(engine, emmettRow(engine, mission), mutate);
      assert.equal(approved(engine, mission), false);
    }
  });

  it('send count is recorded in the approval and bounded by the approved queue', async () => {
    const { engine, mission } = await largeQueueMission();
    await approve(engine, mission);
    const approval = amo.findValidExecutionApproval(contributions(engine, mission), mission.id);
    assert.equal(approval.payload.plannedSendCount, QUEUE_SIZE);
    assert.equal(approval.payload.queueCount, QUEUE_SIZE);
  });

  it('after an in-place change beyond 25, the send step sends nothing', async () => {
    const { engine, mission } = await largeQueueMission();
    await approve(engine, mission);
    rewrite(engine, emmettRow(engine, mission), (p) => { p.queue.items[27].email = 'someone-else@elsewhere.example'; });
    const calls = [];
    const result = await executeOutboundBundle({
      mission: engine.get(mission.id, TENANT),
      contributions: contributions(engine, mission),
      tenantId: TENANT,
      canonicalSender: SENDER,
      sendEmail: async (input) => { calls.push(input); return { success: true, messageId: 'm', providerMessageId: 'm' }; },
    });
    assert.equal(result.blocked, true);
    assert.equal(calls.length, 0);
  });
});
