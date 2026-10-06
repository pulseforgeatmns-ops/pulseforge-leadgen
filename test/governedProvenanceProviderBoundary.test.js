'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const ak = require('../packages/acquisition-knowledge');
const {
  buildVerifiedPaigeOutreachEvidence,
} = require('../services/governedOutreachAkEvidence');
const {
  createProviderBoundaryTracker,
  isPreProviderOutboundFailure,
} = require('../services/governedOutboundProviderBoundary');
const {
  classifyProvenUnsent,
  reconcileUncertainItemFromEvidence,
} = require('../services/governedUncertainSendReconciliation');

const { computePreparedArtifactRevision } = require('../packages/acquisition-mission/ExecutionApproval');
const PAIGE_CONTRIB = 'contrib_b53ae2b3-a0ad-4cae-9ac1-882ca3a2fd5b';

function paigeContributions(subject, body) {
  return [{
    id: PAIGE_CONTRIB,
    missionId: 'mission_daily_x',
    specialist: 'paige',
    kind: 'variants',
    payload: {
      variants: [{
        label: 'Primary',
        candidateId: 'prospect-1',
        subject,
        body,
      }],
    },
  }, {
    id: 'contrib_emmett',
    missionId: 'mission_daily_x',
    specialist: 'emmett',
    kind: 'capacity',
    payload: {
      governor: { outcome: 'proceed' },
      queue: { items: [{ id: 'prospect-1', prospectId: 'prospect-1', email: 'a@example.com' }] },
      capacity: { recommended: 1 },
    },
  }, {
    id: 'contrib_max',
    missionId: 'mission_daily_x',
    specialist: 'max',
    kind: 'prioritization',
    payload: { complete: true },
  }];
}

test('A: valid Paige lineage produces AK evidence that passes OBSERVED semantics', () => {
  const subject = 'Ventura outreach subject';
  const body = 'Ventura outreach body';
  const contributions = paigeContributions(subject, body);
  const revision = computePreparedArtifactRevision('mission_daily_x', contributions);
  const evidence = buildVerifiedPaigeOutreachEvidence({
    missionId: 'mission_daily_x',
    preparedArtifactRevision: revision,
    contributions,
    candidateId: 'prospect-1',
    subject,
    body,
  });
  assert.equal(evidence.length, 1);
  assert.doesNotThrow(() => ak.assertCanonicalSemantics({
    epistemicState: ak.EPISTEMIC_STATES.OBSERVED,
    evidence,
  }));
});

test('B: missing Paige lineage fails closed with ak_observed_provenance_required on OBSERVED object', () => {
  assert.throws(() => buildVerifiedPaigeOutreachEvidence({
    missionId: 'mission_daily_x',
    preparedArtifactRevision: computePreparedArtifactRevision('mission_daily_x', []),
    contributions: [],
    candidateId: 'prospect-1',
    subject: 'x',
    body: 'y',
  }), (err) => err.code === 'governed_paige_lineage_missing');
  assert.throws(() => ak.normalizeKnowledgeObject({
    tenantId: '13',
    objectType: 'outreach_asset',
    title: 'missing evidence',
    content: { subject: 'x', body: 'y' },
    epistemicState: 'OBSERVED',
    evidence: [],
  }), (err) => err.code === 'ak_observed_provenance_required');
  assert.equal(isPreProviderOutboundFailure({ code: 'ak_observed_provenance_required' }), true);
});

test('C: SPEC-252/Emmett-class failures before provider boundary are not uncertain', () => {
  const tracker = createProviderBoundaryTracker();
  assert.equal(isPreProviderOutboundFailure({ code: 'schedule_not_pending' }, tracker), true);
  assert.equal(isPreProviderOutboundFailure({ code: 'emmett_governor_halted' }, tracker), true);
  tracker.markCrossed();
  assert.equal(isPreProviderOutboundFailure({ code: 'emmett_governor_halted' }, tracker), false);
});

test('D: deterministic provider rejection after boundary is failed, not uncertain', () => {
  assert.equal(isPreProviderOutboundFailure({ code: 'brevo_http_400' }), true);
});

test('E: post-boundary unknown acceptance remains uncertain-eligible', () => {
  const tracker = createProviderBoundaryTracker();
  tracker.markCrossed();
  assert.equal(isPreProviderOutboundFailure({ code: 'provider_acceptance_unknown' }, tracker), false);
});

test('G: proven pre-provider uncertain attempt reconciles to releasable pending exactly once', async () => {
  const item = {
    id: 'daily_item_1',
    tenant_id: '13',
    envelope_id: 'env_1',
    program_id: 'prog_1',
    status: 'uncertain',
    reason: 'provider_or_persistence_error',
    provider_message_id: null,
    candidate_id: 'prospect-1',
    prospect_id: 'prospect-1',
    email: 'ops@example.com',
    mission_id: 'mission_daily_x',
    attempted_at: '2026-10-01T12:00:00.000Z',
  };
  const evidence = {
    schedules: [],
    mailboxMessages: [],
    executions: [],
    events: [{ event_type: 'send_uncertain', payload: { reason: 'provider_or_persistence_error' } }],
    emmettReservation: [],
    tickBlocks: [{ payload: { reason: 'ak_observed_provenance_required' } }],
  };
  const classification = classifyProvenUnsent(item, evidence);
  assert.equal(classification.outcome, 'PROVEN_UNSENT');

  let releaseCalls = 0;
  let canonicalReleaseCalls = 0;
  const pool = {
    query: async (sql, params) => {
      const normalized = String(sql).replace(/\s+/g, ' ').trim();
      if (/FROM acquisition_outbound_items i/.test(normalized)) return { rows: [item] };
      if (/tenant_outreach_scheduled_sends/.test(normalized)) return { rows: [] };
      if (/tenant_outreach_messages/.test(normalized)) return { rows: [] };
      if (/UPDATE acquisition_mission_outbound_executions/.test(normalized)) {
        canonicalReleaseCalls += 1;
        assert.equal(params[1], item.mission_id);
        return { rows: [] };
      }
      if (/acquisition_mission_outbound_executions/.test(normalized)) return { rows: [] };
      if (/FROM acquisition_outbound_events/.test(normalized) && params[1] === item.id) {
        return { rows: evidence.events };
      }
      if (/tick_blocked/.test(normalized)) return { rows: evidence.tickBlocks };
      if (/FROM agent_log/.test(normalized)) return { rows: [] };
      if (/INSERT INTO acquisition_outbound_events/.test(normalized)) return { rows: [{ id: params[0] }] };
      if (/pg_try_advisory_lock/.test(normalized)) return { rows: [{ locked: true }] };
      if (/pg_advisory_unlock/.test(normalized)) return { rows: [] };
      if (/UPDATE acquisition_outbound_items SET status='pending'/.test(normalized)) {
        releaseCalls += 1;
        return { rows: [{ ...item, status: 'pending', reason: 'reconciled_not_sent' }] };
      }
      if (/INSERT INTO acquisition_outbound_events\(id,tenant_id,program_id,envelope_id,item_id,event_type,payload\)/.test(normalized)
        && params[5]?.outcome === 'not_accepted') {
        return { rows: [{ id: params[0] }] };
      }
      if (/BEGIN|COMMIT|ROLLBACK/i.test(normalized)) return { rows: [] };
      throw new Error(`unexpected sql: ${normalized}`);
    },
    connect: async () => ({
      query: pool.query,
      release() {},
    }),
  };

  const first = await reconcileUncertainItemFromEvidence(pool, '13', item.id);
  assert.equal(first.applied, true);
  assert.equal(releaseCalls, 1);
  assert.equal(canonicalReleaseCalls, 1);

  item.status = 'pending';
  item.reason = 'reconciled_not_sent';
  const second = await reconcileUncertainItemFromEvidence(pool, '13', item.id);
  assert.equal(second.skipped, true);
  assert.equal(second.reason, 'already_reconciled');
  assert.equal(releaseCalls, 1);
  assert.equal(canonicalReleaseCalls, 1);
});
