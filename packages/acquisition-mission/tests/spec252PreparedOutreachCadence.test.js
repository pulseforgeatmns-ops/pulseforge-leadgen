'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  resolveOutreachSequenceAtPrepare,
  outreachSequenceForRevisionHash,
  freezeOutreachSequenceForApproval,
  buildHistoricalCadenceAnnotation,
  extractOutreachSequenceSteps,
  SOURCE_KINDS,
} = require('../PreparedOutreachSequence');
const {
  computePreparedArtifactRevision,
  buildExecutionApprovalPayload,
} = require('../ExecutionApproval');
const { resolveObserveCadence } = require('../ObserveCadence');
const { SPECIALISTS, CONTRIBUTION_KINDS } = require('../types');

const TEST_CATALOG = {
  anchor_law_firm_draft: [
    { day: 0, subject: 'a', body: 'b' },
    { day: 4, subject: 'a', body: 'b' },
    { day: 8, subject: 'a', body: 'b' },
    { day: 13, subject: 'a', body: 'b' },
  ],
};

const TEST_CLIENT_MAP = {
  10: {
    law_firm: 'anchor_law_firm_draft',
    accounting: 'anchor_accounting_draft',
  },
};

describe('SPEC-252 Prepared Outreach Cadence', () => {
  it('lifts Anchor law-firm template at PREPARE from mission targetSegment', () => {
    const sequence = resolveOutreachSequenceAtPrepare({
      mission: { tenantId: '10', clientId: 10, targetSegment: 'Law Firms' },
      contributions: [],
      catalog: TEST_CATALOG,
      clientSequenceMap: TEST_CLIENT_MAP,
    });
    assert.ok(sequence);
    assert.equal(sequence.source.kind, 'template_catalog');
    assert.equal(sequence.source.templateKey, 'anchor_law_firm_draft');
    assert.deepEqual(sequence.steps.map((row) => row.day), [0, 4, 8, 13]);
  });

  it('returns null when candidate verticals conflict', () => {
    const sequence = resolveOutreachSequenceAtPrepare({
      mission: { tenantId: '10', clientId: 10, targetSegment: 'Law Firms' },
      contributions: [{
        specialist: SPECIALISTS.MAX,
        kind: CONTRIBUTION_KINDS.PRIORITIZATION,
        payload: {
          rankedTargets: [
            { vertical: 'law_firm' },
            { vertical: 'accounting' },
          ],
        },
      }],
      catalog: TEST_CATALOG,
      clientSequenceMap: TEST_CLIENT_MAP,
    });
    assert.equal(sequence, null);
  });

  it('includes outreachSequence steps in prepared artifact revision hash', () => {
    const missionId = 'mission_spec252';
    const paigePayload = {
      variants: [{ label: 'Primary', subject: 'Hi', body: 'Body' }],
      outreachSequence: resolveOutreachSequenceAtPrepare({
        mission: { tenantId: '10', clientId: 10, targetSegment: 'Law Firms' },
        contributions: [],
        catalog: TEST_CATALOG,
        clientSequenceMap: TEST_CLIENT_MAP,
      }),
    };
    const contributions = [
      { id: 'max-1', specialist: SPECIALISTS.MAX, kind: CONTRIBUTION_KINDS.PRIORITIZATION, payload: {} },
      { id: 'paige-1', specialist: SPECIALISTS.PAIGE, kind: CONTRIBUTION_KINDS.VARIANTS, payload: paigePayload },
      { id: 'emmett-1', specialist: SPECIALISTS.EMMETT, kind: CONTRIBUTION_KINDS.CAPACITY, payload: { queue: { items: [] }, governor: {} } },
    ];
    const before = computePreparedArtifactRevision(missionId, contributions);
    const altered = [...contributions];
    altered[1] = {
      ...altered[1],
      payload: {
        ...paigePayload,
        outreachSequence: {
          ...paigePayload.outreachSequence,
          steps: paigePayload.outreachSequence.steps.map((row, index) => (
            index === 1 ? { ...row, day: 5 } : row
          )),
        },
      },
    };
    const after = computePreparedArtifactRevision(missionId, altered);
    assert.notEqual(before, after);
    assert.deepEqual(
      outreachSequenceForRevisionHash(paigePayload.outreachSequence),
      [{ step: 0, day: 0, channel: 'email' }, { step: 1, day: 4, channel: 'email' },
        { step: 2, day: 8, channel: 'email' }, { step: 3, day: 13, channel: 'email' }]
    );
  });

  it('freezes outreachSequence onto execution approval payload', () => {
    const mission = { id: 'mission_spec252', tenantId: '10', targetSegment: 'Law Firms' };
    const outreachSequence = resolveOutreachSequenceAtPrepare({
      mission,
      contributions: [],
      catalog: TEST_CATALOG,
      clientSequenceMap: TEST_CLIENT_MAP,
    });
    const contributions = [
      { id: 'max-1', specialist: SPECIALISTS.MAX, kind: CONTRIBUTION_KINDS.PRIORITIZATION, payload: {} },
      {
        id: 'paige-1',
        specialist: SPECIALISTS.PAIGE,
        kind: CONTRIBUTION_KINDS.VARIANTS,
        payload: { variants: [{ label: 'Primary', subject: 'Hi', body: 'Body' }], outreachSequence },
      },
      {
        id: 'emmett-1',
        specialist: SPECIALISTS.EMMETT,
        kind: CONTRIBUTION_KINDS.CAPACITY,
        payload: { queue: { items: [{ prospectId: 'p1', paige: { candidateId: 'p1', subject: 'Hi', body: 'Body', ready: true, sendable: true, author: 'paige', source: 'paige' } }] }, governor: {} },
      },
    ];
    const approvalPayload = buildExecutionApprovalPayload(mission, contributions, { operatorId: 'op-1' });
    assert.ok(approvalPayload.outreachSequence);
    assert.equal(approvalPayload.outreachSequence.steps.length, 4);
    assert.deepEqual(
      freezeOutreachSequenceForApproval(outreachSequence),
      approvalPayload.outreachSequence
    );
  });

  it('builds additive historical cadence annotation without touching approval rows', () => {
    const outreachSequence = resolveOutreachSequenceAtPrepare({
      mission: { tenantId: '10', clientId: 10, targetSegment: 'Law Firms' },
      contributions: [],
      catalog: TEST_CATALOG,
      clientSequenceMap: TEST_CLIENT_MAP,
    });
    const annotation = buildHistoricalCadenceAnnotation({
      missionId: 'mission_backus',
      tenantId: '10',
      executionRecordId: 'amo_send_test',
      executionApprovalContributionId: 'approval_old',
      preparedArtifactRevision: 'rev-old',
      prospectId: 'prospect-1',
      outreachSequence,
      templateKey: 'anchor_law_firm_draft',
    });
    assert.ok(annotation);
    assert.equal(annotation.source.kind, SOURCE_KINDS.HISTORICAL_BACKFILL);
    assert.equal(extractOutreachSequenceSteps({ outreachSequence: annotation.outreachSequence }).length, 4);
  });

  it('ObserveCadence resolves waitDays from preparedCadence input', () => {
    const cadence = resolveObserveCadence({
      preparedCadence: {
        steps: [{ step: 0, day: 0 }, { step: 1, day: 4 }],
        cadenceProvenance: 'historical_annotation',
        reconstructed: true,
      },
      sequenceStepSent: 0,
      clockStart: '2026-09-14T12:25:57.000Z',
      now: new Date('2026-09-14T12:26:00.000Z'),
    });
    assert.equal(cadence.cadenceSource, 'prepared_sequence');
    assert.equal(cadence.waitDays, 4);
    assert.equal(cadence.kind, 'wait_until');
    assert.equal(cadence.reconstructed, true);
  });
});
