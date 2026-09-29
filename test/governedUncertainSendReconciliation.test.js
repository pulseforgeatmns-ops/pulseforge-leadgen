'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  bindGovernedRefillSend,
  deriveExecutionIdentity,
  deriveIdempotencyKey,
  buildExecutionRecord,
  assertPersistableExecutionRecord,
  EXECUTION_RECORD_STATUS,
} = require('../packages/acquisition-mission/OutboundExecution');
const { persistOutboundExecution } = require('../services/acquisitionMissionOutboundPersistence');

const REFILL = {
  candidate_id: '03c2e326-39c5-4e29-abda-cdb25f077e0b',
  company_id: 'b22946db-7955-4280-b1ac-d8b2b7670e09',
  email: 'hello@sacramentopmg.com',
  snapshot: {
    refill: true,
    email: 'hello@sacramentopmg.com',
    candidateId: '03c2e326-39c5-4e29-abda-cdb25f077e0b',
    companyId: 'b22946db-7955-4280-b1ac-d8b2b7670e09',
    revision: '08d6f7797e64ddbb',
    message: {
      subject: 'Cleaning for Auburn Property Management',
      body: 'Would a written quote help?',
      candidateId: '03c2e326-39c5-4e29-abda-cdb25f077e0b',
    },
    sender: { senderEmail: 'jacob@goanchorcleaning.com', senderName: 'Jacob Maynard' },
  },
};

function mockPool(inserts) {
  return {
    query: async (sql, params = []) => {
      if (String(sql).includes('INSERT INTO acquisition_mission_outbound_executions')) {
        inserts.push(params);
        assert.ok(params[13], 'execution_identity must not be null');
      }
      return { rows: [] };
    },
  };
}

test('governed refill send binds execution_identity from mission, prospect and revision', () => {
  const bound = bindGovernedRefillSend(
    { missionId: 'mission_daily_751fbbc204ae39b6db82b15d', sends: [] },
    REFILL,
    { preparedArtifactRevision: '08d6f7797e64ddbb' },
  );
  const expected = deriveExecutionIdentity({
    missionId: 'mission_daily_751fbbc204ae39b6db82b15d',
    prospectId: REFILL.candidate_id,
    preparedArtifactRevision: '08d6f7797e64ddbb',
  });
  assert.equal(bound.executionIdentity, expected);
  assert.equal(bound.idempotencyKey, deriveIdempotencyKey(expected));
  assert.equal(bound.email, 'hello@sacramentopmg.com');
  assert.equal(bound.message.subject, 'Cleaning for Auburn Property Management');
});

test('persistOutboundExecution derives execution_identity instead of inserting NULL', async () => {
  const inserts = [];
  const record = buildExecutionRecord({
    missionId: 'mission_daily_751fbbc204ae39b6db82b15d',
    tenantId: '10',
    prospectId: REFILL.candidate_id,
    preparedArtifactRevision: '08d6f7797e64ddbb',
    status: EXECUTION_RECORD_STATUS.ATTEMPTED,
    executionIdentity: null,
    payload: { email: REFILL.email },
  });
  const saved = await persistOutboundExecution(record, mockPool(inserts), { skipEnsure: true });
  assert.ok(saved.executionIdentity);
  assert.equal(inserts.length, 1);
  assert.equal(inserts[0][13], saved.executionIdentity);
});

test('persistOutboundExecution fails closed before SQL when a NOT NULL identity field is missing', async () => {
  await assert.rejects(
    () => persistOutboundExecution({
      id: 'amo_send_missing',
      missionId: 'mission_daily_x',
      prospectId: '',
      preparedArtifactRevision: '08d6f7797e64ddbb',
      status: 'attempted',
    }, mockPool([]), { skipEnsure: true }),
    { code: 'execution_prospect_id_required' },
  );
  assert.throws(
    () => assertPersistableExecutionRecord({
      id: 'amo_send_missing',
      missionId: '',
      prospectId: REFILL.candidate_id,
      preparedArtifactRevision: '08d6f7797e64ddbb',
      status: 'attempted',
    }),
    { code: 'execution_mission_id_required' },
  );
});
