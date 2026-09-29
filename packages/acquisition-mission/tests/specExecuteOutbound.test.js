'use strict';

/**
 * Production EXECUTE outbound contract.
 * The canonical bundle builder is OutboundExecution.buildExecutionBundle.
 * The adapter and #761 refill persistence consume that result directly.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const {
  buildExecutionBundle,
  bindGovernedRefillSend,
  deriveExecutionIdentity,
  deriveIdempotencyKey,
} = require('../OutboundExecution');
const { executeOutboundBundle } = require('../../max/workspace/OutboundExecutionAdapter');
const { FIXTURE_CANONICAL_SENDER } = require('../../max/workspace/EmmettCapacityExecution');

describe('production execution bundle contract', () => {
  it('adapter calls the canonical synchronous bundle builder', async () => {
    assert.equal(typeof buildExecutionBundle, 'function');
    const adapterSource = fs.readFileSync(
      require.resolve('../../max/workspace/OutboundExecutionAdapter.js'),
      'utf8'
    );
    assert.match(adapterSource, /require\('\.\.\/\.\.\/acquisition-mission\/OutboundExecution'\)/);
    assert.match(adapterSource, /buildExecutionBundle\(/);
    const routerSource = fs.readFileSync(require.resolve('../ExecutionRouter.js'), 'utf8');
    assert.match(routerSource, /advanceExecuteOutbound/);
    assert.doesNotMatch(routerSource, /executeOutboundMission/);

    let providerCalls = 0;
    const result = await executeOutboundBundle({
      mission: { id: 'mission-contract', tenantId: '10' },
      contributions: [],
      tenantId: '10',
      canonicalSender: { ...FIXTURE_CANONICAL_SENDER },
      senderReadiness: { ready: true },
      requireProviderReadiness: false,
      sendEmail: async () => {
        providerCalls += 1;
        return { success: true, providerMessageId: 'should-not-send' };
      },
    });
    assert.equal(result.blocked, true);
    assert.match(result.blockReason, /approval|Execution/);
    assert.equal(providerCalls, 0);
    assert.equal(result.summary.sent, 0);
  });

  it('governed refill sends keep the #761 execution identity', () => {
    const revision = '08d6f7797e64ddbb';
    const prospectId = '03c2e326-39c5-4e29-abda-cdb25f077e0b';
    const bound = bindGovernedRefillSend(
      { missionId: 'mission_daily_751fbbc204ae39b6db82b15d', sends: [] },
      {
        candidate_id: prospectId,
        company_id: 'b22946db-7955-4280-b1ac-d8b2b7670e09',
        email: 'hello@sacramentopmg.com',
        snapshot: {
          refill: true,
          email: 'hello@sacramentopmg.com',
          candidateId: prospectId,
          companyId: 'b22946db-7955-4280-b1ac-d8b2b7670e09',
          message: {
            subject: 'Cleaning for Auburn Property Management',
            body: 'Would a written quote help?',
            candidateId: prospectId,
          },
        },
      },
      { preparedArtifactRevision: revision }
    );
    const identity = deriveExecutionIdentity({
      missionId: 'mission_daily_751fbbc204ae39b6db82b15d',
      prospectId,
      preparedArtifactRevision: revision,
    });
    assert.equal(bound.executionIdentity, identity);
    assert.equal(bound.idempotencyKey, deriveIdempotencyKey(identity));
    assert.ok(bound.executionIdentity);
    assert.equal(bound.status, 'queued');
  });
});
