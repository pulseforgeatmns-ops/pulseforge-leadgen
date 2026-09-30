'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { parseArgs, run } = require('../pennyAgent');

describe('Penny tenant-scoped daily review', () => {
  it('requires an explicit positive client id in CLI mode', () => {
    assert.deepEqual(parseArgs(['--client-id=10']), { client_id: 10 });
    assert.deepEqual(parseArgs(['--client-id', '10']), { client_id: 10 });
    assert.throws(() => parseArgs([]), /positive integer/);
    assert.throws(() => parseArgs(['--client-id=0']), /positive integer/);
  });

  it('reviews Anchor Google Ads and ignores unsupported account types', async () => {
    const calls = { logs: [], saved: [] };
    const result = await run({ client_id: 10 }, {
      pool: {},
      db: {
        async logAgentAction(...args) { calls.logs.push(args); },
      },
      async getClientConfig(clientId) {
        assert.equal(clientId, 10);
        return { id: 10, name: 'Anchor Cleaning', business_name: 'Anchor Cleaning' };
      },
      async isAgentEnabledForClient(clientId, agent) {
        assert.equal(clientId, 10);
        assert.equal(agent, 'penny');
        return { allowed: true, reason: null };
      },
      async ensureAdAccountsSchema(pool) { assert.deepEqual(pool, {}); },
      async resolveAdAccountsForClient({ clientId }) {
        assert.equal(clientId, 10);
        return [
          { id: 'chatgpt', platform: 'chatgpt_ads', company_name: null },
          { id: 'google', platform: 'google_ads', company_name: null },
        ];
      },
      async fetchPlatformEvidence(account) {
        assert.equal(account.id, 'google');
        return { campaigns: [], keywords: [], flags: [] };
      },
      async generateReport(companyName, platform) {
        assert.equal(companyName, 'Anchor Cleaning');
        assert.equal(platform, 'google_ads');
        return 'Read-only daily report';
      },
      async saveReport(clientId, companyName, platform, report, flags) {
        calls.saved.push({ clientId, companyName, platform, report, flags });
        return '12345678-1234-1234-1234-123456789012';
      },
      async sleep() {},
    });

    assert.deepEqual(result, {
      client_id: 10,
      accounts_analyzed: 1,
      ignored_accounts: 1,
      reports_saved: 1,
      flags: 0,
    });
    assert.equal(calls.saved.length, 1);
    assert.equal(calls.saved[0].clientId, 10);
    assert.equal(calls.logs.at(-1)[1], 'run');
    assert.equal(calls.logs.at(-1)[4].client_id, 10);
  });

  it('fails closed when Penny is not enabled for the tenant', async () => {
    await assert.rejects(
      run({ client_id: 10 }, {
        async getClientConfig() { return { id: 10, name: 'Anchor Cleaning' }; },
        async isAgentEnabledForClient() {
          return { allowed: false, reason: 'agent_not_enabled_for_client' };
        },
      }),
      /not enabled for client_id=10/
    );
  });
});
