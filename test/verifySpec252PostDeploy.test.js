'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  checkSpec252Ancestry,
  checkProductionShaIncludesSpec252,
  checkMigrationApplied,
} = require('../scripts/verifySpec252PostDeployAndScheduleBabrunCanary');

describe('SPEC-252 post-deploy verifier', () => {
  it('checkMigrationApplied uses to_regclass against public schema', async () => {
    const queries = [];
    const client = {
      query(sql) {
        queries.push(sql);
        if (sql.includes('to_regclass')) {
          return {
            rows: [{
              relation: 'tenant_outreach_scheduled_sends',
              current_schema: 'public',
              search_path: '"$user", public',
            }],
          };
        }
        if (sql.includes('pg_catalog.pg_attribute')) {
          return {
            rows: [
              'id', 'tenant_id', 'prospect_id', 'outreach_asset_id', 'sending_identity_id',
              'recipient_email', 'scheduled_for', 'status', 'idempotency_key', 'authorization_snapshot',
            ].map((column_name) => ({ column_name })),
          };
        }
        throw new Error(`unexpected query: ${sql}`);
      },
    };

    const result = await checkMigrationApplied(client);
    assert.match(queries[0], /to_regclass\('public\.tenant_outreach_scheduled_sends'\)/);
    assert.equal(result.tableExists, true);
    assert.equal(result.relation, 'tenant_outreach_scheduled_sends');
    assert.equal(result.relkind, 'r');
    assert.equal(result.currentSchema, 'public');
    assert.equal(result.missingColumns.length, 0);
  });

  it('checkProductionShaIncludesSpec252 accepts git ancestry when merge-base succeeds', async () => {
    const cron = { routeLive: true, cronSecretConfigured: true, emptyQueueOk: true };
    const result = await checkProductionShaIncludesSpec252('11e00cff37173787cb836d90dfe91b1ed8c3358b', cron);
    assert.equal(result.pass, true);
    assert.equal(result.method, 'git_merge_base');
  });

  it('checkProductionShaIncludesSpec252 falls back to live route capability without git history', async () => {
    const cron = { routeLive: true, cronSecretConfigured: true, emptyQueueOk: true };
    const result = await checkProductionShaIncludesSpec252('deadbeefdeadbeefdeadbeefdeadbeefdeadbeef', cron);
    assert.equal(result.pass, true);
    assert.equal(result.method, 'live_route_capability');
    assert.match(result.limitation, /git history|Git ancestry/i);
  });

  it('checkSpec252Ancestry reports unavailable git repo gracefully', () => {
    const result = checkSpec252Ancestry('11e00cf');
    assert.equal(typeof result.ok, 'boolean');
    assert.ok(['git_merge_base', 'git_unavailable'].includes(result.method));
  });
});
