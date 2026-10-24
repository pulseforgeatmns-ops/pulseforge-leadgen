'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  REQUIRED_SCHEDULE_COLUMNS,
  columnNameFromCatalogRow,
  checkSpec252Ancestry,
  checkProductionShaIncludesSpec252,
  checkMigrationApplied,
} = require('../scripts/verifySpec252PostDeployAndScheduleBabrunCanary');

const EXPECTED_COLUMNS = [
  'id', 'tenant_id', 'prospect_id', 'outreach_asset_id', 'sending_identity_id',
  'recipient_email', 'scheduled_for', 'status', 'idempotency_key', 'authorization_snapshot',
];

function mockMigrationClient({
  relation = 'tenant_outreach_scheduled_sends',
  currentSchema = 'app',
  searchPath = '"$user", public',
  columns = EXPECTED_COLUMNS,
  columnKey = 'column_name',
} = {}) {
  const queries = [];
  const client = {
    query(sql) {
      queries.push(sql);
      if (sql.includes('to_regclass')) {
        return {
          rows: [{
            relation,
            current_schema: currentSchema,
            search_path: searchPath,
          }],
        };
      }
      if (sql.includes('information_schema.columns') || sql.includes('pg_catalog.pg_attribute')) {
        return {
          rows: columns.map((name) => ({ [columnKey]: name })),
        };
      }
      throw new Error(`unexpected query: ${sql}`);
    },
  };
  return { client, queries };
}

describe('SPEC-252 post-deploy verifier', () => {
  it('checkMigrationApplied uses to_regclass against public schema', async () => {
    const { client, queries } = mockMigrationClient();
    const result = await checkMigrationApplied(client);
    assert.match(queries[0], /to_regclass\('public\.tenant_outreach_scheduled_sends'\)/);
    assert.equal(result.tableExists, true);
    assert.equal(result.relation, 'tenant_outreach_scheduled_sends');
    assert.equal(result.relkind, 'r');
    assert.equal(result.missingColumns.length, 0);
    assert.equal(result.migrationApplied, true);
  });

  it('existing table + expected columns => migrationApplied=true', async () => {
    const { client, queries } = mockMigrationClient({
      columns: EXPECTED_COLUMNS,
      columnKey: 'column_name',
    });
    const result = await checkMigrationApplied(client);
    assert.equal(result.tableExists, true);
    assert.equal(result.columnCount, EXPECTED_COLUMNS.length);
    assert.deepEqual(result.missingColumns, []);
    assert.equal(result.migrationApplied, true);
    assert.match(queries[1], /information_schema\.columns/);
    assert.match(queries[1], /table_schema\s*=\s*'public'/);
    assert.match(queries[1], /table_name\s*=\s*'tenant_outreach_scheduled_sends'/);
    assert.match(queries[1], /SELECT\s+column_name/i);
  });

  it('missing expected column => migrationApplied=false with missing column list', async () => {
    const incomplete = EXPECTED_COLUMNS.filter((name) => name !== 'idempotency_key' && name !== 'authorization_snapshot');
    const { client } = mockMigrationClient({ columns: incomplete });
    const result = await checkMigrationApplied(client);
    assert.equal(result.tableExists, true);
    assert.equal(result.migrationApplied, false);
    assert.deepEqual(result.missingColumns, ['idempotency_key', 'authorization_snapshot']);
  });

  it('column query does not depend on current_schema string substitution', async () => {
    const { client, queries } = mockMigrationClient({ currentSchema: 'not_public' });
    const result = await checkMigrationApplied(client);
    const columnSql = queries.find((sql) => sql.includes('information_schema.columns'));
    assert.ok(columnSql, 'expected information_schema.columns query');
    assert.doesNotMatch(columnSql, /\$\{/);
    assert.doesNotMatch(columnSql, /current_schema\s*\(/);
    assert.doesNotMatch(columnSql, /not_public/);
    assert.match(columnSql, /table_schema\s*=\s*'public'/);
    assert.equal(result.currentSchema, 'not_public');
    assert.equal(result.tableExists, true);
    assert.equal(result.migrationApplied, true);
  });

  it('catalog result using attname is normalized correctly', () => {
    assert.equal(columnNameFromCatalogRow({ attname: 'idempotency_key' }), 'idempotency_key');
    assert.equal(columnNameFromCatalogRow({ column_name: 'scheduled_for' }), 'scheduled_for');
    assert.equal(columnNameFromCatalogRow({ column_name: 'id', attname: 'ignored' }), 'id');
    assert.equal(columnNameFromCatalogRow({}), null);
    assert.equal(columnNameFromCatalogRow(null), null);
  });

  it('checkMigrationApplied normalizes pg_attribute attname rows to expected columns', async () => {
    const { client, queries } = mockMigrationClient({
      columns: EXPECTED_COLUMNS,
      columnKey: 'attname',
    });
    // Force the column query path to return attname-only rows even though SQL
    // selects information_schema.column_name — proves the catalog mapper.
    const result = await checkMigrationApplied(client);
    assert.ok(queries[1].includes('information_schema.columns'));
    assert.doesNotMatch(queries[1], /FROM\s+pg_catalog\.pg_attribute/);
    assert.equal(result.migrationApplied, true);
    assert.deepEqual(result.missingColumns, []);
    assert.equal(result.columnCount, EXPECTED_COLUMNS.length);
    assert.deepEqual([...REQUIRED_SCHEDULE_COLUMNS], EXPECTED_COLUMNS);
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
