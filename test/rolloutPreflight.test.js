'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const path = require('node:path');
const preflight = require('../scripts/rolloutPreflight');
const { TABLES, SHAPES, PRIMARY_KEYS, UNIQUE_KEYS, FOREIGN_KEYS, STATUSES, IMMUTABLE_BODY, QUERIES } = preflight;
const PRIVATE = 'never-print-this-password-session-token';
const URL = `postgresql://test:${PRIVATE}@127.0.0.1:1234/preflight_test`;
function fixture() {
  const uniqueIndex = (table, keys) => ({ table_name: table, keys, is_unique: true, valid: true, ready: true, immediate: true, no_predicate: true, no_expression: true });
  const catalog = {
    tables: TABLES.map(table_name => ({ table_name, relkind: 'r' })),
    columns: Object.entries(SHAPES).flatMap(([table_name, columns]) => Object.entries(columns).map(([column_name, shape]) => ({ table_name, column_name, type_name: shape.replace('!', ''), not_null: shape.endsWith('!'), default_expr: column_name === 'created_at' ? 'now()' : column_name === 'status' ? "'pending'::text" : column_name.startsWith('ao_') ? 'false' : null }))),
    constraints: [
      ...Object.entries(PRIMARY_KEYS).map(([table_name, keys]) => ({ table_name, kind: 'p', keys, validated: true, deferrable: false })),
      ...FOREIGN_KEYS.map(([table_name, keys, foreign_table, foreign_keys]) => ({ table_name, keys, foreign_table, foreign_keys, foreign_schema: 'public', kind: 'f', validated: true, deferrable: false, delete_action: 'a', update_action: 'a', match_type: 's', triggers_enforced: true })),
      { table_name: 'prospects', keys: ['ao_current_status'], kind: 'c', validated: true, expression: preflight.allowedExpression('ao_current_status', STATUSES, true) },
      { table_name: 'max_spreadsheet_proposals', keys: ['status'], kind: 'c', validated: true, expression: preflight.allowedExpression('status', ['pending', 'committed', 'superseded']) },
      { table_name: 'max_spreadsheet_suppressions', keys: ['channel'], kind: 'c', validated: true, expression: "(channel='call'::text)" },
    ],
    indexes: [...Object.entries(PRIMARY_KEYS).map(([t, k]) => uniqueIndex(t, k)), ...Object.entries(UNIQUE_KEYS).flatMap(([t, ks]) => ks.map(k => uniqueIndex(t, k)))],
    triggers: [{ trigger_name: 'max_spreadsheet_proposal_immutable', enabled: 'O', type: 19, unconditional: true, argument_count: 0, column_filter: '', function_schema: 'public', function_name: 'max_spreadsheet_proposal_immutable', body: IMMUTABLE_BODY, language: 'plpgsql', security_definer: false }],
  };
  return catalog;
}
function fakeClient({ catalog = fixture(), failAt, code, hangAt, safety, endReject = false } = {}) {
  const calls = [], state = { closed: 0, destroyed: 0, config: null };
  class Client {
    constructor(config) {
      state.config = config;
      this.connection = { stream: { destroyed: false, destroy: () => { this.connection.stream.destroyed = true; state.destroyed++; } } };
    }
    on(name, handler) { assert.equal(name, 'error'); assert.equal(typeof handler, 'function'); }
    async step(key) {
      calls.push(key);
      if (key === hangAt) return new Promise(() => {});
      if (key === failAt) throw Object.assign(new Error(`database ${URL} ${PRIVATE}`), { code });
    }
    async connect() { await this.step('connect'); }
    async query(query) {
      const key = typeof query === 'string' ? query : Object.keys(QUERIES).find(k => QUERIES[k] === query.text);
      assert.ok(key, 'only fixed statements may reach the driver');
      await this.step(key);
      if (key === 'safety') return { rows: [safety || { read_only: 'on', statement_timeout: '3s', lock_timeout: '500ms' }] };
      return { rows: catalog[key] || [] };
    }
    async end() { state.closed++; await this.step('end'); if (endReject) throw new Error(PRIVATE); }
  }
  return { Client, calls, state, timeouts: { connect: 15, query: 15, close: 15 } };
}
const find = (r, id) => r.checks.find(c => c.id === id);
function safe(result) { const text = JSON.stringify(result); assert.ok(!text.includes(PRIVATE)); assert.ok(!text.includes(URL)); assert.ok(!text.includes('postgresql://')); }

test('full reviewed migration prerequisites ready; all 16 lock dependencies present', () => {
  const result = preflight.classifySchema(fixture());
  assert.equal(result.status, 'ready'); assert.equal(preflight.LOCK_TABLES.length, 16);
  assert.ok(result.checks.length > 90);
});
test('absent and partial schema fail closed with actionable checks', () => {
  assert.equal(preflight.classifySchema({ tables: [], columns: [], constraints: [], indexes: [], triggers: [] }).status, 'blocked');
  const f = fixture(); f.tables = f.tables.filter(t => !t.table_name.startsWith('max_spreadsheet_'));
  f.columns = f.columns.filter(c => c.table_name !== 'prospects');
  const r = preflight.classifySchema(f);
  for (const id of ['table.max_spreadsheet_proposals', 'column.prospects.ao_call_suppressed', 'column.prospects.ao_outreach_review_required', 'column.prospects.ao_source_address', 'column.prospects.website']) {
    assert.equal(find(r, id).status, 'blocked'); assert.ok(find(r, id).action);
  }
});
test('wrong types, nullability and unsafe defaults block readiness', () => {
  for (const [property, value] of [['type_name', 'text'], ['not_null', false], ['default_expr', 'true']]) {
    const f = fixture(); f.columns.find(c => c.column_name === 'ao_call_suppressed')[property] = value;
    assert.equal(preflight.classifySchema(f).status, 'blocked');
  }
  const f = fixture(); f.columns.find(c => c.table_name === 'max_spreadsheet_proposals' && c.column_name === 'status').default_expr = "'committed'::text";
  assert.equal(preflight.classifySchema(f).status, 'blocked');
});
test('legacy, unvalidated, negated and extra status constraints cannot falsely pass', () => {
  for (const expression of [preflight.allowedExpression('ao_current_status', STATUSES.slice(0, -1), true), `NOT ${preflight.allowedExpression('ao_current_status', STATUSES, true)}`, `ao_current_status <> 'application_in_progress'::text`]) {
    const f = fixture(); f.constraints.find(c => c.table_name === 'prospects').expression = expression;
    assert.notEqual(find(preflight.classifySchema(f), 'check.prospects.ao_current_status').status, 'ready');
  }
  const f = fixture(); const c = f.constraints.find(c => c.table_name === 'prospects'); c.validated = false;
  assert.equal(find(preflight.classifySchema(f), 'check.prospects.ao_current_status').status, 'blocked');
  c.validated = true; f.constraints.push({ ...c, keys: [], expression: '(false)' });
  assert.equal(find(preflight.classifySchema(f), 'check.prospects.additional').status, 'unknown');
});
test('constant and additional CHECKs on every new table fail closed; unrelated legacy checks remain out of scope', () => {
  for (const table of Object.keys(SHAPES)) {
    for (const keys of [[], ['id']]) {
      if (table === 'prospects' && keys.length) continue;
      const f = fixture(); f.constraints.push({ table_name: table, kind: 'c', keys, expression: '(false)', validated: true });
      assert.equal(preflight.classifySchema(f).status, 'unknown');
    }
  }
  const f = fixture(); f.constraints.push({ table_name: 'prospects', kind: 'c', keys: ['website'], expression: '(website IS NULL)', validated: true });
  assert.equal(preflight.classifySchema(f).status, 'unknown');
  f.constraints.pop(); f.constraints.push({ table_name: 'prospects', kind: 'c', keys: ['status'], expression: 'legacy status contract', validated: true });
  assert.equal(preflight.classifySchema(f).status, 'ready');
});
test('composite prerequisites reject missing, wrong-order, partial, invalid, deferred and expression indexes', () => {
  for (const edit of [i => { i.keys.reverse(); }, i => { i.no_predicate = false; }, i => { i.valid = false; }, i => { i.ready = false; }, i => { i.immediate = false; }, i => { i.no_expression = false; }, i => { i.is_unique = false; }]) {
    const f = fixture(); const i = f.indexes.find(i => i.table_name === 'users' && i.keys.length === 2); i.keys = [...i.keys]; edit(i);
    assert.equal(find(preflight.classifySchema(f), 'unique.users.client_id+id').status, 'blocked');
  }
});
test('migration PK/FKs require valid keys and fail on wrong tenant linkage or cascades', () => {
  for (const [property, value] of [['foreign_keys', ['id', 'client_id']], ['validated', false], ['foreign_schema', 'other'], ['delete_action', 'c'], ['update_action', 'c'], ['deferrable', true], ['triggers_enforced', false]]) {
    const f = fixture(); f.constraints.find(c => c.table_name === 'max_spreadsheet_contacts' && c.kind === 'f')[property] = value;
    assert.equal(find(preflight.classifySchema(f), 'foreign_key.max_spreadsheet_contacts.client_id+prospect_id').status, 'blocked');
  }
  const f = fixture(); f.constraints = f.constraints.filter(c => c.kind !== 'p'); assert.equal(preflight.classifySchema(f).status, 'blocked');
});
test('immutable trigger must be enabled, unconditional and match reviewed body', () => {
  for (const [property, value] of [['enabled', 'D'], ['type', 17], ['unconditional', false], ['column_filter', '1'], ['body', 'BEGIN RETURN NEW; END'], ['security_definer', true]]) {
    const f = fixture(); f.triggers[0][property] = value;
    assert.equal(find(preflight.classifySchema(f), 'trigger.max_spreadsheet_proposal_immutable').status, 'blocked');
  }
});
test('catalog/error text never appears in allowlisted output', () => {
  const f = fixture(); f.columns[0].type_name = PRIVATE; f.constraints.push({ table_name: 'prospects', kind: 'c', keys: ['ao_current_status'], expression: PRIVATE });
  f.triggers[0].body = PRIVATE; f.tables.push({ table_name: PRIVATE, relkind: 'r' }); safe(preflight.classifySchema(f));
});
test('SQL allowlist only SELECTs system catalogs and settings, no customer reads or locks', () => {
  for (const sql of Object.values(QUERIES)) {
    assert.match(sql, /^SELECT\b/); assert.doesNotMatch(sql, /\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|CALL|COPY|LOCK|INTO)\b/i);
    assert.doesNotMatch(sql, /(?:FROM|JOIN)\s+(?:public\.)?(?:prospects|max_spreadsheet_\w+|users)\b/i);
    assert.doesNotMatch(sql, /pg_sleep|pg_advisory|pg_read_file|pg_stat_activity/i);
  }
});
test('catalog key arrays use pg text[] decoding rather than unsupported name[] strings', () => {
  const { types } = require('pg');
  assert.deepEqual(types.getTypeParser(1009)('{client_id,id}'), ['client_id', 'id']);
  assert.equal(types.getTypeParser(1003)('{client_id,id}'), '{client_id,id}');
  assert.equal((QUERIES.constraints.match(/ARRAY\(SELECT a\.attname::text FROM unnest\(/g) || []).length, 2);
  assert.equal((QUERIES.indexes.match(/ARRAY\(SELECT a\.attname::text FROM unnest\(/g) || []).length, 1);
});
test('successful database read verifies settings, rolls back and closes owned connection', async () => {
  const fake = fakeClient(); const r = await preflight.inspectDatabase(URL, fake);
  assert.equal(r.status, 'ready'); assert.deepEqual(fake.calls, ['connect', 'BEGIN READ ONLY', ...Object.keys(QUERIES), 'ROLLBACK', 'end']);
  assert.equal(fake.state.closed, 1); assert.equal(fake.state.destroyed, 1);
  assert.match(fake.state.config.options, /default_transaction_read_only=on/); safe(r);
});
test('refuses metadata queries unless read-only and finite server deadlines verified', async () => {
  const fake = fakeClient({ safety: { read_only: 'off', statement_timeout: '0', lock_timeout: '0' } });
  const r = await preflight.inspectDatabase(URL, fake);
  assert.equal(find(r, 'read_only_not_verified').status, 'unknown'); assert.ok(!fake.calls.includes('tables')); assert.equal(fake.state.closed, 1);
});
for (const at of ['connect', 'BEGIN READ ONLY', ...Object.keys(QUERIES), 'ROLLBACK', 'end']) {
  test(`failure at ${at} is sanitized and closes connection`, async () => {
    const fake = fakeClient({ failAt: at, code: '42501' }); const r = await preflight.inspectDatabase(URL, fake);
    assert.equal(r.status, 'unknown'); assert.equal(fake.state.closed, 1); safe(r);
  });
  test(`timeout at ${at} is bounded and closes connection`, async () => {
    const fake = fakeClient({ hangAt: at }); const r = await preflight.inspectDatabase(URL, fake);
    assert.equal(r.status, 'unknown'); assert.equal(fake.state.closed, 1); assert.equal(fake.state.destroyed, 1); safe(r);
  });
}
test('connection input rejects query/SSL/options overrides, malformed URLs and missing identity', async () => {
  for (const value of [`${URL}?options=-c%20default_transaction_read_only=off`, `${URL}?host=other`, `${URL}?sslmode=no-verify`, `${URL}#${PRIVATE}`, 'postgresql://localhost/db', 'not-a-url', 'http://user:pass@host/db']) {
    const fake = fakeClient(); const r = await preflight.inspectDatabase(value, fake); assert.equal(r.status, 'unknown'); assert.equal(fake.calls.length, 0); safe(r);
  }
});
test('password callback cannot fall back to ambient PGPASSWORD or pgpass; remote TLS verifies certificates', () => {
  const c = preflight.connectionConfig('postgresql://test@127.0.0.1/test'); assert.equal(typeof c.password, 'function'); assert.equal(c.password(), '');
  assert.deepEqual(preflight.connectionConfig('postgresql://test:pass@remote.example/test').ssl, { rejectUnauthorized: true });
});
test('default and missing explicit target never import pg or connect; arbitrary arguments are redacted', async () => {
  const fake = fakeClient(); let result = await preflight.main([], { DATABASE_URL: URL, SIGNAL_OPERATOR_FEED_TOKEN: PRIVATE }, fake);
  assert.equal(result.exitCode, 2); assert.equal(fake.calls.length, 0); safe(result);
  result = await preflight.main(['--test-database'], { DATABASE_URL: URL }, fake); assert.equal(result.exitCode, 2); assert.equal(fake.calls.length, 0);
  result = await preflight.main([URL], {}, fake); assert.equal(result.exitCode, 2); safe(result);
});
test('CLI exit codes distinguish inspected ready/blocked/unknown, rollout always remains unknown', async () => {
  for (const [catalog, expected] of [[fixture(), 0], [{ tables: [], columns: [], constraints: [], indexes: [], triggers: [] }, 1]]) {
    const r = await preflight.main(['--test-database'], { PREFLIGHT_TEST_DATABASE_URL: URL }, fakeClient({ catalog }));
    assert.equal(r.exitCode, expected); assert.equal(JSON.parse(r.output).rollout.status, 'unknown'); safe(r);
  }
});
test('token diagnostics check only this process and never claim equality', async () => {
  for (const [token, status] of [[undefined, 'blocked'], ['x'.repeat(31), 'blocked'], [PRIVATE, 'ready'], ['x'.repeat(32), 'ready']]) {
    const r = preflight.localConfig({ SIGNAL_OPERATOR_FEED_TOKEN: token }); assert.equal(r.status, status); safe(r);
  }
  const r = await preflight.main(['--local-config'], { SIGNAL_OPERATOR_FEED_TOKEN: PRIVATE }); safe(r);
  assert.equal(JSON.parse(r.output).rollout.checks.find(c => c.id === 'signal.paired_configuration').status, 'unknown');
});
test('standalone dry-run CLI works without dependencies, dotenv, bootstrap or network imports', () => {
  const script = path.resolve(__dirname, '../scripts/rolloutPreflight.js');
  // Import without launching the app; verify cache contains only this script.
  const output = execFileSync(process.execPath, ['-e', `const m=require('node:module');const load=m._load;m._load=(id,...rest)=>{if(!id.endsWith('rolloutPreflight.js'))throw Error('Import forbidden');return load(id,...rest)};require(${JSON.stringify(script)}).main().then(r=>process.stdout.write(r.output));`], { encoding: 'utf8' });
  assert.equal(JSON.parse(output).schema.status, 'unknown');
});
