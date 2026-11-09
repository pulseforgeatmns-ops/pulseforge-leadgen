'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { TABLES, inspectShadowSchema } = require('../packages/decision-service/shadowSchema');
const { observedRoute } = require('../packages/decision-service/observedRoute');

test('every stored projection column is provided by migration history', () => {
  const sql = fs.readdirSync('migrations').filter(f => /(jev.*shadow|decision-shadow)/.test(f)).map(f => fs.readFileSync(`migrations/${f}`, 'utf8')).join('\n');
  for (const fields of Object.values(TABLES)) for (const field of fields) {
    assert.match(sql, new RegExp(`\\b${field}\\s+(?:TEXT|UUID|JSONB|BOOLEAN|INTEGER|NUMERIC|DOUBLE|TIMESTAMPTZ|BIGINT)`, 'i'), field);
  }
});
test('schema preflight reports exact missing columns and never writes', async () => {
  const rows = Object.entries(TABLES).flatMap(([table_name, fields]) => fields.map(column_name => ({table_name, column_name}))).filter(r => r.column_name !== 'mission_stage');
  const db = { query: async (sql) => { assert.match(sql, /^SELECT/); return {rows}; } };
  const report = await inspectShadowSchema(db);
  assert.equal(report.ready, false);
  assert.deepEqual(report.missing.decision_shadow_events, ['mission_stage']);
  assert.deepEqual(report.missing.decision_shadow_evidence, []);
});
test('operational inspection owner is comparable to JEV inspection, analysis remains intelligence', () => {
  assert.equal(observedRoute({ route: 'intelligence', workspaceOwnership: {owner: 'operational_inspection'} }).route, 'inspection');
  assert.equal(observedRoute({ route: 'intelligence', workspaceOwnership: {owner: 'operational_analysis'} }).route, 'intelligence');
});
