'use strict';

require('dotenv').config();
const fs = require('node:fs');
const path = require('node:path');
const pool = require('../db');

const MIGRATIONS = Object.freeze([
  '2026-07-15-max-prospect-orchestration-v1.sql',
  '2026-07-15-max-prospect-orchestration-phase2.sql',
  '2026-07-15-max-prospect-orchestration-phase2_5.sql',
]);

function migrationSql(filename) {
  return fs.readFileSync(path.join(__dirname, '..', 'migrations', filename), 'utf8')
    .replace(/^\s*BEGIN;\s*$/gim, '')
    .replace(/^\s*COMMIT;\s*$/gim, '');
}

function inspectMigrationSafety(sql, filename) {
  const violations = [];
  if (/UPDATE\s+prospects\s+SET\s+[\s\S]{0,120}\bstatus\s*=/i.test(sql)) violations.push('mutates prospects.status');
  if (/\b(send|enroll|schedule|retry_enrichment|agent_actions)\b/i.test(sql.replace(/^\s*--.*$/gm, ''))) {
    violations.push('contains a prospect-action token in executable SQL');
  }
  return { filename, safe: violations.length === 0, violations };
}

async function validateMigrations(db = pool) {
  const safety = MIGRATIONS.map(filename => inspectMigrationSafety(migrationSql(filename), filename));
  if (safety.some(item => !item.safe)) return { valid: false, migration_order: MIGRATIONS, safety };
  const client = await db.connect();
  const schema = `max_validation_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
  try {
    await client.query('BEGIN');
    await client.query(`CREATE SCHEMA ${schema}`);
    await client.query(`SET LOCAL search_path TO ${schema}, public, pg_catalog`);
    await client.query(`
      CREATE TABLE clients (id INTEGER PRIMARY KEY);
      CREATE TABLE companies (id UUID PRIMARY KEY, client_id INTEGER REFERENCES clients(id));
      CREATE TABLE users (id INTEGER PRIMARY KEY);
      CREATE TABLE prospects (
        id UUID PRIMARY KEY,
        client_id INTEGER NOT NULL REFERENCES clients(id),
        company_id UUID REFERENCES companies(id),
        status TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      INSERT INTO clients (id) VALUES (1);
      INSERT INTO prospects (id, client_id, status) VALUES ('00000000-0000-4000-8000-000000000001', 1, 'cold');
    `);
    const before = await client.query('SELECT id, status FROM prospects ORDER BY id');
    for (const filename of MIGRATIONS) await client.query(migrationSql(filename));
    for (const filename of MIGRATIONS) await client.query(migrationSql(filename));
    const after = await client.query('SELECT id, status FROM prospects ORDER BY id');
    const objects = await client.query(`
      SELECT
        to_regclass('prospect_signal_events') IS NOT NULL AS signals,
        to_regclass('max_decisions') IS NOT NULL AS decisions,
        to_regclass('max_recommendation_reviews') IS NOT NULL AS reviews,
        to_regclass('max_rollout_readiness_config') IS NOT NULL AS rollout_config
    `);
    const invalidFks = await client.query(`
      SELECT conname FROM pg_constraint
      WHERE contype='f' AND connamespace = $1::regnamespace AND NOT convalidated
    `, [schema]);
    const report = {
      valid: JSON.stringify(before.rows) === JSON.stringify(after.rows)
        && Object.values(objects.rows[0]).every(Boolean) && invalidFks.rows.length === 0,
      mode: 'transactional_disposable_schema',
      migration_order: MIGRATIONS,
      rerun_count: 2,
      status_unchanged: JSON.stringify(before.rows) === JSON.stringify(after.rows),
      required_objects: objects.rows[0],
      invalid_foreign_keys: invalidFks.rows.map(row => row.conname),
      safety,
    };
    await client.query('ROLLBACK');
    return report;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    return { valid: false, mode: 'transactional_disposable_schema', migration_order: MIGRATIONS, safety, error: error.message, code: error.code || null };
  } finally {
    client.release();
  }
}

module.exports = { MIGRATIONS, inspectMigrationSafety, migrationSql, validateMigrations };

if (require.main === module) {
  pool.options.connectionTimeoutMillis = 10000;
  pool.options.query_timeout = 30000;
  validateMigrations().then(report => {
    console.log(JSON.stringify(report, null, 2));
    process.exitCode = report.valid ? 0 : 1;
  }).catch(error => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }).finally(() => pool.end());
}
