'use strict';

require('dotenv').config();

const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const pool = require('../db');

const ROOT = path.join(__dirname, '..');
const FORWARD = path.join(ROOT, 'migrations', '2026-07-18-anchor-phone-setter-immediate-cash-v1.sql');
const ROLLBACK = path.join(ROOT, 'migrations', '2026-07-18-anchor-phone-setter-immediate-cash-v1.rollback.sql');

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function deployedRevision() {
  try { return execFileSync('git', ['rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8' }).trim(); } catch (_) { return null; }
}

async function preflight(db = pool) {
  const client = await db.query(`
    SELECT id,active,enabled_agents,autosend_enabled
    FROM clients WHERE id=10
  `);
  const flags = await db.query(`
    SELECT revenue_schema_enabled,revenue_operator_reads_enabled,revenue_operator_writes_enabled,
      revenue_max_reads_enabled,revenue_followup_recommendations_enabled
    FROM revenue_feature_flags WHERE client_id=10
  `).catch(error => error.code === '42P01' ? { rows: [] } : Promise.reject(error));
  // Before the forward migration, `campaigns` deliberately does not exist on
  // some production schemas. That is a failed readiness check, not a broken
  // preflight: callers need the JSON report to attach to an authorization.
  const campaign = await db.query(`
    SELECT campaign_key,status,metadata FROM campaigns
    WHERE client_id=10 AND campaign_key='anchor_phone_setter_immediate_cash_v1'
  `).catch(error => ['42P01', '42703'].includes(error.code) ? { rows: [] } : Promise.reject(error));
  const anchor = client.rows[0] || null;
  const revenue = flags.rows[0] || {};
  const report = {
    revision: deployedRevision(),
    generated_at: new Date().toISOString(),
    artifacts: { forward_sha256: sha256(FORWARD), rollback_sha256: sha256(ROLLBACK) },
    checks: {
      anchor_client_active: Boolean(anchor?.active),
      anchor_scout_only: Array.isArray(anchor?.enabled_agents) && anchor.enabled_agents.length === 1 && anchor.enabled_agents[0] === 'scout',
      anchor_autosend_disabled: anchor?.autosend_enabled === false,
      revenue_flags_all_false: Object.values(revenue).every(value => value !== true),
      anchor_campaign_paused: campaign.rows[0]?.status === 'paused',
      campaign_external_sends_disabled: campaign.rows[0]?.metadata?.external_sends_enabled === false,
      campaign_revenue_writes_disabled: campaign.rows[0]?.metadata?.revenue_writes_enabled === false,
    },
  };
  report.ok = Object.values(report.checks).every(Boolean);
  return report;
}

if (require.main === module) {
  preflight().then(async report => {
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    await pool.end();
    process.exitCode = report.ok ? 0 : 2;
  }).catch(async error => {
    process.stderr.write(`${error.stack || error.message}\n`);
    await pool.end().catch(() => {});
    process.exitCode = 1;
  });
}

module.exports = { deployedRevision, preflight, sha256 };
