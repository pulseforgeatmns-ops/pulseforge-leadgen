'use strict';

/**
 * SPEC-065 — Persist deterministic market observations from market_emails.
 * Fail-soft when called from ingestion. Idempotent upserts.
 */

const defaultPool = require('../db');
const { EXTRACTOR, extractMarketEvidence } = require('../utils/marketEvidenceExtract');

async function loadEmailRow(db, emailId) {
  const result = await db.query(
    `SELECT
       e.id,
       e.company_id,
       e.subject,
       e.body_text,
       e.body_html,
       e.from_name,
       e.from_email,
       e.headers,
       e.links,
       e.received_at,
       c.name AS company_name,
       c.domain AS company_domain
     FROM market_emails e
     JOIN market_companies c ON c.id = e.company_id
     WHERE e.id = $1
     LIMIT 1`,
    [emailId]
  );
  return result.rows[0] || null;
}

async function sequencePositionForEmail(db, companyId, emailId, receivedAt) {
  const result = await db.query(
    `SELECT COUNT(*)::int AS position
       FROM market_emails
      WHERE company_id = $1
        AND (
          received_at < $2
          OR (received_at = $2 AND id <= $3::uuid)
        )`,
    [companyId, receivedAt, emailId]
  );
  return result.rows[0]?.position || 1;
}

async function upsertObservations(db, emailId, companyId, observations) {
  let written = 0;
  for (const row of observations) {
    await db.query(
      `INSERT INTO market_observations (
         email_id, company_id, category, field, value_text, value_json,
         evidence_quote, evidence_path, extractor, extracted_at
       ) VALUES (
         $1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, NOW()
       )
       ON CONFLICT (email_id, category, field, value_text) DO UPDATE SET
         value_json = EXCLUDED.value_json,
         evidence_quote = EXCLUDED.evidence_quote,
         evidence_path = EXCLUDED.evidence_path,
         extractor = EXCLUDED.extractor,
         extracted_at = NOW()`,
      [
        emailId,
        companyId,
        row.category,
        row.field,
        row.valueText,
        JSON.stringify(row.valueJson || {}),
        row.evidenceQuote || '',
        row.evidencePath || 'body_text',
        row.extractor || EXTRACTOR,
      ]
    );
    written += 1;
  }
  return written;
}

/**
 * Extract and persist observations for one email.
 * @returns {{ ok: boolean, emailId: string, observations: number, skipped?: boolean, error?: string }}
 */
async function extractEmailEvidence(emailId, { pool = defaultPool, rebuildProfile = false } = {}) {
  const row = await loadEmailRow(pool, emailId);
  if (!row) {
    return { ok: false, emailId, observations: 0, skipped: true, error: 'email_not_found' };
  }

  const sequencePosition = await sequencePositionForEmail(
    pool,
    row.company_id,
    row.id,
    row.received_at
  );

  const observations = extractMarketEvidence(
    {
      subject: row.subject,
      bodyText: row.body_text,
      bodyHtml: row.body_html,
      fromName: row.from_name,
      fromEmail: row.from_email,
      headers: row.headers,
      links: row.links,
      receivedAt: row.received_at,
    },
    {
      companyName: row.company_name,
      companyDomain: row.company_domain,
      sequencePosition,
    }
  );

  const written = await upsertObservations(pool, row.id, row.company_id, observations);

  if (rebuildProfile) {
    const { rebuildCompanyProfile } = require('./marketIntelligenceQuery');
    await rebuildCompanyProfile(row.company_id, { pool });
  }

  return { ok: true, emailId: row.id, companyId: row.company_id, observations: written };
}

/**
 * Fail-soft wrapper for post-import hook — never throws into ingestion.
 */
async function safeExtractEmailEvidence(emailId, options = {}) {
  try {
    return await extractEmailEvidence(emailId, options);
  } catch (err) {
    console.error('[market-intel-extract] fail-soft', emailId, err && err.message);
    return {
      ok: false,
      emailId,
      observations: 0,
      error: err && err.message ? String(err.message) : 'extract_failed',
    };
  }
}

/**
 * Backfill extraction across market_emails.
 */
async function extractMarketIntelligence(options = {}) {
  const started = Date.now();
  const pool = options.pool || defaultPool;
  const dryRun = Boolean(options.dryRun);
  const limit = Math.max(1, Number(options.limit) || 1000);
  const companyId = options.companyId || null;
  const emailId = options.emailId || null;
  const rebuildProfiles = options.rebuildProfiles !== false;

  const params = [];
  let where = 'TRUE';
  if (emailId) {
    params.push(emailId);
    where += ` AND e.id = $${params.length}`;
  }
  if (companyId) {
    params.push(companyId);
    where += ` AND e.company_id = $${params.length}`;
  }
  params.push(limit);

  const emails = await pool.query(
    `SELECT e.id, e.company_id
       FROM market_emails e
      WHERE ${where}
      ORDER BY e.received_at ASC, e.imported_at ASC
      LIMIT $${params.length}`,
    params
  );

  const stats = {
    ok: true,
    dryRun,
    scanned: emails.rows.length,
    extracted: 0,
    observations: 0,
    failed: 0,
    profilesRebuilt: 0,
    companyIds: new Set(),
  };

  if (dryRun) {
    for (const row of emails.rows) {
      const full = await loadEmailRow(pool, row.id);
      const sequencePosition = await sequencePositionForEmail(
        pool,
        full.company_id,
        full.id,
        full.received_at
      );
      const observations = extractMarketEvidence(
        {
          subject: full.subject,
          bodyText: full.body_text,
          bodyHtml: full.body_html,
          fromName: full.from_name,
          fromEmail: full.from_email,
          headers: full.headers,
          links: full.links,
          receivedAt: full.received_at,
        },
        {
          companyName: full.company_name,
          companyDomain: full.company_domain,
          sequencePosition,
        }
      );
      stats.extracted += 1;
      stats.observations += observations.length;
      stats.companyIds.add(row.company_id);
    }
  } else {
    for (const row of emails.rows) {
      const result = await extractEmailEvidence(row.id, { pool, rebuildProfile: false });
      if (!result.ok) {
        stats.failed += 1;
        continue;
      }
      stats.extracted += 1;
      stats.observations += result.observations;
      stats.companyIds.add(result.companyId);
    }

    if (rebuildProfiles && stats.companyIds.size) {
      const { rebuildCompanyProfile } = require('./marketIntelligenceQuery');
      for (const id of stats.companyIds) {
        await rebuildCompanyProfile(id, { pool });
        stats.profilesRebuilt += 1;
      }
    }
  }

  const durationMs = Date.now() - started;
  return {
    ok: true,
    dryRun,
    scanned: stats.scanned,
    extracted: stats.extracted,
    observations: stats.observations,
    failed: stats.failed,
    profilesRebuilt: stats.profilesRebuilt,
    companies: stats.companyIds.size,
    durationMs,
    durationSeconds: Math.round(durationMs / 1000),
  };
}

function formatExtractReport(result) {
  const lines = [
    `Scanned: ${Number(result.scanned || 0).toLocaleString('en-US')}`,
    `Extracted: ${Number(result.extracted || 0).toLocaleString('en-US')}`,
    `Observations: ${Number(result.observations || 0).toLocaleString('en-US')}`,
    `Failed: ${Number(result.failed || 0).toLocaleString('en-US')}`,
    `Profiles rebuilt: ${Number(result.profilesRebuilt || 0).toLocaleString('en-US')}`,
    `Duration: ${result.durationSeconds ?? Math.round((result.durationMs || 0) / 1000)}s`,
  ];
  if (result.dryRun) lines.unshift('Mode: dry-run');
  return lines.join('\n');
}

module.exports = {
  extractEmailEvidence,
  extractMarketIntelligence,
  formatExtractReport,
  loadEmailRow,
  safeExtractEmailEvidence,
  sequencePositionForEmail,
  upsertObservations,
};
