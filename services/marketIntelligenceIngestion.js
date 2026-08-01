'use strict';

/**
 * SPEC-061 Market Intelligence Ingestion (v0.1)
 * Ingestion only — no recommendations, scoring, or strategy generation.
 */

const defaultPool = require('../db');
const { fetchLabeledMessages } = require('../utils/gmailClient');
const { buildLabelQuery, parseGmailMessage } = require('../utils/marketEmailParse');
const { resolveMarketCompany } = require('../utils/marketCompanyResolve');

const UNKNOWN_COMPANY_NAME = 'Unknown Company';

function emptyStats() {
  return {
    imported: 0,
    skipped: 0,
    duplicates: 0,
    unknownCompany: 0,
    fetched: 0,
    dryRun: false,
  };
}

async function ensureUnknownCompany(db) {
  const existing = await db.query(
    `SELECT id FROM market_companies WHERE is_unknown = TRUE LIMIT 1`
  );
  if (existing.rows[0]) return existing.rows[0].id;

  const inserted = await db.query(
    `INSERT INTO market_companies (name, is_unknown)
     VALUES ($1, TRUE)
     RETURNING id`,
    [UNKNOWN_COMPANY_NAME]
  );
  return inserted.rows[0].id;
}

async function findOrCreateCompany(db, resolution) {
  if (resolution.isUnknown || !resolution.domain) {
    return { companyId: await ensureUnknownCompany(db), isUnknown: true };
  }

  const byDomain = await db.query(
    `SELECT id, is_unknown FROM market_companies
      WHERE domain IS NOT NULL AND LOWER(domain) = LOWER($1)
      LIMIT 1`,
    [resolution.domain]
  );
  if (byDomain.rows[0]) {
    return { companyId: byDomain.rows[0].id, isUnknown: Boolean(byDomain.rows[0].is_unknown) };
  }

  try {
    const inserted = await db.query(
      `INSERT INTO market_companies (domain, name, is_unknown)
       VALUES ($1, $2, FALSE)
       RETURNING id`,
      [resolution.domain, resolution.name]
    );
    return { companyId: inserted.rows[0].id, isUnknown: false };
  } catch (err) {
    if (err.code !== '23505') throw err;
    const raced = await db.query(
      `SELECT id, is_unknown FROM market_companies
        WHERE domain IS NOT NULL AND LOWER(domain) = LOWER($1)
        LIMIT 1`,
      [resolution.domain]
    );
    if (!raced.rows[0]) throw err;
    return { companyId: raced.rows[0].id, isUnknown: Boolean(raced.rows[0].is_unknown) };
  }
}

async function findExistingDuplicate(db, { gmailId, messageId }) {
  const byGmail = await db.query(
    `SELECT id FROM market_emails WHERE gmail_id = $1 LIMIT 1`,
    [gmailId]
  );
  if (byGmail.rows[0]) return { id: byGmail.rows[0].id, reason: 'gmail_id' };

  if (messageId) {
    const byMessage = await db.query(
      `SELECT id FROM market_emails
        WHERE message_id IS NOT NULL AND LOWER(message_id) = LOWER($1)
        LIMIT 1`,
      [messageId]
    );
    if (byMessage.rows[0]) return { id: byMessage.rows[0].id, reason: 'message_id' };
  }

  return null;
}

async function insertMarketEmail(db, email, companyId) {
  const result = await db.query(
    `INSERT INTO market_emails (
       company_id, gmail_id, thread_id, message_id, subject,
       body_text, body_html, from_name, from_email, headers, links, attachments,
       received_at, sent_at, imported_at
     ) VALUES (
       $1, $2, $3, $4, $5,
       $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12::jsonb,
       $13, $14, $15
     )
     ON CONFLICT (gmail_id) DO NOTHING
     RETURNING id`,
    [
      companyId,
      email.gmailId,
      email.threadId,
      email.messageId,
      email.subject,
      email.bodyText,
      email.bodyHtml || null,
      email.fromName || null,
      email.fromEmail || '',
      JSON.stringify(email.headers || {}),
      JSON.stringify(email.links || []),
      JSON.stringify(email.attachments || []),
      email.receivedAt,
      email.sentAt || null,
      email.importedAt,
    ]
  );
  return result.rows[0]?.id || null;
}

/**
 * Ordered chronological touches for a market company. No intelligence — chronology only.
 */
async function getCompanyTimeline(companyId, { pool = defaultPool, limit = 500 } = {}) {
  const result = await pool.query(
    `SELECT
       e.id,
       e.gmail_id,
       e.thread_id,
       e.subject,
       e.from_email,
       e.from_name,
       e.received_at,
       e.sent_at,
       e.links,
       c.name AS company_name,
       c.domain AS company_domain
     FROM market_emails e
     JOIN market_companies c ON c.id = e.company_id
     WHERE e.company_id = $1
     ORDER BY e.received_at ASC, e.imported_at ASC
     LIMIT $2`,
    [companyId, limit]
  );

  return result.rows.map((row, index) => ({
    touch: index + 1,
    id: row.id,
    gmailId: row.gmail_id,
    threadId: row.thread_id,
    subject: row.subject,
    fromEmail: row.from_email,
    fromName: row.from_name,
    receivedAt: row.received_at,
    sentAt: row.sent_at,
    links: row.links,
    companyName: row.company_name,
    companyDomain: row.company_domain,
  }));
}

async function updateSyncState(db, { label, days, stats }) {
  await db.query(
    `INSERT INTO market_intel_sync_state (id, label, days, last_synced_at, last_run_stats, updated_at)
     VALUES ('default', $1, $2, NOW(), $3::jsonb, NOW())
     ON CONFLICT (id) DO UPDATE SET
       label = EXCLUDED.label,
       days = EXCLUDED.days,
       last_synced_at = EXCLUDED.last_synced_at,
       last_run_stats = EXCLUDED.last_run_stats,
       updated_at = NOW()`,
    [label, days, JSON.stringify(stats)]
  );
}

/**
 * Import labeled marketing emails into market_emails.
 *
 * @param {object} options
 * @param {number} [options.days=365]
 * @param {string} [options.label='MARKET_INTEL']
 * @param {number} [options.limit=1000]
 * @param {boolean} [options.dryRun=false]
 * @param {object} [options.pool]
 * @param {Function} [options.fetchMessages] — inject for tests
 * @param {Array} [options.messages] — pre-fetched Gmail message payloads
 */
async function importMarketIntelligence(options = {}) {
  const started = Date.now();
  const days = Math.max(1, Number(options.days) || 365);
  const label = String(options.label || 'MARKET_INTEL').trim() || 'MARKET_INTEL';
  const limit = Math.max(1, Number(options.limit) || 1000);
  const dryRun = Boolean(options.dryRun);
  const pool = options.pool || defaultPool;
  const stats = emptyStats();
  stats.dryRun = dryRun;

  const query = buildLabelQuery({ label, days });
  let rawMessages = options.messages;
  if (!rawMessages) {
    const fetchFn = options.fetchMessages || fetchLabeledMessages;
    rawMessages = await fetchFn({ query, limit });
  }
  stats.fetched = rawMessages.length;

  const importedPreview = [];

  for (const raw of rawMessages) {
    const email = parseGmailMessage(raw);
    if (!email.gmailId) {
      stats.skipped += 1;
      continue;
    }

    const duplicate = await findExistingDuplicate(pool, email);
    if (duplicate) {
      stats.duplicates += 1;
      continue;
    }

    const resolution = resolveMarketCompany({ fromEmail: email.fromEmail });
    if (resolution.isUnknown) stats.unknownCompany += 1;

    if (dryRun) {
      stats.imported += 1;
      importedPreview.push({
        gmailId: email.gmailId,
        subject: email.subject,
        fromEmail: email.fromEmail,
        company: resolution.name,
        domain: resolution.domain,
        receivedAt: email.receivedAt,
      });
      continue;
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const recheck = await findExistingDuplicate(client, email);
      if (recheck) {
        await client.query('ROLLBACK');
        stats.duplicates += 1;
        continue;
      }

      const { companyId } = await findOrCreateCompany(client, resolution);
      const insertedId = await insertMarketEmail(client, email, companyId);
      if (!insertedId) {
        await client.query('ROLLBACK');
        stats.duplicates += 1;
        continue;
      }

      await client.query('COMMIT');
      stats.imported += 1;
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch (_rollbackErr) { /* ignore */ }
      if (err.code === '23505') {
        stats.duplicates += 1;
        continue;
      }
      throw err;
    } finally {
      client.release();
    }
  }

  const durationMs = Date.now() - started;
  const summary = {
    ...stats,
    label,
    days,
    limit,
    query,
    durationMs,
    durationSeconds: Math.round(durationMs / 1000),
  };

  if (!dryRun) {
    await updateSyncState(pool, { label, days, stats: summary });
  }

  return {
    ok: true,
    ...summary,
    preview: dryRun ? importedPreview : undefined,
  };
}

function formatImportReport(result) {
  const lines = [
    `Imported: ${Number(result.imported || 0).toLocaleString('en-US')}`,
    `Skipped: ${Number(result.skipped || 0).toLocaleString('en-US')}`,
    `Duplicates: ${Number(result.duplicates || 0).toLocaleString('en-US')}`,
    `Unknown Company: ${Number(result.unknownCompany || 0).toLocaleString('en-US')}`,
    `Duration: ${result.durationSeconds ?? Math.round((result.durationMs || 0) / 1000)}s`,
  ];
  if (result.dryRun) lines.unshift('Mode: dry-run');
  return lines.join('\n');
}

module.exports = {
  UNKNOWN_COMPANY_NAME,
  ensureUnknownCompany,
  findExistingDuplicate,
  findOrCreateCompany,
  formatImportReport,
  getCompanyTimeline,
  importMarketIntelligence,
  insertMarketEmail,
};
