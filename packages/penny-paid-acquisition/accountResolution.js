'use strict';

/**
 * SPEC-252 — Tenant-scoped ad account resolution.
 */

const { UNAVAILABLE_REASON } = require('./types');

async function ensureAdAccountsSchema(pool) {
  if (!pool || typeof pool.query !== 'function') return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ad_accounts (
      id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
      company_id    UUID         REFERENCES companies(id),
      client_id     INTEGER      REFERENCES clients(id),
      platform      TEXT         NOT NULL,
      account_id    TEXT         NOT NULL,
      access_token  TEXT,
      refresh_token TEXT,
      token_expiry  TIMESTAMPTZ,
      is_active     BOOLEAN      DEFAULT true,
      created_at    TIMESTAMPTZ  DEFAULT NOW()
    )
  `);
  await pool.query(`
    ALTER TABLE ad_accounts
    ADD COLUMN IF NOT EXISTS client_id INTEGER REFERENCES clients(id)
  `);
}

/**
 * Resolve active ad accounts strictly for the supplied client/tenant.
 * Never falls back to another client.
 *
 * @param {object} input
 * @param {number|string} input.clientId
 * @param {string} [input.platform]
 * @param {import('pg').Pool} [input.pool]
 * @param {Function} [input.queryAccounts]
 * @returns {Promise<object[]>}
 */
async function resolveAdAccountsForClient(input = {}) {
  const clientId = Number(input.clientId);
  if (!Number.isInteger(clientId) || clientId <= 0) {
    return [];
  }

  if (typeof input.queryAccounts === 'function') {
    return input.queryAccounts({ clientId, platform: input.platform || null });
  }

  const pool = input.pool;
  if (!pool || typeof pool.query !== 'function') {
    return [];
  }

  await ensureAdAccountsSchema(pool);

  const params = [clientId];
  let platformClause = '';
  if (input.platform) {
    params.push(input.platform);
    platformClause = 'AND a.platform = $2';
  }

  const res = await pool.query(`
    SELECT
      a.id,
      a.company_id,
      a.client_id,
      a.platform,
      a.account_id,
      a.access_token,
      a.refresh_token,
      a.token_expiry,
      a.is_active,
      c.name AS company_name
    FROM ad_accounts a
    LEFT JOIN companies c ON c.id = a.company_id
    WHERE a.is_active = true
      AND (
        a.client_id = $1
        OR (a.client_id IS NULL AND c.client_id = $1)
      )
      ${platformClause}
    ORDER BY a.platform, a.created_at
  `, params);

  return res.rows.map((row) => sanitizeAccountRow(row));
}

function sanitizeAccountRow(row) {
  if (!row) return row;
  return {
    id: row.id,
    company_id: row.company_id,
    client_id: row.client_id,
    platform: row.platform,
    account_id: row.account_id,
    access_token: row.access_token,
    refresh_token: row.refresh_token,
    token_expiry: row.token_expiry,
    is_active: row.is_active,
    company_name: row.company_name || null,
  };
}

function accountUnavailableReason(account, platform) {
  if (!account) return UNAVAILABLE_REASON.NO_LINKED_ACCOUNT;
  if (platform === 'google_ads' && !account.refresh_token) {
    return UNAVAILABLE_REASON.MISSING_CREDENTIALS;
  }
  if (platform === 'meta_ads' && !account.access_token) {
    return UNAVAILABLE_REASON.MISSING_CREDENTIALS;
  }
  return null;
}

module.exports = {
  ensureAdAccountsSchema,
  resolveAdAccountsForClient,
  sanitizeAccountRow,
  accountUnavailableReason,
};
