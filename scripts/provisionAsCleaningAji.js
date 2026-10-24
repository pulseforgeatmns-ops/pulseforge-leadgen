'use strict';

/**
 * SPEC-096 — Provision AS Cleaning Co. + bind Aji as client-role user.
 *
 * Uses existing clients/users schema only. Credentials come from env — never
 * logged or printed.
 *
 *   AS_CLEANING_CLIENT_ID   (default: resolved from slug as-cleaning / id 11)
 *   AJI_EMAIL               (required)
 *   AJI_PASSWORD            (required)
 *   AJI_NAME                (default: Aji)
 *
 * Usage: node scripts/provisionAsCleaningAji.js
 */

require('dotenv').config();

const { bcrypt, ensureUsersTable } = require('../middleware/auth');
const {
  ensureClientArchitecture,
  normalizeClientId,
} = require('../utils/clientContext');
const pool = require('../db');

const AS_CLEANING_SLUG = 'as-cleaning';
const ANCHOR_CLIENT_ID = 10;

async function resolveAsCleaningClientId() {
  const preferred = process.env.AS_CLEANING_CLIENT_ID
    ? normalizeClientId(process.env.AS_CLEANING_CLIENT_ID)
    : null;
  if (preferred && preferred === ANCHOR_CLIENT_ID) {
    throw new Error('Refusing to bind AS Cleaning to Anchor client_id=10');
  }

  if (preferred) {
    const byId = await pool.query(
      `SELECT id, name, slug FROM clients WHERE id = $1`,
      [preferred]
    );
    if (byId.rows[0]) return byId.rows[0];
  }

  const bySlug = await pool.query(
    `SELECT id, name, slug FROM clients WHERE slug = $1`,
    [AS_CLEANING_SLUG]
  );
  if (bySlug.rows[0]) return bySlug.rows[0];

  const byId11 = await pool.query(
    `SELECT id, name, slug FROM clients WHERE id = 11`
  );
  if (byId11.rows[0]) return byId11.rows[0];

  throw new Error(
    'AS Cleaning client not found — run ensureClientArchitecture / server boot first'
  );
}

async function main() {
  const email = String(process.env.AJI_EMAIL || '')
    .toLowerCase()
    .trim();
  const password = String(process.env.AJI_PASSWORD || '');
  const name = String(process.env.AJI_NAME || 'Aji').trim() || 'Aji';

  if (!email || !password) {
    console.error(
      'Set AJI_EMAIL and AJI_PASSWORD in the environment (credentials are never printed).'
    );
    process.exit(1);
  }

  await ensureClientArchitecture();
  await ensureUsersTable();

  const client = await resolveAsCleaningClientId();
  if (Number(client.id) === ANCHOR_CLIENT_ID) {
    throw new Error('Refusing to bind Aji to Anchor Cleaning (client_id=10)');
  }

  const hash = await bcrypt.hash(password, 12);
  const existing = await pool.query(
    `SELECT id, role, client_id FROM users WHERE email = $1`,
    [email]
  );

  let userId;
  if (existing.rows[0]) {
    const updated = await pool.query(
      `UPDATE users
       SET name = $1,
           password_hash = $2,
           role = 'client',
           client_id = $3,
           active = true
       WHERE email = $4
       RETURNING id, role, client_id`,
      [name, hash, client.id, email]
    );
    userId = updated.rows[0].id;
  } else {
    const inserted = await pool.query(
      `INSERT INTO users (name, email, password_hash, role, client_id, active)
       VALUES ($1, $2, $3, 'client', $4, true)
       RETURNING id, role, client_id`,
      [name, email, hash, client.id]
    );
    userId = inserted.rows[0].id;
  }

  // Safe summary only — no password, hash, or token material.
  console.log(
    JSON.stringify(
      {
        ok: true,
        client: {
          id: client.id,
          name: client.name,
          slug: client.slug,
        },
        user: {
          id: userId,
          email,
          role: 'client',
          client_id: client.id,
        },
        anchorIsolated: Number(client.id) !== ANCHOR_CLIENT_ID,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err && err.message ? err.message : err);
  process.exit(1);
});
