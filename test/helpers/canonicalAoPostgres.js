'use strict';

const fs = require('node:fs');
const path = require('node:path');

// Deliberately minimal prerequisites. Applying the actual AO migration here makes
// tests exercise its activity columns, constraints and tenant/prospect FK rather
// than smuggling invented activities onto a prospect returned by a test adapter.
async function applyCanonicalAoFixture(db) {
  await db.query(`
    CREATE TABLE clients (id integer PRIMARY KEY);
    CREATE TABLE users (id integer PRIMARY KEY);
    CREATE TABLE companies (id uuid PRIMARY KEY, name text);
    CREATE TABLE prospects (
      id uuid PRIMARY KEY, client_id integer NOT NULL REFERENCES clients(id),
      company_id uuid, assigned_ao_id integer, status text, acquisition_metadata jsonb,
      UNIQUE (client_id, id)
    );
    CREATE TABLE ao_prospect_updates (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), outcome_type text);
    INSERT INTO clients VALUES (1), (2);
    INSERT INTO users VALUES (10), (11);
  `);
  const migrationDirectory = path.join(__dirname, '..', '..', 'migrations');
  await db.query(fs.readFileSync(path.join(migrationDirectory, '2026-09-28-ao-crm-001.sql'), 'utf8'));
  for (const name of ['state-ingestion', 'decision-execution', 'attention']) {
    await db.query(fs.readFileSync(path.join(migrationDirectory,
      `2026-10-05-max-reliability-${name}.sql`), 'utf8'));
  }
}

async function seedAoActivity(db, expectation, {
  activityType = 'outcome_logged', outcome = 'booked_walkthrough',
  metadata = { internal_outcome: outcome }, createdAt = '2026-10-05T08:00:00.000Z',
  aoId = expectation.ao_id, notes = null,
} = {}) {
  const { rows } = await db.query(`
    INSERT INTO ao_prospect_activity
      (prospect_id, tenant_id, ao_id, activity_type, outcome, notes, metadata, created_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8) RETURNING *
  `, [expectation.prospect_id, expectation.client_id, aoId, activityType, outcome,
    notes, JSON.stringify(metadata), createdAt]);
  return rows[0];
}

module.exports = { applyCanonicalAoFixture, seedAoActivity };
