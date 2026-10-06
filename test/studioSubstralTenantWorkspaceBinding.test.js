'use strict';

/**
 * Studio Substral tenant_workspaces binding — canonical Blueprint approval gate (SPEC-224).
 */

const assert = require('node:assert/strict');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { after, before, describe, it } = require('node:test');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const registrySeed = require('../migrations/2026-09-02-spec-224-production-registry-artifact');
const blueprintAssociation = require('../migrations/2026-09-03-spec-224-blueprint-snapshot-association');
const {
  STUDIO_SUBSTRAL_SLUG,
  ensureStudioSubstralTenant,
} = require('../utils/studioSubstralTenant');
const { ensureClientTenantWorkspaceBinding } = require('../services/tenantWorkspace');

const spec223aMigration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-09-01-spec-223a-canonical-semantic-persistence.sql'),
  'utf8'
);
const cieMigration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-08-06-client-intelligence-engine.sql'),
  'utf8'
);
const tenantWorkspaceMigration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-08-18-client-tenant-workspace.sql'),
  'utf8'
);
const substralWorkspaceBackfill = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-10-06-studio-substral-tenant-workspace.sql'),
  'utf8'
);

describe('Studio Substral tenant workspace binding (canonical authority)', () => {
  let postgres;
  let pool;
  let clientIntelligenceInterview;

  before(async () => {
    postgres = await startDisposablePostgres('substral-tw-');
    pool = new Pool({ connectionString: postgres.connectionString });

    await pool.query(`
      CREATE EXTENSION IF NOT EXISTS pgcrypto;
      CREATE TABLE clients (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        slug TEXT UNIQUE,
        business_name TEXT,
        vertical TEXT,
        email TEXT,
        primary_contact TEXT,
        country TEXT,
        timezone TEXT,
        industry TEXT,
        website TEXT,
        service_area TEXT[],
        verticals TEXT[],
        target_clients TEXT,
        scoring_profile TEXT,
        enabled_agents TEXT[],
        active BOOLEAN DEFAULT true,
        notes TEXT,
        brand_voice TEXT,
        never_say TEXT,
        lead_with TEXT,
        sender_name TEXT,
        sender_email TEXT,
        sending_domain TEXT
      );
    `);
    await pool.query(tenantWorkspaceMigration);
    await pool.query(cieMigration);
    await pool.query(spec223aMigration);
    await registrySeed.up(pool);
    await blueprintAssociation.up(pool);

    clientIntelligenceInterview = require('../services/clientIntelligenceInterview');
  });

  after(async () => {
    await pool.end();
    await postgres.stop();
  });

  async function insertStudioSubstralClientWithoutWorkspace(clientId = 17) {
    await pool.query(
      `INSERT INTO clients (
         id, name, slug, business_name, vertical, email, scoring_profile, enabled_agents, active
       ) VALUES ($1, 'Studio Substral', $2, 'Studio Substral', 'web_design',
         'hello@studiosubstral.com', 'studio_substral', ARRAY['scout','max','paige'], true)
       ON CONFLICT (id) DO NOTHING`,
      [clientId, STUDIO_SUBSTRAL_SLUG]
    );
  }

  async function insertSession(clientId, normalizedFacts) {
    const result = await pool.query(
      `INSERT INTO cie_interview_sessions (client_id, status, interview_state)
       VALUES ($1,'CLIENT_REVIEW',$2::jsonb) RETURNING *`,
      [clientId, JSON.stringify({ normalizedFacts })]
    );
    return result.rows[0];
  }

  async function insertEvidence(clientId, sessionId) {
    return clientIntelligenceInterview.createPostgresStore(pool).insertEvidence({
      id: crypto.randomUUID(),
      client_id: clientId,
      session_id: sessionId,
      source: 'interview',
      category: 'identity',
      statement: 'Studio Substral evidence for canonical commit.',
      confidence: 0.9,
      type: 'EXPLICIT',
    });
  }

  async function insertBlueprint(clientId, sessionId) {
    const id = crypto.randomUUID();
    const result = await pool.query(
      `INSERT INTO cie_business_blueprints (id, client_id, session_id, version, status, sections)
       VALUES ($1,$2,$3,'1.6','in_review','{}'::jsonb) RETURNING *`,
      [id, clientId, sessionId]
    );
    return result.rows[0];
  }

  it('fails Blueprint approval when client 17 has no tenant_workspaces row', async () => {
    const clientId = 17;
    await insertStudioSubstralClientWithoutWorkspace(clientId);
    const session = await insertSession(clientId, { business_name: 'Studio Substral' });
    await insertEvidence(clientId, session.id);
    const blueprint = await insertBlueprint(clientId, session.id);

    await assert.rejects(
      () => clientIntelligenceInterview.approveBlueprint(blueprint.id, { pool }),
      (err) => err.code === 'canonical_commit_failed'
        && /No tenant workspace bound to client 17/i.test(err.message)
    );

    const row = (await pool.query(
      `SELECT status FROM cie_business_blueprints WHERE id = $1`,
      [blueprint.id]
    )).rows[0];
    assert.equal(row.status, 'in_review');
  });

  it('ensureStudioSubstralTenant provisions binding; approval succeeds with canonical authority', async () => {
    const clientId = 17;
    await pool.query(`DELETE FROM tenant_workspaces WHERE client_id = $1`, [clientId]);

    const client = await ensureStudioSubstralTenant(pool);
    assert.equal(Number(client.id), clientId);
    assert.equal(client.slug, STUDIO_SUBSTRAL_SLUG);

    const binding = (await pool.query(
      `SELECT tenant_key, knowledge_namespace FROM tenant_workspaces WHERE client_id = $1`,
      [clientId]
    )).rows[0];
    assert.ok(binding, 'expected tenant_workspaces row');
    assert.equal(binding.tenant_key, STUDIO_SUBSTRAL_SLUG);
    assert.equal(binding.knowledge_namespace, `tenant:${clientId}:knowledge`);

    const session = await insertSession(clientId, { business_name: 'Studio Substral' });
    await insertEvidence(clientId, session.id);
    const blueprint = await insertBlueprint(clientId, session.id);

    const result = await clientIntelligenceInterview.approveBlueprint(blueprint.id, { pool });
    assert.ok(result.canonicalSnapshotId, 'expected canonical snapshot');
    assert.equal(result.blueprint.canonicalSnapshotTenantId, STUDIO_SUBSTRAL_SLUG);

    const approved = await clientIntelligenceInterview.getApprovedClientBlueprint(clientId, { pool });
    assert.ok(approved._canonical_authority, 'approved Blueprint exposes canonical authority');
  });

  it('SQL backfill migration binds studio-substral without disturbing Anchor', async () => {
    await pool.query(
      `INSERT INTO clients (id, name, slug) VALUES (10, 'Anchor Cleaning', 'cleaning-co')
       ON CONFLICT (id) DO NOTHING`
    );
    await pool.query(
      `INSERT INTO tenant_workspaces (client_id, tenant_key, knowledge_namespace, mission_namespace,
         prospect_namespace, outcome_namespace, aim_namespace)
       VALUES (10, 'cleaning-co', 'tenant:10:knowledge', 'tenant:10:mission',
         'tenant:10:prospect', 'tenant:10:outcome', 'tenant:10:aim:cleaning-co')
       ON CONFLICT (client_id) DO NOTHING`
    );

    await insertStudioSubstralClientWithoutWorkspace(17);
    await pool.query(`DELETE FROM tenant_workspaces WHERE client_id = 17`);
    await pool.query(substralWorkspaceBackfill);

    const anchor = (await pool.query(
      `SELECT tenant_key FROM tenant_workspaces WHERE client_id = 10`
    )).rows[0];
    assert.equal(anchor.tenant_key, 'cleaning-co');

    const ws = await ensureClientTenantWorkspaceBinding({ pool, clientId: 17 });
    assert.equal(ws.tenant_key, STUDIO_SUBSTRAL_SLUG);
  });
});
