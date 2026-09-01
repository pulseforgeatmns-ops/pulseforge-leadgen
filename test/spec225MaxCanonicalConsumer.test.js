'use strict';

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
  loadApprovedClientIntelligence,
} = require('../packages/max/workspace/ClientIntelligenceContext');
const {
  loadDurableBusinessUnderstanding,
  buildBusinessUnderstandingContract,
} = require('../packages/max/workspace/BusinessUnderstandingRetrieval');

const spec223aMigration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-09-01-spec-223a-canonical-semantic-persistence.sql'),
  'utf8'
);
const cieMigration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-08-06-client-intelligence-engine.sql'),
  'utf8'
);

function digest(text) {
  return crypto.createHash('sha256').update(text).digest('hex');
}

describe('SPEC-225 -- Max business understanding canonical consumer', () => {
  let postgres;
  let pool;
  let clientIntelligenceInterview;

  before(async () => {
    postgres = await startDisposablePostgres('spec-225-pg-');
    pool = new Pool({ connectionString: postgres.connectionString });

    await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
      CREATE TABLE clients (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
      CREATE TABLE tenant_workspaces (client_id INTEGER PRIMARY KEY REFERENCES clients(id), tenant_key TEXT NOT NULL UNIQUE);
      INSERT INTO clients VALUES (225,'CanonMax'),(226,'BrokenCanon'),(227,'LegacyMax');
      INSERT INTO tenant_workspaces VALUES (225,'tenant:canon-max'),(226,'tenant:broken-canon'),(227,'tenant:legacy-max');`);

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

  async function insertEvidence(clientId, sessionId, category, statement) {
    const result = await pool.query(
      `INSERT INTO cie_evidence (client_id, session_id, category, statement, source_text_sha256, immutable_at)
       VALUES ($1,$2,$3,$4,$5,NOW()) RETURNING *`,
      [clientId, sessionId, category, statement, digest(statement)]
    );
    return result.rows[0];
  }

  async function insertSession(clientId, normalizedFacts) {
    const result = await pool.query(
      `INSERT INTO cie_interview_sessions (client_id, status, interview_state)
       VALUES ($1,'CLIENT_REVIEW',$2::jsonb) RETURNING *`,
      [clientId, JSON.stringify({ normalizedFacts })]
    );
    return result.rows[0];
  }

  async function insertBlueprint(clientId, sessionId) {
    const id = crypto.randomUUID();
    const result = await pool.query(
      `INSERT INTO cie_business_blueprints (id, client_id, session_id, version, status, sections)
       VALUES ($1,$2,$3,'1.0','in_review','{}'::jsonb) RETURNING *`,
      [id, clientId, sessionId]
    );
    return result.rows[0];
  }

  async function approveFixture(clientId, normalizedFacts) {
    const session = await insertSession(clientId, normalizedFacts);
    await insertEvidence(clientId, session.id, 'identity', `${normalizedFacts.business_name} identity evidence.`);
    await insertEvidence(clientId, session.id, 'services', `${normalizedFacts.business_name} service evidence.`);
    await insertEvidence(clientId, session.id, 'customer', `${normalizedFacts.business_name} customer evidence.`);
    const blueprint = await insertBlueprint(clientId, session.id);
    const approved = await clientIntelligenceInterview.approveBlueprint(blueprint.id, { pool });
    return { session, blueprint, approved };
  }

  async function loadContract(clientId) {
    const loaded = await loadApprovedClientIntelligence({
      tenantId: String(clientId),
      cieService: clientIntelligenceInterview,
      cieOpts: { pool },
      propagateLoadErrors: true,
    });
    return {
      loaded,
      contract: buildBusinessUnderstandingContract(loaded.summary, loaded.playbook, []),
    };
  }

  async function canonicalCounts() {
    const tables = [
      'canonical_interpretation_batches',
      'canonical_business_entities',
      'canonical_business_facts',
      'canonical_fact_evidence_refs',
      'canonical_business_snapshots',
    ];
    const counts = {};
    for (const table of tables) {
      counts[table] = Number((await pool.query(`SELECT count(*) AS count FROM ${table}`)).rows[0].count);
    }
    return counts;
  }

  function canonicalContractSlice(contract) {
    return {
      companyName: contract.companyName,
      serviceArea: contract.serviceArea,
      services: contract.services,
      targetCustomers: contract.targetCustomers,
      targetGeography: contract.targetGeography,
      valueProposition: contract.valueProposition,
      businessGoals: contract.businessGoals,
      constraints: contract.constraints,
      semantic_authority: contract.semantic_authority,
      canonical_snapshot_id: contract.canonical_snapshot_id,
      projection_version: contract.projection_version,
    };
  }

  it('loads Max business understanding through SPEC-223C canonical projection', async () => {
    const { session, approved } = await approveFixture(225, {
      business_name: 'CanonMax Co',
      services: [{ name: 'Revenue operations', variants: [{ name: 'Pipeline repair' }] }],
      ideal_customers: 'Founder-led B2B firms',
      ideal_customers_role: 'owner/founder',
      ideal_customers_employee_range: '5-25',
      ideal_customers_geography: 'North America',
      avoid_customers: 'Pre-revenue experiments',
      target_markets: 'North America',
      differentiation: 'Operator-grade execution',
      ninety_day_outcomes: [{ name: 'Ten qualified opportunities' }],
      growth_focus: 'Legacy growth focus',
      brand_voice: 'Legacy voice.',
      success_metrics: ['Legacy success metric'],
      business_facts: { legacy: true },
    });

    const beforeCounts = await canonicalCounts();
    const first = await loadContract(225);

    assert.equal(first.loaded.blueprint._canonical_authority, approved.canonicalSnapshotId);
    assert.equal(first.contract.semantic_authority, 'CANONICAL');
    assert.equal(first.contract.canonical_snapshot_id, approved.canonicalSnapshotId);
    assert.match(first.contract.companyName, /CanonMax Co/);
    assert.match(first.contract.services, /Revenue operations/);
    assert.match(first.contract.services, /Pipeline repair/);
    assert.match(first.contract.targetCustomers, /Founder-led B2B firms/);
    assert.match(first.contract.targetGeography, /North America/);
    assert.match(first.contract.valueProposition, /Operator-grade execution/);
    assert.match(first.contract.businessGoals, /Ten qualified opportunities/);
    assert.match(first.contract.constraints, /Pre-revenue experiments/);
    assert.equal(first.contract.field_sources.companyName.source, 'CANONICAL');
    assert.equal(first.contract.field_sources.services.source, 'CANONICAL');
    assert.ok(first.contract.legacy_fallback_fields.includes('growthFocus'));
    assert.ok(first.contract.unavailable_fields.includes('commercialPreference'));
    assert.ok(first.contract.canonical_trace.fact_ids.length > 0);

    await pool.query(
      `UPDATE cie_interview_sessions
       SET interview_state = $2::jsonb
       WHERE id = $1`,
      [
        session.id,
        JSON.stringify({
          normalizedFacts: {
            business_name: 'Session Override Co',
            services: [{ name: 'Session-only service' }],
            ideal_customers: 'Session-only buyers',
            target_markets: 'Session-only market',
            differentiation: 'Session-only edge',
            ninety_day_outcomes: [{ name: 'Session-only goal' }],
          },
        }),
      ]
    );

    const second = await loadContract(225);
    assert.deepEqual(canonicalContractSlice(second.contract), canonicalContractSlice(first.contract));
    assert.doesNotMatch(second.contract.companyName, /Session Override/);
    assert.doesNotMatch(second.contract.services, /Session-only/);
    assert.doesNotMatch(second.contract.businessGoals, /Session-only/);
    assert.deepEqual(await canonicalCounts(), beforeCounts, 'consumer path must not write canonical storage');

    await pool.query(
      `UPDATE cie_interview_sessions
       SET interview_state = '{}'::jsonb
       WHERE id = $1`,
      [session.id]
    );
    const withoutSessionFacts = await loadContract(225);
    assert.deepEqual(
      canonicalContractSlice(withoutSessionFacts.contract),
      canonicalContractSlice(first.contract),
      'canonical Max fields must not depend on archival session facts'
    );

    const staleSession = {
      id: 'stale-session-authority',
      context: {
        tenantId: '225',
        clientIntelligence: {
          approved: true,
          semanticAuthority: 'session_archival',
          businessName: 'Stale Session Co',
          services: 'Stale session service',
          idealCustomers: 'Stale session buyers',
          geography: 'Stale session market',
        },
      },
    };
    const durable = await loadDurableBusinessUnderstanding({
      session: staleSession,
      context: staleSession.context,
      tenantId: '225',
      cieService: clientIntelligenceInterview,
      cieOpts: { pool },
      propagateLoadErrors: true,
    });
    assert.equal(durable.contract.semantic_authority, 'CANONICAL');
    assert.match(durable.contract.companyName, /CanonMax Co/);
    assert.doesNotMatch(durable.contract.companyName, /Stale Session/);
  });

  it('keeps pre-SPEC-224 approved Blueprints on the legacy path', async () => {
    const session = await insertSession(227, {
      business_name: 'LegacyMax Co',
      services: ['Legacy service'],
      ideal_customers: ['Legacy buyers'],
      geography: ['Legacy market'],
      differentiation: 'Legacy edge',
      ninety_day_outcomes: 'Legacy goal',
    });
    const blueprint = await insertBlueprint(227, session.id);
    await clientIntelligenceInterview.approveBlueprint(blueprint.id, {
      pool,
      canonicalCommit: false,
    });

    const { loaded, contract } = await loadContract(227);
    assert.equal(loaded.blueprint.canonicalSnapshotId, null);
    assert.equal(loaded.summary.semanticAuthority, 'session_archival');
    assert.equal(contract.semantic_authority, 'session_archival');
    assert.match(contract.companyName, /LegacyMax Co/);
    assert.match(contract.services, /Legacy service/);
  });

  it('fails closed when canonical projection cannot reconstruct a snapshot-backed Blueprint', async () => {
    const { approved } = await approveFixture(226, {
      business_name: 'BrokenCanon Co',
      services: [{ name: 'Canonical service' }],
      ideal_customers: 'Canonical buyers',
      target_markets: 'Canonical market',
      differentiation: 'Canonical edge',
    });
    await pool.query(
      `ALTER TABLE cie_business_blueprints DROP CONSTRAINT cie_bp_canonical_snapshot_fk`
    );
    await pool.query(
      `UPDATE cie_business_blueprints
       SET canonical_snapshot_tenant_id = 'tenant:missing-for-failure-test'
       WHERE canonical_snapshot_id = $1`,
      [approved.canonicalSnapshotId]
    );

    await assert.rejects(
      () => loadContract(226),
      err => err.code === 'CANONICAL_PROJECTION_FAILURE'
    );
  });
});
