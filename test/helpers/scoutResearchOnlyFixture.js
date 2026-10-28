'use strict';
// Disposable synthetic fixture for SPEC-SCOUT-P0-003. Mirrors the production
// Tenant 10 shape: recovery maintenance armed, one permanently quarantined
// uncertain email, and one still-pending prepared envelope item (not drained).
const fs = require('node:fs'), path = require('node:path');
const { startDisposablePostgres } = require('./disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./closePostgresPool');
const { seedScopedResume } = require('./anchorScopedResumePostgresFixture');
const { applyCrmPreparationSchema } = require('./anchorCrmPreparationSchema');

const REVISION = 'b'.repeat(40);
const ENV = Object.freeze({ RAILWAY_GIT_COMMIT_SHA: REVISION, SCOUT_RESEARCH_ONLY_GRANTS_ENABLED: 'true',
  ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true', ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true',
  ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED: 'true',
  GOOGLE_PLACES_KEY: 'synthetic-local-places-key', DATABASE_SSL: 'false' });
const ENV_KEYS = [...Object.keys(ENV), 'ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID', 'DATABASE_URL',
  'SCOUT_DURABLE_CANDIDATES_ENABLED', 'SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED'];
const MIGRATIONS = ['2026-10-17-scout-query-checkpoints.sql', '2026-10-19-scout-query-pages.sql',
  '2026-10-19-scout-candidate-tickets.sql', '2026-10-19-scout-query-coverage-epochs.sql', '2026-10-20-scout-coverage-grants.sql',
  '2026-10-14-api-usage-control.sql', '2026-10-20-scout-bounded-acquisition.sql', '2026-10-26-scout-bounded-research-authority.sql',
  '2026-10-27-scout-tenant-search-memory.sql', '2026-10-28-scout-research-only-grants.sql'];
const SESSION_ID = 'synthetic-research-admin-session';
const ADMIN_ID = '71';
const MISSION_ID = 'synthetic-anchor-research-mission';
const PROGRAM_ID = 'synthetic-anchor-research-program';
// Enough approved geography that eight one-page cycles never exhaust the plan.
const CITIES = Object.freeze(['Concord', 'Bow', 'Pembroke', 'Hopkinton', 'Loudon', 'Canterbury', 'Boscawen', 'Penacook', 'Dunbarton', 'Chichester']);

// Started before any application module that captures the shared pool.
async function startResearchOnlyHarness() {
  const postgres = await startDisposablePostgres('scout-research-only-');
  const previous = Object.fromEntries(ENV_KEYS.map(key => [key, process.env[key]]));
  Object.assign(process.env, ENV, { DATABASE_URL: postgres.connectionString });
  for (const key of ['ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID', 'SCOUT_DURABLE_CANDIDATES_ENABLED',
    'SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED']) delete process.env[key];
  const pool = createTrackedPostgresPool({ connectionString: postgres.connectionString, max: 10, connectionTimeoutMillis: 2000,
    statement_timeout: 30000, query_timeout: 30000, lock_timeout: 2000 });
  async function close() {
    try { await require('../../db').end().catch(() => {}); await closePostgresFixture(pool, postgres); }
    finally { for (const key of ENV_KEYS) previous[key] === undefined ? delete process.env[key] : process.env[key] = previous[key]; }
  }
  try {
    const legacy = await seedScopedResume(pool);
    await applyCrmPreparationSchema(pool);
    await pool.query(`ALTER TABLE acquisition_outbound_programs ADD COLUMN last_tick_at timestamptz,ADD COLUMN last_error text;
      ALTER TABLE acquisition_missions ADD COLUMN objective text,ADD COLUMN target_segment text,ADD COLUMN stage text,ADD COLUMN status text,ADD COLUMN client_id int;
      ALTER TABLE prospects ADD COLUMN service_area_match text,ADD COLUMN vertical text,ADD COLUMN icp_score int,ADD COLUMN updated_at timestamptz,ADD COLUMN created_at timestamptz DEFAULT now();
      ALTER TABLE companies ADD COLUMN industry text,ADD COLUMN location text,ADD COLUMN created_at timestamptz DEFAULT now();
      ALTER TABLE users ADD COLUMN email_verified boolean DEFAULT true,ADD COLUMN password_change_required boolean DEFAULT false;
      ALTER TABLE clients ADD COLUMN operator_switchable boolean DEFAULT true;
      CREATE TABLE "session"(sid text PRIMARY KEY,sess json NOT NULL,expire timestamp NOT NULL);
      CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,domain text,vertical text,location text,source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);
      CREATE TABLE agent_actions(id serial PRIMARY KEY,created_by text,action_type text,title text,description text,payload jsonb,status text,client_id int,created_at timestamptz DEFAULT now());`);
    for (const name of MIGRATIONS) await pool.query(fs.readFileSync(path.join(__dirname, '../../migrations', name), 'utf8'));
    await require('../../utils/placesCostAttribution').ensurePlacesAttributionSchema(pool);
    await pool.query("INSERT INTO users(id,client_id,role,active) VALUES($1,10,'admin',true)", [ADMIN_ID]);
    await pool.query('INSERT INTO "session"(sid,sess,expire) VALUES($1,$2,$3)', [SESSION_ID,
      { authenticated: true, active_client_id: 10, user: { id: ADMIN_ID, role: 'admin', client_id: 10, active: true,
        email_verified: true, password_change_required: false } }, '2099-01-01']);
    // Each call approves a newer program + immutable mission (a fresh search
    // fingerprint), the way an operator-approved successor would.
    let programs = 0;
    async function approveProgram(cities = CITIES) {
      const n = programs++, missionId = n ? `${MISSION_ID}-${n}` : MISSION_ID, programId = n ? `${PROGRAM_ID}-${n}` : PROGRAM_ID;
      const payload = { id: missionId, tenantId: '10', stage: 'discover', structuredMission: { immutable: true,
        market: { segment: 'commercial_cleaning_buyers', industry: 'property_and_office_operations', eligibleSubsegments: ['property_manager'] },
        geography: { region: `${cities[0]} NH`, scope: 'local', cities: [...cities] } } };
      await pool.query("INSERT INTO acquisition_missions(id,tenant_id,objective,target_segment,payload,stage,client_id) VALUES($1,'10','Synthetic property buyers','commercial_cleaning_buyers',$2,'discover',10)",
        [missionId, payload]);
      await pool.query(`INSERT INTO acquisition_outbound_programs(id,tenant_id,source_mission_id,mode,policy,authorized_at,policy_hash,scope_hash,authorized_by)
        VALUES($1,'10',$2,'active',$3,now()+make_interval(mins => 1 + $4),'synthetic-policy','synthetic-scope','synthetic-operator')`,
      [programId, missionId, { tenantId: '10', dailyCap: 5 }, n]);
      return { programId, missionId };
    }
    await approveProgram();
    return { pool, postgres, legacy, close, approveProgram, actor: { id: ADMIN_ID, role: 'admin', sessionId: SESSION_ID } };
  } catch (error) { await close(); throw error; }
}

// Synthetic Google Places: each text search returns three new businesses for
// its exact query; details echo the place id. Never a real network request.
function placesFetch() {
  const calls = [], cityByPlace = new Map();
  const fetchImpl = async input => {
    const url = new URL(input);
    if (url.hostname !== 'maps.googleapis.com') throw new Error(`unexpected host ${url.hostname}`);
    calls.push({ path: url.pathname, query: url.searchParams.get('query'), placeId: url.searchParams.get('place_id') });
    const slug = String(url.searchParams.get('query') || '').toLowerCase().replace(/[^a-z0-9]+/g, '-');
    const city = url.pathname.endsWith('/textsearch/json')
      ? (/ ([A-Za-z]+) NH$/.exec(url.searchParams.get('query') || '')?.[1] || 'Concord')
      : (cityByPlace.get(url.searchParams.get('place_id')) || 'Concord');
    const data = url.pathname.endsWith('/textsearch/json')
      ? { status: 'OK', results: [0, 1, 2].map(i => (cityByPlace.set(`${slug}-${i}`, city), { place_id: `${slug}-${i}`, name: `Concord Property Managers ${slug} ${i}`,
        formatted_address: `1 Main St, ${city}, NH 03301`, types: ['real_estate_agency'] })) }
      : { status: 'OK', result: { place_id: url.searchParams.get('place_id'), name: `Concord Property Managers ${url.searchParams.get('place_id')}`,
        website: `https://${url.searchParams.get('place_id')}.example`, formatted_address: `1 Main St, ${city}, NH 03301`,
        formatted_phone_number: '(603) 555-0100', types: ['real_estate_agency'], business_status: 'OPERATIONAL',
        address_components: [{ long_name: city, types: ['locality'] }, { short_name: 'NH', types: ['administrative_area_level_1'] }] } };
    return { ok: true, status: 200, json: async () => data, text: async () => JSON.stringify(data) };
  };
  return { calls, fetchImpl };
}

module.exports = { startResearchOnlyHarness, placesFetch, REVISION, ENV, SESSION_ID, ADMIN_ID, MISSION_ID, PROGRAM_ID };
