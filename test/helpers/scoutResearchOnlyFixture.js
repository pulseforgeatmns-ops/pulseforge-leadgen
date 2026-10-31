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
  '2026-10-27-scout-tenant-search-memory.sql', '2026-10-28-scout-research-only-grants.sql', '2026-10-30-scout-research-cycle-releases.sql'];
const SESSION_ID = 'synthetic-research-admin-session';
const ADMIN_ID = '71';
const MISSION_ID = 'synthetic-anchor-research-mission';
const PROGRAM_ID = 'synthetic-anchor-research-program';
const OUTBOUND_CITIES = Object.freeze(['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown']);
const OUTBOUND_SUBSEGMENTS = Object.freeze(['realtor', 'str_manager', 'property_manager']);
// A business that Tenant 10 already owns; Places returns it for every query.
const OWNED_DOMAIN = 'owned-anchor-customer.example';

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
    // The shared pool follows DATABASE_URL; a later harness must get a new one.
    try {
      const shared = require.resolve('../../db');
      if (require.cache[shared]) { await require.cache[shared].exports.end().catch(() => {}); delete require.cache[shared]; }
      await closePostgresFixture(pool, postgres);
    }
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
    // Each call approves a newer outbound program + mission. The default shape
    // mirrors Anchor's obsolete Oct 3 mission (realtor/STR/property manager over
    // six cities), which research must never use as its discovery scope.
    let programs = 0;
    async function approveProgram({ cities = OUTBOUND_CITIES, subsegments = OUTBOUND_SUBSEGMENTS } = {}) {
      const n = programs++, missionId = n ? `${MISSION_ID}-${n}` : MISSION_ID, programId = n ? `${PROGRAM_ID}-${n}` : PROGRAM_ID;
      const payload = { id: missionId, tenantId: '10', stage: 'discover', objective: 'Anchor acquisition',
        constraints: ['Synthetic Oct 3 constraint.'], structuredMission: { immutable: true,
          market: { segment: 'commercial_cleaning_buyers', industry: 'property_and_office_operations', eligibleSubsegments: [...subsegments] },
          geography: { region: 'Manchester NH', scope: 'local', cities: [...cities] } } };
      await pool.query("INSERT INTO acquisition_missions(id,tenant_id,objective,target_segment,payload,stage,status,client_id) VALUES($1,'10','Anchor acquisition','commercial_cleaning_buyers',$2,'discover','active',10)",
        [missionId, payload]);
      await pool.query(`INSERT INTO acquisition_outbound_programs(id,tenant_id,source_mission_id,mode,policy,authorized_at,policy_hash,scope_hash,authorized_by)
        VALUES($1,'10',$2,'active',$3,now()+make_interval(mins => 1 + $4),'synthetic-policy','synthetic-scope','synthetic-operator')`,
      [programId, missionId, { tenantId: '10', dailyCap: 5 }, n]);
      return { programId, missionId };
    }
    await approveProgram();
    // As in production, the un-drained prepared envelope belongs to the active
    // program, so the successor-activation guard sees prepared_envelope_not_drained:1.
    await pool.query(`UPDATE acquisition_outbound_envelopes SET program_id=$1
      WHERE id IN (SELECT envelope_id FROM acquisition_outbound_items WHERE tenant_id='10' AND status='pending')`, [PROGRAM_ID]);
    // Tenant 10 already owns this business (AO-assigned, previously contacted).
    await pool.query(`INSERT INTO companies(id,client_id,name,domain,website) VALUES('owned-company',10,'Owned Anchor Customer',$1,$2)`,
      [OWNED_DOMAIN, `https://${OWNED_DOMAIN}`]);
    await pool.query(`INSERT INTO prospects(id,client_id,company_id,email,do_not_contact,is_synthetic,ao_outreach_review_required,assigned_ao_id,last_contacted_at)
      VALUES('owned-prospect',10,'owned-company',$1,false,false,false,7,now()-interval '3 days')`, [`owner@${OWNED_DOMAIN}`]);
    return { pool, postgres, legacy, close, approveProgram, actor: { id: ADMIN_ID, role: 'admin', sessionId: SESSION_ID } };
  } catch (error) { await close(); throw error; }
}

// Synthetic Google Places: each text search returns three businesses that look
// like what the query names (plus, as result 0 of every page, one business
// Tenant 10 already owns). Details echo the place. Never a real network request.
// unclassifiable:true appends one business whose name and Google types carry no
// business-type evidence, so only its own website could classify it.
function kindOf(query) {
  const q = query.toLowerCase();
  if (/campus|college|universit/.test(q)) return { label: 'College', types: ['university', 'point_of_interest'] };
  if (/school|academy/.test(q)) return { label: 'Private School', types: ['school', 'point_of_interest'] };
  if (/office park/.test(q)) return { label: 'Office Park', types: ['point_of_interest', 'establishment'] };
  if (/restaurant|cafe|bistro|diner|grill|eatery|tavern/.test(q)) return { label: 'Restaurant', types: ['restaurant', 'food'] };
  if (/warehouse|distribution|manufactur|industrial/.test(q)) return { label: 'Distribution Center', types: ['storage', 'point_of_interest'] };
  if (/dealer/.test(q)) return { label: 'Auto Dealership', types: ['car_dealer'] };
  if (/property management/.test(q)) return { label: 'Commercial Property Management', types: ['point_of_interest', 'establishment'] };
  return { label: 'Facility', types: ['point_of_interest', 'establishment'] };
}
function placesFetch({ unclassifiable = false, historicalMarker = false } = {}) {
  const calls = [], places = new Map();
  let serial = 0;
  const fetchImpl = async input => {
    const url = new URL(input);
    if (url.hostname !== 'maps.googleapis.com') throw new Error(`unexpected host ${url.hostname}`);
    calls.push({ path: url.pathname, query: url.searchParams.get('query'), placeId: url.searchParams.get('place_id') });
    let data;
    if (url.pathname.endsWith('/textsearch/json')) {
      const query = url.searchParams.get('query') || '';
      const slug = query.toLowerCase().replace(/[^a-z0-9]+/g, '-');
      const city = / ([A-Za-z]+) NH$/.exec(query)?.[1] || 'Manchester';
      const kind = kindOf(query);
      const results = [0, 1, 2, ...(unclassifiable ? [3] : []), ...(historicalMarker ? [4] : [])].map(i => {
        const place = i === 0 ? { place_id: `owned-${slug}`, name: 'Owned Anchor Customer', website: `https://${OWNED_DOMAIN}`, types: kind.types }
          : i === 4 ? { place_id: `${slug}-${i}`, name: 'Synthetic Factory Historical Marker',
            website: `https://${slug}-${i}.example`, types: ['historical_landmark', 'point_of_interest'] }
          : i === 3 ? { place_id: `${slug}-${i}`, name: `${city} Granite Works ${++serial}`, website: `https://${slug}-${i}.example`,
            types: ['point_of_interest', 'establishment'] }
          : { place_id: `${slug}-${i}`, name: `${city} ${kind.label} ${++serial}`, website: `https://${slug}-${i}.example`, types: kind.types };
        places.set(place.place_id, { ...place, city });
        return { place_id: place.place_id, name: place.name, formatted_address: `1 Main St, ${city}, NH 03101`, types: place.types };
      });
      data = { status: 'OK', results };
    } else {
      const place = places.get(url.searchParams.get('place_id')) || { place_id: url.searchParams.get('place_id'), name: 'Unknown', city: 'Manchester', types: [] };
      data = { status: 'OK', result: { place_id: place.place_id, name: place.name, website: place.website,
        formatted_address: `1 Main St, ${place.city}, NH 03101`, formatted_phone_number: '(603) 555-0100', types: place.types,
        business_status: 'OPERATIONAL', address_components: [{ long_name: place.city, types: ['locality'] },
          { short_name: 'NH', types: ['administrative_area_level_1'] }] } };
    }
    return { ok: true, status: 200, json: async () => data, text: async () => JSON.stringify(data) };
  };
  return { calls, fetchImpl };
}

module.exports = { startResearchOnlyHarness, placesFetch, REVISION, ENV, SESSION_ID, ADMIN_ID, MISSION_ID, PROGRAM_ID, OWNED_DOMAIN,
  OUTBOUND_CITIES, OUTBOUND_SUBSEGMENTS };
