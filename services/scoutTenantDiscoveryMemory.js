'use strict';

// Knowledge reuse, never research authority. Callers still own all source,
// provider pause, spending, page, ticket and admission checks. No runtime DDL.
const { randomUUID } = require('node:crypto');
const { PLAN_VERSION, queryIdentity } = require('./scoutQueryCheckpoints');
const ENABLED_TENANTS = new Set(['10']);
// Separate from the global durable-candidate flag: rollout must not opt other
// tenants into a different discovery path. Neither flag grants provider access.
const isTenantDiscoveryMemoryEnabled = (tenantId, env = process.env) =>
  ENABLED_TENANTS.has(String(tenantId)) && env.SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED === 'true';
const fail = code => Object.assign(new Error(code), { code, retryAllowed:false });
const validId = value => typeof value === 'string' && value.length > 0 && value.length <= 512;

function businessObservation(raw) {
  if (!raw || raw.source !== 'google_places' || !validId(raw.placeId)
    || typeof raw.companyName !== 'string' || !raw.companyName.trim()) return null;
  // Preserve observed facts, not old query labels, ticket authority or opaque fields.
  return { placeId:raw.placeId, companyName:raw.companyName.slice(0,1024), source:'google_places',
    website:typeof raw.website === 'string' ? raw.website.slice(0,2048) : null,
    phone:typeof raw.phone === 'string' ? raw.phone.slice(0,100) : null,
    address:typeof raw.address === 'string' ? raw.address.slice(0,2048) : '',
    placeTypes:Array.isArray(raw.placeTypes) ? raw.placeTypes.filter(x=>typeof x==='string').slice(0,30) : [],
    googleRating:Number.isFinite(raw.googleRating) ? raw.googleRating : null, snippet:'',
    ...(raw.facilityResearchRecovery === true ? { facilityResearchRecovery: true,
      addressComponents: Array.isArray(raw.addressComponents) ? raw.addressComponents.map(row => ({
        long_name: typeof row.long_name === 'string' ? row.long_name : '',
        short_name: typeof row.short_name === 'string' ? row.short_name : '',
        types: Array.isArray(row.types) ? row.types.filter(x => typeof x === 'string') : [] })) : [],
      businessStatus: typeof raw.businessStatus === 'string' ? raw.businessStatus : null } : {}) };
}

function createTenantDiscoveryMemory({ pool, tenantId, fingerprint }) {
  const tenant = String(tenantId);
  if (!ENABLED_TENANTS.has(tenant) || !pool?.query || !pool?.connect || !validId(fingerprint))
    throw fail('scout_tenant_memory_binding_required');
  const counts = { queriesSkipped:0, queriesUnconfirmed:0, lowNoveltyQueriesDeferred:0, detailsReused:0, detailsReserved:0 };
  async function assertReady() {
    try {
      await pool.query('SELECT tenant_id,query_key,root_query_key,query_text,search_fingerprint,claimed_at FROM scout_tenant_query_owners LIMIT 0');
      await pool.query('SELECT tenant_id,place_id,search_fingerprint,reservation_id,state,candidate,claimed_at,observed_at FROM scout_tenant_place_observations LIMIT 0');
      const {rows} = await pool.query(`SELECT c.conrelid::regclass::text AS relation,
        ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(attnum,ord)
          JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum ORDER BY k.ord) AS columns
        FROM pg_constraint c WHERE c.contype='p' AND c.convalidated
          AND c.conrelid IN ('scout_tenant_query_owners'::regclass,'scout_tenant_place_observations'::regclass)`);
      for (const [relation,key] of [['scout_tenant_query_owners','query_key'],['scout_tenant_place_observations','place_id']]) {
        if (!rows.some(r=>r.relation===relation && JSON.stringify(r.columns)===JSON.stringify(['tenant_id',key])))
          throw fail('scout_tenant_memory_identity_required');
      }
    } catch (error) {
      if (error.code === 'scout_tenant_memory_identity_required') throw error;
      throw fail('scout_tenant_memory_schema_required');
    }
  }
  async function transaction(key, work) {
    const db = await pool.connect(); let committing = false, discard = false;
    try {
      await db.query('BEGIN');
      await db.query("SET LOCAL lock_timeout='2s'");
      await db.query("SET LOCAL statement_timeout='5s'");
      await db.query('SELECT pg_advisory_xact_lock(271027,hashtext($1))',[`${tenant}:${key}`]);
      const result = await work(db);
      committing = true; await db.query('COMMIT'); committing = false;
      return result;
    } catch (error) {
      try { await db.query('ROLLBACK'); } catch { discard = true; }
      if (committing) throw fail('scout_tenant_memory_commit_uncertain');
      throw error;
    } finally { db.release(discard || committing); }
  }
  function queryKeys(key,query) {
    const keys=[queryIdentity(query,false),queryIdentity(query,true)];
    if (!keys.includes(key)) throw fail('scout_tenant_memory_query_mismatch');
    return keys;
  }
  async function queryStatus(db, key, query) {
    const keys=queryKeys(key,query);
    const owner = (await db.query(`SELECT search_fingerprint,root_query_key FROM scout_tenant_query_owners
      WHERE tenant_id=$1 AND query_key=$2`,[tenant,keys[0]])).rows[0];
    if (owner?.search_fingerprint === fingerprint && owner.root_query_key===key) return null;
    // Read legacy records too: changing the mission never erases pre-migration
    // history. A fetched response is known coverage even before its work is drained.
    const history = (await db.query(`SELECT search_fingerprint,state FROM scout_query_checkpoints
      WHERE tenant_id=$1 AND query_key=ANY($2::text[]) AND plan_version=$3
        AND (search_fingerprint<>$4 OR query_key<>$5)
      UNION ALL SELECT search_fingerprint,state FROM scout_query_pages
      WHERE tenant_id=$1 AND root_query_key=ANY($2::text[])
        AND (search_fingerprint<>$4 OR root_query_key<>$5)`,[tenant,keys,PLAN_VERSION,fingerprint,key])).rows;
    if (history.some(row=>['handled','fetched'].includes(row.state))) return 'tenant_query_previously_searched';
    return owner || history.length ? 'tenant_query_outcome_unconfirmed' : null;
  }
  async function checkQuery(key, query) {
    const status = await queryStatus(pool,key,query);
    if (status) counts[status==='tenant_query_previously_searched'?'queriesSkipped':'queriesUnconfirmed']++;
    return status;
  }
  async function reserveQuery(key, query) {
    const keys=queryKeys(key,query);
    return transaction(`query:${keys[0]}`,async db=>{
      const status = await queryStatus(db,key,query);
      if (status) return status;
      await db.query(`INSERT INTO scout_tenant_query_owners(tenant_id,query_key,root_query_key,query_text,search_fingerprint)
        VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,[tenant,keys[0],key,query,fingerprint]);
      return queryStatus(db,key,query);
    });
  }
  async function continuationStatus(rootKey) {
    const row=(await pool.query(`SELECT c.candidates FROM scout_query_pages p JOIN scout_query_checkpoints c
      USING(tenant_id,search_fingerprint,query_key)
      WHERE p.tenant_id=$1 AND p.search_fingerprint=$2 AND p.root_query_key=$3
        AND p.state='fetched' AND c.state='handled' ORDER BY p.page_index DESC LIMIT 1`,[tenant,fingerprint,rootKey])).rows[0];
    // Deprioritize only measured overlap. This is not a claim of market/page
    // exhaustion; the remaining page needs review after fresh roots are covered.
    const candidates=row?.candidates;
    if (Array.isArray(candidates) && candidates.length>=5
      && candidates.filter(c=>c.detailsReused===true).length/candidates.length>=0.8) {
      counts.lowNoveltyQueriesDeferred++;
      return 'tenant_query_low_novelty';
    }
    return null;
  }
  async function knownPlace(db, placeId) {
    const record = (await db.query(`SELECT state,candidate,observed_at FROM scout_tenant_place_observations
      WHERE tenant_id=$1 AND place_id=$2`,[tenant,placeId])).rows[0];
    if (record) {
      if (record.state !== 'observed') throw fail('scout_tenant_memory_detail_unconfirmed');
      const candidate = businessObservation(record.candidate);
      if (!candidate || candidate.placeId!==placeId || !record.observed_at) throw fail('scout_tenant_memory_observation_invalid');
      return { candidate, observedAt:new Date(record.observed_at).toISOString() };
    }
    const legacy = (await db.query(`SELECT raw_candidate,created_at FROM scout_candidate_memberships
      WHERE tenant_id=$1 AND raw_candidate->>'placeId'=$2
      ORDER BY created_at DESC LIMIT 1`,[tenant,placeId])).rows[0];
    if (legacy) {
      const candidate = businessObservation(legacy.raw_candidate);
      if (!candidate) throw fail('scout_tenant_memory_observation_invalid');
      // Membership time proves durable retention, not a fresh detail observation.
      return { candidate, observedAt:null };
    }
    const checkpoint = (await db.query(`SELECT raw FROM scout_query_checkpoints c
      CROSS JOIN LATERAL jsonb_array_elements(c.candidates) raw
      WHERE c.tenant_id=$1 AND c.plan_version=$3 AND c.state IN ('fetched','handled')
        AND raw->>'placeId'=$2 LIMIT 1`,[tenant,placeId,PLAN_VERSION])).rows[0];
    if (!checkpoint) return null;
    const candidate = businessObservation(checkpoint.raw);
    if (!candidate) throw fail('scout_tenant_memory_observation_invalid');
    return { candidate, observedAt:null };
  }
  async function claimPlace(placeId) {
    if (!validId(placeId)) throw fail('scout_tenant_memory_place_invalid');
    const result = await transaction(`place:${placeId}`,async db=>{
      const known = await knownPlace(db,placeId);
      if (known) return { kind:'reuse', ...known };
      const reservationId=randomUUID();
      const inserted=await db.query(`INSERT INTO scout_tenant_place_observations
        (tenant_id,place_id,search_fingerprint,reservation_id,state) VALUES($1,$2,$3,$4,'reserved')
        ON CONFLICT DO NOTHING RETURNING place_id`,[tenant,placeId,fingerprint,reservationId]);
      if (inserted.rows.length!==1) throw fail('scout_tenant_memory_detail_unconfirmed');
      return { kind:'fetch',placeId,reservationId };
    });
    counts[result.kind==='reuse'?'detailsReused':'detailsReserved']++;
    return result;
  }
  async function finishPlace(claim, raw) {
    if (claim?.kind!=='fetch') throw fail('scout_tenant_memory_claim_required');
    const candidate=raw ? businessObservation(raw) : null;
    if (raw && (!candidate || candidate.placeId!==claim.placeId)) throw fail('scout_tenant_memory_observation_invalid');
    const result=await pool.query(`UPDATE scout_tenant_place_observations
      SET state=$5,candidate=$6::jsonb,observed_at=CASE WHEN $5='observed' THEN clock_timestamp() ELSE NULL END
      WHERE tenant_id=$1 AND place_id=$2 AND search_fingerprint=$3 AND reservation_id=$4 AND state='reserved'
      RETURNING place_id`,[tenant,claim.placeId,fingerprint,claim.reservationId,candidate?'observed':'uncertain',candidate ? JSON.stringify(candidate) : null]);
    if (result.rows.length!==1) throw fail('scout_tenant_memory_observation_unconfirmed');
  }
  return { tenantId:tenant, fingerprint, assertReady, checkQuery, reserveQuery, continuationStatus, claimPlace, finishPlace,
    snapshot:()=>({...counts}) };
}
module.exports={ENABLED_TENANTS,isTenantDiscoveryMemoryEnabled,businessObservation,createTenantDiscoveryMemory};
