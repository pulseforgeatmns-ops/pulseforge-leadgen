'use strict';
const { epochRootIdentity, permissionStatus } = require('./scoutCoverageEpoch');
const { allocationIdentity } = require('./scoutCoverageGrant');

// Version2 is additive. Version1 receipts never become renewable grants merely
// because this code or its migration is installed.
function wrapCoverageGrantStore(base, legacy, { db, tenantId, fingerprint, coverageEpoch: config = null, now = () => Date.now() }) {
  const scope = [String(tenantId), String(fingerprint)];
  // Ordinary checkpoint scopes are broader than the opt-in legacy grant feature.
  if (!['10','13'].includes(scope[0]) || !/^[a-f0-9]{64}(?![\s\S])/.test(scope[1])) return legacy;
  const expectedAllocation = allocationIdentity(...scope);
  let schema;
  const error = code => Object.assign(Error(code), { code });
  async function ready() {
    if (schema === undefined) {
      const result = await db.query(`SELECT to_regclass('scout_query_coverage_allocations') AS allocation,
        to_regclass('scout_query_coverage_grants') AS grants,
        to_regclass('scout_query_coverage_grant_roots') AS roots,
        to_regclass('scout_query_coverage_grant_pages') AS pages`);
      const row = result.rows?.[0];
      if (result.rows?.length !== 1 || ['allocation','grants','roots','pages'].some(k => !Object.hasOwn(row,k))) throw error('coverage_grant_schema_unconfirmed');
      const present = Object.values(row).filter(x => x != null).length;
      if (present !== 0 && present !== 4) throw error('coverage_grant_schema_incomplete');
      if (present === 4) {
        await db.query('SELECT allocation_hash,epoch,max_roots,max_pages_per_root FROM scout_query_coverage_allocations LIMIT 0');
        await db.query('SELECT grant_hash,allocation_hash,approval_ref,root_query_keys,not_before,expires_at,max_new_pages,revoked_at FROM scout_query_coverage_grants LIMIT 0');
        await db.query('SELECT allocation_hash,original_root_key,epoch_root_key FROM scout_query_coverage_grant_roots LIMIT 0');
        await db.query('SELECT query_key,grant_hash,reservation_id FROM scout_query_coverage_grant_pages LIMIT 0');
        const guards = await db.query(`SELECT count(*)::int AS count FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O'
          AND tgfoid=to_regprocedure('public.scout_exclude_mixed_coverage_allocations()')
          AND ((tgrelid='scout_query_coverage_epoch_receipts'::regclass AND tgname='scout_exclude_v2_coverage_allocation')
           OR (tgrelid='scout_query_coverage_allocations'::regclass AND tgname='scout_exclude_v1_coverage_allocation'))`);
        if (guards.rows[0]?.count !== 2) throw error('coverage_grant_format_guards_missing');
      }
      schema = present === 4;
    }
    if (!schema && config?.version === 2) throw error('coverage_grant_migration_required');
    return schema;
  }
  async function allocation(client = db, lock = false) {
    const { rows } = await client.query(`SELECT * FROM scout_query_coverage_allocations WHERE tenant_id=$1 AND search_fingerprint=$2${lock ? ' FOR UPDATE' : ''}`, scope);
    const row = rows[0];
    if (row && (row.allocation_hash !== expectedAllocation || row.epoch !== 1 || row.max_roots !== 54 || row.max_pages_per_root !== 3)) throw error('coverage_allocation_corrupt');
    return row || null;
  }
  async function mapping(rootKey, client = db) {
    const { rows } = await client.query('SELECT * FROM scout_query_coverage_grant_roots WHERE tenant_id=$1 AND search_fingerprint=$2 AND original_root_key=$3', [...scope,rootKey]);
    const row = rows[0];
    if (row && (row.allocation_hash !== expectedAllocation || row.original_root_key !== rootKey || row.epoch_root_key !== epochRootIdentity(rootKey))) throw error('coverage_grant_mapping_corrupt');
    return row || null;
  }
  async function rejectVersion1(client) {
    const { rows } = await client.query(`SELECT EXISTS(SELECT 1 FROM scout_query_coverage_epoch_receipts WHERE tenant_id=$1 AND search_fingerprint=$2)
      OR EXISTS(SELECT 1 FROM scout_query_coverage_epochs WHERE tenant_id=$1 AND search_fingerprint=$2) AS legacy`, scope);
    if (rows.length !== 1 || rows[0].legacy !== false) throw error('coverage_grant_legacy_reconciliation_required');
  }
  async function transaction(action) {
    const c = await db.connect(); let committing = false, discard = false;
    try {
      await c.query('BEGIN');
      await c.query("SET LOCAL lock_timeout='2s'"); await c.query("SET LOCAL statement_timeout='30s'");
      // Stable scope lock orders allocation creation and all root/grant quotas.
      await c.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [`scout-coverage-epoch:${scope[0]}:${scope[1]}`]);
      const result = await action(c);
      committing = true; await c.query('COMMIT'); committing = false;
      return result;
    } catch (failure) {
      try { await c.query('ROLLBACK'); } catch (_) { discard = true; }
      if (committing) throw error('coverage_grant_commit_uncertain');
      throw failure;
    } finally { c.release(discard || committing); }
  }
  function checkGrant(row, rootKey, at) {
    if (!row || config?.version !== 2 || row.grant_hash !== config.manifestHash || row.allocation_hash !== expectedAllocation
      || row.approval_ref !== config.approvalRef || JSON.stringify(row.root_query_keys) !== JSON.stringify(config.rootQueryKeys)
      || +new Date(row.not_before) !== Date.parse(config.notBefore) || +new Date(row.expires_at) !== Date.parse(config.expiresAt)
      || row.max_new_pages !== config.maxNewPages) throw error('coverage_grant_changed');
    if (row.revoked_at != null) return 'coverage_grant_revoked';
    return permissionStatus(config,rootKey,at);
  }
  async function selected(rootKey, row) {
    const plan = await base.plan(row.epoch_root_key);
    return { ...plan, originalRootKey:rootKey, allocationHash:expectedAllocation, coverageVersion:2, coverageEpoch:1,
      ...(config?.version === 2 ? { grantHash:config.manifestHash } : {}) };
  }
  async function permission(rootKey, client = db) {
    if (config?.version !== 2) return 'coverage_epoch_permission_required';
    const { rows } = await client.query('SELECT * FROM scout_query_coverage_grants WHERE tenant_id=$1 AND search_fingerprint=$2 AND grant_hash=$3', [...scope,config.manifestHash]);
    return checkGrant(rows[0],rootKey,now());
  }
  return {
    ...legacy,
    async assertReady() { await legacy.assertReady(); await ready(); },
    async plan(rootKey) {
      if (!(await ready())) return legacy.plan(rootKey);
      const current = await allocation();
      if (!current && config?.version !== 2) return legacy.plan(rootKey);
      const original = await base.plan(rootKey);
      // Historical fetched payload transfers through the unchanged ticket path.
      if (original.legacy && original.row?.state === 'fetched') return original;
      let row = await mapping(rootKey);
      if (!row) {
        const reverse = await db.query('SELECT original_root_key FROM scout_query_coverage_grant_roots WHERE tenant_id=$1 AND search_fingerprint=$2 AND epoch_root_key=$3',[...scope,rootKey]);
        if (reverse.rows.length) throw error('coverage_grant_reservation_identity_mismatch');
        // An allocation governs legacy refresh only, not new ordinary queries.
        if (!original.legacy && original.status !== 'legacy_continuation_unknown') return original;
      }
      if (row) {
        const plan = await selected(rootKey,row);
        if (plan.row?.state === 'fetched' || plan.status) return plan;
      }
      if (config?.version !== 2) return { status:'coverage_epoch_permission_required',terminal:false };
      const denied = permissionStatus(config,rootKey,now());
      if (denied) return { status:denied,terminal:false };
      const installed = (await db.query('SELECT * FROM scout_query_coverage_grants WHERE tenant_id=$1 AND search_fingerprint=$2 AND grant_hash=$3',[...scope,config.manifestHash])).rows[0];
      if (installed) { const blocked = checkGrant(installed,rootKey,now()); if (blocked) return { status:blocked,terminal:false }; }
      if (original.status !== 'legacy_continuation_unknown') return { status:'coverage_epoch_legacy_required',terminal:true };
      await transaction(async c => {
        await rejectVersion1(c);
        await c.query(`INSERT INTO scout_query_coverage_allocations
          (tenant_id,search_fingerprint,allocation_hash,epoch,max_roots,max_pages_per_root) VALUES($1,$2,$3,1,54,3) ON CONFLICT DO NOTHING`, [...scope,expectedAllocation]);
        await allocation(c,true);
        if (permissionStatus(config,rootKey,now())) throw error('coverage_grant_window_changed');
        const eligible = await c.query(`SELECT 1 FROM scout_query_checkpoints c WHERE c.tenant_id=$1 AND c.search_fingerprint=$2 AND c.query_key=$3
          AND c.plan_version='places-market-query-v1' AND c.state='handled'
          AND NOT EXISTS(SELECT 1 FROM scout_query_pages p WHERE p.tenant_id=$1 AND p.search_fingerprint=$2 AND p.root_query_key=$3)`, [...scope,rootKey]);
        if (eligible.rows.length !== 1) throw error('coverage_epoch_legacy_required');
        await c.query(`INSERT INTO scout_query_coverage_grants
          (tenant_id,search_fingerprint,allocation_hash,grant_hash,approval_ref,root_query_keys,not_before,expires_at,max_new_pages)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`, [...scope,expectedAllocation,config.manifestHash,config.approvalRef,config.rootQueryKeys,config.notBefore,config.expiresAt,config.maxNewPages]);
        const grant = (await c.query('SELECT * FROM scout_query_coverage_grants WHERE tenant_id=$1 AND search_fingerprint=$2 AND approval_ref=$3 FOR UPDATE',[...scope,config.approvalRef])).rows[0];
        const blocked = checkGrant(grant,rootKey,now()); if (blocked) throw error(blocked);
        const existing = await mapping(rootKey,c);
        if (!existing) {
          const unknown = await c.query(`SELECT EXISTS(SELECT 1 FROM scout_query_pages WHERE tenant_id=$1 AND search_fingerprint=$2 AND root_query_key=$3)
            OR EXISTS(SELECT 1 FROM scout_query_checkpoints WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3) AS present`, [...scope,epochRootIdentity(rootKey)]);
          if (unknown.rows[0]?.present !== false) throw error('coverage_grant_history_reconciliation_required');
          const count = (await c.query('SELECT count(*)::int AS count FROM scout_query_coverage_grant_roots WHERE tenant_id=$1 AND search_fingerprint=$2',scope)).rows[0].count;
          if (count >= 54) throw error('coverage_allocation_root_limit');
          await c.query(`INSERT INTO scout_query_coverage_grant_roots(tenant_id,search_fingerprint,allocation_hash,original_root_key,epoch_root_key)
            VALUES($1,$2,$3,$4,$5)`, [...scope,expectedAllocation,rootKey,epochRootIdentity(rootKey)]);
        }
      });
      row = await mapping(rootKey);
      const plan = await selected(rootKey,row);
      if (plan.row?.state === 'fetched' || plan.status) return plan;
      const blocked = await permission(rootKey);
      return blocked ? { status:blocked,terminal:false } : plan;
    },
    async reserve(plan) {
      if (!(await ready())) return legacy.reserve(plan);
      const current = await allocation();
      if (!current && config?.version !== 2 && plan.coverageVersion !== 2) return legacy.reserve(plan);
      if (plan.coverageVersion !== 2) {
        const mapped = await db.query('SELECT original_root_key FROM scout_query_coverage_grant_roots WHERE tenant_id=$1 AND search_fingerprint=$2 AND epoch_root_key=$3',[...scope,plan.rootKey]);
        if (!mapped.rows.length && !plan.originalRootKey && !plan.allocationHash && !plan.grantHash) return legacy.reserve(plan);
      }
      if (!current || config?.version !== 2 || plan.coverageVersion !== 2 || plan.grantHash !== config.manifestHash
        || plan.allocationHash !== expectedAllocation || !config.rootQueryKeys.includes(plan.originalRootKey)) throw error('coverage_grant_reservation_identity_mismatch');
      return transaction(async c => {
        await rejectVersion1(c); await allocation(c,true);
        const row = await mapping(plan.originalRootKey,c);
        if (!row || row.epoch_root_key !== plan.rootKey) throw error('coverage_grant_reservation_identity_mismatch');
        const grant = (await c.query('SELECT * FROM scout_query_coverage_grants WHERE tenant_id=$1 AND search_fingerprint=$2 AND grant_hash=$3 FOR UPDATE',[...scope,config.manifestHash])).rows[0];
        if (checkGrant(grant,plan.originalRootKey,now())) return null;
        const count = (await c.query('SELECT count(*)::int AS count FROM scout_query_coverage_grant_pages WHERE tenant_id=$1 AND search_fingerprint=$2 AND grant_hash=$3',[...scope,config.manifestHash])).rows[0].count;
        if (count >= grant.max_new_pages) return null;
        const id = await base.reserve(plan,c);
        if (!id) return null;
        await c.query(`INSERT INTO scout_query_coverage_grant_pages(tenant_id,search_fingerprint,query_key,grant_hash,reservation_id)
          VALUES($1,$2,$3,$4,$5)`,[...scope,plan.key,config.manifestHash,id]);
        return id;
      });
    },
    async fail(plan,failure = {}) {
      if (plan.coverageVersion !== 2 || failure.noNetwork !== true) return base.fail(plan,failure);
      // Refund only the existing provider's proof that ALL attempts had no
      // network. Page and grant debit are released together, never separately.
      return transaction(async c => {
        await allocation(c,true);
        const owned = await c.query(`SELECT 1 FROM scout_query_coverage_grant_pages g JOIN scout_query_pages p
          USING(tenant_id,search_fingerprint,query_key) WHERE g.tenant_id=$1 AND g.search_fingerprint=$2
          AND g.query_key=$3 AND g.grant_hash=$4 AND g.reservation_id=$5 AND p.reservation_id=$5 AND p.state='reserved' FOR UPDATE OF g,p`,[...scope,plan.key,plan.grantHash,plan.reservationId]);
        if (owned.rows.length !== 1) throw error('coverage_grant_reservation_lost');
        await c.query('DELETE FROM scout_query_coverage_grant_pages WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3 AND reservation_id=$4',[...scope,plan.key,plan.reservationId]);
        await base.fail(plan,failure,c);
      });
    },
  };
}
module.exports = { wrapCoverageGrantStore };
