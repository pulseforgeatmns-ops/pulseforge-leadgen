'use strict';
const { validateCoverageEpochConfig, epochRootIdentity, permissionStatus } = require('./scoutCoverageEpoch');

function wrapCoverageEpochStore(base, { db, tenantId, fingerprint, coverageEpoch = null, now = () => Date.now() }) {
  const config = validateCoverageEpochConfig(coverageEpoch, { tenantId, fingerprint });
  const scope = [String(tenantId), String(fingerprint)];
  let schema;
  async function ready() {
    if (schema === undefined) {
      const result = await db.query("SELECT to_regclass('scout_query_coverage_epochs') AS relation, to_regclass('scout_query_coverage_epoch_receipts') AS receipt");
      if (!Array.isArray(result.rows) || result.rows.length !== 1 || !Object.hasOwn(result.rows[0], 'relation') || !Object.hasOwn(result.rows[0], 'receipt')) throw Error('Scout coverage epoch schema status unconfirmed');
      const exists = result.rows[0].relation != null;
      if (exists !== (result.rows[0].receipt != null)) throw Error('Incomplete Scout coverage epoch schema');
      if (exists) {
        await db.query(`SELECT original_root_key,epoch,epoch_root_key,manifest_hash,approval_ref,not_before,expires_at
          FROM scout_query_coverage_epochs WHERE tenant_id=$1 AND search_fingerprint=$2 LIMIT 0`, scope);
        await db.query(`SELECT epoch,manifest_hash,approval_ref,root_query_keys,not_before,expires_at
          FROM scout_query_coverage_epoch_receipts WHERE tenant_id=$1 AND search_fingerprint=$2 LIMIT 0`, scope);
      }
      schema = exists;
    }
    if (!schema && config) throw Error('Scout coverage epoch migration required');
    return schema;
  }
  async function mapping(rootKey) {
    if (!(await ready())) return null;
    const { rows } = await db.query(`SELECT * FROM scout_query_coverage_epochs
      WHERE tenant_id=$1 AND search_fingerprint=$2 AND original_root_key=$3`, [...scope, rootKey]);
    return rows[0] || null;
  }
  function decorate(plan, row) {
    return { ...plan, originalRootKey: row.original_root_key, manifestHash: row.manifest_hash, coverageEpoch: 1 };
  }
  function validateMapping(row, rootKey) {
    if (row.original_root_key !== rootKey || row.epoch !== 1 || row.epoch_root_key !== epochRootIdentity(rootKey)) throw Error('Corrupt Scout coverage epoch mapping');
  }
  return {
    ...base,
    async assertReady() { await base.assertReady(); await ready(); },
    async plan(rootKey) {
      const original = await base.plan(rootKey);
      // Fetched historical payload always transfers before any reacquisition.
      if (original.legacy && original.row?.state === 'fetched') return original;
      let row = await mapping(rootKey);
      if (!row) {
        if (!config?.rootQueryKeys.includes(rootKey)) return original;
        const blocked = permissionStatus(config, rootKey, now());
        if (blocked) return { status: blocked, terminal: false };
        if (original.status !== 'legacy_continuation_unknown') return { status: 'coverage_epoch_legacy_required', terminal: true };
        const derived = epochRootIdentity(rootKey);
        // First eligible lookup pins the WHOLE allocation, even if this cycle's
        // page allowance is already consumed. No extension or root-set rotation.
        await db.query(`INSERT INTO scout_query_coverage_epoch_receipts
          (tenant_id,search_fingerprint,epoch,manifest_hash,approval_ref,root_query_keys,not_before,expires_at)
          SELECT $1,$2,1,$3,$4,$5,$6,$7
          WHERE EXISTS (SELECT 1 FROM scout_query_checkpoints c WHERE c.tenant_id=$1 AND c.search_fingerprint=$2
            AND c.query_key=$8 AND c.state='handled' AND c.plan_version='places-market-query-v1')
          AND NOT EXISTS (SELECT 1 FROM scout_query_pages p WHERE p.tenant_id=$1 AND p.search_fingerprint=$2 AND p.root_query_key=$8)
          ON CONFLICT DO NOTHING`, [...scope, config.manifestHash, config.approvalRef, config.rootQueryKeys,
          config.notBefore, config.expiresAt, rootKey]);
        const receipt = await db.query(`SELECT manifest_hash FROM scout_query_coverage_epoch_receipts
          WHERE tenant_id=$1 AND search_fingerprint=$2`, scope);
        if (receipt.rows[0] && receipt.rows[0].manifest_hash !== config.manifestHash) return { status: 'coverage_epoch_manifest_changed', terminal: true };
        await db.query(`INSERT INTO scout_query_coverage_epochs
          (tenant_id,search_fingerprint,original_root_key,epoch,epoch_root_key,manifest_hash,approval_ref,not_before,expires_at)
          SELECT $1,$2,$3,1,$4,$5,$6,$7,$8
          WHERE EXISTS (SELECT 1 FROM scout_query_checkpoints c WHERE c.tenant_id=$1 AND c.search_fingerprint=$2
            AND c.query_key=$3 AND c.state='handled' AND c.plan_version='places-market-query-v1')
          AND NOT EXISTS (SELECT 1 FROM scout_query_pages p WHERE p.tenant_id=$1 AND p.search_fingerprint=$2 AND p.root_query_key=$3)
          AND EXISTS (SELECT 1 FROM scout_query_coverage_epoch_receipts r WHERE r.tenant_id=$1 AND r.search_fingerprint=$2
            AND r.epoch=1 AND r.manifest_hash=$5 AND $3=ANY(r.root_query_keys) AND r.not_before=$7 AND r.expires_at=$8)
          ON CONFLICT DO NOTHING`, [...scope, rootKey, derived, config.manifestHash, config.approvalRef, config.notBefore, config.expiresAt]);
        row = await mapping(rootKey);
        if (!row) return { status: 'coverage_epoch_legacy_required', terminal: true };
      }
      validateMapping(row, rootKey);
      const selected = await base.plan(row.epoch_root_key);
      // Existing fetched/reserved/failed/finite work remains discoverable without
      // permission for another network reservation (including after expiry).
      if (selected.row?.state === 'fetched' || selected.status) return decorate(selected, row);
      const blocked = permissionStatus(config, rootKey, now());
      if (blocked) return { status: blocked, terminal: false };
      if (config.manifestHash !== row.manifest_hash) return { status: 'coverage_epoch_manifest_changed', terminal: true };
      return decorate(selected, row);
    },
    async reserve(plan) {
      let row = plan.originalRootKey ? await mapping(plan.originalRootKey) : null;
      // A crafted plan cannot evade the epoch guard by dropping its metadata.
      if (!row && await ready()) {
        const found = await db.query(`SELECT * FROM scout_query_coverage_epochs
          WHERE tenant_id=$1 AND search_fingerprint=$2 AND epoch_root_key=$3`, [...scope, plan.rootKey]);
        row = found.rows[0] || null;
      }
      if (row) {
        validateMapping(row, row.original_root_key);
        if (plan.originalRootKey !== row.original_root_key || plan.rootKey !== row.epoch_root_key
          || plan.manifestHash !== row.manifest_hash) throw Error('Scout coverage epoch reservation identity mismatch');
        if (permissionStatus(config, row.original_root_key, now()) || config.manifestHash !== row.manifest_hash) return null;
      } else if (plan.originalRootKey || plan.coverageEpoch) throw Error('Scout coverage epoch reservation mapping missing');
      return base.reserve(plan);
    },
  };
}
module.exports = { wrapCoverageEpochStore };
