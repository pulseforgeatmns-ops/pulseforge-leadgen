'use strict';

/**
 * SPEC-247B — Audit and repair Babrun outreach_asset executable copy.
 *
 * Usage:
 *   DATABASE_URL=... node scripts/canonicalizeBabrunOutreachAssets.js
 *   DATABASE_URL=... node scripts/canonicalizeBabrunOutreachAssets.js --apply
 *   DATABASE_URL=... node scripts/canonicalizeBabrunOutreachAssets.js --asset-id=ak_babrun_outreach_final_05 --apply
 */

require('dotenv').config();

const pool = require('../db');
const {
  auditOutreachAssetContent,
  resolveOutreachAssetMessage,
} = require('../packages/acquisition-knowledge');
const { canonicalizeOutreachAssetContent } = require('../services/acquisitionKnowledgePersistence');

const BABRUN_TENANT_ID = '13';

function parseArgs(argv = process.argv.slice(2)) {
  const args = { apply: false, assetId: null, tenantId: BABRUN_TENANT_ID };
  for (const arg of argv) {
    if (arg === '--apply') args.apply = true;
    else if (arg.startsWith('--asset-id=')) args.assetId = arg.split('=')[1];
    else if (arg.startsWith('--tenant-id=')) args.tenantId = arg.split('=')[1];
  }
  return args;
}

function targetProspectRelationship(row = {}) {
  const rels = Array.isArray(row.relationships) ? row.relationships : [];
  const match = rels.find((rel) => {
    const type = String(rel.type || rel.predicate || '').toLowerCase();
    return type === 'targets_prospect';
  });
  return match?.target?.id || match?.target?.ref || null;
}

async function loadOutreachAssets(client, tenantId, assetId) {
  if (assetId) {
    const result = await client.query(
      `SELECT * FROM acquisition_knowledge_objects
       WHERE tenant_id = $1 AND id = $2 AND object_type = 'outreach_asset'`,
      [tenantId, assetId]
    );
    return result.rows;
  }
  const result = await client.query(
    `SELECT * FROM acquisition_knowledge_objects
     WHERE tenant_id = $1 AND object_type = 'outreach_asset'
     ORDER BY id`,
    [tenantId]
  );
  return result.rows;
}

async function main() {
  const args = parseArgs();
  const client = await pool.connect();
  try {
    const rows = await loadOutreachAssets(client, args.tenantId, args.assetId);
    const inspected = [];
    const repaired = [];
    const manualReview = [];

    for (const row of rows) {
      const audit = auditOutreachAssetContent(row.content || {});
      const entry = {
        assetId: row.id,
        lifecycleState: row.lifecycle_state,
        validationState: row.validation_state,
        status: row.status,
        contentKeys: audit.contentKeys,
        hasStructuredSubject: audit.hasStructuredSubject,
        hasStructuredStatement: audit.hasStructuredStatement,
        hasSourceText: audit.hasSourceText,
        sourceTextParseable: audit.sourceTextParseable,
        parseError: audit.parseError,
        needsRepair: audit.needsRepair,
        requiresManualReview: audit.requiresManualReview,
        targetProspectId: targetProspectRelationship(row),
      };
      inspected.push(entry);

      if (audit.requiresManualReview) {
        manualReview.push({ assetId: row.id, reason: audit.parseError || 'partial_structured_copy' });
        continue;
      }

      if (!audit.needsRepair) continue;

      if (!args.apply) {
        repaired.push({ assetId: row.id, dryRun: true });
        continue;
      }

      const saved = await canonicalizeOutreachAssetContent(row.id, {
        tenantId: args.tenantId,
      }, pool, {
        client,
        actorId: 'spec247b_backfill',
        actorRole: 'operator',
      });

      if (saved.canonicalization?.changed) {
        repaired.push({
          assetId: row.id,
          dryRun: false,
          version: saved.version,
          subject: saved.canonicalization.subject,
        });
      }
    }

    const summary = {
      spec: 'SPEC-247B',
      tenantId: args.tenantId,
      apply: args.apply,
      assetsInspected: inspected.length,
      assetsNeedingRepair: inspected.filter((row) => row.needsRepair).length,
      assetsRepaired: repaired.filter((row) => !row.dryRun).length,
      assetsRepairPlanned: repaired.filter((row) => row.dryRun).length,
      assetsRequiringManualReview: manualReview.length,
      inspected,
      repaired,
      manualReview,
    };

    if (args.assetId) {
      const assetRow = rows[0];
      if (assetRow) {
        try {
          const resolved = resolveOutreachAssetMessage({
            id: assetRow.id,
            object_type: assetRow.object_type,
            channel: assetRow.channel,
            lifecycle_state: assetRow.lifecycle_state,
            validation_state: assetRow.validation_state,
            status: assetRow.status,
            version: assetRow.version,
            updated_at: assetRow.updated_at,
            content: assetRow.content,
          }, { requireStakeholderValidated: true });
          summary.resolvedBeforeApply = {
            subject: resolved.subject,
            bodyPreview: resolved.body.slice(0, 120),
            source: resolved.source,
          };
        } catch (err) {
          summary.resolvedBeforeApply = { error: err.code || err.message };
        }
      }
    }

    console.log(JSON.stringify(summary, null, 2));
  } finally {
    client.release();
  }
}

main()
  .catch((err) => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
