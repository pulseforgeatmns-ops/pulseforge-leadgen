'use strict';

/**
 * One-time reconciliation for legacy Max buffer replenishment rows in scout_unenriched.
 *
 * Dry-run (default):
 *   node scripts/reconcileLegacyMaxReplenishment.js
 *
 * Apply (production writes):
 *   node scripts/reconcileLegacyMaxReplenishment.js --confirm-production --apply
 */

require('dotenv').config();

const pool = require('../db');
const { normalizeVertical } = require('../utils/normalize');
const {
  ENRICHABLE_SCOUT_VERTICALS,
  evaluateReplenishmentAdmission,
  resolveLegacyReconciliationOutcome,
} = require('../utils/replenishmentVertical');
const { ensureScoutUnenrichedTable } = require('../utils/scoutUnenrichedSchema');

const CLIENT_ID = 10;
const SOURCE = 'max_buffer_replenishment';
const DEFAULT_SERVICE_AREAS = [
  'Manchester',
  'Bedford',
  'Goffstown',
  'Hooksett',
  'Londonderry',
  'Auburn',
];

function parseArgs(argv = process.argv.slice(2)) {
  const apply = argv.includes('--apply');
  const confirmProduction = argv.includes('--confirm-production');
  if (argv.includes('--help')) {
    console.log(`Usage: node scripts/reconcileLegacyMaxReplenishment.js [--confirm-production --apply]

Defaults to dry-run. Writes require both --confirm-production and --apply.`);
    process.exit(0);
  }
  if (apply && !confirmProduction) {
    throw Object.assign(
      new Error('Refusing writes without --confirm-production.'),
      { code: 'confirm_production_required' }
    );
  }
  return {
    dryRun: !apply,
    apply,
    confirmProduction,
  };
}

function isNonCanonicalVertical(vertical) {
  const normalized = normalizeVertical(vertical);
  if (!normalized) return true;
  return !ENRICHABLE_SCOUT_VERTICALS.includes(normalized);
}

function parseDiscoveryFromNotes(notes) {
  const text = String(notes || '');
  const match = text.match(/\|\s*discovery:\s*(\{.*?\})\s*(?:\||$)/);
  if (!match) return {};
  try {
    const parsed = JSON.parse(match[1]);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

function legacyConceptFromVertical(vertical) {
  const normalized = normalizeVertical(vertical);
  if (!normalized) return null;
  return normalized.replace(/_/g, ' ');
}

function appendReconciliationNote(notes, payload) {
  const base = String(notes || '').trim();
  const marker = ` | legacy_reconciliation: ${JSON.stringify(payload)}`;
  return base ? `${base}${marker}` : marker.trim();
}

function rowToCandidate(row) {
  const discovery = parseDiscoveryFromNotes(row.notes);
  const legacyConcept = legacyConceptFromVertical(row.vertical);
  return {
    id: row.id,
    name: row.company,
    company: row.company,
    website: row.website_url,
    website_url: row.website_url,
    domain: row.domain,
    location: row.location,
    vertical: row.vertical,
    industry: row.vertical,
    businessType: row.businessType || null,
    description: row.description || null,
    snippet: row.snippet || null,
    discoveryQuery: discovery.discoveryQuery || legacyConcept,
    discoveryConcept: discovery.discoveryConcept || legacyConcept,
    discoveryCity: discovery.discoveryCity || row.location,
    discoverySource: discovery.discoverySource || row.source || SOURCE,
  };
}

async function loadMissionAdmissionContext(db) {
  const program = (await db.query(`
    SELECT source_mission_id
    FROM acquisition_outbound_programs
    WHERE tenant_id = $1 AND mode <> 'revoked'
    ORDER BY authorized_at DESC NULLS LAST, id DESC
    LIMIT 1
  `, [String(CLIENT_ID)])).rows[0];

  let missionSegment = 'short_term_rental';
  if (program?.source_mission_id) {
    const source = (await db.query(`
      SELECT payload
      FROM acquisition_missions
      WHERE tenant_id = $1 AND id = $2
      LIMIT 1
    `, [String(CLIENT_ID), program.source_mission_id])).rows[0];
    const structured = source?.payload?.structuredMission || {};
    missionSegment = normalizeVertical(structured.market?.segment || structured.market?.industry || missionSegment)
      || missionSegment;
  }

  const client = (await db.query(`
    SELECT service_area
    FROM clients
    WHERE id = $1
    LIMIT 1
  `, [CLIENT_ID])).rows[0];

  const serviceAreas = Array.isArray(client?.service_area) && client.service_area.length
    ? client.service_area
    : DEFAULT_SERVICE_AREAS;

  return {
    missionSegment,
    service_area: serviceAreas,
    clientConfig: { service_area: serviceAreas },
  };
}

async function loadLegacyRows(db) {
  const { rows } = await db.query(`
    SELECT id, client_id, company, website_url, domain, vertical, location, source, notes,
           enrichment_attempts, last_attempt_at
    FROM scout_unenriched
    WHERE client_id = $1
      AND source = $2
      AND COALESCE(enrichment_attempts, 0) = 0
    ORDER BY id ASC
  `, [CLIENT_ID, SOURCE]);

  return rows.filter(row => isNonCanonicalVertical(row.vertical));
}

function planLegacyReconciliation(rows, admissionContext, options = {}) {
  const runId = options.runId || `legacy_reconcile_${Date.now()}`;
  const at = options.at || new Date().toISOString();
  const plan = {
    runId,
    at,
    clientId: CLIENT_ID,
    source: SOURCE,
    missionSegment: admissionContext.missionSegment,
    dryRun: options.dryRun !== false,
    scanned: rows.length,
    canonicalize: [],
    hold: [],
    remove: [],
    summary: {
      scanned: rows.length,
      canonicalize: 0,
      hold: 0,
      remove: 0,
      rejected: {
        outside_geography: 0,
        segment_mismatch: 0,
        contradictory_business_type: 0,
        unclassifiable_vertical: 0,
        suppressed: 0,
        owned_elsewhere: 0,
        insufficient_business_fit: 0,
      },
    },
  };

  for (const row of rows) {
    const candidate = rowToCandidate(row);
    const admission = evaluateReplenishmentAdmission(candidate, admissionContext);
    const decision = resolveLegacyReconciliationOutcome(admission);

    if (decision.outcome === 'canonicalize') {
      const reconciliation = {
        runId,
        at,
        action: 'canonicalized',
        previousVertical: row.vertical,
        canonicalVertical: decision.vertical,
        dryRun: plan.dryRun,
      };
      plan.canonicalize.push({
        id: row.id,
        company: row.company,
        domain: row.domain,
        previousVertical: row.vertical,
        canonicalVertical: decision.vertical,
        notes: appendReconciliationNote(row.notes, reconciliation),
        reconciliation,
      });
      plan.summary.canonicalize += 1;
      continue;
    }

    const reason = decision.reason || 'unclassifiable_vertical';
    plan.summary.rejected[reason] = (plan.summary.rejected[reason] || 0) + 1;

    if (decision.outcome === 'hold') {
      const reconciliation = {
        runId,
        at,
        action: 'hold',
        previousVertical: row.vertical,
        reason,
        detail: decision.detail || null,
        dryRun: plan.dryRun,
      };
      plan.hold.push({
        id: row.id,
        company: row.company,
        domain: row.domain,
        previousVertical: row.vertical,
        reason,
        detail: decision.detail || null,
        reconciliation,
      });
      plan.summary.hold += 1;
      continue;
    }

    const reconciliation = {
      runId,
      at,
      action: 'removed',
      previousVertical: row.vertical,
      reason,
      detail: decision.detail || null,
      dryRun: plan.dryRun,
    };
    plan.remove.push({
      id: row.id,
      company: row.company,
      domain: row.domain,
      previousVertical: row.vertical,
      reason,
      detail: decision.detail || null,
      reconciliation,
    });
    plan.summary.remove += 1;
  }

  return plan;
}

async function applyLegacyReconciliation(db, plan) {
  if (plan.dryRun) {
    return { ...plan, applied: false };
  }

  let canonicalized = 0;
  let removed = 0;

  for (const item of plan.canonicalize) {
    const result = await db.query(`
      UPDATE scout_unenriched
      SET vertical = $1,
          notes = $2
      WHERE id = $3
        AND client_id = $4
        AND source = $5
        AND COALESCE(enrichment_attempts, 0) = 0
        AND vertical = $6
      RETURNING id
    `, [
      item.canonicalVertical,
      item.notes,
      item.id,
      CLIENT_ID,
      SOURCE,
      item.previousVertical,
    ]);
    canonicalized += result.rowCount;
  }

  for (const item of plan.remove) {
    const result = await db.query(`
      DELETE FROM scout_unenriched
      WHERE id = $1
        AND client_id = $2
        AND source = $3
        AND COALESCE(enrichment_attempts, 0) = 0
        AND vertical = $4
      RETURNING id
    `, [item.id, CLIENT_ID, SOURCE, item.previousVertical]);
    removed += result.rowCount;
  }

  return {
    ...plan,
    applied: true,
    appliedCounts: {
      canonicalized,
      removed,
    },
  };
}

async function runLegacyMaxReplenishmentReconciliation(options = {}) {
  const db = options.db || pool;
  const args = options.args || parseArgs(options.argv || []);
  if (options.skipEnsureTable !== true) {
    await ensureScoutUnenrichedTable();
  }

  const admissionContext = options.admissionContext || await loadMissionAdmissionContext(db);
  const rows = options.rows || await loadLegacyRows(db);
  const plan = planLegacyReconciliation(rows, admissionContext, {
    dryRun: args.dryRun,
    runId: options.runId,
    at: options.at,
  });

  if (args.apply) {
    return applyLegacyReconciliation(db, plan);
  }
  return plan;
}

async function main() {
  const report = await runLegacyMaxReplenishmentReconciliation();
  console.log(JSON.stringify(report, null, 2));
}

module.exports = {
  CLIENT_ID,
  SOURCE,
  parseArgs,
  isNonCanonicalVertical,
  parseDiscoveryFromNotes,
  rowToCandidate,
  loadLegacyRows,
  loadMissionAdmissionContext,
  planLegacyReconciliation,
  applyLegacyReconciliation,
  resolveLegacyReconciliationOutcome,
  runLegacyMaxReplenishmentReconciliation,
};

if (require.main === module) {
  main().catch(error => {
    console.error(`[reconcileLegacyMaxReplenishment] ${error.stack || error.message}`);
    process.exit(error.code === 'confirm_production_required' ? 2 : 1);
  });
}
