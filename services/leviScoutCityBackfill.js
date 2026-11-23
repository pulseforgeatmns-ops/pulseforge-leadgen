'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');
const { applyCityToMetadata } = require('./leviScoutProvenance');

function fail(code) { throw Object.assign(new Error(code), { code }); }

function text(value) {
  return value == null ? '' : String(value).trim();
}

function metadataOf(row) {
  const value = row && row.acquisition_metadata;
  if (!value) return {};
  if (typeof value === 'object') return { ...value };
  try { return JSON.parse(value); } catch (_) { return {}; }
}

function hostClass(url) {
  try {
    return new URL(String(url)).hostname.toLowerCase();
  } catch (_) {
    return null;
  }
}

function planRow(row) {
  const metadata = metadataOf(row);
  const applied = applyCityToMetadata(metadata, {
    location: row.service_area_match,
    serviceAreaMatch: row.service_area_match,
    permalink: Array.isArray(metadata.sourceUrls) ? metadata.sourceUrls[0] : null,
  });
  const beforeCity = text(metadata.city) || null;
  const afterCity = text(applied.metadata.city) || null;
  const wouldWrite = !beforeCity && Boolean(afterCity);
  return {
    id: row.id,
    clientId: row.client_id,
    source: row.source,
    beforeCity,
    afterCity,
    provider: applied.metadata.provider || null,
    inferenceSource: applied.inferred.source,
    sourceHosts: [...new Set((metadata.sourceUrls || []).map(hostClass).filter(Boolean))],
    wouldWrite,
    preservedOriginalEvidence: true,
  };
}

async function inspectMissingCity({ db, tenantId = TENANT_ID } = {}) {
  if (String(tenantId) !== String(TENANT_ID)) fail('levi_research_tenant_invalid');
  if (!db || typeof db.query !== 'function') fail('levi_research_db_unavailable');
  const result = await db.query(
    `SELECT id, client_id, source, service_area_match, acquisition_metadata
       FROM prospects
      WHERE client_id = $1
        AND source = 'scout_research_only'
      ORDER BY id ASC`,
    [Number(TENANT_ID)]
  );
  const plans = result.rows.map(planRow);
  return {
    tenantId: Number(TENANT_ID),
    inspected: plans.length,
    missingCity: plans.filter(row => !row.beforeCity).length,
    writable: plans.filter(row => row.wouldWrite).length,
    alreadySet: plans.filter(row => row.beforeCity).length,
    plans,
  };
}

async function backfillMissingCity({ db, persist = false, tenantId = TENANT_ID } = {}) {
  const inspection = await inspectMissingCity({ db, tenantId });
  if (!persist) {
    return { ...inspection, persisted: 0, dryRun: true, otherTenantWrites: 0 };
  }
  let persisted = 0;
  for (const plan of inspection.plans) {
    if (!plan.wouldWrite) continue;
    const row = (await db.query(
      `SELECT id, client_id, source, service_area_match, acquisition_metadata
         FROM prospects
        WHERE id = $1 AND client_id = $2 AND source = 'scout_research_only'
        FOR UPDATE`,
      [plan.id, Number(TENANT_ID)]
    )).rows[0];
    if (!row) continue;
    const next = planRow(row);
    if (!next.wouldWrite) continue;
    const metadata = applyCityToMetadata(metadataOf(row), {
      location: row.service_area_match,
      serviceAreaMatch: row.service_area_match,
      permalink: Array.isArray(metadataOf(row).sourceUrls) ? metadataOf(row).sourceUrls[0] : null,
    }).metadata;
    const updated = await db.query(
      `UPDATE prospects
          SET acquisition_metadata = $3::jsonb
        WHERE id = $1
          AND client_id = $2
          AND source = 'scout_research_only'
          AND (acquisition_metadata->>'city' IS NULL OR btrim(acquisition_metadata->>'city') = '')
        RETURNING id`,
      [plan.id, Number(TENANT_ID), JSON.stringify(metadata)]
    );
    if (updated.rows[0]) persisted += 1;
  }
  return {
    ...inspection,
    persisted,
    dryRun: false,
    otherTenantWrites: 0,
  };
}

module.exports = {
  inspectMissingCity,
  backfillMissingCity,
  planRow,
};
