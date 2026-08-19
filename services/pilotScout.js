'use strict';

/**
 * SPEC-115 Phase 6 — tenant-scoped Scout for Pilot 0.
 * Discovery → evidence → ranking → review. Never writes another tenant's rows.
 * Never executes outreach.
 */

const {
  runScoutAcquisitionIntelligence,
} = require('./scoutAcquisitionIntelligence');
const { scoutUnlocked, FAILURE } = require('./pilotOnboarding');
const { loadPublishedAimForClient } = require('./aicPersistence');

function scoutLockedError() {
  const err = new Error(FAILURE.NO_AIM.message);
  err.code = FAILURE.NO_AIM.code;
  err.status = 403;
  return err;
}

function toProspectRow(opportunity, clientId) {
  const company = opportunity.company || opportunity;
  const person = (company.people && company.people[0]) || opportunity.person || {};
  const name = String(company.name || opportunity.name || '').trim();
  const parts = String(person.name || name).trim().split(/\s+/);
  return {
    first_name: person.firstName || parts[0] || name || 'Founder',
    last_name: person.lastName || parts.slice(1).join(' ') || '',
    email: person.email || null,
    phone: person.phone || company.phone || null,
    job_title: person.title || person.jobTitle || 'Founder',
    company_name: name,
    website: company.website || null,
    location: company.location || company.address || null,
    source: 'scout_pilot',
    icp_score: Number(opportunity.score || opportunity.icpFit || company.icpScore || 0) || 0,
    vertical: company.industry || null,
    client_id: Number(clientId),
    notes: opportunity.rationale || opportunity.reason || opportunity.topPain || null,
  };
}

async function persistTenantProspects(pool, clientId, opportunities = []) {
  if (!pool || clientId == null) return [];
  const saved = [];
  for (const opportunity of opportunities) {
    const row = toProspectRow(opportunity, clientId);
    if (!row.company_name) continue;
    const existing = await pool.query(
      `SELECT id FROM prospects
       WHERE client_id = $1
         AND COALESCE(first_name,'') = $2
         AND COALESCE(last_name,'') = $3
         AND COALESCE(notes,'') = COALESCE($4,'')
       LIMIT 1`,
      [clientId, row.first_name, row.last_name, row.notes]
    );
    if (existing.rows[0]) {
      saved.push({ id: existing.rows[0].id, ...row, duplicate: true });
      continue;
    }
    try {
      const inserted = await pool.query(
        `INSERT INTO prospects (
            first_name, last_name, email, phone, job_title, source, icp_score,
            client_id, vertical, notes, status
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'cold')
          RETURNING id, first_name, last_name, email, job_title, icp_score, client_id, notes`,
        [
          row.first_name,
          row.last_name,
          row.email,
          row.phone,
          row.job_title,
          row.source,
          row.icp_score,
          clientId,
          row.vertical,
          row.notes,
        ]
      );
      saved.push(inserted.rows[0]);
    } catch (err) {
      if (/column .* does not exist|relation .* does not exist/i.test(err.message || '')) {
        throw err;
      }
      saved.push({ error: err.message, ...row });
    }
  }
  return saved;
}

function opportunitiesFromResult(result) {
  if (!result) return [];
  const payload = result.payload || result;
  if (Array.isArray(payload.opportunities)) return payload.opportunities;
  if (Array.isArray(result.opportunities)) return result.opportunities;
  if (Array.isArray(payload.evaluatedCandidates)) return payload.evaluatedCandidates;
  if (Array.isArray(result.ranked)) return result.ranked;
  if (Array.isArray(result.candidates)) return result.candidates;
  return [];
}

async function runPilotScout({
  pool,
  clientId,
  aim,
  question = 'Find founders struggling with founder dependency.',
  discoveryAdapters,
  discover,
  companies,
  persist = true,
} = {}) {
  const published = aim && (aim.published !== false)
    ? aim
    : (pool ? await loadPublishedAimForClient(clientId, pool) : null);
  if (!scoutUnlocked({ aim: published || { published: false } })) {
    throw scoutLockedError();
  }

  const result = await runScoutAcquisitionIntelligence(
    {
      tenantId: String(clientId),
      authority: 'observe',
      objective: question,
    },
    {
      aim: published,
      discoveryAdapters,
      discover,
      companies: companies || [],
    }
  );
  const opportunities = opportunitiesFromResult(result);
  const prospects = persist && pool
    ? await persistTenantProspects(pool, clientId, opportunities)
    : opportunities.map((o) => toProspectRow(o, clientId));

  return {
    ok: true,
    spec: 'SPEC-115',
    client_id: Number(clientId),
    question,
    count: prospects.length,
    opportunities,
    prospects,
    message: prospects.length
      ? null
      : 'Scout found no tenant-scoped prospects yet. Discovery ran; nothing was invented.',
    result,
  };
}

async function listTenantProspects(pool, clientId, { limit = 50 } = {}) {
  if (!pool || clientId == null) return [];
  const { rows } = await pool.query(
    `SELECT id, first_name, last_name, email, job_title, icp_score, status,
            client_id, source, notes, created_at
     FROM prospects
     WHERE client_id = $1
     ORDER BY created_at DESC NULLS LAST, id DESC
     LIMIT $2`,
    [Number(clientId), Number(limit) || 50]
  );
  return rows;
}

module.exports = {
  runPilotScout,
  persistTenantProspects,
  listTenantProspects,
  toProspectRow,
  scoutLockedError,
};
