'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');
const { identityKey } = require('./leviScoutResearchOnlyExecution');
const { AGENT } = require('./leviScoutEnablement');
const { discover: discoverBorrowers, citiesOf } = require('./leviScoutBorrowerDiscovery');
const { importReviewOnlyContacts } = require('./reviewOnlyProspectImportService');

function fail(code) { throw Object.assign(new Error(code), { code }); }

function asContacts(records = []) {
  return records.map((row, index) => {
    const candidate = row.candidate || row;
    const metadata = candidate.acquisition_metadata || {};
    const sourceUrl = Array.isArray(metadata.sourceUrls) ? metadata.sourceUrls[0] : null;
    return {
      name: candidate.name || candidate.company || `Permit operator ${index + 1}`,
      company: candidate.company || candidate.name,
      email: candidate.email || undefined,
      phone: candidate.phone || undefined,
      linkedin: candidate.linkedin_url || undefined,
      reviewState: metadata.reviewState || 'qualify_first',
      market: candidate.service_area_match,
      category: metadata.category || 'borrower',
      fit: String(candidate.notes || '').replace(/^Review state:[^\n]*\n?/i, '').replace(/^Fit evidence:\s*/im, ''),
      sourceUrl,
      evidenceDate: metadata.evidenceDate || undefined,
      sourceType: metadata.sourceType || 'property_record_signal',
    };
  });
}

function createProductionDeps({ db, fetch: fetchImpl, env = process.env, tenantId = TENANT_ID, now } = {}) {
  if (String(tenantId) !== String(TENANT_ID)) fail('levi_research_tenant_invalid');
  const state = { lastDiscover: null, diagnostics: [], claims: [] };

  async function claimRun({ cycleId, authorityHash }) {
    const payload = { cycleId, authorityHash, tenantId: String(TENANT_ID) };
    if (!db || typeof db.query !== 'function') {
      if (state.claims.includes(cycleId)) return { claimed: false };
      state.claims.push(cycleId);
      return { claimed: true };
    }
    const inserted = await db.query(
      `INSERT INTO agent_log (agent_name, action, status, payload, ran_at, client_id)
       SELECT $1, 'research_cycle_claim', 'ok', $2::jsonb, NOW(), $3
       WHERE NOT EXISTS (
         SELECT 1 FROM agent_log
          WHERE client_id = $3 AND agent_name = $1 AND action = 'research_cycle_claim'
            AND payload->>'cycleId' = $4
       )
       RETURNING id`,
      [AGENT, JSON.stringify(payload), Number(TENANT_ID), cycleId]
    );
    return { claimed: Boolean(inserted.rows[0]) };
  }

  async function discover(input) {
    const rows = await discoverBorrowers({
      tenantId: TENANT_ID,
      maxCandidates: input.maxCandidates,
      maxProviderRequests: input.maxProviderRequests,
      fetch: fetchImpl,
      now,
    });
    state.lastDiscover = { rows, cities: citiesOf(rows), tenantId: String(TENANT_ID) };
    return rows;
  }

  async function existingIdentityKeys({ keys }) {
    if (!db || typeof db.query !== 'function') return [];
    const result = await db.query(
      `SELECT first_name, last_name, email, phone, linkedin_url, service_area_match
         FROM prospects WHERE client_id = $1`,
      [Number(TENANT_ID)]
    );
    const present = new Set(result.rows.map(row => identityKey({
      email: row.email,
      phone: row.phone,
      linkedin_url: row.linkedin_url,
      name: [row.first_name, row.last_name].filter(Boolean).join(' '),
      service_area_match: row.service_area_match,
    })));
    return [...keys].filter(key => present.has(key));
  }

  async function persistReviewOnly({ records, cycleId, authorityHash }) {
    if (!records.length) return { persisted: 0, skipped: 0 };
    if (!db || typeof db.query !== 'function') {
      return { persisted: records.length, skipped: 0, records, cycleId, authorityHash };
    }
    const imported = await importReviewOnlyContacts(asContacts(records), {
      clientId: Number(TENANT_ID),
      db,
      actorId: AGENT,
      source: 'scout_research_only',
      discoveryMethod: 'open_data_permit',
    });
    return {
      persisted: imported.inserted.length,
      skipped: imported.skipped.length,
      cycleId,
      authorityHash,
    };
  }

  async function recordDiagnostic(diagnostic) {
    const payload = { ...diagnostic, outreachAttempted: false, paidProvidersUsed: false, envFlag: env.SCOUT_LEVI_RESEARCH_ONLY_ENABLED };
    state.diagnostics.push(payload);
    if (db && typeof db.query === 'function') {
      await db.query(
        `INSERT INTO agent_log (agent_name, action, status, payload, ran_at, client_id)
         VALUES ($1, 'research_cycle_diagnostic', $2, $3::jsonb, NOW(), $4)`,
        [AGENT, diagnostic.status === 'failed' ? 'error' : 'ok', JSON.stringify(payload), Number(TENANT_ID)]
      );
    }
    return payload;
  }

  return { state, claimRun, discover, existingIdentityKeys, persistReviewOnly, recordDiagnostic };
}

module.exports = { createProductionDeps, asContacts };
