'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');
const { identityKey } = require('./leviScoutResearchOnlyExecution');
const { AGENT } = require('./leviScoutEnablement');
const { discover: discoverBorrowers, citiesOf } = require('./leviScoutBorrowerDiscovery');
const { importReviewOnlyContacts, importIdentityKey } = require('./reviewOnlyProspectImportService');
const { applyCityToMetadata } = require('./leviScoutProvenance');

function fail(code) { throw Object.assign(new Error(code), { code }); }

function asContacts(records = []) {
  return records.map((row, index) => {
    const candidate = row.candidate || row;
    const metadata = candidate.acquisition_metadata || {};
    const review = row.review || metadata.review || {};
    const sourceUrl = Array.isArray(metadata.sourceUrls) ? metadata.sourceUrls[0] : null;
    const applied = applyCityToMetadata(metadata, {
      permalink: sourceUrl,
      location: candidate.service_area_match,
      serviceAreaMatch: candidate.service_area_match,
    });
    const cityMetadata = applied.metadata;
    return {
      name: candidate.name || candidate.company || `Permit operator ${index + 1}`,
      company: candidate.company || candidate.name,
      email: undefined,
      phone: undefined,
      linkedin: candidate.linkedin_url || undefined,
      reviewState: metadata.reviewState || 'qualify_first',
      market: candidate.service_area_match || cityMetadata.city,
      category: metadata.category || 'borrower',
      fit: String(candidate.notes || '').replace(/^Review state:[^\n]*\n?/i, '').replace(/^Fit evidence:\s*/im, ''),
      sourceUrl,
      evidenceDate: metadata.evidenceDate || undefined,
      sourceType: metadata.sourceType || 'property_record_signal',
      nextAction: review.nextResearchAction,
      risks: Array.isArray(review.missingInformation) ? review.missingInformation.join(' ') : undefined,
      extraMetadata: {
        city: cityMetadata.city || null,
        provider: cityMetadata.provider || null,
        cityInference: cityMetadata.cityInference || null,
        signals: metadata.signals || [],
        contactResearch: metadata.contactResearch || null,
        review,
        bucket: review.bucket || metadata.bucket || 'research_candidate',
        permitDoesNotEstablishIntent: true,
        verifiedBorrowerIntent: false,
      },
    };
  });
}

// R1: one owner can hold several permits in the same weekly batch. The
// review-only import rejects a batch that repeats an import identity and rolls
// back everything, which used to fail the cycle after its claim. Merge records
// that share the import's own identity key before persisting: the first record
// is kept, every other permit's evidence is retained on it. A same-city permit
// adds its source URL to the record's sources; a permit from another city is
// kept only under mergedPermits so the record's city provenance stays
// single-city (no conflict is manufactured).
function cityOfContact(contact) {
  return (contact.extraMetadata && contact.extraMetadata.city) || null;
}

function mergeSameImportIdentity(contacts = []) {
  const kept = new Map();
  const out = [];
  let merged = 0;
  contacts.forEach((contact, index) => {
    const key = importIdentityKey(contact, index);
    const first = kept.get(key);
    if (!first) {
      const copy = { ...contact, sourceUrls: contact.sourceUrl ? [contact.sourceUrl] : [],
        extraMetadata: { ...(contact.extraMetadata || {}) } };
      kept.set(key, copy);
      out.push(copy);
      return;
    }
    merged += 1;
    const sameCity = cityOfContact(contact) === cityOfContact(first);
    const permit = Object.freeze({
      sourceUrl: contact.sourceUrl || null,
      market: contact.market || null,
      evidenceDate: contact.evidenceDate || null,
      city: cityOfContact(contact),
      provider: (contact.extraMetadata && contact.extraMetadata.provider) || null,
      sameCity,
    });
    first.extraMetadata.mergedPermits = [...(first.extraMetadata.mergedPermits || []), permit];
    first.extraMetadata.mergedPermitCount = first.extraMetadata.mergedPermits.length;
    if (!sameCity) first.extraMetadata.crossCityIdentityCollision = true;
    if (sameCity && contact.sourceUrl && !first.sourceUrls.includes(contact.sourceUrl)) {
      first.sourceUrls.push(contact.sourceUrl);
    }
  });
  return { contacts: out, merged };
}

// Advisory-lock class for weekly claims (int4). Tenant id is the second key.
const CLAIM_LOCK_CLASS = 0x4c455649; // 'LEVI'
const CLAIM_SQL = `INSERT INTO agent_log (agent_name, action, status, payload, ran_at, client_id)
       SELECT $1, 'research_cycle_claim', 'success', $2::jsonb, NOW(), $3
       WHERE NOT EXISTS (
         SELECT 1 FROM agent_log
          WHERE client_id = $3 AND agent_name = $1 AND action = 'research_cycle_claim'
            AND payload->>'cycleId' = $4
       )
       RETURNING id`;

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
    // R2: INSERT ... WHERE NOT EXISTS alone is not concurrency-safe under READ
    // COMMITTED (two transactions can both see "not exists"). Claims for this
    // tenant are serialized by a transaction-scoped advisory lock taken before
    // the insert, so the insert's snapshot sees any claim committed first. The
    // guarded unique index (migrations/2026-11-24-levi-scout-cycle-claim-unique.sql)
    // is a second key; its unique violation is read as "already claimed".
    const connection = typeof db.connect === 'function' ? await db.connect() : null;
    const q = connection || db;
    try {
      if (connection) {
        await q.query('BEGIN');
        await q.query('SELECT pg_advisory_xact_lock($1::int, $2::int)', [CLAIM_LOCK_CLASS, Number(TENANT_ID)]);
      }
      const inserted = await q.query(CLAIM_SQL, [AGENT, JSON.stringify(payload), Number(TENANT_ID), cycleId]);
      if (connection) await q.query('COMMIT');
      return { claimed: Boolean(inserted.rows[0]) };
    } catch (error) {
      if (connection) await q.query('ROLLBACK').catch(() => {});
      if (error && error.code === '23505') return { claimed: false, reason: 'unique_claim_exists' };
      throw error;
    } finally {
      if (connection) connection.release();
    }
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
      `SELECT first_name, last_name, email, phone, linkedin_url, service_area_match,
              discovery_method, source, acquisition_metadata
         FROM prospects WHERE client_id = $1`,
      [Number(TENANT_ID)]
    );
    const present = new Set(result.rows.map(row => identityKey({
      email: row.email,
      phone: row.phone,
      linkedin_url: row.linkedin_url,
      name: [row.first_name, row.last_name].filter(Boolean).join(' '),
      service_area_match: row.service_area_match,
      acquisition_metadata: row.acquisition_metadata || {
        sourceType: row.discovery_method === 'open_data_permit' ? 'property_record_signal' : undefined,
      },
    })));
    return [...keys].filter(key => present.has(key));
  }

  async function persistReviewOnly({ records, cycleId, authorityHash }) {
    if (!records.length) return { persisted: 0, skipped: 0, merged: 0 };
    if (!db || typeof db.query !== 'function') {
      const { contacts, merged } = mergeSameImportIdentity(asContacts(records));
      return { persisted: contacts.length, skipped: 0, merged, records, cycleId, authorityHash };
    }
    const { contacts, merged } = mergeSameImportIdentity(asContacts(records));
    const imported = await importReviewOnlyContacts(contacts, {
      clientId: Number(TENANT_ID),
      db,
      actorId: AGENT,
      source: 'scout_research_only',
      discoveryMethod: 'open_data_permit',
    });
    return {
      persisted: imported.inserted.length,
      skipped: imported.skipped.length,
      merged,
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
        [AGENT, diagnostic.status === 'failed' ? 'failed' : 'completed', JSON.stringify(payload), Number(TENANT_ID)]
      );
    }
    return payload;
  }

  return { state, claimRun, discover, existingIdentityKeys, persistReviewOnly, recordDiagnostic };
}

module.exports = { createProductionDeps, asContacts, mergeSameImportIdentity, CLAIM_LOCK_CLASS, CLAIM_SQL };
