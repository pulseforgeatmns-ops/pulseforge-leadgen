'use strict';

/**
 * Strictly classifies existing Babrun promoted contacts from already persisted evidence.
 * It does not discover email patterns and never marks a founder without attributable evidence.
 *
 * Review: node scripts/backfillBabrunPromotionContactResolution.js
 * Apply:  node scripts/backfillBabrunPromotionContactResolution.js --apply --confirm=babrun-contact-resolution-backfill
 */

require('dotenv').config();
const pool = require('../db');
const {
  candidateRecord,
  classifyCandidate,
  mapVerificationResult,
  CONTACT_FINAL_STATE,
} = require('./lib/babrunContactResolution');
const { normalizeDomain } = require('../utils/canonicalEmailEligibility');

const CLIENT_ID = 13;
const CONFIRMATION = 'babrun-contact-resolution-backfill';

function founderName(row = {}) {
  return [row.first_name, row.last_name].filter(Boolean).join(' ').trim()
    || String(row.acquisition_metadata?.contactResolution?.contactName || '').trim();
}

function resolutionFromPersistedEvidence(row = {}) {
  if (!row.email) return null;
  const emailEvidence = row.enrichment_provenance?.email || {};
  const sourceUrl = emailEvidence.source_url || emailEvidence.discovery_source || null;
  const companyDomain = normalizeDomain(row.company_domain || row.company_website || '');
  const sourceDomain = normalizeDomain(sourceUrl || '');
  const firstParty = Boolean(companyDomain && sourceDomain && companyDomain === sourceDomain);
  const existing = row.acquisition_metadata?.contactResolution || {};
  const discoveryMethod = existing.discoveryMethod
    || (firstParty ? 'first_party_website' : 'existing_prospect_email');
  const candidate = candidateRecord(row.email, discoveryMethod, sourceUrl || 'prospects.email', {
    firstParty,
    publicFounderSource: discoveryMethod === 'public_founder_source',
  });
  const verification = {
    ...mapVerificationResult({ status: row.email_status }),
    verified: row.email_verified === true,
    method: row.email_verification_method || emailEvidence.verifier || null,
    verifiedAt: row.verified_at || row.verifier_checked_at || null,
  };
  const finalState = classifyCandidate(candidate, verification, founderName(row));
  return {
    resolvedAt: new Date().toISOString(),
    finalState,
    classification: finalState,
    bestEmail: String(row.email).toLowerCase(),
    contactName: founderName(row) || null,
    discoverySource: candidate.discoverySource,
    discoveryMethod,
    verification,
    evidenceBackfill: true,
  };
}

async function run({ db = pool, apply = false } = {}) {
  const { rows } = await db.query(`
    SELECT p.*, c.domain AS company_domain, c.website AS company_website
    FROM prospects p
    JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.client_id=$1 AND p.email IS NOT NULL
      AND COALESCE(p.do_not_contact, false)=false
      AND COALESCE(p.acquisition_metadata->'contactResolution'->>'finalState', '')=''
    ORDER BY p.created_at
  `, [CLIENT_ID]);

  const counts = Object.fromEntries(Object.values(CONTACT_FINAL_STATE).map(state => [state, 0]));
  const planned = [];
  for (const row of rows) {
    const resolution = resolutionFromPersistedEvidence(row);
    if (!resolution) continue;
    counts[resolution.finalState] = (counts[resolution.finalState] || 0) + 1;
    planned.push({ id: row.id, email: row.email, finalState: resolution.finalState });
    if (apply) {
      await db.query(`UPDATE prospects
        SET acquisition_metadata=COALESCE(acquisition_metadata, '{}'::jsonb)
          || jsonb_build_object('contactResolution', $1::jsonb), updated_at=NOW()
        WHERE id=$2 AND client_id=$3`, [JSON.stringify(resolution), row.id, CLIENT_ID]);
    }
  }
  return { apply, considered: rows.length, classified: planned.length, counts, sample: planned.slice(0, 12) };
}

if (require.main === module) {
  const apply = process.argv.includes('--apply');
  const confirm = process.argv.find(arg => arg.startsWith('--confirm='))?.split('=')[1];
  if (apply && confirm !== CONFIRMATION) throw new Error(`Refusing apply without --confirm=${CONFIRMATION}`);
  run({ apply })
    .then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(err => { console.error(err.message || err); process.exitCode = 1; })
    .finally(() => pool.end().catch(() => {}));
}

module.exports = { run, founderName, resolutionFromPersistedEvidence };
