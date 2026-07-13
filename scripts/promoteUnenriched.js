/**
 * Manually re-enrich an unreachable Scout company and promote to prospects.
 * Usage:
 *   node scripts/promoteUnenriched.js --domain=example.com
 *   node scripts/promoteUnenriched.js --company="Acme Cleaning"
 */
require('dotenv').config();
const pool = require('../db');
const { normalizeDomain, runEnrichmentChain, resolveEmailVerification } = require('../leadgen');
const { ensureEmailVerificationColumns } = require('../utils/emailVerificationSchema');
const { ensureScoutUnenrichedTable } = require('../utils/scoutUnenrichedSchema');
const { normalizeVertical } = require('../utils/normalize');
const { deriveBusinessNameShort, ensureBusinessNameShortColumns } = require('../utils/businessNameShort');
const { getClientConfig } = require('../utils/clientContext');
const { configuredServiceAreas, matchServiceAreaFromLocation } = require('../utils/serviceArea');

function parseArg(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.slice(2).find(a => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length).trim() : null;
}

async function findUnenrichedRecord({ domain, company }) {
  if (domain) {
    const { rows } = await pool.query(`
      SELECT * FROM scout_unenriched
      WHERE LOWER(domain) = LOWER($1)
      ORDER BY last_attempt_at DESC
      LIMIT 1
    `, [domain]);
    if (rows.length) return rows[0];
  }
  if (company) {
    const { rows } = await pool.query(`
      SELECT * FROM scout_unenriched
      WHERE LOWER(TRIM(company)) = LOWER(TRIM($1))
      ORDER BY last_attempt_at DESC
      LIMIT 1
    `, [company]);
    if (rows.length) return rows[0];
  }
  return null;
}

async function findOrCreateCompanyForClient({ name, domain, websiteUrl, vertical, location, clientId }, db = pool) {
  await ensureBusinessNameShortColumns(db);
  const shortName = deriveBusinessNameShort(name);
  const existing = await db.query(
    `SELECT id FROM companies
     WHERE client_id = $2 AND LOWER(TRIM(name)) = LOWER(TRIM($1))
     LIMIT 1`,
    [name, clientId]
  );
  if (existing.rows.length) {
    await db.query(`
      UPDATE companies
      SET business_name_short = COALESCE(NULLIF(business_name_short, ''), $1),
          business_name_short_confidence = COALESCE(NULLIF(business_name_short_confidence, ''), $2),
          business_name_short_flags = CASE
            WHEN COALESCE(array_length(business_name_short_flags, 1), 0) = 0 THEN $3::text[]
            ELSE business_name_short_flags
          END,
          updated_at = NOW()
      WHERE id = $4
        AND client_id = $5
    `, [
      shortName.business_name_short,
      shortName.confidence,
      shortName.flags,
      existing.rows[0].id,
      clientId,
    ]);
    return existing.rows[0].id;
  }

  const inserted = await db.query(
    `INSERT INTO companies (
       name, business_name_short, business_name_short_confidence, business_name_short_flags,
       domain, website, industry, location, client_id, created_at
     )
     VALUES ($1, $2, $3, $4::text[], $5, $6, $7, $8, $9, NOW())
     RETURNING id`,
    [
      name,
      shortName.business_name_short,
      shortName.confidence,
      shortName.flags,
      domain,
      websiteUrl,
      vertical,
      location,
      clientId,
    ]
  );
  return inserted.rows[0]?.id || null;
}

function promotionServiceAreaMatch(record, clientConfig) {
  return matchServiceAreaFromLocation(record?.location, configuredServiceAreas(clientConfig));
}

async function promoteRecord(record, {
  db = pool,
  enrich = runEnrichmentChain,
  verify = resolveEmailVerification,
  loadClientConfig = getClientConfig,
} = {}) {
  const domain = record.domain || normalizeDomain(record.website_url);
  if (!domain) throw new Error('Record has no domain or website_url');

  const clientConfig = await loadClientConfig(record.client_id);
  if (!clientConfig) throw new Error(`Active client not found: ${record.client_id}`);
  const allowedServiceAreas = configuredServiceAreas(clientConfig);
  const serviceAreaMatch = promotionServiceAreaMatch(record, clientConfig);
  if (allowedServiceAreas.length > 0 && serviceAreaMatch === null) {
    console.log(`[promoteUnenriched] Out-of-area location rejected: ${record.location || 'no location'}`);
    return false;
  }

  console.log(`[promoteUnenriched] Re-enriching ${record.company || domain} (${domain})...`);
  const enriched = await enrich(domain, 'owner');
  if (!enriched?.email) {
    await db.query(`
      UPDATE scout_unenriched
      SET enrichment_attempts = enrichment_attempts + 1,
          last_attempt_at = NOW(),
          notes = COALESCE(notes, '') || ' | manual promote: still no email'
      WHERE id = $1
    `, [record.id]);
    console.log('[promoteUnenriched] No email found — record updated, not promoted.');
    return false;
  }

  const lead = {
    email: enriched.email,
    contact: enriched.contact || '',
    source: enriched.source || ['manual_promote'],
    url: record.website_url,
  };
  const verification = await verify(enriched.email, lead);
  if (verification.reject) {
    console.log(`[promoteUnenriched] Email rejected (${verification.rejectReason}) — not promoted.`);
    return false;
  }

  const companyId = await findOrCreateCompanyForClient({
    name: record.company || domain,
    domain,
    websiteUrl: record.website_url,
    vertical: record.vertical,
    location: record.location,
    clientId: record.client_id,
  }, db);
  if (!companyId) throw new Error('Unable to create company row');

  const discoveryMethod = record.source || 'manual_promote';
  const vertical = normalizeVertical(record.vertical) || 'unknown';
  const insert = await db.query(`
    INSERT INTO prospects (
      company_id, first_name, last_name, email, phone, status, source, icp_score, notes, vertical,
      client_id, service_area_match, discovery_method, website_url,
      email_verified, email_verification_method, verified_at, do_not_contact,
      email_status, verifier_response, verifier_checked_at
    ) VALUES ($1, NULL, NULL, $2, NULL, 'cold', 'scout', 70, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15)
    ON CONFLICT (email) DO NOTHING
    RETURNING id
  `, [
    companyId,
    enriched.email,
    verification.note || `Promoted from scout_unenriched (${record.id})`,
    vertical,
    record.client_id,
    serviceAreaMatch,
    discoveryMethod,
    record.website_url,
    verification.emailVerified,
    verification.emailVerificationMethod,
    verification.verifiedAt,
    verification.doNotContact,
    verification.emailStatus,
    JSON.stringify(verification.verifierResponse || null),
    verification.verifierCheckedAt,
  ]);

  if (!insert.rows.length) {
    console.log('[promoteUnenriched] Prospect already exists for this email.');
    return false;
  }

  await db.query('DELETE FROM scout_unenriched WHERE id = $1', [record.id]);
  console.log(`[promoteUnenriched] Promoted prospect ${insert.rows[0].id} (${enriched.email})`);
  return true;
}

async function run() {
  const domain = parseArg('domain');
  const company = parseArg('company');
  if (!domain && !company) {
    console.error('Usage: node scripts/promoteUnenriched.js --domain=example.com OR --company="Business Name"');
    process.exit(1);
  }

  await ensureScoutUnenrichedTable();
  await ensureEmailVerificationColumns();

  const record = await findUnenrichedRecord({ domain, company });
  if (!record) {
    console.error('[promoteUnenriched] No matching scout_unenriched record found.');
    process.exit(1);
  }

  const ok = await promoteRecord(record);
  process.exit(ok ? 0 : 2);
}

module.exports = { promoteRecord, promotionServiceAreaMatch };

if (require.main === module) {
  run().catch(err => {
    console.error('[promoteUnenriched] Fatal:', err.message);
    process.exit(1);
  });
}
