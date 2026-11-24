'use strict';

const { createHash } = require('node:crypto');
const { evaluateProspect, authorityCriteria, TENANT_ID } = require('./leviScoutProductionContract');
const { composeRecordReview } = require('./leviScoutMaxReview');

const VERSION = 1;
const MAX_CANDIDATES = 20;
const MAX_PROVIDER_REQUESTS = 3;
const ENABLE_FLAG = 'SCOUT_LEVI_RESEARCH_ONLY_ENABLED';

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
  return JSON.stringify(value);
}
function hash(value) { return createHash('sha256').update(canonical(value)).digest('hex'); }
function fail(code) { throw Object.assign(new Error(code), { code }); }

function validateAuthority(input) {
  const keys = ['version','kind','tenantId','cycleId','approvalRef','criteria','limits','permissions'];
  if (!input || Object.keys(input).length !== keys.length || keys.some(key => !Object.hasOwn(input, key))
    || input.version !== VERSION || input.kind !== 'levi_scout_research_only' || input.tenantId !== String(TENANT_ID)
    || !/^[a-zA-Z0-9_.:-]{1,120}$/.test(input.cycleId || '')
    || !/^[a-zA-Z0-9_.:-]{1,120}$/.test(input.approvalRef || '')
    || !authorityCriteria(input.criteria)
    || !input.limits || !Number.isSafeInteger(input.limits.maxCandidates) || input.limits.maxCandidates < 1
    || input.limits.maxCandidates > MAX_CANDIDATES
    || !Number.isSafeInteger(input.limits.maxProviderRequests) || input.limits.maxProviderRequests < 0
    || input.limits.maxProviderRequests > MAX_PROVIDER_REQUESTS
    || !input.permissions || Object.keys(input.permissions).length !== 5
    || input.permissions.discover !== true || input.permissions.persistReviewOnly !== true
    || input.permissions.outreach !== false || input.permissions.campaignEnrollment !== false
    || input.permissions.modifySuppression !== false) fail('levi_research_authority_invalid');
  return JSON.parse(JSON.stringify(input));
}

function assertEnvironment(authority, env = process.env) {
  if (env[ENABLE_FLAG] !== 'true') fail('levi_research_disabled');
  if (authority.limits.maxProviderRequests > 0 && env.SCOUT_LEVI_PAID_PROVIDERS_APPROVED !== 'true') {
    fail('levi_research_paid_provider_approval_required');
  }
}

function metadataOf(row) {
  const value = row && row.acquisition_metadata;
  if (!value) return {};
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_) { return {}; }
}

function identityKey(row) {
  const metadata = metadataOf(row);
  // Property-record signals stay entity-keyed so later public phones do not
  // create a second identity and a duplicate persist.
  if (metadata.sourceType === 'property_record_signal') {
    return `entity:${String(row.name || row.company || '').trim().toLowerCase()}|${String(row.service_area_match || row.location || '').trim().toLowerCase()}`;
  }
  const email = String(row.email || '').trim().toLowerCase();
  if (email) return `email:${email}`;
  const phone = String(row.phone || '').replace(/\D/g, '');
  if (phone) return `phone:${phone}`;
  const linkedIn = String(row.linkedin_url || '').trim().toLowerCase();
  if (linkedIn) return `linkedin:${linkedIn}`;
  return `entity:${String(row.name || row.company || '').trim().toLowerCase()}|${String(row.service_area_match || row.location || '').trim().toLowerCase()}`;
}

async function runResearchOnlyCycle({ authority: raw, env = process.env, deps }) {
  const authority = validateAuthority(raw);
  assertEnvironment(authority, env);
  if (!deps || !['claimRun','discover','existingIdentityKeys','persistReviewOnly','recordDiagnostic']
    .every(name => typeof deps[name] === 'function')) fail('levi_research_dependencies_required');

  const authorityHash = hash(authority);
  const claim = await deps.claimRun({ tenantId: String(TENANT_ID), cycleId: authority.cycleId, authorityHash });
  if (!claim || claim.claimed !== true) {
    return { status: 'replay_blocked', tenantId: String(TENANT_ID), cycleId: authority.cycleId,
      discovered: 0, persisted: 0, qualified: 0, outreachAttempted: false };
  }

  let diagnostic;
  try {
    const discovered = await deps.discover({ tenantId: String(TENANT_ID), cycleId: authority.cycleId,
      maxCandidates: authority.limits.maxCandidates, maxProviderRequests: authority.limits.maxProviderRequests });
    if (!Array.isArray(discovered) || discovered.length > authority.limits.maxCandidates) fail('levi_research_discovery_contract_invalid');
    const existing = new Set(await deps.existingIdentityKeys({ tenantId: String(TENANT_ID), keys: discovered.map(identityKey) }));
    const seen = new Set();
    const records = discovered.map(row => {
      const key = identityKey(row);
      const duplicate = existing.has(key) || seen.has(key);
      seen.add(key);
      const evaluation = evaluateProspect({ ...row, duplicate_of: duplicate ? 'existing' : null }, { criteria: authority.criteria });
      const review = composeRecordReview({
        candidate: row,
        evaluation,
        criteria: authority.criteria,
      });
      return Object.freeze({ candidate: row, identityKey: key, classification: evaluation.classification,
        qualified: evaluation.qualified, blockers: Object.freeze([...evaluation.blockers]),
        review, bucket: review.bucket,
        outreachAuthorized: false, campaignEnrollmentAuthorized: false, setterVisible: false,
        aoOutreachReviewRequired: true, source: 'scout_research_only' });
    });
    const saved = await deps.persistReviewOnly({ tenantId: String(TENANT_ID), cycleId: authority.cycleId,
      authorityHash, records: Object.freeze(records) });
    diagnostic = { status: 'completed', tenantId: String(TENANT_ID), cycleId: authority.cycleId,
      discovered: discovered.length, persisted: Number(saved?.persisted || 0),
      mergedSameIdentity: Number(saved?.merged || 0), skippedExisting: Number(saved?.skipped || 0),
      qualified: records.filter(row => row.qualified).length,
      classifications: records.reduce((out, row) => ({ ...out, [row.classification]: (out[row.classification] || 0) + 1 }), {}),
      duplicateBlocked: records.filter(row => row.blockers.includes('duplicate')).length,
      researchCandidates: records.filter(row => row.bucket === 'research_candidate').length,
      qualifiedProspects: records.filter(row => row.bucket === 'qualified_prospect').length,
      verifiableBusinessContacts: records.filter(row => row.review && row.review.verifiableBusinessContact).length,
      outreachAttempted: false, paidProvidersUsed: false };
  } catch (error) {
    diagnostic = { status: 'failed', tenantId: String(TENANT_ID), cycleId: authority.cycleId,
      reason: error.code || 'levi_research_cycle_failed', outreachAttempted: false, paidProvidersUsed: false };
  }
  await deps.recordDiagnostic(diagnostic);
  if (diagnostic.status === 'failed') fail(diagnostic.reason);
  return diagnostic;
}

module.exports = { VERSION, MAX_CANDIDATES, MAX_PROVIDER_REQUESTS, ENABLE_FLAG, hash,
  validateAuthority, assertEnvironment, identityKey, runResearchOnlyCycle };
