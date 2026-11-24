'use strict';

const { extractDealClaims } = require('../packages/max/workspace/ProspectDealClaims');

const TENANT_ID = 21;
const PROPOSED_MIN_AMOUNT = 5000;
const PROPOSED_MAX_AMOUNT = 20000;
const RESEARCH_SOURCE = 'scout_research_only';

function metadataOf(row) {
  const value = row && row.acquisition_metadata;
  if (!value) return {};
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_) { return {}; }
}

function reviewState(row, metadata) {
  const direct = metadata.reviewState || metadata.review_state || row.review_state;
  if (direct) return String(direct).toLowerCase();
  const match = String(row.notes || '').match(/Review state:\s*([^.\n]+)/i);
  return match ? match[1].trim().toLowerCase().replace(/\s+/g, '_') : null;
}

function sourceReferences(row, metadata) {
  const listed = [metadata.sourceUrls, metadata.source_urls, metadata.sources].flat()
    .filter(Boolean).map(String);
  const notes = String(row.notes || '');
  const line = notes.match(/^Sources:\s*(.+)$/im);
  if (line) listed.push(...line[1].split('|').map(value => value.trim()).filter(Boolean));
  return [...new Set(listed)];
}

function evidenceTimestamp(row, metadata) {
  return metadata.evidenceDate || metadata.evidenceTimestamp || metadata.evidence_timestamp || metadata.observedAt
    || metadata.importedAt || row.evidence_timestamp || row.created_at || null;
}

function approvedCriteria(input) {
  if (!input || input.approved !== true || input.source !== 'approved_blueprint'
    || !Number.isSafeInteger(input.minAmount) || !Number.isSafeInteger(input.maxAmount)
    || input.minAmount < 1 || input.maxAmount < input.minAmount) return null;
  return { minAmount: input.minAmount, maxAmount: input.maxAmount };
}

function unverifiedBlueprintCriteria(input) {
  if (!input || input.approved !== false || input.source !== 'approved_blueprint') return null;
  if (Object.prototype.hasOwnProperty.call(input, 'minAmount') || Object.prototype.hasOwnProperty.call(input, 'maxAmount')) return null;
  const keys = Object.keys(input);
  if (keys.length !== 2 || !keys.includes('approved') || !keys.includes('source')) return null;
  return { approved: false, source: 'approved_blueprint' };
}

function authorityCriteria(input) {
  return approvedCriteria(input) ? input : unverifiedBlueprintCriteria(input);
}

function amountInRange(claim, criteria) {
  if (!claim || !criteria) return false;
  if (Number.isFinite(claim.amount)) return claim.amount >= criteria.minAmount && claim.amount <= criteria.maxAmount;
  if (claim.range) return claim.range.min >= criteria.minAmount && claim.range.max <= criteria.maxAmount;
  return false;
}

function evaluateProspect(row = {}, options = {}) {
  const metadata = metadataOf(row);
  const claims = extractDealClaims(row.notes || '');
  const criteria = approvedCriteria(options.criteria);
  const review = reviewState(row, metadata);
  const sources = sourceReferences(row, metadata);
  const blockers = [];
  const category = String(metadata.category || row.category || row.job_title || '').toLowerCase();
  const publicContact = Boolean(metadata.contactResearch && metadata.contactResearch.verifiable);
  const contactPath = Boolean(row.email || row.phone || row.linkedin_url || metadata.profileUrl || publicContact);
  const location = Boolean(row.service_area_match || row.company_location || metadata.projectLocation);
  const duplicateClear = !row.duplicate_of && metadata.duplicate !== true && metadata.duplicateOf == null;
  const suppressed = row.do_not_contact === true || metadata.suppressed === true;

  const sourceType = metadata.sourceType || null;
  if (!/(borrower|operator|real[- ]estate investor)/.test(category)) blockers.push('borrower_identity_unverified');
  // Ownership/investment evidence alone never establishes borrower intent.
  if (sourceType === 'property_record_signal') blockers.push('borrower_intent_unverified');
  if (!contactPath) blockers.push('contact_path_missing');
  if (!claims.claims.stage) blockers.push('current_project_evidence_missing');
  if (!criteria) blockers.push('financing_criteria_unverified');
  else if (!amountInRange(claims.claims.amount, criteria)) blockers.push(claims.claims.amount ? 'financing_amount_outside_blueprint' : 'financing_amount_missing');
  if (!location) blockers.push('project_location_missing');
  if (!sources.length) blockers.push('source_reference_missing');
  if (!evidenceTimestamp(row, metadata)) blockers.push('evidence_timestamp_missing');
  if (!duplicateClear) blockers.push('duplicate');
  if (suppressed) blockers.push('suppressed');
  if (claims.conflicts.length) blockers.push('contradictory_deal_evidence');

  const qualified = review === 'contact_now' && blockers.length === 0;
  let classification = 'Hold';
  if (qualified) classification = 'Contact Now';
  else if (review === 'contact_now' || review === 'qualify_first') classification = 'Qualify First';

  return {
    qualified,
    classification,
    reviewState: review,
    blockers,
    evidence: {
      contactPath,
      currentProject: Boolean(claims.claims.stage),
      amountInRange: amountInRange(claims.claims.amount, criteria),
      location,
      sourceReferenceCount: sources.length,
      evidenceTimestamp: evidenceTimestamp(row, metadata),
      evidencePublishedAt: metadata.evidenceDate || null,
      evidenceImportedAt: metadata.importedAt || row.created_at || null,
      sourceType,
      permitDoesNotEstablishIntent: sourceType === 'property_record_signal' || metadata.permitDoesNotEstablishIntent === true,
      verifiableBusinessContact: publicContact,
      signalLayers: Array.isArray(metadata.signals) ? metadata.signals.map(item => item && item.layer).filter(Boolean) : [],
      duplicateClear,
      suppressionClear: !suppressed,
      securityKnown: Boolean(claims.claims.security),
      exitKnown: Boolean(claims.claims.exit),
    },
  };
}

function weeklyProduction(rows = [], now = new Date(), options = {}) {
  const end = new Date(now);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - 29);
  const weeks = new Map();
  for (const row of rows) {
    const created = new Date(row.created_at);
    if (!Number.isFinite(created.getTime()) || created < start || created > end) continue;
    const day = new Date(Date.UTC(created.getUTCFullYear(), created.getUTCMonth(), created.getUTCDate()));
    const offset = (day.getUTCDay() + 6) % 7;
    day.setUTCDate(day.getUTCDate() - offset);
    const key = day.toISOString().slice(0, 10);
    const evaluation = evaluateProspect(row, options);
    const bucket = weeks.get(key) || { weekOf: key, discovered: 0, contractQualifiedCandidates: 0, contactNow: 0, qualifyFirst: 0, hold: 0 };
    bucket.discovered += 1;
    if (evaluation.qualified) bucket.contractQualifiedCandidates += 1;
    if (evaluation.classification === 'Contact Now') bucket.contactNow += 1;
    else if (evaluation.classification === 'Qualify First') bucket.qualifyFirst += 1;
    else bucket.hold += 1;
    weeks.set(key, bucket);
  }
  return [...weeks.values()].sort((a, b) => b.weekOf.localeCompare(a.weekOf));
}

module.exports = { TENANT_ID, PROPOSED_MIN_AMOUNT, PROPOSED_MAX_AMOUNT, RESEARCH_SOURCE, approvedCriteria, unverifiedBlueprintCriteria, authorityCriteria, evaluateProspect, weeklyProduction };
