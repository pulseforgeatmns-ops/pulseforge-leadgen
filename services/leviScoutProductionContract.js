'use strict';

const { extractDealClaims } = require('../packages/max/workspace/ProspectDealClaims');

const TENANT_ID = 21;
const MIN_AMOUNT = 5000;
const MAX_AMOUNT = 20000;

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
  return metadata.evidenceTimestamp || metadata.evidence_timestamp || metadata.observedAt
    || metadata.importedAt || row.evidence_timestamp || row.created_at || null;
}

function amountInRange(claim) {
  if (!claim) return false;
  if (Number.isFinite(claim.amount)) return claim.amount >= MIN_AMOUNT && claim.amount <= MAX_AMOUNT;
  if (claim.range) return claim.range.min >= MIN_AMOUNT && claim.range.max <= MAX_AMOUNT;
  return false;
}

function evaluateProspect(row = {}) {
  const metadata = metadataOf(row);
  const claims = extractDealClaims(row.notes || '');
  const review = reviewState(row, metadata);
  const sources = sourceReferences(row, metadata);
  const blockers = [];
  const category = String(metadata.category || row.category || row.job_title || '').toLowerCase();
  const contactPath = Boolean(row.email || row.phone || row.linkedin_url || metadata.profileUrl);
  const location = Boolean(row.service_area_match || row.company_location || metadata.projectLocation);
  const duplicateClear = !row.duplicate_of && metadata.duplicate !== true && metadata.duplicateOf == null;
  const suppressed = row.do_not_contact === true || metadata.suppressed === true;

  if (!/(borrower|operator|real[- ]estate investor)/.test(category)) blockers.push('borrower_identity_unverified');
  if (!contactPath) blockers.push('contact_path_missing');
  if (!claims.claims.stage) blockers.push('current_project_evidence_missing');
  if (!amountInRange(claims.claims.amount)) blockers.push(claims.claims.amount ? 'financing_amount_outside_blueprint' : 'financing_amount_missing');
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
      amountInRange: amountInRange(claims.claims.amount),
      location,
      sourceReferenceCount: sources.length,
      evidenceTimestamp: evidenceTimestamp(row, metadata),
      duplicateClear,
      suppressionClear: !suppressed,
      securityKnown: Boolean(claims.claims.security),
      exitKnown: Boolean(claims.claims.exit),
    },
  };
}

function weeklyProduction(rows = [], now = new Date()) {
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
    const evaluation = evaluateProspect(row);
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

module.exports = { TENANT_ID, MIN_AMOUNT, MAX_AMOUNT, evaluateProspect, weeklyProduction };
