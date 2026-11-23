'use strict';

const { TENANT_ID, evaluateProspect } = require('./leviScoutProductionContract');
const { fromPermit, qualificationSignal } = require('./leviScoutSignalModel');
const { researchContact } = require('./leviScoutContactResearch');
const { composeRecordReview } = require('./leviScoutMaxReview');

function fail(code) { throw Object.assign(new Error(code), { code }); }

function cloneCandidate(candidate) {
  return {
    ...candidate,
    notes: candidate.notes,
    acquisition_metadata: {
      ...(candidate.acquisition_metadata || {}),
      sourceUrls: [...((candidate.acquisition_metadata && candidate.acquisition_metadata.sourceUrls) || [])],
    },
  };
}

async function enrichCandidate({
  tenantId = TENANT_ID,
  candidate,
  sourceRow = {},
  fetch: fetchImpl,
  now = new Date(),
  cache,
  criteria,
} = {}) {
  if (String(tenantId) !== String(TENANT_ID)) fail('levi_research_tenant_invalid');
  if (!candidate) return null;
  const enriched = cloneCandidate(candidate);
  const metadata = enriched.acquisition_metadata;
  const permalink = Array.isArray(metadata.sourceUrls) ? metadata.sourceUrls[0] : null;
  const observedAt = metadata.evidenceDate || new Date(now).toISOString().slice(0, 10);
  const permitSignals = fromPermit({
    row: sourceRow,
    candidate: enriched,
    permalink,
    observedAt,
    city: metadata.city,
  });
  const contact = await researchContact({
    tenantId,
    entityName: enriched.company || enriched.name,
    city: metadata.city,
    sourceRow,
    permalink,
    observedAt,
    fetch: fetchImpl,
    cache,
  });
  metadata.signals = [...(permitSignals.signals || [])];
  metadata.contactResearch = contact;
  metadata.verifiedBorrowerIntent = false;
  metadata.permitDoesNotEstablishIntent = true;
  metadata.paidProvidersUsed = false;

  const evaluation = evaluateProspect(enriched, { criteria });
  const qualification = qualificationSignal({
    evaluation,
    source: permalink || 'approved_blueprint',
    observedAt,
  });
  if (qualification) metadata.signals.push(qualification);
  const review = composeRecordReview({ candidate: enriched, evaluation });
  metadata.review = review;
  metadata.bucket = review.bucket;
  return enriched;
}

async function enrichCandidates({
  tenantId = TENANT_ID,
  items = [],
  fetch: fetchImpl,
  now = new Date(),
  criteria,
} = {}) {
  const cache = new Map();
  const out = [];
  for (const item of items) {
    const candidate = item && item.candidate ? item.candidate : item;
    const sourceRow = item && item.sourceRow ? item.sourceRow : {};
    const enriched = await enrichCandidate({
      tenantId,
      candidate,
      sourceRow,
      fetch: fetchImpl,
      now,
      cache,
      criteria,
    });
    if (enriched) out.push(enriched);
  }
  return out;
}

function enrichmentCounts(rows = []) {
  let verifiableBusinessContacts = 0;
  let qualified = 0;
  for (const row of rows) {
    const metadata = row.acquisition_metadata || {};
    if (metadata.contactResearch && metadata.contactResearch.verifiable) verifiableBusinessContacts += 1;
    if (metadata.review && metadata.review.bucket === 'qualified_prospect') qualified += 1;
  }
  return {
    enriched: rows.length,
    verifiableBusinessContacts,
    qualified,
    researchCandidates: rows.length - qualified,
  };
}

module.exports = { enrichCandidate, enrichCandidates, enrichmentCounts };
