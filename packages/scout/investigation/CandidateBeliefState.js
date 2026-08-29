'use strict';

/**
 * SPEC-199 — Durable Candidate Belief State.
 *
 * Canonical hydration boundary for candidate intelligence across investigation
 * continuations. Investigation refines belief; it does not reset belief.
 */

const { asText, QUALIFICATION_STATUSES, READINESS_STATES, PROSPECT_BUCKETS } = require('../../max/scoutAcquisition/Types');
const { recordsMatch, mergeResolved } = require('../../max/scoutAcquisition/EntityResolution');
const { buildProspectEvaluation, businessFitQualifiedCount } = require('../../max/scoutAcquisition/ProspectEvaluation');
const { attachFitToClassified } = require('../../max/scoutAcquisition/FitEvaluation');

function beliefMapKey(belief = {}) {
  return (
    asText(belief.canonicalIdentity || belief.candidateId || belief.identity?.name) || ''
  ).toLowerCase();
}

function canonicalCandidateId(row = {}) {
  return (
    asText(
      row.candidateId ||
        row.candidate_id ||
        row.id ||
        row.companyId ||
        row.canonicalIdentity ||
        row._identityKey
    ) || asText(row.name)
  );
}

function evidenceIdentity(ev = {}) {
  return asText(ev.id || ev.evidenceId || ev.externalRef || ev.label || ev.text) || null;
}

function mergeEvidenceArrays(prior = [], incoming = []) {
  const merged = [...(prior || [])];
  const seen = new Set(merged.map((row) => evidenceIdentity(row)).filter(Boolean));
  for (const row of incoming || []) {
    const key = evidenceIdentity(row);
    if (key && seen.has(key)) continue;
    if (key) seen.add(key);
    merged.push(row);
  }
  return merged;
}

function mergeStringArrays(prior = [], incoming = []) {
  const merged = [...(prior || [])];
  const seen = new Set(merged.map((v) => String(v)));
  for (const row of incoming || []) {
    const value = typeof row === 'object' ? row.text || row.label || JSON.stringify(row) : String(row);
    if (seen.has(value)) continue;
    seen.add(value);
    merged.push(row);
  }
  return merged;
}

function pickIdentity(row = {}) {
  const nested = row.identity && typeof row.identity === 'object' ? row.identity : {};
  return {
    name: row.name || row.company || nested.name || null,
    website: row.website || row.url || nested.website || null,
    placeId: row.placeId || row.place_id || nested.placeId || null,
    phone: row.phone || row.formatted_phone_number || nested.phone || null,
    address: row.address || row.location || nested.address || nested.location || null,
    location:
      row.location ||
      row.address ||
      nested.location ||
      nested.address ||
      (Array.isArray(row.cities) ? row.cities[0] : null) ||
      (Array.isArray(nested.cities) ? nested.cities[0] : null) ||
      null,
    industry: row.industry || row.vertical || row.segment || nested.industry || null,
  };
}

function pickQualification(row = {}, evaluation = null) {
  const evalQual = evaluation && evaluation.qualification;
  const direct = row.qualification || evalQual || null;
  if (!direct && !row.qualificationStatus) return null;
  return {
    status:
      (direct && direct.status) ||
      row.qualificationStatus ||
      QUALIFICATION_STATUSES.UNCERTAIN,
    reason: (direct && direct.reason) || row.qualificationReason || null,
    reasonCode: (direct && direct.reasonCode) || null,
    evidence: (direct && direct.evidence) || row.qualificationEvidence || null,
    evaluatedAt: (direct && direct.evaluatedAt) || row.qualificationEvaluatedAt || null,
  };
}

function pickReadiness(row = {}, evaluation = null) {
  const evalReady = evaluation && evaluation.readiness;
  const direct = row.readiness || evalReady || null;
  if (!direct && !row.readinessState) return null;
  return {
    status:
      (direct && direct.status) ||
      row.readinessState ||
      READINESS_STATES.UNKNOWN,
    signals: (direct && direct.signals) || row.readinessSignals || [],
    confidence: direct && direct.confidence != null ? Number(direct.confidence) : null,
    evaluatedAt: (direct && direct.evaluatedAt) || row.readinessEvaluatedAt || null,
  };
}

function pickBusinessFit(row = {}, evaluation = null) {
  const fitEval = row.fitEvaluation || row.businessFit || null;
  const fitFromEval = evaluation && evaluation.businessFit;
  if (!fitEval && !fitFromEval && row.fit == null) return null;
  return {
    score: fitFromEval && fitFromEval.score != null ? fitFromEval.score : row.fit != null ? Number(row.fit) : null,
    dimensions: (fitFromEval && fitFromEval.dimensions) || (fitEval && fitEval.dimensions) || null,
    basicFit:
      fitFromEval && fitFromEval.basicFit != null
        ? fitFromEval.basicFit
        : fitEval && fitEval.basicFit != null
          ? fitEval.basicFit
          : row.basicFit != null
            ? row.basicFit
            : null,
    reasons: (fitFromEval && fitFromEval.reasons) || (fitEval && fitEval.reasons) || row.fitReasons || [],
  };
}

function pickInvestigationState(row = {}, evaluation = null) {
  const investigation = (evaluation && evaluation.investigation) || row.investigationState || {};
  if (typeof investigation !== 'object') return null;
  return {
    unresolvedGaps: investigation.unresolvedGaps || investigation.canonicalGaps || [],
    completedTasks: investigation.completedTasks || [],
    blockedTasks: investigation.blockedTasks || [],
    missingEvidence: investigation.missingEvidence || [],
    unresolvedHypotheses: investigation.unresolvedHypotheses || [],
    canonicalGaps: investigation.canonicalGaps || [],
    lastInvestigatedAt: investigation.lastInvestigatedAt || row.lastInvestigatedAt || null,
  };
}

/**
 * Normalize one representation into canonical candidate belief state.
 * @param {object} priorCandidateState
 * @returns {object|null}
 */
function hydrateCandidateBelief(priorCandidateState = {}) {
  if (!priorCandidateState || typeof priorCandidateState !== 'object') return null;

  const candidateId = canonicalCandidateId(priorCandidateState);
  if (!candidateId) return null;

  const evaluation =
    priorCandidateState.evaluation ||
    priorCandidateState.prospectEvaluation ||
    null;
  const identity = pickIdentity(priorCandidateState);

  return {
    candidateId,
    canonicalIdentity:
      priorCandidateState.canonicalIdentity ||
      priorCandidateState._identityKey ||
      candidateId,
    identity,
    evidence: mergeEvidenceArrays([], priorCandidateState.evidence || []),
    evidenceRefs: mergeEvidenceArrays([], priorCandidateState.evidenceRefs || []),
    observations: mergeStringArrays([], priorCandidateState.observations || []),
    signals: mergeEvidenceArrays([], priorCandidateState.signals || []),
    hypothesisState: priorCandidateState.hypothesisState || null,
    businessFit: pickBusinessFit(priorCandidateState, evaluation),
    qualification: pickQualification(priorCandidateState, evaluation),
    readiness: pickReadiness(priorCandidateState, evaluation),
    provenance: mergeStringArrays([], priorCandidateState.provenance || priorCandidateState._mergedFrom || []),
    investigationState: pickInvestigationState(priorCandidateState, evaluation),
    evaluation,
    prospectBucket: priorCandidateState.prospectBucket || (evaluation && evaluation.bucket) || null,
    unknowns: mergeStringArrays([], priorCandidateState.unknowns || []),
    origin: priorCandidateState.origin || null,
    confidence:
      priorCandidateState.confidence != null ? Number(priorCandidateState.confidence) : null,
    excluded: priorCandidateState.excluded === true,
    _preservedFromContinuation: priorCandidateState._preservedFromContinuation === true,
  };
}

/**
 * Merge two canonical belief states for the same candidateId.
 * @param {object} prior
 * @param {object} incoming
 * @returns {object}
 */
function mergeCandidateBeliefs(prior = {}, incoming = {}) {
  const left = hydrateCandidateBelief(prior) || prior;
  const right = hydrateCandidateBelief(incoming) || incoming;
  const candidateId = canonicalCandidateId(right) || canonicalCandidateId(left);
  if (!candidateId) return left;

  const mergedEval = right.evaluation || left.evaluation || null;
  const merged = {
    ...left,
    ...right,
    candidateId,
    canonicalIdentity: right.canonicalIdentity || left.canonicalIdentity || candidateId,
    identity: {
      ...(left.identity || {}),
      ...(right.identity || {}),
    },
    evidence: mergeEvidenceArrays(left.evidence, right.evidence),
    evidenceRefs: mergeEvidenceArrays(left.evidenceRefs, right.evidenceRefs),
    observations: mergeStringArrays(left.observations, right.observations),
    signals: mergeEvidenceArrays(left.signals, right.signals),
    hypothesisState: right.hypothesisState || left.hypothesisState || null,
    businessFit: right.businessFit || left.businessFit || null,
    qualification: right.qualification || left.qualification || null,
    readiness: right.readiness || left.readiness || null,
    provenance: mergeStringArrays(left.provenance, right.provenance),
    investigationState: {
      ...(left.investigationState || {}),
      ...(right.investigationState || {}),
      missingEvidence: mergeStringArrays(
        (left.investigationState && left.investigationState.missingEvidence) || [],
        (right.investigationState && right.investigationState.missingEvidence) || []
      ),
      unresolvedHypotheses: mergeStringArrays(
        (left.investigationState && left.investigationState.unresolvedHypotheses) || [],
        (right.investigationState && right.investigationState.unresolvedHypotheses) || []
      ),
    },
    evaluation: mergedEval,
    unknowns: mergeStringArrays(left.unknowns, right.unknowns),
    excluded: right.excluded === true || left.excluded === true,
    _preservedFromContinuation:
      right._preservedFromContinuation === true || left._preservedFromContinuation === true,
  };

  return hydrateCandidateBelief(merged);
}

/**
 * Canonical persisted candidate sources for a normalized Scout discovery contribution.
 * Scalar counts (candidateUniverseCount, discoveryReport.candidateUniverse) are excluded.
 * @param {object} payload
 * @returns {Array<{ row: object, source: string }>}
 */
function enumerateCanonicalCandidateSources(payload = {}) {
  const artifact = payload.discoveryArtifact || {};
  const buckets = [
    { rows: payload.rankedProspects, source: 'rankedProspects' },
    { rows: artifact.rankedProspects, source: 'rankedProspects' },
    { rows: payload.opportunities, source: 'opportunities' },
    { rows: payload.companies, source: 'companies' },
    { rows: payload.fitCandidates, source: 'fitCandidates' },
    { rows: artifact.fitCandidates, source: 'fitCandidates' },
    { rows: payload.uncertainCandidates, source: 'uncertainCandidates' },
    { rows: payload.watchCandidates, source: 'uncertainCandidates' },
    { rows: artifact.uncertainCandidates, source: 'uncertainCandidates' },
    { rows: artifact.watchCandidates, source: 'uncertainCandidates' },
    { rows: payload.candidateUniverse, source: 'candidateUniverse' },
    { rows: artifact.candidateUniverse, source: 'candidateUniverse' },
    { rows: payload.prospectEvaluations, source: 'prospectEvaluations' },
    { rows: artifact.prospectEvaluations, source: 'prospectEvaluations' },
  ];

  const entries = [];
  for (const bucket of buckets) {
    if (!Array.isArray(bucket.rows)) continue;
    for (const row of bucket.rows) {
      entries.push({ row, source: bucket.source });
    }
  }
  return entries;
}

function normalizeCandidateRowForBelief(row = {}, source = '') {
  if (source === 'candidateUniverse') {
    return {
      ...row,
      id: row.candidateId || row.candidate_id || row.id,
      candidateId: row.candidateId || row.candidate_id || row.id,
      location: row.address || (Array.isArray(row.cities) ? row.cities[0] : null) || row.location,
      canonicalIdentity: row.canonicalIdentity || row.candidateId || row.candidate_id || row.id,
    };
  }
  if (source === 'prospectEvaluations') {
    return {
      id: row.candidateId || row.companyId || row.id,
      candidateId: row.candidateId || row.companyId || row.id,
      name: row.name || row.companyName || null,
      evaluation: row,
      qualification: row.qualification,
      readiness: row.readiness,
      prospectBucket: row.bucket,
      businessFit: row.businessFit,
      evidenceRefs: row.evidenceRefs,
    };
  }
  if (source === 'rankedProspects') {
    return {
      ...row,
      id: row.id || row.companyId || row.candidateId || row.candidate_id,
      candidateId: row.candidateId || row.candidate_id || row.id || row.companyId,
      name: row.name,
      evaluation: row.evaluation,
      qualification:
        row.qualification ||
        (row.evaluation && row.evaluation.qualification) ||
        (row.qualificationStatus ? { status: row.qualificationStatus } : null),
      readiness:
        row.readiness ||
        (row.evaluation && row.evaluation.readiness) ||
        (row.readinessState ? { status: row.readinessState } : null),
      businessFit: row.businessFit || (row.evaluation && row.evaluation.businessFit),
      evidenceRefs: row.evidenceRefs || row.signals,
    };
  }
  return { ...row };
}

/**
 * Collect and merge canonical beliefs from all payload representations.
 * @param {object} payload
 * @returns {Map<string, object>}
 */
function collectCandidateBeliefsFromPayload(payload = {}) {
  const beliefs = new Map();

  function ingest(row, source) {
    if (!row || typeof row !== 'object') return;
    if (row.dedupeStatus === 'duplicate') return;

    const normalized = normalizeCandidateRowForBelief(row, source);
    const belief = hydrateCandidateBelief(normalized);
    if (!belief) return;
    const key = beliefMapKey(belief) || belief.candidateId.toLowerCase();
    if (beliefs.has(key)) {
      beliefs.set(key, mergeCandidateBeliefs(beliefs.get(key), belief));
    } else {
      beliefs.set(key, belief);
    }
  }

  for (const { row, source } of enumerateCanonicalCandidateSources(payload)) {
    ingest(row, source);
  }

  return beliefs;
}

function countQualifiedBeliefs(beliefs = new Map()) {
  return [...beliefs.values()].filter(
    (row) => row.qualification && row.qualification.status === QUALIFICATION_STATUSES.QUALIFIED
  ).length;
}

function candidateBeliefPersistenceError(message, details = {}) {
  const err = new Error(message);
  err.code = 'CANDIDATE_BELIEF_PERSISTENCE_FAILURE';
  err.details = details;
  return err;
}

/**
 * Fail closed when scalar candidate counts outlive durable candidate records.
 * @param {object} payload
 */
function assertCandidateBeliefPersistence(payload = {}) {
  if (payload.blocked === true) return;

  const beliefs = collectCandidateBeliefsFromPayload(payload);
  const recordCount = beliefs.size;
  const declaredCount =
    payload.candidateUniverseCount != null ? Number(payload.candidateUniverseCount) : null;
  const reportCount =
    payload.discoveryReport &&
    payload.discoveryReport.candidateUniverse != null &&
    !Array.isArray(payload.discoveryReport.candidateUniverse)
      ? Number(payload.discoveryReport.candidateUniverse)
      : null;
  const expectedCount = Math.max(declaredCount || 0, reportCount || 0);

  if (expectedCount > 0 && recordCount === 0) {
    throw candidateBeliefPersistenceError(
      `Candidate belief persistence failure: declared universe of ${expectedCount} has no recoverable candidate records.`,
      {
        declaredCount,
        reportCount,
        recordCount,
      }
    );
  }

  if (expectedCount > 0 && recordCount > 0 && recordCount < expectedCount) {
    throw candidateBeliefPersistenceError(
      `Candidate belief persistence failure: declared universe of ${expectedCount} but only ${recordCount} canonical candidate records are recoverable.`,
      {
        declaredCount,
        reportCount,
        recordCount,
      }
    );
  }
}

/**
 * Convert canonical beliefs to preserved candidate rows for continuation seeding.
 * @param {Map<string, object>|object[]} beliefs
 * @returns {object[]}
 */
function beliefsToPreservedCandidates(beliefs) {
  const rows = beliefs instanceof Map ? [...beliefs.values()] : beliefs;
  return rows.map((belief) => beliefToCompanyRow(belief));
}

function beliefToCompanyRow(belief = {}) {
  const hydrated = hydrateCandidateBelief(belief) || belief;
  const id = hydrated.candidateId;
  const identity = hydrated.identity || {};

  return {
    id,
    candidateId: id,
    companyId: id,
    name: identity.name || id,
    company: identity.name || id,
    industry: identity.industry || null,
    location: identity.location || identity.address || null,
    address: identity.address || identity.location || null,
    website: identity.website || null,
    placeId: identity.placeId || null,
    phone: identity.phone || null,
    origin: hydrated.origin || 'prior_discovery',
    canonicalIdentity: hydrated.canonicalIdentity || id,
    _identityKey: hydrated.canonicalIdentity || id,
    _preservedFromContinuation: true,
    _preservedBelief: hydrated,
    evaluation: hydrated.evaluation || null,
    prospectEvaluation: hydrated.evaluation || null,
    qualification: hydrated.qualification || null,
    readiness: hydrated.readiness || null,
    businessFit: hydrated.businessFit || null,
    hypothesisState: hydrated.hypothesisState || null,
    investigationState: hydrated.investigationState || null,
    evidence: hydrated.evidence || [],
    evidenceRefs: hydrated.evidenceRefs || [],
    observations: hydrated.observations || [],
    signals: hydrated.signals || [],
    unknowns: hydrated.unknowns || [],
    prospectBucket: hydrated.prospectBucket || null,
    qualificationStatus: hydrated.qualification && hydrated.qualification.status,
    readinessState: hydrated.readiness && hydrated.readiness.status,
    fit: hydrated.businessFit && hydrated.businessFit.score,
    basicFit: hydrated.businessFit && hydrated.businessFit.basicFit,
    confidence: hydrated.confidence,
    provenance: hydrated.provenance || [],
  };
}

/**
 * Apply canonical belief onto a company record before classification.
 * @param {object} company
 * @param {object} belief
 * @returns {object}
 */
function applyBeliefToCompany(company = {}, belief = null) {
  const hydrated = belief ? hydrateCandidateBelief(belief) : company._preservedBelief ? hydrateCandidateBelief(company._preservedBelief) : null;
  if (!hydrated) return company;

  const identity = hydrated.identity || {};
  return {
    ...company,
    name: company.name || identity.name,
    location: company.location || identity.location,
    address: company.address || identity.address,
    website: company.website || identity.website,
    placeId: company.placeId || identity.placeId,
    phone: company.phone || identity.phone,
    industry: company.industry || identity.industry,
    signals: mergeEvidenceArrays(hydrated.signals, company.signals),
    evidence: mergeEvidenceArrays(hydrated.evidence, company.evidence),
    prospectEvaluation: hydrated.evaluation || company.prospectEvaluation,
    fitEvaluation: hydrated.businessFit || company.fitEvaluation,
    _preservedBelief: hydrated,
    _preservedFromContinuation: true,
  };
}

/**
 * Seed classified row from preserved belief so re-evaluation does not lose prior evidence.
 * @param {object} classified
 * @param {object} company
 * @returns {object}
 */
function seedClassifiedFromBelief(classified = {}, company = {}) {
  const belief = company._preservedBelief ? hydrateCandidateBelief(company._preservedBelief) : null;
  if (!belief) return classified;

  const identity = belief.identity || {};
  const next = { ...classified };
  next.companyId = next.companyId || company.id || belief.candidateId;
  next.name = next.name || identity.name || company.name;

  if (!next.location && (identity.location || company.location)) {
    next.location = identity.location || company.location;
  }

  next.signals = mergeEvidenceArrays(belief.signals, next.signals);
  next.evidenceRefs = mergeEvidenceArrays(belief.evidenceRefs, next.evidenceRefs);
  next.observations = mergeStringArrays(belief.observations, next.observations);
  next.unknowns = mergeStringArrays(belief.unknowns, next.unknowns);

  if (belief.evaluation) {
    next.evaluation = belief.evaluation;
    next.qualificationStatus = belief.qualification && belief.qualification.status;
    next.readinessState = belief.readiness && belief.readiness.status;
    next.prospectBucket = belief.prospectBucket || (belief.evaluation && belief.evaluation.bucket);
    if (belief.businessFit && belief.businessFit.score != null) {
      next.fit = belief.businessFit.score;
    }
  }

  return next;
}

/**
 * Merge newly discovered provider intelligence into a known company (dedup ≠ discard).
 * @param {object} existing
 * @param {object} discovered
 * @returns {object}
 */
function mergeDiscoveredIntelligence(existing = {}, discovered = {}) {
  const merged = mergeResolved(existing, discovered);
  merged.signals = mergeEvidenceArrays(existing.signals, discovered.signals);
  merged.evidence = mergeEvidenceArrays(existing.evidence, discovered.evidence);
  merged.people = [...(existing.people || []), ...(discovered.people || [])];
  if (existing._preservedBelief) {
    merged._preservedBelief = mergeCandidateBeliefs(existing._preservedBelief, {
      ...discovered,
      signals: discovered.signals,
      evidence: discovered.evidence,
    });
  }
  return merged;
}

/**
 * Partition discovered rows: merge intelligence for known identities, keep new rows.
 * @param {object[]} existingCompanies
 * @param {object[]} discoveredRaw
 * @returns {{ existingCompanies: object[], discoveredRaw: object[] }}
 */
function partitionDiscoveredCandidates(existingCompanies = [], discoveredRaw = []) {
  const existing = [...existingCompanies];
  const novel = [];

  for (const row of discoveredRaw) {
    const matchIndex = existing.findIndex((c) => recordsMatch(c, row));
    if (matchIndex >= 0) {
      existing[matchIndex] = mergeDiscoveredIntelligence(existing[matchIndex], row);
    } else {
      novel.push(row);
    }
  }

  return { existingCompanies: existing, discoveredRaw: novel };
}

/**
 * Project canonical belief onto candidateUniverse record for durable commit.
 * @param {object} input
 * @returns {object}
 */
function projectBeliefToUniverseRecord(input = {}) {
  const { company = {}, classified = {}, priorRecord = null } = input;
  const belief = hydrateCandidateBelief({
    ...(priorRecord || {}),
    ...company,
    ...(classified || {}),
    evaluation: classified.evaluation || company.prospectEvaluation || priorRecord?.evaluation,
    qualification: (classified.evaluation && classified.evaluation.qualification) || company.qualification || priorRecord?.qualification,
    readiness: (classified.evaluation && classified.evaluation.readiness) || company.readiness || priorRecord?.readiness,
    signals: classified.signals || company.signals,
    evidenceRefs: classified.evidenceRefs || company.evidenceRefs,
    unknowns: classified.unknowns || company.unknowns,
    hypothesisState: company.hypothesisState || priorRecord?.hypothesisState,
    investigationState: company.investigationState || priorRecord?.investigationState,
  });

  if (!belief) {
    const id = canonicalCandidateId(company) || canonicalCandidateId(priorRecord);
    return priorRecord || { candidate_id: id, name: company.name || null };
  }

  const identity = belief.identity || {};
  return {
    candidateId: belief.candidateId,
    candidate_id: belief.candidateId,
    canonicalIdentity: belief.canonicalIdentity,
    name: identity.name || company.name || null,
    placeId: identity.placeId || null,
    website: identity.website || null,
    phone: identity.phone || null,
    address: identity.address || identity.location || null,
    origin: belief.origin || priorRecord?.origin || 'existing_intelligence',
    sources: priorRecord?.sources || [company.source || company.discoverySource].filter(Boolean),
    cities: [identity.location || company.location].filter(Boolean),
    confidence: belief.confidence != null ? belief.confidence : priorRecord?.confidence,
    dedupeStatus: priorRecord?.dedupeStatus || 'primary',
    concept: priorRecord?.concept || null,
    evidence: belief.evidence.length ? belief.evidence : priorRecord?.evidence || null,
    evidenceRefs: belief.evidenceRefs.length ? belief.evidenceRefs : priorRecord?.evidenceRefs || null,
    observations: belief.observations.length ? belief.observations : null,
    signals: belief.signals.length ? belief.signals : null,
    qualification: belief.qualification,
    readiness: belief.readiness,
    evaluation: belief.evaluation,
    businessFit: belief.businessFit,
    hypothesisState: belief.hypothesisState,
    investigationState: belief.investigationState,
    unknowns: belief.unknowns.length ? belief.unknowns : null,
    prospectBucket: belief.prospectBucket,
    excluded: belief.excluded === true,
  };
}

/**
 * Build candidateUniverse records with canonical belief from evaluated companies.
 * @param {object[]} companies
 * @param {object[]} classified
 * @param {object[]} priorRecords
 * @returns {object[]}
 */
function buildCandidateUniverseWithBelief(companies = [], classified = [], priorRecords = []) {
  const priorById = new Map();
  for (const row of priorRecords || []) {
    const id = canonicalCandidateId(row);
    if (id) priorById.set(id.toLowerCase(), row);
  }

  const records = [];
  const seen = new Set();

  for (let i = 0; i < companies.length; i += 1) {
    const company = companies[i];
    const row = classified[i] || {};
    const id = canonicalCandidateId(company);
    if (!id || seen.has(id.toLowerCase())) continue;
    seen.add(id.toLowerCase());

    records.push(
      projectBeliefToUniverseRecord({
        company,
        classified: row,
        priorRecord: priorById.get(id.toLowerCase()) || null,
      })
    );
  }

  for (const prior of priorRecords || []) {
    const id = canonicalCandidateId(prior);
    if (!id || seen.has(id.toLowerCase()) || prior.dedupeStatus === 'duplicate') continue;
    seen.add(id.toLowerCase());
    records.push(projectBeliefToUniverseRecord({ company: {}, classified: {}, priorRecord: prior }));
  }

  return records;
}

/**
 * Rebuild prospectEvaluations and bucket projections from post-investigation classified state.
 * @param {object} input
 * @returns {object}
 */
function rebuildProspectProjections(input = {}) {
  const {
    classified = [],
    companies = [],
    searchDefinition = {},
    now = Date.now(),
    OPPORTUNITY_CLASSES,
    PROSPECT_BUCKETS: Buckets,
    READINESS_STATES: Readiness,
    QUALIFICATION_STATUSES: QualStatuses,
  } = input;

  const supported = [];
  const fitCandidates = [];
  const watchCandidates = [];
  const uncertainCandidates = [];
  const prospectEvaluations = [];
  let basicFitCount = 0;
  let signalBearingCount = 0;

  classified.forEach((row, index) => {
    const company = companies[index] || {};
    const attached = attachFitToClassified(row, company, searchDefinition, now);
    const next = attached.classified;
    const evaluation =
      attached.evaluation ||
      buildProspectEvaluation({
        candidate: company,
        classified: next,
        fit: attached.fit,
        qualification: attached.qualification,
        searchDefinition,
      });

    next.evaluation = evaluation;
    companies[index] = {
      ...company,
      prospectEvaluation: evaluation,
      fitEvaluation: attached.fit,
      lastEvaluatedAt: attached.lastEvaluatedAt,
    };

    prospectEvaluations.push(evaluation);

    const qualification = evaluation.qualification || attached.qualification;
    if (qualification.basicFit || attached.fit.basicFit) basicFitCount += 1;
    if (qualification.signalBearing) signalBearingCount += 1;

    if (evaluation.bucket === Buckets.HIGH_PRIORITY || qualification.supported) {
      next.classification = OPPORTUNITY_CLASSES.SUPPORTED;
      supported.push(next);
      return;
    }

    if (evaluation.bucket === Buckets.INVESTIGATION_REQUIRED) {
      fitCandidates.push(next);
    } else if (evaluation.bucket === Buckets.NURTURE) {
      watchCandidates.push(next);
    } else if (evaluation.bucket === Buckets.FIT_INVESTIGATION) {
      uncertainCandidates.push(next);
      watchCandidates.push(next);
    } else if (next.classification === OPPORTUNITY_CLASSES.FIT) {
      fitCandidates.push(next);
    } else if (next.classification === OPPORTUNITY_CLASSES.WATCH) {
      watchCandidates.push(next);
    }
  });

  const qualifiedProspectCount = businessFitQualifiedCount(prospectEvaluations);
  const readinessUnknownCount = prospectEvaluations.filter(
    (row) =>
      row.qualification.status === QualStatuses.QUALIFIED &&
      row.readiness.status === Readiness.UNKNOWN
  ).length;
  const readinessNotReadyCount = prospectEvaluations.filter(
    (row) =>
      row.qualification.status === QualStatuses.QUALIFIED &&
      row.readiness.status === Readiness.NOT_READY
  ).length;

  return {
    classified,
    companies,
    supported,
    fitCandidates,
    watchCandidates,
    uncertainCandidates,
    prospectEvaluations,
    basicFitCount,
    signalBearingCount,
    qualifiedProspectCount,
    readinessUnknownCount,
    readinessNotReadyCount,
  };
}

function isRecognizedCandidateTransition(prior = {}, next = null) {
  if (!next) {
    if (prior.excluded === true) return true;
    const status = prior.qualification && prior.qualification.status;
    if (status === QUALIFICATION_STATUSES.NOT_QUALIFIED && prior.excludedReason) return true;
    return false;
  }

  if (prior.excluded === true || next.excluded === true) return true;

  const priorStatus = prior.qualification && prior.qualification.status;
  const nextStatus = next.qualification && next.qualification.status;
  if (priorStatus === QUALIFICATION_STATUSES.QUALIFIED && nextStatus !== QUALIFICATION_STATUSES.QUALIFIED) {
    const reasonCode = next.qualification && next.qualification.reasonCode;
    const reason = next.qualification && next.qualification.reason;
    const contradictoryReasonCodes = new Set([
      'segment_mismatch',
      'excluded_segment',
      'negative_segment_evidence',
      'geography_mismatch',
      'not_in_service_area',
    ]);
    if (reasonCode && contradictoryReasonCodes.has(reasonCode)) return true;
    if (reason && /exclusive|does not|not (?:a |an )?|residential only|no str|no vacation/i.test(reason)) {
      return true;
    }
    return false;
  }

  return true;
}

function findBeliefMatchForPrior(prior = {}, nextBeliefs = new Map()) {
  const direct = nextBeliefs.get(beliefMapKey(prior));
  if (direct) return direct;
  for (const next of nextBeliefs.values()) {
    if (
      recordsMatch(
        { id: prior.candidateId, name: prior.identity && prior.identity.name },
        { id: next.candidateId, name: next.identity && next.identity.name }
      )
    ) {
      return next;
    }
  }
  return null;
}

/**
 * Detect destructive belief regression (qualified collapse or durable universe disappearance).
 * @param {object} input
 * @returns {{ violation: boolean, message: string|null, priorQualified: number, nextQualified: number, priorTotal?: number, nextTotal?: number }}
 */
function checkBeliefRegressionIntegrity(input = {}) {
  const priorBeliefs = input.priorBeliefs instanceof Map ? input.priorBeliefs : collectCandidateBeliefsFromPayload(input.priorPayload || {});
  const nextBeliefs = input.nextBeliefs instanceof Map ? input.nextBeliefs : collectCandidateBeliefsFromPayload(input.nextPayload || {});

  const priorQualified = countQualifiedBeliefs(priorBeliefs);
  const nextQualified = countQualifiedBeliefs(nextBeliefs);
  const priorTotal = priorBeliefs.size;
  const nextTotal = nextBeliefs.size;

  if (priorTotal > 0 && nextTotal === 0) {
    return {
      violation: true,
      message: `Candidate belief regression: ${priorTotal} hydrated candidates collapsed to 0 without attributable evidence.`,
      priorQualified,
      nextQualified,
      priorTotal,
      nextTotal,
      lostCandidates: [...priorBeliefs.values()].map((row) => ({
        candidateId: row.candidateId,
        name: row.identity && row.identity.name,
      })),
    };
  }

  if (priorTotal > 0 && nextTotal > 0 && nextTotal < priorTotal) {
    const unexplainedLoss = [];
    for (const [, prior] of priorBeliefs.entries()) {
      const next = findBeliefMatchForPrior(prior, nextBeliefs);
      if (!next && !isRecognizedCandidateTransition(prior, null)) {
        unexplainedLoss.push({
          candidateId: prior.candidateId,
          name: prior.identity && prior.identity.name,
        });
      }
    }
    if (unexplainedLoss.length === priorTotal) {
      return {
        violation: true,
        message: `Candidate belief regression: all ${priorTotal} hydrated candidates disappeared without recognized evidence-backed transitions.`,
        priorQualified,
        nextQualified,
        priorTotal,
        nextTotal,
        lostCandidates: unexplainedLoss,
      };
    }
  }

  if (priorQualified === 0 || nextQualified >= priorQualified) {
    return { violation: false, message: null, priorQualified, nextQualified, priorTotal, nextTotal };
  }

  const lost = [];
  for (const [key, prior] of priorBeliefs.entries()) {
    if (!prior.qualification || prior.qualification.status !== QUALIFICATION_STATUSES.QUALIFIED) continue;
    const next = nextBeliefs.get(key) || findBeliefMatchForPrior(prior, nextBeliefs);
    if (!next || !next.qualification || next.qualification.status !== QUALIFICATION_STATUSES.QUALIFIED) {
      if (!isRecognizedCandidateTransition(prior, next)) {
        lost.push({ candidateId: prior.candidateId, name: prior.identity && prior.identity.name });
      }
    }
  }

  if (lost.length === priorQualified && priorQualified > 0) {
    return {
      violation: true,
      message: `Belief regression: ${priorQualified} qualified candidates collapsed to ${nextQualified} without per-candidate contradictory evidence.`,
      priorQualified,
      nextQualified,
      priorTotal,
      nextTotal,
      lostCandidates: lost,
    };
  }

  return { violation: false, message: null, priorQualified, nextQualified, priorTotal, nextTotal, lostCandidates: lost };
}

/**
 * Preserve prior qualification when re-evaluation would regress without contradictory evidence.
 * @param {object} attached - result from attachFitToClassified
 * @param {object} company
 * @returns {object}
 */
function reconcilePreservedEvaluation(attached = {}, company = {}) {
  const belief = company._preservedBelief ? hydrateCandidateBelief(company._preservedBelief) : null;
  if (!belief || !belief.evaluation || !belief.qualification) return attached;

  const priorStatus = belief.qualification.status;
  const nextEval = attached.evaluation || {};
  const nextStatus = nextEval.qualification && nextEval.qualification.status;

  if (priorStatus !== QUALIFICATION_STATUSES.QUALIFIED) return attached;
  if (nextStatus === QUALIFICATION_STATUSES.QUALIFIED) return attached;

  const reasonCode = nextEval.qualification && nextEval.qualification.reasonCode;
  const reason = nextEval.qualification && nextEval.qualification.reason;
  const contradictoryReasonCodes = new Set([
    'segment_mismatch',
    'excluded_segment',
    'negative_segment_evidence',
    'geography_mismatch',
    'not_in_service_area',
  ]);
  const hasContradictory =
    (reasonCode && contradictoryReasonCodes.has(reasonCode)) ||
    (reason && /exclusive|does not|not (?:a |an )?|residential only|no str|no vacation/i.test(reason));

  if (hasContradictory) return attached;

  const preservedEval = {
    ...belief.evaluation,
    readiness: nextEval.readiness || belief.evaluation.readiness,
    investigation: nextEval.investigation || belief.evaluation.investigation,
  };

  return {
    ...attached,
    classified: {
      ...attached.classified,
      evaluation: preservedEval,
      qualificationStatus: preservedEval.qualification && preservedEval.qualification.status,
      readinessState: preservedEval.readiness && preservedEval.readiness.status,
      prospectBucket: preservedEval.bucket,
      location: attached.classified.location || (belief.identity && belief.identity.location),
    },
    evaluation: preservedEval,
    qualification: preservedEval.qualification,
  };
}

module.exports = {
  canonicalCandidateId,
  hydrateCandidateBelief,
  mergeCandidateBeliefs,
  mergeEvidenceArrays,
  enumerateCanonicalCandidateSources,
  collectCandidateBeliefsFromPayload,
  countQualifiedBeliefs,
  assertCandidateBeliefPersistence,
  candidateBeliefPersistenceError,
  beliefsToPreservedCandidates,
  beliefToCompanyRow,
  applyBeliefToCompany,
  seedClassifiedFromBelief,
  mergeDiscoveredIntelligence,
  partitionDiscoveredCandidates,
  projectBeliefToUniverseRecord,
  buildCandidateUniverseWithBelief,
  rebuildProspectProjections,
  checkBeliefRegressionIntegrity,
  reconcilePreservedEvaluation,
};
