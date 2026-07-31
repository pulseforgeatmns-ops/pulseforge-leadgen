'use strict';

/**
 * Prospect Acquisition domain types (SPEC-060 / ADR-044).
 * Providers publish Candidates. Verification owns ProspectList creation.
 */

const ACQUISITION_SOURCES = Object.freeze({
  DISCOVERY: 'discovery',
  GOOGLE_PLACES: 'google_places',
  MANUAL: 'manual_prospect_list',
  CSV_IMPORT: 'csv_import',
  SPREADSHEET_PASTE: 'spreadsheet_paste',
  EXISTING_REPOSITORY: 'existing_prospect_repository',
  FIXTURE: 'fixture',
});

const ACQUISITION_STRATEGIES = Object.freeze({
  DISCOVERY: 'discovery',
  MANUAL: 'manual',
  CSV: 'csv',
  EXISTING: 'existing',
});

const IMPORT_METHODS = Object.freeze({
  NONE: null,
  MANUAL_ENTRY: 'manual_entry',
  CSV: 'csv',
  SPREADSHEET_PASTE: 'spreadsheet_paste',
  EXISTING_LIST: 'existing_list',
  PROVIDER_SEARCH: 'provider_search',
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildProvenance(partial = {}) {
  return {
    acquisitionSource:
      partial.acquisitionSource ||
      partial.source ||
      ACQUISITION_SOURCES.MANUAL,
    acquisitionTime:
      partial.acquisitionTime ||
      partial.acquiredAt ||
      new Date().toISOString(),
    missionId: partial.missionId != null ? String(partial.missionId) : null,
    provider: partial.provider != null ? String(partial.provider) : null,
    importMethod:
      partial.importMethod != null
        ? partial.importMethod
        : IMPORT_METHODS.NONE,
    operator: partial.operator != null ? String(partial.operator) : null,
    originalData:
      partial.originalData && typeof partial.originalData === 'object'
        ? partial.originalData
        : partial.originalData != null
          ? { value: partial.originalData }
          : null,
  };
}

/**
 * Normalize a raw hit into a Candidate record.
 * @param {object|string} raw
 * @param {object} [opts]
 * @returns {object}
 */
function buildCandidate(raw, opts = {}) {
  const index = Number.isFinite(Number(opts.index)) ? Number(opts.index) : 0;
  const provenance = buildProvenance(opts.provenance || opts);

  if (raw == null) {
    return emptyCandidate(index, provenance);
  }

  if (typeof raw === 'string') {
    const companyName = String(raw).trim();
    return {
      id: `cand_${index + 1}`,
      companyName,
      website: null,
      address: null,
      phone: null,
      email: null,
      contactName: null,
      notes: null,
      naics: null,
      industry: null,
      metadata: {},
      provenance,
      status: 'candidate',
    };
  }

  const obj = typeof raw === 'object' ? raw : {};
  const companyName = String(
    obj.companyName ||
      obj.company ||
      obj.name ||
      obj.businessName ||
      obj.business_name ||
      ''
  ).trim();

  const unknown = { ...obj };
  for (const key of [
    'id',
    'companyName',
    'company',
    'name',
    'businessName',
    'business_name',
    'website',
    'url',
    'domain',
    'address',
    'street',
    'location',
    'phone',
    'telephone',
    'email',
    'contactName',
    'contact',
    'notes',
    'naics',
    'industry',
    'vertical',
    'source',
    'provenance',
    'metadata',
    'status',
  ]) {
    delete unknown[key];
  }

  return {
    id: String(obj.id || obj.candidateId || `cand_${index + 1}`),
    companyName,
    website: emptyToNull(obj.website || obj.url || obj.domain),
    address: emptyToNull(obj.address || obj.street || obj.location),
    phone: emptyToNull(obj.phone || obj.telephone),
    email: emptyToNull(obj.email),
    contactName: emptyToNull(obj.contactName || obj.contact),
    notes: emptyToNull(obj.notes),
    naics: emptyToNull(obj.naics),
    industry: emptyToNull(obj.industry || obj.vertical),
    metadata: {
      ...(obj.metadata && typeof obj.metadata === 'object' ? obj.metadata : {}),
      ...unknown,
    },
    provenance: buildProvenance({
      ...provenance,
      originalData:
        provenance.originalData ||
        (opts.includeOriginal === false ? null : sanitizeOriginal(obj)),
    }),
    status: 'candidate',
  };
}

/**
 * @param {object} [input]
 * @returns {object}
 */
function buildCandidateSet(input = {}) {
  const candidates = (Array.isArray(input.candidates) ? input.candidates : [])
    .map((c, i) =>
      c && c.provenance
        ? {
            ...c,
            companyName: String(c.companyName || '').trim(),
            status: c.status || 'candidate',
          }
        : buildCandidate(c, {
            index: i,
            provenance: input.provenance || {
              acquisitionSource: input.acquisitionSource || input.provider,
              provider: input.provider,
              missionId: input.missionId,
              operator: input.operator,
              importMethod: input.importMethod,
            },
          })
    )
    .filter((c) => c && String(c.companyName || '').trim());

  return {
    type: 'CandidateSet',
    candidates,
    candidateCount: candidates.length,
    provider: input.provider || null,
    acquisitionSource:
      input.acquisitionSource ||
      (candidates[0] &&
        candidates[0].provenance &&
        candidates[0].provenance.acquisitionSource) ||
      null,
    missionId: input.missionId != null ? String(input.missionId) : null,
    evidence: Array.isArray(input.evidence) ? input.evidence : [],
    warnings: Array.isArray(input.warnings) ? input.warnings.map(String) : [],
    summary: {
      acquired: candidates.length,
      provider: input.provider || null,
      acquisitionSource: input.acquisitionSource || null,
      ...(input.summary && typeof input.summary === 'object'
        ? input.summary
        : {}),
    },
  };
}

/**
 * @param {object} [input]
 * @returns {object}
 */
function buildAcquisitionEvidence(input = {}) {
  return {
    kind: 'prospect_acquisition',
    provider: input.provider || null,
    acquisitionSource: input.acquisitionSource || null,
    candidateCount:
      input.candidateCount != null ? Number(input.candidateCount) : 0,
    importMethod: input.importMethod || null,
    operator: input.operator || null,
    missionId: input.missionId || null,
    summary: input.summary || 'Prospect acquisition completed',
    at: input.at || new Date().toISOString(),
    details:
      input.details && typeof input.details === 'object' ? input.details : {},
  };
}

/**
 * @param {object} acquireResult
 * @returns {boolean}
 */
function isValidAcquireResult(acquireResult) {
  if (!acquireResult || typeof acquireResult !== 'object') return false;
  if (!Array.isArray(acquireResult.candidates)) return false;
  return true;
}

function emptyCandidate(index, provenance) {
  return {
    id: `cand_${index + 1}`,
    companyName: '',
    website: null,
    address: null,
    phone: null,
    email: null,
    contactName: null,
    notes: null,
    naics: null,
    industry: null,
    metadata: {},
    provenance,
    status: 'candidate',
  };
}

function emptyToNull(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s ? s : null;
}

function sanitizeOriginal(obj) {
  try {
    return JSON.parse(JSON.stringify(obj));
  } catch {
    return { note: 'unserializable_original' };
  }
}

module.exports = {
  ACQUISITION_SOURCES,
  ACQUISITION_STRATEGIES,
  IMPORT_METHODS,
  buildProvenance,
  buildCandidate,
  buildCandidateSet,
  buildAcquisitionEvidence,
  isValidAcquireResult,
};
