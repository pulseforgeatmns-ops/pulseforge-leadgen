'use strict';

/**
 * Mail Package Generator types (SPEC-033).
 */

const PACKAGE_STATUS = Object.freeze({
  READY_TO_PRINT: 'ready_to_print',
  NEEDS_REVIEW: 'needs_review',
  SKIPPED: 'skipped',
  APPROVED: 'approved',
});

const MAIL_PROGRESS_STAGES = Object.freeze({
  GATHERING: 'Gathering campaign context',
  VALIDATING: 'Validating prospect mail data',
  COMPOSING: 'Composing personalized letters',
  RENDERING: 'Rendering print assets',
  EXPORTING: 'Building export files',
  COMPLETED: 'Completed',
});

const OPERATOR_ACTIONS = Object.freeze([
  'edit_letter',
  'regenerate_letter',
  'skip_prospect',
  'mark_address_invalid',
  'replace_recipient',
  'approve_package',
]);

/** Default insert kit for commercial cleaning direct mail. */
const DEFAULT_INSERT_CHECKLIST = Object.freeze([
  { id: 'letter', label: 'Letter', required: true },
  { id: 'business_card', label: 'Business Card', required: true },
  { id: 'brochure', label: 'Brochure', required: false },
  { id: 'microfiber_cloth', label: 'Microfiber Cloth', required: false },
  { id: 'coupon', label: 'Coupon', required: false },
  { id: 'handwritten_note', label: 'Handwritten Note', required: false },
]);

/** Minimum personalization confidence for Ready to Print. */
const DEFAULT_CONFIDENCE_THRESHOLD = 0.65;

/** Deterministic time estimates (seconds). */
const PRINT_SECONDS_PER_LETTER = 12;
const ASSEMBLY_SECONDS_PER_PACKAGE = 45;

const PLACEHOLDER_PATTERNS = Object.freeze([
  /\[insert[^\]]*\]/i,
  /\bTODO\b/,
  /\bTBD\b/,
  /\blorem ipsum\b/i,
  /\bplaceholder\b/i,
  /\byour company here\b/i,
  /\b\[company\]\b/i,
  /\b\[name\]\b/i,
]);

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildLetter(partial = {}) {
  return {
    recipientName: String(partial.recipientName || '').trim(),
    companyName: String(partial.companyName || '').trim(),
    personalizedOpening: String(partial.personalizedOpening || '').trim(),
    valueProposition: String(partial.valueProposition || '').trim(),
    cta: String(partial.cta || '').trim(),
    signature: String(partial.signature || '').trim(),
    body: String(partial.body || '').trim(),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildEnvelope(partial = {}) {
  return {
    recipientName: String(partial.recipientName || '').trim(),
    companyName: String(partial.companyName || '').trim(),
    mailingAddress: String(partial.mailingAddress || '').trim(),
    returnAddress: String(partial.returnAddress || '').trim(),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildPersonalizationSummary(partial = {}) {
  return {
    whySelected: String(partial.whySelected || '').trim(),
    personalizationFacts: Array.isArray(partial.personalizationFacts)
      ? partial.personalizationFacts.map(String)
      : [],
    letterConfidence: Number.isFinite(Number(partial.letterConfidence))
      ? Number(partial.letterConfidence)
      : 0,
    missingDataWarnings: Array.isArray(partial.missingDataWarnings)
      ? partial.missingDataWarnings.map(String)
      : [],
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildInsertItem(partial = {}) {
  return {
    id: String(partial.id || ''),
    label: String(partial.label || partial.id || ''),
    required: Boolean(partial.required),
    included: partial.included !== false,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildMailPackage(partial = {}) {
  return {
    id: String(partial.id || ''),
    prospectId: partial.prospectId != null ? String(partial.prospectId) : null,
    status: partial.status || PACKAGE_STATUS.NEEDS_REVIEW,
    letter: buildLetter(partial.letter || {}),
    envelope: buildEnvelope(partial.envelope || {}),
    personalizationSummary: buildPersonalizationSummary(
      partial.personalizationSummary || {}
    ),
    insertChecklist: Array.isArray(partial.insertChecklist)
      ? partial.insertChecklist.map(buildInsertItem)
      : DEFAULT_INSERT_CHECKLIST.map((i) => buildInsertItem({ ...i, included: true })),
    confidence: Number.isFinite(Number(partial.confidence))
      ? Number(partial.confidence)
      : 0,
    warnings: Array.isArray(partial.warnings) ? partial.warnings.map(String) : [],
    revision: Number.isFinite(Number(partial.revision)) ? Number(partial.revision) : 1,
    skipped: Boolean(partial.skipped),
    addressInvalid: Boolean(partial.addressInvalid),
    approved: Boolean(partial.approved),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildCampaignSummary(partial = {}) {
  const prospects = Number(partial.prospects) || 0;
  const readyToPrint = Number(partial.readyToPrint) || 0;
  const needsReview = Number(partial.needsReview) || 0;
  const missingAddresses = Number(partial.missingAddresses) || 0;
  const insertCount = Number(partial.insertCount) || DEFAULT_INSERT_CHECKLIST.length;
  const estimatedPrintTimeSec = readyToPrint * PRINT_SECONDS_PER_LETTER;
  const estimatedAssemblyTimeSec = Math.round(
    readyToPrint * ASSEMBLY_SECONDS_PER_PACKAGE * (0.7 + insertCount * 0.05)
  );
  return {
    prospects,
    readyToPrint,
    needsReview,
    missingAddresses,
    estimatedPrintTimeSec:
      partial.estimatedPrintTimeSec != null
        ? Number(partial.estimatedPrintTimeSec)
        : estimatedPrintTimeSec,
    estimatedAssemblyTimeSec:
      partial.estimatedAssemblyTimeSec != null
        ? Number(partial.estimatedAssemblyTimeSec)
        : estimatedAssemblyTimeSec,
    estimatedPrintTimeLabel: formatDuration(
      partial.estimatedPrintTimeSec != null
        ? Number(partial.estimatedPrintTimeSec)
        : estimatedPrintTimeSec
    ),
    estimatedAssemblyTimeLabel: formatDuration(
      partial.estimatedAssemblyTimeSec != null
        ? Number(partial.estimatedAssemblyTimeSec)
        : estimatedAssemblyTimeSec
    ),
  };
}

/**
 * @param {number} seconds
 * @returns {string}
 */
function formatDuration(seconds) {
  const s = Math.max(0, Math.round(Number(seconds) || 0));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) return rem ? `${m}m ${rem}s` : `${m}m`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return rm ? `${h}h ${rm}m` : `${h}h`;
}

/**
 * @param {string} text
 * @returns {string[]}
 */
function findPlaceholders(text) {
  const hits = [];
  for (const re of PLACEHOLDER_PATTERNS) {
    if (re.test(text)) hits.push(re.source);
  }
  return hits;
}

module.exports = {
  PACKAGE_STATUS,
  MAIL_PROGRESS_STAGES,
  OPERATOR_ACTIONS,
  DEFAULT_INSERT_CHECKLIST,
  DEFAULT_CONFIDENCE_THRESHOLD,
  PRINT_SECONDS_PER_LETTER,
  ASSEMBLY_SECONDS_PER_PACKAGE,
  PLACEHOLDER_PATTERNS,
  buildLetter,
  buildEnvelope,
  buildPersonalizationSummary,
  buildInsertItem,
  buildMailPackage,
  buildCampaignSummary,
  formatDuration,
  findPlaceholders,
};
