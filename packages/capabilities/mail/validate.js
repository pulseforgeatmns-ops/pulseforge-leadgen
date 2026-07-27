'use strict';

/**
 * Mail package validation — Ready to Print vs Needs Review (SPEC-033).
 */

const {
  PACKAGE_STATUS,
  DEFAULT_CONFIDENCE_THRESHOLD,
  findPlaceholders,
} = require('./types');

/**
 * Normalize mailing address from prospect / intelligence shapes.
 * @param {object} prospect
 * @returns {string}
 */
function resolveMailingAddress(prospect = {}) {
  const candidates = [
    prospect.mailingAddress,
    prospect.verifiedAddress,
    prospect.address,
    prospect.formattedAddress,
    prospect.location && prospect.location.address,
    intelligenceAddress(prospect.companyIntelligence || prospect.intelligence),
  ];
  for (const c of candidates) {
    if (c != null && String(c).trim()) return String(c).trim();
  }
  return '';
}

/**
 * @param {object|null|undefined} intel
 * @returns {string}
 */
function intelligenceAddress(intel) {
  if (!intel || typeof intel !== 'object') return '';
  return (
    intel.mailingAddress ||
    intel.address ||
    (intel.location && intel.location.address) ||
    ''
  );
}

/**
 * Recipient with company fallback.
 * @param {object} prospect
 * @returns {{ name: string, usedCompanyFallback: boolean }}
 */
function resolveRecipient(prospect = {}) {
  const fromDecision =
    prospect.recipientName ||
    prospect.contactName ||
    prospect.decisionMaker ||
    (Array.isArray(prospect.decisionMakers) &&
      prospect.decisionMakers[0] &&
      (prospect.decisionMakers[0].name || prospect.decisionMakers[0])) ||
    (prospect.companyIntelligence &&
      Array.isArray(prospect.companyIntelligence.decisionMakers) &&
      prospect.companyIntelligence.decisionMakers[0] &&
      (prospect.companyIntelligence.decisionMakers[0].name ||
        prospect.companyIntelligence.decisionMakers[0]));

  if (fromDecision != null && String(fromDecision).trim()) {
    return { name: String(fromDecision).trim(), usedCompanyFallback: false };
  }

  const company = String(prospect.companyName || prospect.name || '').trim();
  if (company) {
    return { name: company, usedCompanyFallback: true };
  }
  return { name: '', usedCompanyFallback: false };
}

/**
 * @param {object} prospect
 * @returns {string}
 */
function resolveCompanyName(prospect = {}) {
  return String(prospect.companyName || prospect.name || '').trim();
}

/**
 * Validate a single prospect for mail readiness.
 * @param {object} prospect
 * @param {object} [opts]
 * @returns {{
 *   ok: boolean,
 *   status: string,
 *   reasons: string[],
 *   warnings: string[],
 *   mailingAddress: string,
 *   companyName: string,
 *   recipientName: string,
 *   usedCompanyFallback: boolean,
 *   missingAddress: boolean,
 *   confidence: number,
 * }}
 */
function validateProspectForMail(prospect = {}, opts = {}) {
  const threshold =
    Number.isFinite(Number(opts.confidenceThreshold))
      ? Number(opts.confidenceThreshold)
      : DEFAULT_CONFIDENCE_THRESHOLD;

  const reasons = [];
  const warnings = [];
  const companyName = resolveCompanyName(prospect);
  const mailingAddress = resolveMailingAddress(prospect);
  const recipient = resolveRecipient(prospect);
  const confidence = Number.isFinite(Number(opts.confidence))
    ? Number(opts.confidence)
    : Number.isFinite(Number(prospect.personalizationConfidence))
      ? Number(prospect.personalizationConfidence)
      : Number.isFinite(Number(prospect.confidence))
        ? Number(prospect.confidence)
        : 0;

  if (prospect.skipped || opts.skipped) {
    return {
      ok: false,
      status: PACKAGE_STATUS.SKIPPED,
      reasons: ['skipped'],
      warnings,
      mailingAddress,
      companyName,
      recipientName: recipient.name,
      usedCompanyFallback: recipient.usedCompanyFallback,
      missingAddress: !mailingAddress,
      confidence,
    };
  }

  if (prospect.addressInvalid || opts.addressInvalid) {
    reasons.push('address_marked_invalid');
  }
  if (!mailingAddress) {
    reasons.push('missing_mailing_address');
  }
  if (!companyName) {
    reasons.push('missing_company_name');
  }
  if (!recipient.name) {
    reasons.push('missing_recipient');
  } else if (recipient.usedCompanyFallback) {
    warnings.push('Recipient missing — using company name as envelope addressee');
  }
  if (confidence < threshold) {
    reasons.push('personalization_confidence_below_threshold');
    warnings.push(
      `Letter confidence ${confidence.toFixed(2)} below threshold ${threshold.toFixed(2)}`
    );
  }

  const letterText = opts.letterText != null ? String(opts.letterText) : '';
  if (letterText) {
    const placeholders = findPlaceholders(letterText);
    if (placeholders.length) {
      reasons.push('placeholder_text');
      warnings.push(`Letter contains placeholder patterns: ${placeholders.join(', ')}`);
    }
  }

  const missingAddress = !mailingAddress || reasons.includes('address_marked_invalid');
  const ok = reasons.length === 0;

  return {
    ok,
    status: ok ? PACKAGE_STATUS.READY_TO_PRINT : PACKAGE_STATUS.NEEDS_REVIEW,
    reasons,
    warnings,
    mailingAddress,
    companyName,
    recipientName: recipient.name,
    usedCompanyFallback: recipient.usedCompanyFallback,
    missingAddress,
    confidence,
  };
}

/**
 * Aggregate campaign counts from packages.
 * @param {object[]} packages
 * @returns {{ prospects: number, readyToPrint: number, needsReview: number, missingAddresses: number }}
 */
function summarizePackageStatuses(packages = []) {
  let readyToPrint = 0;
  let needsReview = 0;
  let missingAddresses = 0;
  for (const pkg of packages) {
    if (pkg.status === PACKAGE_STATUS.SKIPPED) continue;
    if (pkg.status === PACKAGE_STATUS.READY_TO_PRINT || pkg.status === PACKAGE_STATUS.APPROVED) {
      readyToPrint += 1;
    } else {
      needsReview += 1;
    }
    if (pkg.addressInvalid || (pkg.warnings || []).some((w) => /address/i.test(w))) {
      missingAddresses += 1;
    } else if (!pkg.envelope || !pkg.envelope.mailingAddress) {
      missingAddresses += 1;
    } else if (
      Array.isArray(pkg.personalizationSummary && pkg.personalizationSummary.missingDataWarnings) &&
      pkg.personalizationSummary.missingDataWarnings.some((w) => /address/i.test(w))
    ) {
      missingAddresses += 1;
    }
  }
  const inScope = packages.filter((p) => p.status !== PACKAGE_STATUS.SKIPPED).length;
  return {
    prospects: inScope,
    readyToPrint,
    needsReview,
    missingAddresses,
  };
}

module.exports = {
  resolveMailingAddress,
  resolveRecipient,
  resolveCompanyName,
  validateProspectForMail,
  summarizePackageStatuses,
};
