'use strict';

/**
 * Proposal Generator types (SPEC-027B / ADR-014).
 */

const PROPOSAL_STATUS = Object.freeze({
  DRAFT: 'draft',
  REVIEW: 'review',
  APPROVED: 'approved',
  SENT: 'sent',
  WON: 'won',
  LOST: 'lost',
});

const PROPOSAL_PROGRESS_STAGES = Object.freeze({
  GATHERING: 'Gathering discovery context',
  COMPOSING: 'Composing personalized sections',
  PRICING: 'Applying investment package',
  RENDERING: 'Rendering proposal',
  COMPLETED: 'Completed',
});

const OPERATOR_ACTIONS = Object.freeze([
  'edit_pricing',
  'edit_timeline',
  'edit_strategy',
  'edit_recommendations',
  'edit_closing',
  'edit_notes',
  'approve',
  'reject',
  'regenerate',
]);

const SECTION_IDS = Object.freeze([
  'cover',
  'executive_summary',
  'understanding',
  'why_pulseforge',
  'recommended_strategy',
  'what_we_handle',
  'your_role',
  'first_90_days',
  'long_term_advantage',
  'investment',
  'next_steps',
]);

/** Approved messaging block for Long-Term Advantage (SPEC-027B §9). */
const LONG_TERM_ADVANTAGE_BLOCK =
  'Every campaign improves future targeting through evidence and market learning. ' +
  'As outreach runs, Pulseforge records what resonates, which segments convert, and where follow-up stalls — ' +
  'then feeds that signal back into discovery, ranking, and campaign refinement. ' +
  'The advantage compounds: each month of work makes the next month more precise.';

const PLACEHOLDER_PATTERNS = Object.freeze([
  /\[insert[^\]]*\]/i,
  /\bTODO\b/,
  /\bTBD\b/,
  /\blorem ipsum\b/i,
  /\bplaceholder\b/i,
  /\byour company here\b/i,
  /\b\[company\]\b/i,
  /\b\[client\]\b/i,
]);

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildDiscoverySummary(partial = {}) {
  return {
    companyName: String(partial.companyName || '').trim(),
    contactName: partial.contactName != null ? String(partial.contactName).trim() : null,
    industry: partial.industry != null ? String(partial.industry).trim() : null,
    geography: partial.geography != null ? String(partial.geography).trim() : null,
    companyStage: partial.companyStage != null ? String(partial.companyStage).trim() : null,
    currentClients: normalizeList(partial.currentClients),
    revenue: partial.revenue != null ? String(partial.revenue).trim() : null,
    currentMarketingChannels: normalizeList(partial.currentMarketingChannels),
    icp: normalizeList(partial.icp),
    currentProcess: partial.currentProcess != null ? String(partial.currentProcess).trim() : null,
    challenges: normalizeList(partial.challenges),
    goals: normalizeList(partial.goals),
    growthVision: partial.growthVision != null ? String(partial.growthVision).trim() : null,
    notes: partial.notes != null ? String(partial.notes).trim() : null,
  };
}

/**
 * @param {unknown} value
 * @returns {string[]}
 */
function normalizeList(value) {
  if (value == null || value === '') return [];
  if (Array.isArray(value)) {
    return value.map((v) => String(v).trim()).filter(Boolean);
  }
  const s = String(value).trim();
  if (!s) return [];
  if (s.includes('\n')) {
    return s
      .split(/\n+/)
      .map((x) => x.replace(/^[-*•]\s*/, '').trim())
      .filter(Boolean);
  }
  if (s.includes(';')) {
    return s
      .split(';')
      .map((x) => x.trim())
      .filter(Boolean);
  }
  if (s.includes(',') && s.length < 200) {
    return s
      .split(',')
      .map((x) => x.trim())
      .filter(Boolean);
  }
  return [s];
}

/**
 * @param {object} section
 * @returns {object}
 */
function buildSection(section) {
  return {
    id: String(section.id),
    title: String(section.title || ''),
    body: String(section.body || ''),
    bullets: Array.isArray(section.bullets) ? section.bullets.map(String) : [],
    evidenceRefs: Array.isArray(section.evidenceRefs) ? section.evidenceRefs : [],
    editable: section.editable !== false,
    uncertain: Boolean(section.uncertain),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildProposalDocument(partial = {}) {
  return {
    title: String(partial.title || 'Commercial Growth Proposal'),
    preparedFor: String(partial.preparedFor || ''),
    preparedBy: String(partial.preparedBy || 'Pulseforge'),
    contactName: partial.contactName != null ? String(partial.contactName) : null,
    sections: Array.isArray(partial.sections) ? partial.sections.map(buildSection) : [],
    pricing: partial.pricing && typeof partial.pricing === 'object' ? partial.pricing : null,
    nextStepsFlow: Array.isArray(partial.nextStepsFlow)
      ? partial.nextStepsFlow.map(String)
      : [],
    personalizationScore: Number.isFinite(Number(partial.personalizationScore))
      ? Number(partial.personalizationScore)
      : 0,
    evidenceCount: Number.isFinite(Number(partial.evidenceCount))
      ? Number(partial.evidenceCount)
      : 0,
    warnings: Array.isArray(partial.warnings) ? partial.warnings.map(String) : [],
    playbookId: partial.playbookId != null ? String(partial.playbookId) : null,
    playbookVersion:
      partial.playbookVersion != null ? String(partial.playbookVersion) : null,
  };
}

/**
 * Fail closed if proposal body looks like a template.
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

/**
 * Interchangeability check: body must mention company and at least two
 * company-specific discovery facts beyond the name (ADR-014).
 * @param {object} document
 * @param {object} summary
 * @returns {{ ok: boolean, reasons: string[], specificSignals?: string[] }}
 */
function assertPersonalized(document, summary) {
  const reasons = [];
  const company = summary.companyName;
  const fullText = [
    document.title,
    document.preparedFor,
    ...(document.sections || []).flatMap((s) => [s.body, ...(s.bullets || [])]),
  ]
    .join('\n')
    .toLowerCase();

  if (!company) {
    reasons.push('missing_company_name');
    return { ok: false, reasons };
  }
  if (!fullText.includes(company.toLowerCase())) {
    reasons.push('company_name_not_referenced');
  }

  const specificSignals = [];
  for (const field of [
    'companyStage',
    'geography',
    'industry',
    'growthVision',
    'currentProcess',
  ]) {
    const v = summary[field];
    if (v && String(v).length > 3) {
      const needle = String(v).toLowerCase().slice(0, Math.min(24, String(v).length));
      if (needle.length >= 4 && fullText.includes(needle.slice(0, 12))) {
        specificSignals.push(field);
      }
    }
  }
  for (const listField of ['challenges', 'goals', 'icp', 'currentClients']) {
    for (const item of summary[listField] || []) {
      const needle = String(item).toLowerCase().slice(0, 20);
      if (needle.length >= 4 && fullText.includes(needle.slice(0, 10))) {
        specificSignals.push(`${listField}:${item}`);
      }
    }
  }

  if (specificSignals.length < 2) {
    reasons.push('insufficient_discovery_references');
  }

  const placeholderHits = findPlaceholders(fullText);
  if (placeholderHits.length) {
    reasons.push(`placeholders:${placeholderHits.join(',')}`);
  }

  return { ok: reasons.length === 0, reasons, specificSignals };
}

module.exports = {
  PROPOSAL_STATUS,
  PROPOSAL_PROGRESS_STAGES,
  OPERATOR_ACTIONS,
  SECTION_IDS,
  LONG_TERM_ADVANTAGE_BLOCK,
  PLACEHOLDER_PATTERNS,
  buildDiscoverySummary,
  normalizeList,
  buildSection,
  buildProposalDocument,
  findPlaceholders,
  assertPersonalized,
};
