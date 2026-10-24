'use strict';

/**
 * Planning Diagnostics — deterministic operator-facing explanations
 * (SPEC-054 / ADR-038). Never emit bare "Unknown capability" or
 * "Acquire via unavailable".
 */

const DIAGNOSTIC_STATUS = Object.freeze({
  SELECTED: 'Selected',
  RESOLVED: 'Resolved',
  BLOCKED: 'Blocked',
  REJECTED: 'Rejected',
  NO_MATCH: 'No matching mission alias',
});

const POSSIBLE_CAUSES = Object.freeze([
  'Capability not registered',
  'Capability disabled',
  'Version mismatch',
  'Artifact contract mismatch',
]);

/** Catalog hints when registry has zero producers for a known artifact. */
const EXPECTED_PRODUCER_HINTS = Object.freeze({
  ProspectList: 'Prospect Discovery',
  prospect_list: 'Prospect Discovery',
  CompanyIntelligence: 'Enriching Companies',
  company_intelligence: 'Enriching Companies',
  enriched_list: 'Enriching Companies',
  OpportunityRanking: 'Opportunity Ranking',
  ranked_prospects: 'Opportunity Ranking',
  BusinessIntelligenceProfile: 'Business Intelligence',
  business_intelligence_profile: 'Business Intelligence',
  SalesIntelligenceProfile: 'Sales Intelligence',
  sales_intelligence_profile: 'Sales Intelligence',
  Campaign: 'Campaign Builder',
  campaign: 'Campaign Builder',
  MailPackage: 'Mail Package Generator',
  mail_package: 'Mail Package Generator',
  mail_packages: 'Mail Package Generator',
  ReviewDecision: 'Campaign Review',
  review_decision: 'Campaign Review',
  approved_campaign: 'Campaign Review',
  ExecutionPackage: 'Direct Mail Execution',
  execution_package: 'Direct Mail Execution',
  DeliveryResults: 'Direct Mail Execution',
  delivery_results: 'Direct Mail Execution',
  OutcomeSummary: 'Outcome Intelligence',
  outcome_summary: 'Outcome Intelligence',
});

/**
 * @param {object} partial
 * @returns {object}
 */
function buildMissingProducerDiagnostic(partial = {}) {
  const artifact =
    partial.artifact || partial.artifactType || 'UnknownArtifact';
  const registered = Array.isArray(partial.registeredProducers)
    ? partial.registeredProducers
    : [];
  const disabled = Array.isArray(partial.disabledProducers)
    ? partial.disabledProducers
    : [];
  const expected =
    partial.expectedProducer ||
    EXPECTED_PRODUCER_HINTS[artifact] ||
    (registered[0] && (registered[0].name || registered[0])) ||
    null;

  const causes = [];
  if (!registered.length && !disabled.length) {
    causes.push('Capability not registered');
  }
  if (disabled.length) {
    causes.push('Capability disabled');
  }
  if (partial.versionMismatch) {
    causes.push('Version mismatch');
  }
  if (partial.contractMismatch) {
    causes.push('Artifact contract mismatch');
  }
  if (!causes.length) {
    causes.push(...POSSIBLE_CAUSES);
  }

  const recommendedAction =
    partial.recommendedAction ||
    (expected
      ? `Register a capability that produces ${artifact} (expected: ${expected}).`
      : `Register a capability that produces ${artifact}.`);

  return Object.freeze({
    kind: 'missing_producer',
    artifact,
    status: DIAGNOSTIC_STATUS.BLOCKED,
    expectedProducer: expected,
    registeredProducers: registered.map((p) =>
      typeof p === 'string' ? p : p.name || p.id
    ),
    disabledProducers: disabled.map((p) =>
      typeof p === 'string' ? p : p.name || p.id
    ),
    possibleCauses: causes,
    recommendedAction,
    rankingLosses: Array.isArray(partial.rankingLosses)
      ? partial.rankingLosses
      : [],
  });
}

/**
 * @param {object} partial
 * @returns {object}
 */
function buildUnknownMissionDiagnostic(partial = {}) {
  const input = String(partial.input || partial.text || '').trim();
  const suggestions = Array.isArray(partial.suggestedMatches)
    ? partial.suggestedMatches
    : [];
  const confidence =
    partial.confidence != null ? Number(partial.confidence) : 0;
  const intent = partial.intent || null;
  const resolution = partial.resolution || null;

  if (resolution) {
    return Object.freeze({
      kind: 'mission_segment',
      input,
      intent: intent || resolution,
      confidence: Number.isFinite(confidence) ? confidence : 0.94,
      resolution,
      status: DIAGNOSTIC_STATUS.RESOLVED,
      suggestedMatches: suggestions,
      recommendedAction: null,
    });
  }

  return Object.freeze({
    kind: 'unknown_mission',
    input,
    intent: intent || null,
    confidence: Number.isFinite(confidence) ? confidence : 0,
    status: DIAGNOSTIC_STATUS.NO_MATCH,
    suggestedMatches: suggestions.map((s) =>
      typeof s === 'string' ? s : s.name || s.id
    ),
    possibleCauses: ['No matching mission alias'],
    recommendedAction: suggestions.length
      ? `Use a registered capability alias such as: ${suggestions
          .map((s) => (typeof s === 'string' ? s : s.name || s.id))
          .slice(0, 3)
          .join(', ')}.`
      : 'Rephrase using a registered mission alias (e.g. Campaign Builder, Direct Mail Campaign, Review Campaign).',
  });
}

/**
 * @param {object} partial
 * @returns {object}
 */
function buildCapabilityDecision(partial = {}) {
  const selected = Boolean(partial.selected);
  return Object.freeze({
    kind: 'capability_decision',
    capabilityId: partial.capabilityId || null,
    name: partial.name || partial.capabilityId || 'Capability',
    selected,
    status: selected
      ? DIAGNOSTIC_STATUS.SELECTED
      : partial.status || DIAGNOSTIC_STATUS.REJECTED,
    reason: partial.reason || (selected ? 'Selected by planner' : 'Not selected'),
    possibleCauses: Array.isArray(partial.possibleCauses)
      ? partial.possibleCauses
      : [],
    recommendedAction: partial.recommendedAction || null,
    ranking: partial.ranking || null,
  });
}

/**
 * Operator-readable multi-line summary (never bare Unknown / Acquire via unavailable).
 * @param {object} diagnostic
 * @returns {string}
 */
function formatDiagnosticMessage(diagnostic) {
  if (!diagnostic || typeof diagnostic !== 'object') {
    return 'Planning blocked. Recommended Action: Inspect Planning Diagnostics.';
  }

  if (diagnostic.kind === 'missing_producer') {
    const lines = [
      `Artifact: ${diagnostic.artifact}`,
      `Status: ${diagnostic.status}`,
      `Expected Producer: ${diagnostic.expectedProducer || 'None'}`,
      `Registered Producers: ${
        diagnostic.registeredProducers && diagnostic.registeredProducers.length
          ? diagnostic.registeredProducers.join(', ')
          : 'None'
      }`,
      `Possible Causes: ${
        (diagnostic.possibleCauses || []).join(' · ') ||
        POSSIBLE_CAUSES.join(' · ')
      }`,
      `Recommended Action: ${diagnostic.recommendedAction}`,
    ];
    return lines.join('\n');
  }

  if (diagnostic.kind === 'unknown_mission') {
    const lines = [
      'Mission Segment',
      `Input: ${diagnostic.input}`,
      `Status: ${diagnostic.status}`,
      `Suggested Matches: ${
        diagnostic.suggestedMatches && diagnostic.suggestedMatches.length
          ? diagnostic.suggestedMatches.join(', ')
          : 'None'
      }`,
      `Recommended Action: ${diagnostic.recommendedAction}`,
    ];
    return lines.join('\n');
  }

  if (diagnostic.kind === 'mission_segment') {
    return [
      'Mission Segment',
      `Input: ${diagnostic.input}`,
      `Intent: ${diagnostic.intent}`,
      `Confidence: ${diagnostic.confidence}`,
      `Resolution: ${diagnostic.resolution}`,
    ].join('\n');
  }

  if (diagnostic.kind === 'capability_decision') {
    const mark = diagnostic.selected ? '✓' : '✗';
    const lines = [
      `${mark} ${diagnostic.name}`,
      `Status: ${diagnostic.status}`,
      `Reason: ${diagnostic.reason}`,
    ];
    if (diagnostic.recommendedAction) {
      lines.push(`Suggested Fix: ${diagnostic.recommendedAction}`);
    }
    return lines.join('\n');
  }

  if (diagnostic.recommendedAction) {
    return `Status: ${diagnostic.status || DIAGNOSTIC_STATUS.BLOCKED}\nRecommended Action: ${diagnostic.recommendedAction}`;
  }

  return String(diagnostic.reason || diagnostic.status || 'Planning diagnostic');
}

/**
 * Compact single-line note for Mission Plan Notes (still never bare Unknown capability).
 * @param {object} diagnostic
 * @returns {string}
 */
function formatDiagnosticNote(diagnostic) {
  if (!diagnostic) return 'No matching mission alias.';
  if (diagnostic.kind === 'unknown_mission') {
    const matches =
      diagnostic.suggestedMatches && diagnostic.suggestedMatches.length
        ? ` Suggested: ${diagnostic.suggestedMatches.slice(0, 3).join(', ')}.`
        : '';
    return `No matching mission alias for "${diagnostic.input}".${matches} ${diagnostic.recommendedAction || ''}`.trim();
  }
  if (diagnostic.kind === 'missing_producer') {
    return `${diagnostic.artifact}: blocked — ${diagnostic.recommendedAction}`;
  }
  return formatDiagnosticMessage(diagnostic).replace(/\n/g, ' · ');
}

/**
 * Build the Review Workspace Planning Diagnostics payload.
 * @param {object} input
 * @returns {object}
 */
function buildPlanningDiagnostics(input = {}) {
  const decisions = Array.isArray(input.decisions) ? input.decisions : [];
  const blocked = Array.isArray(input.blocked) ? input.blocked : [];
  const missionSegments = Array.isArray(input.missionSegments)
    ? input.missionSegments
    : [];
  const items = [...decisions, ...blocked, ...missionSegments];
  return Object.freeze({
    version: '1.0.0',
    decisions: Object.freeze(decisions.map((d) => ({ ...d }))),
    blocked: Object.freeze(blocked.map((d) => ({ ...d }))),
    missionSegments: Object.freeze(missionSegments.map((d) => ({ ...d }))),
    summary: Object.freeze(items.map((d) => formatDiagnosticMessage(d))),
    hasFailures: blocked.length > 0 || missionSegments.some((m) => m.kind === 'unknown_mission'),
  });
}

module.exports = {
  DIAGNOSTIC_STATUS,
  POSSIBLE_CAUSES,
  EXPECTED_PRODUCER_HINTS,
  buildMissingProducerDiagnostic,
  buildUnknownMissionDiagnostic,
  buildCapabilityDecision,
  formatDiagnosticMessage,
  formatDiagnosticNote,
  buildPlanningDiagnostics,
};
