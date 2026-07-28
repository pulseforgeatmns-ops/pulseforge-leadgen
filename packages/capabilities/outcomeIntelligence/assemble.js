'use strict';

/**
 * Assemble Outcome Intelligence workspace from execution + inputs (SPEC-036).
 */

const {
  OUTCOME_TYPES,
  RESPONSE_STATUS_TO_OUTCOME,
  buildOutcomeRecord,
  buildOutcomeSummary,
  buildMissionOutcomeEvent,
  buildMissionTimelineEntry,
  RECOMMENDATION_STATUS,
  LEARNING_STATUS,
} = require('./types');
const { generateLearnings } = require('./learn');
const { generateRecommendations } = require('./recommend');
const {
  buildRankingFeedbackFromOutcomes,
  toHistoricalOutcomes,
} = require('./rankingFeedback');
const { trackPersonalization } = require('./personalization');
const { computeCampaignAnalytics } = require('./analytics');

function newId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Capture outcome records from Direct Mail Execution + optional response events.
 * @param {object} context
 * @param {object} [opts]
 * @returns {object[]}
 */
function captureOutcomes(context, opts = {}) {
  const inputs = (context && context.inputs) || {};
  const operator = opts.operator || inputs.operator || 'operator';
  const missionId = context.missionId || inputs.missionId || null;
  const campaignId =
    inputs.campaignId ||
    (inputs.campaign && (inputs.campaign.id || inputs.campaign.name)) ||
    null;
  const campaignName =
    inputs.campaignName ||
    (inputs.campaign && inputs.campaign.name) ||
    (inputs.execution &&
      inputs.execution.summary &&
      inputs.execution.summary.campaignName) ||
    'Campaign';

  /** @type {object[]} */
  const records = [];

  // Explicit outcome records
  const explicit = Array.isArray(inputs.outcomes)
    ? inputs.outcomes
    : Array.isArray(inputs.outcomeRecords)
      ? inputs.outcomeRecords
      : [];
  for (const raw of explicit) {
    records.push(
      buildOutcomeRecord({
        ...enrichFromCompanyIntel(raw, inputs),
        id: raw.id || newId('out'),
        missionId: raw.missionId || missionId,
        campaignId: raw.campaignId || campaignId,
        campaignName: raw.campaignName || campaignName,
        operator: raw.operator || operator,
        evidence: [
          ...(Array.isArray(raw.evidence) ? raw.evidence : []),
          { kind: 'explicit_input' },
        ],
      })
    );
  }

  // Response events
  const events = Array.isArray(inputs.responseEvents)
    ? inputs.responseEvents
    : [];
  for (const ev of events) {
    records.push(
      buildOutcomeRecord({
        ...enrichFromCompanyIntel(ev, inputs),
        id: ev.id || newId('out'),
        missionId: ev.missionId || missionId,
        campaignId: ev.campaignId || campaignId,
        campaignName: ev.campaignName || campaignName,
        prospectId: ev.prospectId,
        company: ev.company || ev.companyName,
        companyId: ev.companyId,
        outcomeType: ev.outcomeType || ev.type || ev.responseStatus,
        timestamp: ev.timestamp || ev.at,
        operator: ev.operator || operator,
        notes: ev.notes,
        confidence: ev.confidence,
        attributes: ev.attributes,
        evidence: [
          ...(Array.isArray(ev.evidence) ? ev.evidence : []),
          { kind: 'response_event', ref: ev.id || null },
        ],
      })
    );
  }

  // Direct Mail Execution prospects
  const execution =
    inputs.execution ||
    (inputs.priorOutputs && inputs.priorOutputs.execution) ||
    null;
  const prospects = Array.isArray(inputs.prospects)
    ? inputs.prospects
    : execution && Array.isArray(execution.prospects)
      ? execution.prospects
      : inputs.priorOutputs && Array.isArray(inputs.priorOutputs.prospects)
        ? inputs.priorOutputs.prospects
        : [];

  for (const p of prospects) {
    const responseStatus = p.responseStatus || 'no_response';
    const outcomeType =
      RESPONSE_STATUS_TO_OUTCOME[responseStatus] ||
      OUTCOME_TYPES.NO_RESPONSE;
    // Also emit delivered when flagged
    if (p.delivered && outcomeType === OUTCOME_TYPES.NO_RESPONSE) {
      records.push(
        buildOutcomeRecord({
          ...enrichFromCompanyIntel(p, inputs),
          id: newId('out'),
          missionId,
          campaignId,
          campaignName,
          prospectId: p.prospectId,
          company: p.company,
          outcomeType: OUTCOME_TYPES.DELIVERED,
          timestamp: p.deliveredAt || p.lastModified,
          operator,
          notes: p.responseNotes,
          attributes: mergeAttrs(p, inputs),
          evidence: [
            {
              kind: 'direct_mail_execution',
              prospectId: p.prospectId,
              responseStatus,
            },
          ],
          confidence: 0.85,
        })
      );
    }
    records.push(
      buildOutcomeRecord({
        ...enrichFromCompanyIntel(p, inputs),
        id: newId('out'),
        missionId,
        campaignId,
        campaignName,
        prospectId: p.prospectId,
        company: p.company,
        outcomeType,
        timestamp: p.responseAt || p.lastModified || new Date().toISOString(),
        operator,
        notes: p.responseNotes,
        attributes: mergeAttrs(p, inputs),
        evidence: [
          {
            kind: 'direct_mail_execution',
            prospectId: p.prospectId,
            responseStatus,
            mailed: Boolean(p.mailed),
            delivered: Boolean(p.delivered),
          },
        ],
        confidence: responseStatus === 'no_response' ? 0.7 : 0.9,
      })
    );
  }

  return records;
}

/**
 * Build full outcome intelligence package.
 * @param {object} context
 * @param {object} [opts]
 * @returns {object}
 */
function assembleOutcomeIntelligence(context, opts = {}) {
  const inputs = (context && context.inputs) || {};
  const operator = opts.operator || inputs.operator || 'operator';
  const now = new Date().toISOString();

  const outcomes =
    Array.isArray(opts.outcomes) && opts.outcomes.length
      ? opts.outcomes
      : captureOutcomes(context, { operator });

  const learnings =
    Array.isArray(opts.learnings) && opts.learnings.length
      ? opts.learnings
      : generateLearnings(outcomes);

  const recommendations =
    Array.isArray(opts.recommendations) && opts.recommendations.length
      ? opts.recommendations
      : generateRecommendations(learnings);

  const rankingFeedback = buildRankingFeedbackFromOutcomes(outcomes);
  const historicalOutcomes = toHistoricalOutcomes(outcomes, rankingFeedback);
  const personalizationFeedback = trackPersonalization(outcomes);

  const executionMetrics =
    inputs.metrics ||
    (inputs.execution && inputs.execution.summary && inputs.execution.summary.metrics) ||
    (inputs.priorOutputs && inputs.priorOutputs.metrics) ||
    null;

  const analytics = computeCampaignAnalytics(outcomes, {
    mailed: executionMetrics && executionMetrics.mailed,
    executionMetrics,
    cost: inputs.cost,
    revenue: inputs.revenue,
  });

  const pending = recommendations.filter(
    (r) => r.status === RECOMMENDATION_STATUS.PENDING
  );
  const evidenceLearnings = learnings.filter(
    (l) => l.status === LEARNING_STATUS.EVIDENCE_BACKED
  );

  const campaignName =
    inputs.campaignName ||
    (inputs.campaign && inputs.campaign.name) ||
    (outcomes[0] && outcomes[0].campaignName) ||
    'Campaign';
  const campaignId =
    inputs.campaignId ||
    (outcomes[0] && outcomes[0].campaignId) ||
    null;

  const objectiveText = context.objective || inputs.objectiveText || '';
  const objectiveAchieved =
    analytics.wins > 0 ||
    analytics.walkthroughs > 0 ||
    (opts.objectiveAchieved === true);

  const outcomeSummary = buildOutcomeSummary({
    missionId: context.missionId || null,
    campaignId,
    campaignName,
    objectiveAchieved,
    objectiveText,
    lessonsLearned: evidenceLearnings.map((l) => l.statement),
    recommendationsGenerated: recommendations.length,
    recommendationsPending: pending.length,
    outcomeCount: outcomes.length,
    analytics,
    concludedAt: now,
  });

  const missionEvents = [
    buildMissionOutcomeEvent({
      eventType: 'outcomes_captured',
      operator,
      timestamp: now,
      summary: `Captured ${outcomes.length} outcome(s) for ${campaignName}`,
    }),
    buildMissionOutcomeEvent({
      eventType: 'learnings_generated',
      operator,
      timestamp: now,
      summary: `${evidenceLearnings.length} evidence-backed learning(s); ${learnings.length - evidenceLearnings.length} candidate(s)`,
    }),
    buildMissionOutcomeEvent({
      eventType: 'recommendations_generated',
      operator,
      timestamp: now,
      summary: `${recommendations.length} recommendation(s) pending approval`,
    }),
  ];

  const timeline = [
    buildMissionTimelineEntry({
      stage: 'outcome_intelligence',
      status: 'completed',
      timestamp: now,
      summary: `Outcome Summary: ${outcomes.length} outcomes, ${pending.length} pending recommendations`,
      operator,
    }),
  ];

  return {
    outcomes,
    learnings,
    recommendations,
    rankingFeedback,
    historicalOutcomes,
    personalizationFeedback,
    analytics,
    outcomeSummary,
    missionEvents,
    timeline,
    summary: {
      campaignName,
      campaignId,
      outcomeCount: outcomes.length,
      learningCount: learnings.length,
      evidenceBackedLearnings: evidenceLearnings.length,
      recommendationCount: recommendations.length,
      pendingRecommendations: pending.length,
      objectiveAchieved,
      updatedAt: now,
    },
  };
}

/**
 * @param {object} raw
 * @param {object} inputs
 * @returns {object}
 */
function enrichFromCompanyIntel(raw, inputs) {
  const intel =
    inputs.companyIntelligence ||
    (inputs.priorOutputs && inputs.priorOutputs.companyIntelligence) ||
    null;
  const companies = Array.isArray(inputs.companies) ? inputs.companies : [];
  const companyId = raw.companyId || raw.company_id;
  const companyName = raw.company || raw.companyName;
  let match = null;
  if (intel && typeof intel === 'object') {
    if (intel.companyId && companyId && String(intel.companyId) === String(companyId)) {
      match = intel;
    } else if (
      intel.companyName &&
      companyName &&
      String(intel.companyName).toLowerCase() === String(companyName).toLowerCase()
    ) {
      match = intel;
    }
  }
  if (!match && companies.length) {
    match =
      companies.find(
        (c) =>
          (companyId && String(c.id) === String(companyId)) ||
          (companyName &&
            String(c.name || c.companyName || '')
              .toLowerCase() === String(companyName).toLowerCase())
      ) || null;
  }
  if (!match) {
    return {
      vertical: raw.vertical || raw.industry || null,
      industry: raw.industry || raw.vertical || null,
      region: raw.region || raw.location || null,
    };
  }
  return {
    vertical: raw.vertical || match.vertical || match.industry || null,
    industry: raw.industry || match.industry || match.vertical || null,
    region: raw.region || match.region || match.location || null,
    companyId: raw.companyId || match.companyId || match.id || null,
  };
}

/**
 * @param {object} prospect
 * @param {object} inputs
 * @returns {object}
 */
function mergeAttrs(prospect, inputs) {
  const base =
    prospect.attributes && typeof prospect.attributes === 'object'
      ? { ...prospect.attributes }
      : {};
  if (prospect.handwritten != null) base.handwritten = prospect.handwritten;
  if (inputs.mailDay) base.mailDay = inputs.mailDay;
  if (inputs.personalizationDefaults) {
    Object.assign(base, inputs.personalizationDefaults);
  }
  // Enrich from playbook / package if present
  const pkg =
    prospect.mailPackage ||
    (inputs.packages &&
      Array.isArray(inputs.packages) &&
      inputs.packages.find((p) => p.prospectId === prospect.prospectId));
  if (pkg) {
    if (pkg.handwritten) base.handwritten = true;
    if (pkg.opening) base.openingParagraph = pkg.opening;
    if (pkg.offer) base.offer = pkg.offer;
    if (pkg.cta) base.cta = pkg.cta;
    if (pkg.inserts) base.insertPackage = pkg.inserts;
    if (pkg.personalizationFacts) {
      base.personalizationFacts = pkg.personalizationFacts;
    }
  }
  return base;
}

module.exports = {
  captureOutcomes,
  assembleOutcomeIntelligence,
};
