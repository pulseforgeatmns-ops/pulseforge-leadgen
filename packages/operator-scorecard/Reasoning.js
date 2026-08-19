'use strict';

/**
 * SPEC-116 — reasoning pipeline.
 * Understand → objectives → stage → model → outcomes → draft scorecard.
 * Drafts are advisory. They are never used for reporting.
 */

const {
  SCORECARD_STATUS,
  METRIC_STATUS,
  METRIC_SOURCE,
  BUSINESS_STAGES,
  asText,
  asList,
  clone,
  nowIso,
  newId,
  slugify,
  clampConfidence,
  osiError,
} = require('./types');
const {
  CATALOG,
  PROFILE_GOALS,
  getCatalogEntry,
  metricsForProfile,
  detectProfile,
} = require('./Catalog');

function sectionSummary(sections, key) {
  if (!sections) return '';
  const row = sections[key] || sections.successMetrics || null;
  if (!row) return '';
  return asText(row.summary || row.body || row);
}

function collectObjectives(input = {}) {
  const fromList = asList(input.objectives || input.operatorObjectives);
  const fromRows = (input.objectiveRecords || [])
    .map((row) => asText(row.objective_text || row.objectiveText || row.title))
    .filter(Boolean);
  const fromBlueprint = asList(
    sectionSummary(input.blueprint && input.blueprint.sections, 'campaignGoals')
  );
  const fromFacts = asList(
    input.normalizedFacts && (input.normalizedFacts.ninety_day_outcomes || input.normalizedFacts.growth_focus)
  );
  const goal = asText(input.businessGoal);
  const merged = [...fromList, ...fromRows, ...fromBlueprint, ...fromFacts, goal]
    .map(asText)
    .filter(Boolean);
  const seen = new Set();
  return merged.filter((item) => {
    const key = item.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function inferBusinessStage(input = {}, objectives = []) {
  if (input.businessStage && Object.values(BUSINESS_STAGES).includes(input.businessStage)) {
    return input.businessStage;
  }
  const blob = [input.businessGoal, input.stageHint, ...objectives, input.vertical]
    .map(asText)
    .join(' ')
    .toLowerCase();
  if (/operational scale|utilization|retention at scale|mature operations/.test(blob)) {
    return BUSINESS_STAGES.OPERATIONAL_SCALE;
  }
  if (/mature|margin expansion|category leadership/.test(blob)) {
    return BUSINESS_STAGES.MATURE_GROWTH;
  }
  if (
    /validate|methodology|market validation|pain confirmation|icp confidence|establish a repeatable/.test(
      blob
    ) && /validate|methodology|pain/.test(blob)
  ) {
    return BUSINESS_STAGES.MARKET_VALIDATION;
  }
  if (/repeatable acquisition|walkthrough|commercial acquisition engine|pipeline/.test(blob)) {
    return BUSINESS_STAGES.REPEATABLE_ACQUISITION;
  }
  if (/validate|methodology|pain/.test(blob)) return BUSINESS_STAGES.MARKET_VALIDATION;
  return BUSINESS_STAGES.REPEATABLE_ACQUISITION;
}

function inferBusinessModel(input = {}, profile) {
  if (asText(input.businessModel)) return asText(input.businessModel);
  if (profile === 'founder_transformation') return 'cohort_transformation';
  if (profile === 'commercial_cleaning') return 'recurring_commercial_service';
  if (profile === 'home_renovation') return 'project_services';
  return 'acquisition_led_services';
}

function inferRevenueModel(input = {}, profile) {
  if (asText(input.revenueModel)) return asText(input.revenueModel);
  if (profile === 'founder_transformation') return 'enrollment';
  if (profile === 'commercial_cleaning') return 'monthly_recurring';
  if (profile === 'home_renovation') return 'project_revenue';
  return 'mixed';
}

function operatorStatedMetrics(input = {}) {
  const facts = input.normalizedFacts || {};
  const fromFacts = asList(facts.success_metrics);
  const fromSection = asList(sectionSummary(input.blueprint && input.blueprint.sections, 'successMetrics'));
  const fromInput = asList(input.operatorMetrics);
  const merged = [...fromFacts, ...fromSection, ...fromInput];
  const seen = new Set();
  return merged.filter((item) => {
    const key = item.toLowerCase();
    if (seen.has(key)) return false;
    if (/i don'?t know|not sure|tbd/i.test(item)) return false;
    seen.add(key);
    return true;
  });
}

function matchCatalogForOperatorMetric(name) {
  const blob = asText(name).toLowerCase();
  return (
    CATALOG.find((row) => blob.includes(row.name.toLowerCase()) || row.name.toLowerCase().includes(blob)) ||
    CATALOG.find((row) => blob.includes(row.key.replace(/_/g, ' '))) ||
    null
  );
}

function applyLearning(entries, learning = []) {
  const suppressed = new Set(
    learning
      .filter((row) => row.action === 'remove' && row.suppress !== false)
      .map((row) => row.metricKey || row.metric_key)
  );
  const boosted = learning
    .filter((row) => row.action === 'add' || row.prioritize)
    .map((row) => row.metricKey || row.metric_key)
    .filter(Boolean);
  const kept = entries.filter((entry) => !suppressed.has(entry.key));
  for (const key of boosted) {
    if (kept.some((row) => row.key === key)) continue;
    const catalog = getCatalogEntry(key);
    if (catalog) kept.push(catalog);
    else {
      kept.push({
        key,
        name: asText(key).replace(/_/g, ' ').replace(/\b\w/g, (ch) => ch.toUpperCase()),
        category: 'acquisition',
        indicator: 'leading',
        defaultConfidence: 0.8,
        reason: 'The operator added this metric. Future scorecards should keep it in view.',
        businessOutcome: 'Operator-defined success',
      });
    }
  }
  kept.sort((a, b) => {
    const aBoost = boosted.includes(a.key) ? 0 : 1;
    const bBoost = boosted.includes(b.key) ? 0 : 1;
    return aBoost - bBoost;
  });
  return kept;
}

function outcomeBias(input = {}) {
  const outcomes = input.outcomes || input.historicalOutcomes || [];
  const blob = JSON.stringify(outcomes).toLowerCase();
  const prefer = [];
  if (/retention|churn/.test(blob)) prefer.push('client_retention');
  if (/walkthrough/.test(blob)) prefer.push('walkthrough_requests');
  if (/reply|response/.test(blob)) prefer.push('positive_reply_rate', 'outreach_response_rate');
  if (/enrollment/.test(blob)) prefer.push('pilot_enrollments');
  return prefer;
}

function understandBusiness(input = {}) {
  const objectives = collectObjectives(input);
  const profile = detectProfile({ ...input, objectives });
  const stage = inferBusinessStage(input, objectives);
  const businessModel = inferBusinessModel(input, profile);
  const revenueModel = inferRevenueModel(input, profile);
  const businessGoal =
    asText(input.businessGoal) || PROFILE_GOALS[profile] || PROFILE_GOALS.default;
  return {
    tenantId: asText(input.tenantId || input.tenant_id) || null,
    clientId: input.clientId || input.client_id || null,
    businessName: asText(input.businessName || (input.normalizedFacts && input.normalizedFacts.business_name)),
    objectives,
    profile,
    stage,
    businessModel,
    revenueModel,
    businessGoal,
    hasBlueprint: Boolean(input.blueprint || (input.normalizedFacts && Object.keys(input.normalizedFacts).length)),
    hasAim: Boolean(input.aim && (input.aim.status === 'published' || input.aim.status === 'complete' || input.aim.published)),
    hasOutcomes: Boolean((input.outcomes || input.historicalOutcomes || []).length),
  };
}

function toRecommendation(entry, understanding, extra = {}) {
  return {
    id: extra.id || newId('metric'),
    key: extra.key || entry.key,
    name: extra.name || entry.name,
    category: extra.category || entry.category,
    indicator: extra.indicator || entry.indicator,
    confidence: clampConfidence(extra.confidence != null ? extra.confidence : entry.defaultConfidence),
    reason: extra.reason || entry.reason,
    businessOutcome: extra.businessOutcome || entry.businessOutcome,
    whyItBelongs:
      extra.whyItBelongs ||
      `Max recommends ${extra.name || entry.name} because it measures progress toward ${
        extra.businessOutcome || entry.businessOutcome
      } for a ${understanding.stage.replace(/_/g, ' ')} ${understanding.profile.replace(/_/g, ' ')} business.`,
    status: extra.status || METRIC_STATUS.RECOMMENDED,
    source: extra.source || METRIC_SOURCE.MAX,
    sortOrder: extra.sortOrder != null ? extra.sortOrder : 0,
  };
}

function generateDraftScorecard(input = {}) {
  const understanding = understandBusiness(input);
  if (!understanding.objectives.length && !understanding.hasBlueprint && !asText(input.businessGoal)) {
    throw osiError(
      'osi_insufficient_understanding',
      'Max needs business objectives or a Business Blueprint before recommending a scorecard.'
    );
  }

  const catalogEntries = metricsForProfile(understanding.profile, understanding.stage);
  const learning = input.learning || input.operatorLearning || [];
  const biased = [...catalogEntries];
  for (const key of outcomeBias(input)) {
    const entry = getCatalogEntry(key);
    if (entry && !biased.some((row) => row.key === key)) biased.unshift(entry);
  }
  const selected = applyLearning(biased, learning);

  const metrics = selected.map((entry, index) =>
    toRecommendation(entry, understanding, { sortOrder: index })
  );

  for (const stated of operatorStatedMetrics(input)) {
    const matched = matchCatalogForOperatorMetric(stated);
    if (matched && metrics.some((row) => row.key === matched.key)) {
      const existing = metrics.find((row) => row.key === matched.key);
      existing.reason = `${existing.reason} The operator already named this as a success signal (${stated}).`;
      continue;
    }
    if (matched && !metrics.some((row) => row.key === matched.key)) {
      metrics.push(
        toRecommendation(matched, understanding, {
          sortOrder: metrics.length,
          reason: `${matched.reason} The operator already named this as a success signal (${stated}).`,
        })
      );
      continue;
    }
    metrics.push(
      toRecommendation(
        {
          key: slugify(stated),
          name: stated.replace(/\b\w/g, (ch) => ch.toUpperCase()),
          category: 'business_outcomes',
          indicator: 'lagging',
          defaultConfidence: 0.82,
          reason: `The operator named "${stated}" as a way to judge progress. Max is recommending it so the scorecard stays faithful to that intent.`,
          businessOutcome: understanding.businessGoal,
        },
        understanding,
        { sortOrder: metrics.length }
      )
    );
  }

  const suggestions = ['qualified replies', 'booked conversations', 'estimate requests'];
  const blob = metrics.map((m) => `${m.name} ${m.key}`).join(' ').toLowerCase();
  const extraExplore = suggestions.filter((item) => !blob.includes(item.split(' ')[0]));

  return {
    id: input.id || newId('scorecard'),
    tenantId: understanding.tenantId,
    clientId: understanding.clientId,
    status: SCORECARD_STATUS.DRAFT,
    version: 1,
    isRuntime: false,
    businessGoal: understanding.businessGoal,
    businessStage: understanding.stage,
    businessModel: understanding.businessModel,
    revenueModel: understanding.revenueModel,
    profile: understanding.profile,
    objectives: understanding.objectives,
    reasoning: {
      pipeline: [
        'business_understanding',
        'business_objectives',
        'business_stage',
        'business_model',
        'outcome_intelligence',
        'draft_operator_scorecard',
      ],
      understanding,
      extraExplore,
    },
    metrics,
    reviews: [],
    learning: [],
    approvedAt: null,
    approvedBy: null,
    createdAt: nowIso(),
    updatedAt: nowIso(),
  };
}

function activeMetrics(scorecard) {
  return (scorecard.metrics || []).filter((m) => m.status !== METRIC_STATUS.REMOVED);
}

function recommendedMetrics(scorecard) {
  return (scorecard.metrics || []).filter(
    (m) => m.status === METRIC_STATUS.RECOMMENDED || m.status === METRIC_STATUS.UNDER_REVIEW
  );
}

function approvedMetrics(scorecard) {
  if (!scorecard || scorecard.status !== SCORECARD_STATUS.APPROVED) return [];
  return activeMetrics(scorecard).filter((m) =>
    [METRIC_STATUS.ACCEPTED, METRIC_STATUS.MODIFIED, METRIC_STATUS.ADDED].includes(m.status)
  );
}

function assertNotRuntime(scorecard) {
  if (scorecard && scorecard.status !== SCORECARD_STATUS.APPROVED && scorecard.isRuntime) {
    throw osiError('osi_draft_not_runtime', 'A draft scorecard cannot be used for reporting.');
  }
}

function getRuntimeScorecard(scorecard) {
  if (!scorecard || scorecard.status !== SCORECARD_STATUS.APPROVED) {
    return {
      status: 'absent',
      source: 'none',
      definitionOfSuccess: null,
      metrics: [],
      note: 'No operator-approved scorecard. Draft recommendations are not used for reporting.',
    };
  }
  const metrics = approvedMetrics(scorecard);
  return {
    status: 'approved',
    source: 'operator_approved',
    definitionOfSuccess: metrics,
    metrics,
    businessGoal: scorecard.businessGoal,
    note: 'These metrics have been explicitly approved by the operator and define business success.',
  };
}

module.exports = {
  collectObjectives,
  inferBusinessStage,
  inferBusinessModel,
  understandBusiness,
  generateDraftScorecard,
  toRecommendation,
  activeMetrics,
  recommendedMetrics,
  approvedMetrics,
  assertNotRuntime,
  getRuntimeScorecard,
  operatorStatedMetrics,
  clone,
};
