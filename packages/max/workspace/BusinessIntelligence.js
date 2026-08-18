'use strict';

/**
 * SPEC-110 — Business Intelligence synthesis.
 *
 * Bounded transformations of grounded operating evidence into first-class
 * intelligence objects. Never invents. Evidence remains attributable.
 *
 * Distinct from SPEC-053 prospect BusinessIntelligenceProfile: this object
 * is operator-facing operating intelligence. Max, Rex, dashboards, daily
 * briefings, and Cal can consume the same synthesized objects instead of
 * each reinventing inventory summaries.
 *
 * Allowed: identify bottlenecks, progress, missing evidence, operating state,
 * comparison against goals, and uncertainty.
 * Forbidden: speculate beyond evidence, invent causes, or forecast.
 */

const {
  assembleOperatingState,
  classifySupply,
} = require('./OperatingStateRecommendation');

const CATEGORIES = Object.freeze({
  BOTTLENECK: 'bottleneck',
  MOMENTUM: 'momentum',
  RISK: 'risk',
  READINESS: 'readiness',
  UNKNOWN: 'unknown',
});

const CONFIDENCE = Object.freeze({
  HIGH: 'high',
  MODERATE: 'moderate',
  LOW: 'low',
  UNKNOWN: 'unknown',
});

const CHANNEL_EFFECTIVENESS_RE = new RegExp(
  [
    String.raw`\b(?:are|is|how (?:are|is))\b.{0,80}\b(?:yelp(?:\s+ads?)?|google ads|facebook ads|(?:the )?ads?)\b.{0,40}\b(?:working|effective|performing|converting)\b`,
    String.raw`\b(?:yelp(?:\s+ads?)?|google ads|facebook ads)\b.{0,40}\b(?:working|effective|performing)\b`,
  ].join('|'),
  'i'
);

const OUTREACH_RETRIEVAL_RE =
  /\bwhat outreach has (?:already )?(?:been )?sent\b|\bwhat (?:emails?|mail|outreach) (?:has|have) (?:already )?(?:been )?(?:sent|mailed)\b|\bwhat has already been sent\b/i;

const COMPLETED_RETRIEVAL_RE =
  /\bwhat have we (?:completed|done|finished|accomplished)(?: recently)?\b|\brecently completed\b/i;

const FOCUS_RE =
  /\bwhere should (?:i|we) focus(?: next)?\b|\bwhat should (?:i|we) do next\b/i;

const {
  OPERATOR_INTENTS,
  looksLikeDiagnosis,
  looksLikeUnknownAnalysis,
  looksLikeRisk,
  looksLikeProgress,
} = require('./OperatorIntentRegistry');

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function freezeClaims(claims) {
  return Object.freeze(
    (claims || [])
      .map((claim) => {
        if (!claim) return null;
        const text = present(claim.text || claim.claim);
        if (!text) return null;
        return Object.freeze({
          text,
          epistemic: present(claim.epistemic) || 'verified',
          provenance: present(claim.provenance) || '',
          sourceKind: claim.sourceKind || null,
        });
      })
      .filter(Boolean)
  );
}

function intelligenceObject({
  id,
  finding,
  category,
  confidence,
  supportingClaims = [],
  unknowns = [],
  operatorImpact = '',
}) {
  const supporting_claims = freezeClaims(supportingClaims);
  if (!supporting_claims.length) return null;
  if (!Object.values(CATEGORIES).includes(category)) {
    return null;
  }
  return Object.freeze({
    id: present(id),
    finding: present(finding),
    category,
    confidence: confidence || CONFIDENCE.UNKNOWN,
    supporting_claims,
    unknowns: Object.freeze((unknowns || []).map(present).filter(Boolean)),
    operator_impact: present(operatorImpact),
  });
}

function isChannelEffectivenessQuestion(question) {
  return CHANNEL_EFFECTIVENESS_RE.test(String(question || ''));
}

function isOutreachRetrievalQuestion(question) {
  return OUTREACH_RETRIEVAL_RE.test(String(question || ''));
}

function isCompletedRetrievalQuestion(question) {
  return COMPLETED_RETRIEVAL_RE.test(String(question || ''));
}

function isFocusQuestion(question) {
  return FOCUS_RE.test(String(question || ''));
}

function channelFromQuestion(question) {
  const q = String(question || '');
  if (/\byelp\b/i.test(q)) return { id: 'yelp', label: 'Yelp Ads' };
  if (/\bgoogle ads\b/i.test(q)) return { id: 'google_ads', label: 'Google Ads' };
  if (/\bfacebook ads\b/i.test(q)) return { id: 'facebook_ads', label: 'Facebook Ads' };
  if (/\bads?\b/i.test(q)) return { id: 'ads', label: 'ads' };
  return null;
}

function claimFromItem(item) {
  if (!item || !item.claim) return null;
  return {
    text: present(item.claim),
    epistemic: item.epistemic || 'not_recorded',
    provenance: item.provenance || '',
    sourceKind: item.sourceKind || null,
  };
}

function isRecordedClaim(item) {
  const epistemic = item && item.epistemic;
  return (
    epistemic === 'verified' ||
    epistemic === 'operator_attested' ||
    epistemic === 'system_observed'
  );
}

function itemsByKind(items, kinds) {
  const set = new Set(kinds);
  return (items || []).filter((item) => item && set.has(item.sourceKind) && item.claim);
}

function blob(item) {
  return `${item && item.sourceKind ? item.sourceKind : ''} ${item && item.claim ? item.claim : ''} ${
    item && item.provenance ? item.provenance : ''
  }`;
}

function channelItems(items, channel) {
  if (!channel) return [];
  return (items || []).filter((item) => {
    if (channel.id === 'yelp') return /yelp/i.test(blob(item));
    if (channel.id === 'google_ads') return /google ads|google_ads/i.test(blob(item));
    if (channel.id === 'facebook_ads') return /facebook ads|facebook_ads/i.test(blob(item));
    return /\bads?\b/i.test(blob(item));
  });
}

function hasChannelOutcome(items, channel) {
  return channelItems(items, channel).some(
    (item) =>
      isRecordedClaim(item) &&
      item.sourceKind === 'outcome' &&
      /yelp|google ads|facebook ads|\bads?\b/i.test(blob(item))
  );
}

function supplyIsHealthy(state) {
  if (!state) return false;
  if (state.supply === 'healthy') return true;
  return (
    classifySupply(state.prospects || {}, state.scout || {}) === 'healthy'
  );
}

function conversionsMissing(state) {
  const outcomes = (state && state.outcomes) || {};
  return !Number(outcomes.jobs || 0) && !Number(outcomes.payments || 0) && !Number(outcomes.walkthroughs || 0);
}

function conversionsPresent(state) {
  return !conversionsMissing(state);
}

function hasVerifiedOutreach(state, items) {
  if (state && (state.mailExecuted || state.mailAttestedAt)) return true;
  if (state && state.emailMotion && (state.emailMotion.current || state.emailMotion.kind === 'historical')) {
    return true;
  }
  return (items || []).some(
    (item) =>
      isRecordedClaim(item) &&
      /mail|outreach|touchpoint|activity|operator/i.test(blob(item)) &&
      !/no (?:recent |durable )?(?:tenant-scoped )?touchpoints/i.test(item.claim || '')
  );
}

function blueprintApproved(extras = {}) {
  const understanding = extras.businessUnderstanding || extras.understanding || null;
  if (!understanding) return false;
  if (understanding.approved === true) return true;
  if (understanding.knowledgeState === 'available') return true;
  const contract = understanding.contract || {};
  return Boolean(contract.companyName || contract.businessGoals);
}

function fallbackMissingClaim(text, sourceKind) {
  return {
    text,
    epistemic: 'not_recorded',
    provenance: sourceKind || 'operating evidence',
    sourceKind: sourceKind || 'operating',
  };
}

function maybeChannelUnknown(question, items) {
  if (!isChannelEffectivenessQuestion(question)) return [];
  const channel = channelFromQuestion(question) || { id: 'ads', label: 'ads' };
  const related = channelItems(items, channel);
  const recorded = related.filter(isRecordedClaim);
  const supporting =
    related.map(claimFromItem).filter(Boolean).length
      ? related.map(claimFromItem).filter(Boolean)
      : [
          fallbackMissingClaim(
            `No durable ${channel.label} activity is recorded for this tenant.`,
            channel.id
          ),
        ];

  if (hasChannelOutcome(items, channel)) {
    const obj = intelligenceObject({
      id: `${channel.id}_effectiveness_unknown`,
      finding: `Insufficient evidence to determine ${channel.label} effectiveness. Recorded activity is not attributed conversion.`,
      category: CATEGORIES.UNKNOWN,
      confidence: CONFIDENCE.UNKNOWN,
      supportingClaims: supporting,
      unknowns: [`Whether ${channel.label} produced walkthroughs or recurring clients`],
      operatorImpact: 'Do not treat channel activity as proof of acquisition effectiveness.',
    });
    return obj ? [obj] : [];
  }

  if (recorded.length) {
    const obj = intelligenceObject({
      id: `${channel.id}_effectiveness_unknown`,
      finding: `Insufficient evidence to determine ${channel.label} effectiveness.`,
      category: CATEGORIES.UNKNOWN,
      confidence: CONFIDENCE.UNKNOWN,
      supportingClaims: supporting,
      unknowns: [`${channel.label} conversion rate`],
      operatorImpact: 'Do not infer that the channel is working from presence or absence of adjacent inventory.',
    });
    return obj ? [obj] : [];
  }

  const obj = intelligenceObject({
    id: `${channel.id}_effectiveness_unknown`,
    finding: `Insufficient evidence to determine ${channel.label} effectiveness.`,
    category: CATEGORIES.UNKNOWN,
    confidence: CONFIDENCE.UNKNOWN,
    supportingClaims: supporting,
    unknowns: [`Whether ${channel.label} is producing results`],
    operatorImpact: 'Do not speculate about channel performance without recorded evidence.',
  });
  return obj ? [obj] : [];
}

function maybeExecutionBottleneck(state, items) {
  const inventoryClaims = itemsByKind(items, ['prospects', 'scout', 'campaign', 'ao'])
    .filter(isRecordedClaim)
    .map(claimFromItem)
    .filter(Boolean);
  if (!supplyIsHealthy(state) || !inventoryClaims.length || !conversionsMissing(state)) {
    return [];
  }
  const conversionClaims = itemsByKind(items, ['outcome'])
    .map(claimFromItem)
    .filter(Boolean);
  const supporting = [...inventoryClaims, ...conversionClaims];
  const objects = [];
  const supply = intelligenceObject({
    id: 'prospect_supply_sufficient',
    finding: 'Prospect generation appears sufficient for your current acquisition strategy.',
    category: CATEGORIES.BOTTLENECK,
    confidence: CONFIDENCE.MODERATE,
    supportingClaims: inventoryClaims,
    unknowns: ['Conversion rate'],
    operatorImpact: 'Focus should shift toward execution rather than prospect discovery.',
  });
  if (supply) objects.push(supply);
  const execution = intelligenceObject({
    id: 'execution_bottleneck',
    finding: 'Execution—not prospect discovery—is the primary uncertainty.',
    category: CATEGORIES.BOTTLENECK,
    confidence: CONFIDENCE.MODERATE,
    supportingClaims: supporting.length ? supporting : inventoryClaims,
    unknowns: ['Whether existing inventory converts into walkthroughs or recurring clients'],
    operatorImpact: 'The largest uncertainty is execution and conversion rather than prospect discovery.',
  });
  if (execution) objects.push(execution);
  const conversion = intelligenceObject({
    id: 'conversion_unknown',
    finding:
      'No evidence currently demonstrates that outreach has produced walkthroughs or recurring clients.',
    category: CATEGORIES.UNKNOWN,
    confidence: CONFIDENCE.UNKNOWN,
    supportingClaims:
      conversionClaims.length
        ? conversionClaims
        : [fallbackMissingClaim('No durable conversion, job, or payment outcomes are recorded for this tenant.', 'outcome')],
    unknowns: ['Acquisition effectiveness', 'Conversion rate'],
    operatorImpact: 'Cannot determine acquisition effectiveness from inventory alone.',
  });
  if (conversion) objects.push(conversion);
  return objects;
}

function maybeSupplyBottleneck(state, items) {
  if (supplyIsHealthy(state) || state.supply !== 'thin') return [];
  const inventoryClaims = itemsByKind(items, ['prospects', 'scout', 'campaign', 'ao'])
    .map(claimFromItem)
    .filter(Boolean);
  if (!inventoryClaims.length) {
    inventoryClaims.push(
      fallbackMissingClaim('Qualified prospect inventory is thin or unrecorded for this tenant.', 'prospects')
    );
  }
  const obj = intelligenceObject({
    id: 'supply_bottleneck',
    finding: 'Prospect supply is the current bottleneck.',
    category: CATEGORIES.BOTTLENECK,
    confidence: CONFIDENCE.MODERATE,
    supportingClaims: inventoryClaims,
    unknowns: ['Whether a bounded discovery pass can produce enough qualified accounts'],
    operatorImpact: 'Relieve prospect-supply scarcity before adding another outreach channel.',
  });
  return obj ? [obj] : [];
}

function maybeMomentum(state, items) {
  if (!conversionsPresent(state)) return [];
  const outcomeClaims = itemsByKind(items, ['outcome'])
    .filter(isRecordedClaim)
    .map(claimFromItem)
    .filter(Boolean);
  if (!outcomeClaims.length) return [];
  const obj = intelligenceObject({
    id: 'market_validation_momentum',
    finding: 'Market validation is improving.',
    category: CATEGORIES.MOMENTUM,
    confidence: Number((state.outcomes && state.outcomes.jobs) || 0) > 0 ? CONFIDENCE.HIGH : CONFIDENCE.MODERATE,
    supportingClaims: outcomeClaims,
    unknowns: [],
    operatorImpact: 'Protect and learn from the converting motion rather than restarting discovery.',
  });
  return obj ? [obj] : [];
}

function maybeReadiness(state, items, extras) {
  if (!blueprintApproved(extras)) return [];
  if (state.mailExecuted || state.mailAttestedAt) return [];
  if (conversionsPresent(state)) return [];
  const campaignExists = Number(state.aoLeads || 0) > 0 || Boolean(state.campaignName);
  if (!campaignExists) return [];
  const claims = itemsByKind(items, ['campaign', 'ao', 'objective', 'mission'])
    .map(claimFromItem)
    .filter(Boolean);
  if (!claims.length) return [];
  const obj = intelligenceObject({
    id: 'pilot_readiness',
    finding: 'Ready to begin a bounded acquisition pilot.',
    category: CATEGORIES.READINESS,
    confidence: CONFIDENCE.MODERATE,
    supportingClaims: claims,
    unknowns: ['Whether the first motion produces conversations'],
    operatorImpact: 'Start the first bounded test against recorded inventory rather than expanding discovery.',
  });
  return obj ? [obj] : [];
}

function maybeFreshnessRisk(state, items) {
  if (hasVerifiedOutreach(state, items)) return [];
  if (conversionsPresent(state)) return [];
  const activityClaims = itemsByKind(items, ['activity', 'yelp', 'touchpoint'])
    .map(claimFromItem)
    .filter(Boolean);
  if (!activityClaims.length) {
    activityClaims.push(
      fallbackMissingClaim('No recent tenant-scoped touchpoints or setter activity events are recorded.', 'activity')
    );
  }
  const obj = intelligenceObject({
    id: 'pipeline_freshness_risk',
    finding: 'Pipeline freshness cannot be confirmed from recorded outreach.',
    category: CATEGORIES.RISK,
    confidence: CONFIDENCE.LOW,
    supportingClaims: activityClaims,
    unknowns: ['Whether any current outreach motion exists'],
    operatorImpact: 'Treat the pipeline as unverified until outreach or conversion evidence appears.',
  });
  return obj ? [obj] : [];
}

function maybeVerifiedOutreach(state, items, question) {
  if (!isOutreachRetrievalQuestion(question) && !isCompletedRetrievalQuestion(question)) {
    return [];
  }
  const outreachClaims = (items || [])
    .filter(
      (item) =>
        isRecordedClaim(item) &&
        /mail|campaign|operator|touchpoint|activity|\bao\b/i.test(blob(item)) &&
        !/no (?:recent |durable )?/i.test(item.claim || '')
    )
    .map(claimFromItem)
    .filter(Boolean);
  if (outreachClaims.length) {
    const mailBit = state.mailAttestedAt
      ? `${present(state.campaignName) || 'Campaign 001'} physical mail (operator-attested)`
      : state.mailExecuted
        ? `${present(state.campaignName) || 'Campaign 001'} mail (system-verified)`
        : 'recorded campaign and activity evidence';
    const obj = intelligenceObject({
      id: 'verified_outreach',
      finding: `Verified outreach on file is ${mailBit}.`,
      category: conversionsPresent(state) ? CATEGORIES.MOMENTUM : CATEGORIES.BOTTLENECK,
      confidence: CONFIDENCE.MODERATE,
      supportingClaims: outreachClaims,
      unknowns: conversionsMissing(state) ? ['Whether that outreach produced conversations or clients'] : [],
      operatorImpact: 'Treat verified outreach as recorded activity, not as proof of conversion.',
    });
    return obj ? [obj] : [];
  }
  const missing = (items || [])
    .filter((item) => /mail|outreach|activity|yelp/i.test(blob(item)))
    .map(claimFromItem)
    .filter(Boolean);
  const obj = intelligenceObject({
    id: 'outreach_absent',
    finding: 'No verified outreach has been recorded yet.',
    category: CATEGORIES.UNKNOWN,
    confidence: CONFIDENCE.UNKNOWN,
    supportingClaims: missing.length
      ? missing
      : [fallbackMissingClaim('No verified outreach is recorded for this tenant.', 'activity')],
    unknowns: ['What outreach, if any, has left the building'],
    operatorImpact: 'Do not describe unrecorded outreach as sent.',
  });
  return obj ? [obj] : [];
}

function maybeUnprovenMotionRisk(state, items) {
  if (!hasVerifiedOutreach(state, items) || !conversionsMissing(state)) return [];
  const supporting = [
    ...itemsByKind(items, ['campaign', 'ao', 'activity', 'outcome'])
      .map(claimFromItem)
      .filter(Boolean),
  ];
  if (!supporting.length) {
    supporting.push(
      fallbackMissingClaim(
        'No durable conversion, job, or payment outcomes are recorded for this tenant.',
        'outcome'
      )
    );
  }
  const obj = intelligenceObject({
    id: 'unproven_motion_risk',
    finding:
      'The recorded motion has not yet produced conversions, so operational effectiveness remains unproven.',
    category: CATEGORIES.RISK,
    confidence: CONFIDENCE.MODERATE,
    supportingClaims: supporting,
    unknowns: ['Whether mailed or recorded outreach converts into walkthroughs or clients'],
    operatorImpact: 'Do not treat recorded activity as proof the motion is working.',
  });
  return obj ? [obj] : [];
}

function maybeCriticalUnknowns(state, items) {
  const objects = [];
  const conversionClaims = itemsByKind(items, ['outcome']).map(claimFromItem).filter(Boolean);
  const conversion = intelligenceObject({
    id: 'unknown_conversions',
    finding: 'Conversions are not yet recorded.',
    category: CATEGORIES.UNKNOWN,
    confidence: CONFIDENCE.UNKNOWN,
    supportingClaims: conversionClaims.length
      ? conversionClaims
      : [fallbackMissingClaim('No durable conversion, job, or payment outcomes are recorded for this tenant.', 'outcome')],
    unknowns: ['Conversion rate', 'Whether outreach produced recurring clients'],
    operatorImpact: 'Cannot determine acquisition effectiveness without conversion evidence.',
  });
  if (conversion && conversionsMissing(state)) objects.push(conversion);

  const walkthroughCount = Number(state.walkthroughs || 0);
  const walkthroughItems = (items || []).filter((item) => /walkthrough/i.test(blob(item)));
  const walkthroughClaims = walkthroughItems.map(claimFromItem).filter(Boolean);
  if (walkthroughCount <= 0) {
    const walkthrough = intelligenceObject({
      id: 'unknown_walkthroughs',
      finding: 'Walkthroughs are not yet recorded.',
      category: CATEGORIES.UNKNOWN,
      confidence: CONFIDENCE.UNKNOWN,
      supportingClaims: walkthroughClaims.length
        ? walkthroughClaims
        : [fallbackMissingClaim('No walkthrough-request operational states are recorded in AO for this tenant.', 'campaign')],
      unknowns: ['Whether any account requested a walkthrough'],
      operatorImpact: 'Pipeline movement cannot be confirmed from inventory counts.',
    });
    if (walkthrough) objects.push(walkthrough);
  }

  const yelpItems = (items || []).filter((item) => /yelp/i.test(blob(item)));
  const yelpRecorded = yelpItems.some(isRecordedClaim);
  const yelp = intelligenceObject({
    id: 'unknown_yelp_performance',
    finding: 'Yelp performance cannot be determined from recorded evidence.',
    category: CATEGORIES.UNKNOWN,
    confidence: CONFIDENCE.UNKNOWN,
    supportingClaims: yelpItems.map(claimFromItem).filter(Boolean).length
      ? yelpItems.map(claimFromItem).filter(Boolean)
      : [fallbackMissingClaim('No durable Yelp activity is recorded for this tenant.', 'yelp')],
    unknowns: ['Yelp Ads effectiveness'],
    operatorImpact: 'Do not treat unrecorded channel performance as a known.',
  });
  if (yelp && !yelpRecorded) objects.push(yelp);

  const campaignItems = itemsByKind(items, ['campaign', 'ao']);
  const campaignExecutionUnknown = conversionsMissing(state);
  if (campaignExecutionUnknown) {
    const campaign = intelligenceObject({
      id: 'unknown_campaign_execution_results',
      finding: 'Campaign execution results are not yet known.',
      category: CATEGORIES.UNKNOWN,
      confidence: CONFIDENCE.UNKNOWN,
      supportingClaims: campaignItems.map(claimFromItem).filter(Boolean).length
        ? campaignItems.map(claimFromItem).filter(Boolean)
        : [fallbackMissingClaim('Campaign execution has not produced recorded conversion results.', 'campaign')],
      unknowns: ['Whether Campaign 001 produced conversations or clients'],
      operatorImpact: 'Treat mailed or prepared campaign work as activity, not as proven execution outcomes.',
    });
    if (campaign) objects.push(campaign);
  }
  return objects;
}

function maybeProgressAgainstGoals(state, extras, items) {
  const understanding = extras.businessUnderstanding || extras.understanding || null;
  const contract = (understanding && understanding.contract) || {};
  const goals = present(
    contract.businessGoals ||
      (understanding && understanding.summary && understanding.summary.goals) ||
      ''
  );
  const objectiveTexts = (state.objectives || [])
    .map((row) => present(row && (row.title || row.objectiveText || row.statement)))
    .filter(Boolean);
  const goalText = goals || objectiveTexts[0] || 'stated acquisition goals';
  const completedBits = [];
  if (state.mailExecuted || state.mailAttestedAt) {
    completedBits.push(present(state.campaignName) || 'Campaign 001');
  }
  if (Number(state.aoLeads || 0) > 0) {
    completedBits.push(`${Number(state.aoLeads)} AO leads seeded`);
  }
  if (blueprintApproved(extras)) {
    completedBits.push('approved Blueprint');
  }
  const remaining = [];
  if (conversionsMissing(state)) remaining.push('conversions against the stated goal');
  if (!Number(state.walkthroughs || 0)) {
    remaining.push('walkthroughs');
  }
  const supporting = [
    {
      text: `Stated goal: ${goalText}`,
      epistemic: 'verified',
      provenance: objectiveTexts.length ? 'operator objective' : 'approved Blueprint',
      sourceKind: 'objective',
    },
    ...itemsByKind(items, ['campaign', 'ao', 'outcome']).map(claimFromItem).filter(Boolean),
  ];
  if (!supporting.length) return [];
  const finding = completedBits.length
    ? `Progress against goals: ${completedBits.join(', ')} are recorded; the stated goal is not yet demonstrated by conversions.`
    : `Observed operating state does not yet demonstrate progress against ${goalText}.`;
  const obj = intelligenceObject({
    id: 'progress_against_goals',
    finding,
    category: conversionsPresent(state) ? CATEGORIES.MOMENTUM : CATEGORIES.UNKNOWN,
    confidence: conversionsPresent(state) ? CONFIDENCE.MODERATE : CONFIDENCE.UNKNOWN,
    supportingClaims: supporting,
    unknowns: remaining,
    operatorImpact: remaining.length
      ? `Remaining work: ${remaining.join(', ')}.`
      : 'Protect demonstrated progress rather than restarting discovery.',
  });
  return obj ? [obj] : [];
}

function maybeGoalGap(state, extras) {
  const understanding = extras.businessUnderstanding || extras.understanding || null;
  const contract = (understanding && understanding.contract) || {};
  const goals = present(
    contract.businessGoals ||
      (understanding && understanding.summary && understanding.summary.goals) ||
      ''
  );
  const objectiveTexts = (state.objectives || [])
    .map((row) => present(row && (row.title || row.objectiveText || row.statement)))
    .filter(Boolean);
  const goalText = goals || objectiveTexts[0];
  if (!goalText) return [];
  if (!conversionsMissing(state)) return [];
  const supporting = [
    {
      text: `Stated goal: ${goalText}`,
      epistemic: 'verified',
      provenance: objectiveTexts.length ? 'operator objective' : 'approved Blueprint',
      sourceKind: 'objective',
    },
    fallbackMissingClaim(
      'No durable conversion, job, or payment outcomes are recorded for this tenant.',
      'outcome'
    ),
  ];
  const obj = intelligenceObject({
    id: 'goal_versus_observed',
    finding: 'Observed operating state does not yet demonstrate progress against stated acquisition goals.',
    category: CATEGORIES.UNKNOWN,
    confidence: CONFIDENCE.UNKNOWN,
    supportingClaims: supporting,
    unknowns: ['Whether the current motion will produce the stated outcome'],
    operatorImpact: 'Keep goals separate from observed operating state.',
  });
  return obj ? [obj] : [];
}

function dedupeObjects(objects) {
  const seen = new Set();
  const out = [];
  for (const obj of objects) {
    if (!obj || !obj.id || seen.has(obj.id)) continue;
    seen.add(obj.id);
    out.push(obj);
  }
  return out;
}

function selectForQuestion(objects, question, analysisMode) {
  const list = objects.slice();
  const mode = analysisMode || null;
  if (mode === OPERATOR_INTENTS.DIAGNOSIS || looksLikeDiagnosis(question)) {
    const preferred = list.filter((obj) =>
      obj.category === CATEGORIES.BOTTLENECK ||
      obj.category === CATEGORIES.READINESS ||
      obj.category === CATEGORIES.MOMENTUM
    );
    const execution = preferred.filter((obj) => obj.id === 'execution_bottleneck');
    const rest = preferred.filter((obj) => obj.id !== 'execution_bottleneck');
    return execution.length || preferred.length ? [...execution, ...rest] : list;
  }
  if (mode === OPERATOR_INTENTS.UNKNOWN_ANALYSIS || looksLikeUnknownAnalysis(question)) {
    const unknowns = list.filter((obj) => obj.category === CATEGORIES.UNKNOWN);
    return unknowns.length ? unknowns : list;
  }
  if (mode === OPERATOR_INTENTS.RISK || looksLikeRisk(question)) {
    const risks = list.filter((obj) => obj.category === CATEGORIES.RISK);
    return risks.length ? risks : list;
  }
  if (mode === OPERATOR_INTENTS.PROGRESS || looksLikeProgress(question)) {
    const progress = list.filter(
      (obj) =>
        obj.id === 'progress_against_goals' ||
        obj.category === CATEGORIES.MOMENTUM ||
        obj.id === 'goal_versus_observed'
    );
    return progress.length ? progress : list;
  }
  if (isChannelEffectivenessQuestion(question)) {
    return list.filter(
      (obj) => obj.category === CATEGORIES.UNKNOWN && /effectiveness/i.test(obj.id)
    );
  }
  if (isOutreachRetrievalQuestion(question) || isCompletedRetrievalQuestion(question)) {
    const lead = list.filter((obj) => obj.id === 'verified_outreach' || obj.id === 'outreach_absent');
    const rest = list.filter((obj) => obj.id !== 'verified_outreach' && obj.id !== 'outreach_absent');
    return lead.length ? [...lead, ...rest] : list;
  }
  if (isFocusQuestion(question)) {
    const bottleneck = list.filter((obj) => obj.category === CATEGORIES.BOTTLENECK);
    const rest = list.filter((obj) => obj.category !== CATEGORIES.BOTTLENECK);
    return bottleneck.length ? [...bottleneck, ...rest] : list;
  }
  return list;
}

function formatIntelligenceProse(synthesis) {
  if (!synthesis) return '';
  const objects = Array.isArray(synthesis.objects) ? synthesis.objects : [];
  return objects
    .map((obj) => present(obj && obj.finding))
    .filter(Boolean)
    .join('\n');
}

function emptySynthesis() {
  return Object.freeze({
    objects: Object.freeze([]),
    primary: null,
    prose: '',
  });
}

/**
 * Synthesize first-class Business Intelligence objects from retrieved,
 * claim-grounded operating evidence. Does not persist. Does not execute.
 *
 * @param {object} input
 * @param {object} [input.bundle] SPEC-105 operating-evidence bundle
 * @param {object} [input.state] assembled operating state
 * @param {string} [input.question]
 * @param {object} [input.extras]
 * @returns {{ objects: object[], primary: object|null, prose: string }}
 */
function synthesizeBusinessIntelligence(input = {}) {
  const bundle = input.bundle || {};
  if (bundle.failClosed) return emptySynthesis();

  const extras = input.extras || {};
  const state =
    input.state ||
    assembleOperatingState(bundle, {
      businessUnderstanding: extras.businessUnderstanding,
      now: extras.now,
      capability: extras.capability || bundle.capability,
      retractedPremises: extras.retractedPremises,
      operatorDeniedEmailActive: extras.operatorDeniedEmailActive,
    });
  const items = Array.isArray(input.items)
    ? input.items
    : Array.isArray(bundle.items)
      ? bundle.items
      : Array.isArray(state.items)
        ? state.items
        : [];
  const question = String(input.question || '');

  const execution = maybeExecutionBottleneck(state, items);
  const objects = dedupeObjects(
    [
      ...maybeChannelUnknown(question, items),
      ...maybeVerifiedOutreach(state, items, question),
      ...maybeMomentum(state, items),
      ...execution,
      ...maybeSupplyBottleneck(state, items),
      ...maybeReadiness(state, items, extras),
      ...maybeFreshnessRisk(state, items),
      ...maybeUnprovenMotionRisk(state, items),
      ...maybeCriticalUnknowns(state, items),
      ...maybeProgressAgainstGoals(state, extras, items),
      ...(execution.length ? [] : maybeGoalGap(state, extras)),
    ].filter(Boolean)
  );

  const selected = selectForQuestion(objects, question, input.analysisMode);
  if (!selected.length) {
    const inspect = intelligenceObject({
      id: 'operating_picture_unknown',
      finding: 'Insufficient evidence to determine current operating effectiveness.',
      category: CATEGORIES.UNKNOWN,
      confidence: CONFIDENCE.UNKNOWN,
      supportingClaims: items.length
        ? items.slice(0, 4).map(claimFromItem).filter(Boolean)
        : [fallbackMissingClaim('Recorded operating evidence is too thin to name a current constraint.', 'operating')],
      unknowns: ['Which of supply, execution, or conversion is actually missing'],
      operatorImpact: 'Make the current operating picture inspectable before choosing a new motion.',
    });
    const fallback = inspect ? [inspect] : [];
    return Object.freeze({
      objects: Object.freeze(fallback),
      primary: fallback[0] || null,
      prose: formatIntelligenceProse({ objects: fallback }),
    });
  }

  return Object.freeze({
    objects: Object.freeze(selected),
    primary: selected[0] || null,
    prose: formatIntelligenceProse({ objects: selected }),
  });
}

function serializeBusinessIntelligence(synthesis) {
  if (!synthesis) return null;
  return {
    objects: (synthesis.objects || []).map((obj) => ({
      id: obj.id,
      finding: obj.finding,
      category: obj.category,
      confidence: obj.confidence,
      supporting_claims: (obj.supporting_claims || []).map((claim) => ({ ...claim })),
      unknowns: [...(obj.unknowns || [])],
      operator_impact: obj.operator_impact,
    })),
    primary: synthesis.primary
      ? {
          id: synthesis.primary.id,
          finding: synthesis.primary.finding,
          category: synthesis.primary.category,
          confidence: synthesis.primary.confidence,
          supporting_claims: (synthesis.primary.supporting_claims || []).map((claim) => ({ ...claim })),
          unknowns: [...(synthesis.primary.unknowns || [])],
          operator_impact: synthesis.primary.operator_impact,
        }
      : null,
    prose: synthesis.prose || '',
  };
}

function claimLines(objects) {
  const lines = [];
  for (const obj of objects || []) {
    for (const claim of obj.supporting_claims || []) {
      const text = present(claim.text || claim.claim);
      if (text) lines.push(`- ${text}`);
    }
  }
  return [...new Set(lines)].join('\n');
}

function suggestedInvestigationsFromUnknowns(objects) {
  const suggestions = [];
  const blob = (objects || []).map((obj) => `${obj.id} ${obj.finding} ${(obj.unknowns || []).join(' ')}`).join(' ');
  if (/conversion/i.test(blob)) suggestions.push('Record conversion, job, or payment outcomes for the current motion.');
  if (/walkthrough/i.test(blob)) suggestions.push('Confirm whether any walkthroughs were requested or completed.');
  if (/yelp/i.test(blob)) suggestions.push('Attribute Yelp performance only if channel activity is recorded.');
  if (/campaign execution|Campaign 001/i.test(blob)) {
    suggestions.push('Measure Campaign 001 execution results against recorded activity, not rumors.');
  }
  return suggestions.join('\n');
}

function blueprintUnknownsFromExtras(extras = {}) {
  const understanding = extras.businessUnderstanding || extras.understanding || {};
  const summary = understanding.blueprint || understanding.summary || understanding;
  const lines = [];
  const push = (value) => {
    const text = present(value);
    if (text) lines.push(text);
  };
  if (Array.isArray(understanding.unknowns)) {
    understanding.unknowns.forEach(push);
  }
  if (summary && Array.isArray(summary.unknowns)) {
    summary.unknowns.forEach(push);
  }
  if (summary && summary.idealCustomers) {
    push(
      `Whether ${summary.idealCustomers} will outperform alternatives is not yet evidenced.`
    );
  } else if (understanding.industries) {
    push(
      `Whether ${understanding.industries} will outperform alternatives is not yet evidenced.`
    );
  }
  return [...new Set(lines)];
}

function analysisSectionsFromIntelligence(synthesis, extras = {}) {
  const objects = (synthesis && synthesis.objects) || [];
  const primary = (synthesis && synthesis.primary) || objects[0] || null;
  const confidence = primary ? present(primary.confidence) : 'unknown';
  const impact = objects
    .map((obj) => present(obj.operator_impact))
    .filter(Boolean)
    .filter((line, idx, arr) => arr.indexOf(line) === idx)
    .join('\n');
  const findings = objects.map((obj) => present(obj.finding)).filter(Boolean).join('\n');
  const unknowns = objects
    .flatMap((obj) => obj.unknowns || [])
    .map(present)
    .filter(Boolean)
    .filter((line, idx, arr) => arr.indexOf(line) === idx);
  const blueprintUnknowns = blueprintUnknownsFromExtras(extras);
  const mergedUnknowns = [...unknowns];
  for (const line of blueprintUnknowns) {
    if (!mergedUnknowns.includes(line)) mergedUnknowns.push(line);
  }
  const remaining = objects
    .filter((obj) => obj.id === 'progress_against_goals' || obj.id === 'goal_versus_observed')
    .flatMap((obj) => obj.unknowns || [])
    .map(present)
    .filter(Boolean);
  return {
    bottleneck: primary && primary.finding ? present(primary.finding) : findings,
    confidence,
    evidence: claimLines(objects),
    operatorImpact: impact,
    unknowns:
      extras.unknownList ||
      [findings, mergedUnknowns.length ? mergedUnknowns.map((u) => `- ${u}`).join('\n') : '']
        .filter(Boolean)
        .join('\n'),
    evidenceGaps: extras.evidenceGaps || claimLines(objects),
    suggestedInvestigations: suggestedInvestigationsFromUnknowns(objects),
    risks: findings,
    potentialImpact: impact,
    progress: findings,
    remainingWork: remaining.length
      ? remaining.map((line) => `- ${line}`).join('\n')
      : impact || 'Remaining work is not yet demonstrated against stated goals.',
  };
}

module.exports = {
  CATEGORIES,
  CONFIDENCE,
  CHANNEL_EFFECTIVENESS_RE,
  intelligenceObject,
  isChannelEffectivenessQuestion,
  isOutreachRetrievalQuestion,
  isCompletedRetrievalQuestion,
  isFocusQuestion,
  channelFromQuestion,
  synthesizeBusinessIntelligence,
  formatIntelligenceProse,
  serializeBusinessIntelligence,
  analysisSectionsFromIntelligence,
  suggestedInvestigationsFromUnknowns,
};
