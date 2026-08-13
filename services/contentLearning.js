'use strict';

/**
 * SPEC-093 — Paige Outcome Learning Loop (v1 thin slice).
 *
 * Reasons over SPEC-092 content outcomes to produce durable learnings and
 * explainable next-experiment recommendations. Deterministic layer only —
 * no LLM confidence, no autonomous publishing, no strategy mutation.
 */

const crypto = require('crypto');
const {
  ContentOutcomeError,
  getPublicationOutcome,
  listContentOutcomes,
  createMemoryStore: createOutcomeMemoryStore,
  createPostgresStore: createOutcomePostgresStore,
} = require('./contentOutcomeIntelligence');

const LEARNING_TYPES = Object.freeze([
  'distribution_pattern',
  'topic_performance',
  'message_resonance',
  'audience_response',
  'format_performance',
  'business_outcome_pattern',
  'conversion_pattern',
  'language_adoption',
  'objection_pattern',
  'talent_signal',
  'partnership_signal',
  'content_sequence_pattern',
  'other',
]);

const LEARNING_STATUSES = Object.freeze([
  'signal',
  'emerging',
  'supported',
  'contradicted',
  'stale',
]);

const AUDIENCE_CLASSES = Object.freeze([
  'SMB_operator',
  'founder',
  'buyer',
  'prospect',
  'AI_builder',
  'engineer',
  'strategic_partner',
  'creator',
  'investor',
  'unknown',
]);

const ATTRIBUTION_WEIGHT = Object.freeze({
  direct: 1,
  likely: 0.7,
  possible: 0.4,
  unknown: 0.2,
});

const COMMERCIAL_OUTCOME_TYPES = Object.freeze([
  'qualified_dm',
  'prospect_conversation',
  'partner_conversation',
  'builder_connection',
  'demo_interest',
  'meeting_booked',
  'pilot_interest',
  'customer_opportunity',
]);

const PARTNERSHIP_OUTCOME_TYPES = Object.freeze([
  'partner_conversation',
  'builder_connection',
]);

const CONFIG = Object.freeze({
  emergingMinSample: 2,
  supportedMinSample: 4,
  supportedMinGeneralization: 0.55,
  staleDays: 90,
  strongOutOfNetworkPct: 70,
  strongImpressions: 10000,
  strongMembersReached: 5000,
  strongFollowersGained: 30,
  strongProfileViews: 50,
  strongComments: 50,
  weakOutOfNetworkPct: 40,
  weakImpressions: 2000,
});

class ContentLearningError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {number} [status]
   */
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'ContentLearningError';
    this.code = code;
    this.status = status;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function newId() {
  return crypto.randomUUID();
}

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function asClientId(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function tenantForClient(clientId) {
  return String(clientId);
}

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function clamp01(n) {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

function round4(n) {
  return Math.round(clamp01(n) * 10000) / 10000;
}

function normalizeTopic(topic) {
  const t = asText(topic);
  if (!t) return null;
  return t.toLowerCase().replace(/\s+/g, ' ').trim();
}

function daysBetween(isoA, isoB) {
  const a = new Date(isoA).getTime();
  const b = new Date(isoB).getTime();
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
  return Math.abs(b - a) / (1000 * 60 * 60 * 24);
}

function uniqueIds(ids) {
  return [...new Set((ids || []).map(String).filter(Boolean))];
}

function assertEnum(value, allowed, fieldName, code) {
  const v = asText(value);
  if (!v || !allowed.includes(v)) {
    throw new ContentLearningError(
      code || `invalid_${fieldName}`,
      `${fieldName} must be one of: ${allowed.join(', ')}`
    );
  }
  return v;
}

function classifyAudience(raw) {
  const t = asText(raw);
  if (!t) return 'unknown';
  const lower = t.toLowerCase();
  if (/(smb|operator|business owner|office manager)/.test(lower)) return 'SMB_operator';
  if (/founder|ceo|co-founder/.test(lower)) return 'founder';
  if (/buyer|customer|prospect/.test(lower)) return 'buyer';
  if (/ai.?builder|builder/.test(lower)) return 'AI_builder';
  if (/engineer|developer|technical/.test(lower)) return 'engineer';
  if (/partner|strategic/.test(lower)) return 'strategic_partner';
  if (/creator|influencer/.test(lower)) return 'creator';
  if (/investor|vc/.test(lower)) return 'investor';
  if (AUDIENCE_CLASSES.includes(t)) return t;
  return 'unknown';
}

function extractOutOfNetworkPct(snapshot) {
  if (!snapshot) return null;
  const meta = snapshot.metadata || {};
  const candidates = [
    meta.outOfNetworkPct,
    meta.out_of_network_pct,
    meta.outOfNetworkShare,
    meta.out_of_network_share,
    meta.nonFollowerPct,
    meta.non_follower_pct,
    snapshot.outOfNetworkPct,
    snapshot.out_of_network_pct,
  ];
  for (const c of candidates) {
    if (c == null || c === '') continue;
    const n = Number(c);
    if (Number.isFinite(n)) {
      return n > 1 ? n : n * 100;
    }
  }
  return null;
}

function latestSnapshot(snapshots) {
  if (!snapshots || !snapshots.length) return null;
  const sorted = [...snapshots].sort(
    (a, b) => new Date(a.observedAt) - new Date(b.observedAt)
  );
  return sorted[sorted.length - 1];
}

function attributionBreakdown(outcomes) {
  const counts = { direct: 0, likely: 0, possible: 0, unknown: 0 };
  let weighted = 0;
  for (const o of outcomes || []) {
    const level = ATTRIBUTION_WEIGHT[o.attribution] != null ? o.attribution : 'unknown';
    counts[level] += 1;
    weighted += ATTRIBUTION_WEIGHT[level];
  }
  const total = outcomes?.length || 0;
  return {
    counts,
    total,
    averageWeight: total ? weighted / total : 0,
    summary:
      total === 0
        ? 'No business outcomes recorded.'
        : `${counts.direct} direct, ${counts.likely} likely, ${counts.possible} possible, ${counts.unknown} unknown attribution.`,
  };
}

function objectiveLens(objective) {
  const o = objective || 'awareness';
  const lenses = {
    category_creation: [
      'out_of_network_reach',
      'discussion_depth',
      'language_adoption',
      'profile_activity',
      'follower_growth',
      'qualified_audience_composition',
    ],
    lead_generation: [
      'qualified_dms',
      'prospect_conversations',
      'demo_interest',
      'meetings',
      'customer_opportunities',
    ],
    partnership_generation: [
      'partner_conversations',
      'founder_outreach',
      'technical_collaboration',
      'strategic_introductions',
      'meetings',
    ],
    thought_leadership: [
      'qualified_comments',
      'out_of_network_distribution',
      'technical_discussion',
      'peer_engagement',
      'language_adoption',
      'profile_activity',
    ],
    audience_growth: ['follower_growth', 'profile_activity', 'out_of_network_reach'],
    engagement: ['comments', 'reactions', 'discussion_depth'],
    awareness: ['impressions', 'members_reached', 'out_of_network_reach'],
    launch_runway: [
      'out_of_network_reach',
      'qualified_audience_composition',
      'profile_activity',
      'partner_conversations',
      'prospect_conversations',
    ],
  };
  return lenses[o] || lenses.awareness;
}

/**
 * Deterministic objective-relative assessment for one publication.
 * Distinguishes confidence-in-observation from generalizability.
 */
function assessPublicationAgainstObjective(full) {
  const pub = full.publication;
  const latest = latestSnapshot(full.performanceSnapshots);
  const outcomes = full.businessOutcomes || [];
  const signals = full.qualitativeSignals || [];
  const objective = pub.objective || null;
  const lens = objectiveLens(objective);
  const outOfNetworkPct = extractOutOfNetworkPct(latest);
  const attr = attributionBreakdown(outcomes);

  const observed = {
    impressions: latest?.impressions ?? null,
    membersReached: latest?.membersReached ?? null,
    outOfNetworkPct,
    profileViewsAttributed: latest?.profileViewsAttributed ?? null,
    followersGained: latest?.followersGained ?? null,
    comments: latest?.comments ?? null,
    reactions: latest?.reactions ?? null,
    businessOutcomeCount: outcomes.length,
    qualitativeSignalCount: signals.length,
    attribution: attr.counts,
  };

  const evidenceBits = [];
  let observationScore = 0.35;
  let commercialWeight = 0;

  if (outOfNetworkPct != null && outOfNetworkPct >= CONFIG.strongOutOfNetworkPct) {
    evidenceBits.push(`${outOfNetworkPct}% out-of-network distribution`);
    if (lens.includes('out_of_network_reach') || lens.includes('out_of_network_distribution')) {
      observationScore += 0.25;
    } else {
      observationScore += 0.1;
    }
  }
  if (latest?.impressions != null && latest.impressions >= CONFIG.strongImpressions) {
    evidenceBits.push(`${latest.impressions.toLocaleString()} impressions`);
    if (lens.includes('impressions') || lens.includes('out_of_network_reach')) {
      observationScore += 0.12;
    }
  }
  if (latest?.membersReached != null && latest.membersReached >= CONFIG.strongMembersReached) {
    evidenceBits.push(`${latest.membersReached.toLocaleString()} members reached`);
    observationScore += 0.08;
  }
  if (latest?.followersGained != null && latest.followersGained >= CONFIG.strongFollowersGained) {
    evidenceBits.push(`${latest.followersGained} followers gained`);
    if (lens.includes('follower_growth')) observationScore += 0.1;
    else observationScore += 0.04;
  }
  if (
    latest?.profileViewsAttributed != null &&
    latest.profileViewsAttributed >= CONFIG.strongProfileViews
  ) {
    evidenceBits.push(`${latest.profileViewsAttributed} attributed profile views`);
    if (lens.includes('profile_activity')) observationScore += 0.08;
  }
  if (latest?.comments != null && latest.comments >= CONFIG.strongComments) {
    evidenceBits.push(`${latest.comments} comments`);
    if (lens.includes('discussion_depth') || lens.includes('qualified_comments')) {
      observationScore += 0.08;
    }
  }

  for (const o of outcomes) {
    const w = ATTRIBUTION_WEIGHT[o.attribution] ?? ATTRIBUTION_WEIGHT.unknown;
    commercialWeight += w;
    if (COMMERCIAL_OUTCOME_TYPES.includes(o.outcomeType)) {
      const commercialRelevant =
        lens.some((x) =>
          /conversation|meeting|demo|partner|qualified|customer|opportunity/.test(x)
        ) ||
        objective === 'category_creation' ||
        objective === 'launch_runway' ||
        objective === 'thought_leadership';
      if (commercialRelevant) observationScore += 0.03 * w;
    }
  }
  if (outcomes.length) {
    evidenceBits.push(
      `${outcomes.length} business outcome(s) (${attr.summary})`
    );
  }
  if (signals.length) {
    evidenceBits.push(`${signals.length} qualitative signal(s)`);
    if (lens.includes('language_adoption') || lens.includes('qualified_audience_composition')) {
      observationScore += Math.min(0.1, signals.length * 0.025);
    }
  }

  const observationConfidence = round4(clamp01(observationScore));
  // Single publication: high observation confidence possible; generalization stays low.
  const generalizationConfidence = round4(observationConfidence * 0.25);

  let assessment;
  if (objective === 'category_creation' || objective === 'launch_runway') {
    assessment =
      outOfNetworkPct != null && outOfNetworkPct >= CONFIG.strongOutOfNetworkPct
        ? 'Strong evidence that the publication achieved broad category-level discovery beyond the existing network.'
        : outcomes.length
          ? 'Mixed category evidence: downstream activity present, but distribution strength is limited or unmeasured.'
          : 'Insufficient evidence that category discovery occurred.';
  } else if (objective === 'lead_generation') {
    const leadish = outcomes.filter((o) =>
      ['qualified_dm', 'prospect_conversation', 'demo_interest', 'meeting_booked', 'customer_opportunity'].includes(
        o.outcomeType
      )
    );
    assessment = leadish.length
      ? `Lead-generation lens: ${leadish.length} relevant business outcome(s) recorded with varying attribution strength.`
      : 'Lead-generation lens: no qualified lead outcomes recorded yet.';
  } else if (objective === 'partnership_generation') {
    const partners = outcomes.filter((o) =>
      PARTNERSHIP_OUTCOME_TYPES.includes(o.outcomeType)
    );
    assessment = partners.length
      ? `Partnership lens: ${partners.length} partner/builder outcome(s) recorded.`
      : 'Partnership lens: no partner outcomes recorded yet.';
  } else {
    assessment =
      evidenceBits.length > 0
        ? `Objective ${objective || '(unset)'}: observed signals include ${evidenceBits.slice(0, 4).join('; ')}.`
        : `Objective ${objective || '(unset)'}: insufficient recorded evidence.`;
  }

  return {
    publicationId: pub.id,
    objective,
    lens,
    observed,
    assessment,
    observationConfidence,
    generalizationConfidence,
    confidenceLabel: {
      observation:
        observationConfidence >= 0.7
          ? 'high'
          : observationConfidence >= 0.4
            ? 'medium'
            : 'low',
      generalization:
        generalizationConfidence >= 0.55
          ? 'high'
          : generalizationConfidence >= 0.3
            ? 'medium'
            : 'low',
    },
    reason:
      'Only the evidence attached to this publication is scored here; generalizability requires comparable repeats.',
    attributionSummary: attr.summary,
    commercialWeight: round4(commercialWeight),
    evidenceBits,
  };
}

function fingerprintFor(derived) {
  const parts = [
    derived.learningType,
    derived.patternKey || 'default',
    derived.objective || '',
    normalizeTopic(derived.topic) || '',
    derived.format || '',
    derived.audienceType || '',
    derived.channel || '',
  ];
  return parts.join('|').toLowerCase();
}

function computeConfidence({
  observationStrength,
  supportingCount,
  contradictingCount,
  attributionQuality,
  daysSinceEvidence,
  objectiveConsistent,
}) {
  const evidenceStrength = clamp01(observationStrength * (0.5 + 0.5 * attributionQuality));
  const sample = Math.max(1, supportingCount);
  const repetition = clamp01(sample / CONFIG.supportedMinSample);
  const consistency =
    supportingCount + contradictingCount === 0
      ? 0.5
      : supportingCount / (supportingCount + contradictingCount * 1.5);
  const relevance = objectiveConsistent === false ? 0.75 : 1;
  const recency = clamp01(1 - daysSinceEvidence / (CONFIG.staleDays * 1.5));

  const observationConfidence = round4(evidenceStrength);
  const generalizationConfidence = round4(
    evidenceStrength * repetition * consistency * relevance * recency
  );
  // Primary confidence for ranking = generalization when sample>1, else observation (labeled separately).
  const confidence = round4(
    sample === 1
      ? observationConfidence * 0.35 + generalizationConfidence * 0.65
      : generalizationConfidence
  );

  return {
    confidence,
    observationConfidence,
    generalizationConfidence,
    factors: {
      evidenceStrength: round4(evidenceStrength),
      repetition: round4(repetition),
      consistency: round4(consistency),
      relevance: round4(relevance),
      recency: round4(recency),
      attributionQuality: round4(attributionQuality),
    },
  };
}

function statusFromEvidence({
  supportingCount,
  contradictingCount,
  generalizationConfidence,
  daysSinceEvidence,
  now = nowIso(),
}) {
  void now;
  if (daysSinceEvidence > CONFIG.staleDays && supportingCount > 0) {
    return 'stale';
  }
  if (contradictingCount > 0 && contradictingCount >= supportingCount) {
    return 'contradicted';
  }
  if (supportingCount <= 1) return 'signal';
  if (
    supportingCount >= CONFIG.supportedMinSample &&
    generalizationConfidence >= CONFIG.supportedMinGeneralization &&
    contradictingCount === 0
  ) {
    return 'supported';
  }
  if (supportingCount >= CONFIG.emergingMinSample) return 'emerging';
  return 'signal';
}

function supportsDistribution(latest, outOfNetworkPct) {
  if (outOfNetworkPct != null && outOfNetworkPct >= CONFIG.strongOutOfNetworkPct) return true;
  if (latest?.impressions != null && latest.impressions >= CONFIG.strongImpressions) return true;
  if (latest?.membersReached != null && latest.membersReached >= CONFIG.strongMembersReached) {
    return true;
  }
  return false;
}

function contradictsDistribution(latest, outOfNetworkPct) {
  if (!latest) return false;
  const weakNetwork =
    outOfNetworkPct != null && outOfNetworkPct <= CONFIG.weakOutOfNetworkPct;
  const weakImpressions =
    latest.impressions != null && latest.impressions <= CONFIG.weakImpressions;
  return weakNetwork || (weakImpressions && (outOfNetworkPct == null || weakNetwork));
}

/**
 * Derive candidate learning signals from one publication outcome (not yet persisted).
 */
function deriveContentSignalsFromOutcome(full, assessment) {
  const pub = full.publication;
  const latest = latestSnapshot(full.performanceSnapshots);
  const outcomes = full.businessOutcomes || [];
  const signals = full.qualitativeSignals || [];
  const outOfNetworkPct = extractOutOfNetworkPct(latest);
  const attr = attributionBreakdown(outcomes);
  const derived = [];

  if (supportsDistribution(latest, outOfNetworkPct)) {
    const bits = [];
    if (outOfNetworkPct != null) bits.push(`${outOfNetworkPct}% out-of-network`);
    if (latest?.membersReached != null) {
      bits.push(`${latest.membersReached.toLocaleString()} members reached`);
    }
    if (latest?.followersGained != null) bits.push(`${latest.followersGained} followers gained`);
    if (latest?.profileViewsAttributed != null) {
      bits.push(`${latest.profileViewsAttributed} attributed profile views`);
    }
    derived.push({
      learningType: 'distribution_pattern',
      patternKey: 'strong_discovery',
      statement: pub.topic
        ? `The "${pub.topic}" publication demonstrated strong discovery outside the existing network.`
        : 'This publication demonstrated strong discovery outside the existing network.',
      objective: pub.objective,
      topic: pub.topic,
      format: pub.format,
      channel: pub.channel,
      audienceType: null,
      evidenceSummary: bits.join('; ') || 'Strong distribution metrics recorded.',
      uncertaintySummary:
        'Generalizability is low until comparable publications repeat the pattern.',
      observationStrength: Math.max(0.7, assessment.observationConfidence),
      attributionQuality: 1,
      polarity: 'support',
      publicationId: pub.id,
    });
  }

  const partnerOutcomes = outcomes.filter((o) =>
    PARTNERSHIP_OUTCOME_TYPES.includes(o.outcomeType)
  );
  if (partnerOutcomes.length > 0) {
    derived.push({
      learningType: 'partnership_signal',
      patternKey: 'inbound_partner_builder',
      statement:
        'The publication coincided with meaningful inbound interest from AI builders, technical peers, and/or potential strategic partners.',
      objective: pub.objective,
      topic: pub.topic,
      format: pub.format,
      channel: pub.channel,
      audienceType: 'strategic_partner',
      evidenceSummary: `${partnerOutcomes.length} partner/builder outcome(s). ${attr.summary}`,
      uncertaintySummary:
        'Attribution strength varies between direct, likely, and possible — do not inflate causal certainty.',
      observationStrength: clamp01(0.45 + partnerOutcomes.length * 0.04),
      attributionQuality: Math.max(0.2, attr.averageWeight),
      polarity: 'support',
      publicationId: pub.id,
    });
  }

  const commercial = outcomes.filter((o) =>
    COMMERCIAL_OUTCOME_TYPES.includes(o.outcomeType)
  );
  if (commercial.length >= 3) {
    derived.push({
      learningType: 'business_outcome_pattern',
      patternKey: 'downstream_business_activity',
      statement:
        'The publication was followed by multiple recorded business outcomes distinct from vanity engagement.',
      objective: pub.objective,
      topic: pub.topic,
      format: pub.format,
      channel: pub.channel,
      audienceType: null,
      evidenceSummary: `${commercial.length} business outcomes. ${attr.summary}`,
      uncertaintySummary: 'Business outcomes remain attribution-weighted; not all are direct.',
      observationStrength: clamp01(0.5 + commercial.length * 0.02),
      attributionQuality: Math.max(0.2, attr.averageWeight),
      polarity: 'support',
      publicationId: pub.id,
    });
  }

  const talentish = signals.filter((s) =>
    /talent|employment|hire|job|engineer outreach/i.test(s.description || '')
  );
  const talentOutcomes = outcomes.filter((o) => o.outcomeType === 'builder_connection');
  if (talentish.length || talentOutcomes.length) {
    derived.push({
      learningType: 'talent_signal',
      patternKey: 'technical_talent_inbound',
      statement:
        'Technical talent initiated unsolicited outreach or employment-adjacent conversations after the publication.',
      objective: pub.objective,
      topic: pub.topic,
      format: pub.format,
      channel: pub.channel,
      audienceType: 'engineer',
      evidenceSummary: `${talentOutcomes.length} builder connection(s); ${talentish.length} talent-related qualitative signal(s).`,
      uncertaintySummary: 'Talent interest is a signal, not a hiring outcome.',
      observationStrength: 0.55,
      attributionQuality: Math.max(0.3, attr.averageWeight || 0.4),
      polarity: 'support',
      publicationId: pub.id,
    });
  }

  const audiences = new Set();
  for (const s of signals) {
    audiences.add(classifyAudience(s.audienceType));
  }
  audiences.delete('unknown');
  if (audiences.size >= 2) {
    derived.push({
      learningType: 'audience_response',
      patternKey: 'mixed_qualified_audience',
      statement: `The post attracted a mixed qualified audience (${[...audiences].join(', ')}), not a single respondent class.`,
      objective: pub.objective,
      topic: pub.topic,
      format: pub.format,
      channel: pub.channel,
      audienceType: 'mixed',
      evidenceSummary: `Audience classes observed: ${[...audiences].join(', ')}.`,
      uncertaintySummary: 'Audience labels come from operator/qualitative capture quality.',
      observationStrength: 0.6,
      attributionQuality: 0.7,
      polarity: 'support',
      publicationId: pub.id,
    });
  }

  for (const s of signals) {
    if (s.signalType === 'language_adoption') {
      derived.push({
        learningType: 'language_adoption',
        patternKey: 'phrase_adoption',
        statement: s.description,
        objective: pub.objective,
        topic: pub.topic,
        format: pub.format,
        channel: pub.channel,
        audienceType: classifyAudience(s.audienceType),
        evidenceSummary: `Qualitative language_adoption signal (${s.strength || 'unspecified strength'}).`,
        uncertaintySummary:
          'Operator observations are not silently treated as externally verified facts.',
        observationStrength: 0.55,
        attributionQuality: 0.6,
        polarity: 'support',
        publicationId: pub.id,
      });
    }
    if (s.signalType === 'message_resonance' || s.signalType === 'technical_interest') {
      derived.push({
        learningType: 'message_resonance',
        patternKey: s.signalType,
        statement: s.description,
        objective: pub.objective,
        topic: pub.topic,
        format: pub.format,
        channel: pub.channel,
        audienceType: classifyAudience(s.audienceType),
        evidenceSummary: `Qualitative ${s.signalType} signal.`,
        uncertaintySummary: 'Qualitative resonance requires repetition before strategy claims.',
        observationStrength: 0.5,
        attributionQuality: 0.55,
        polarity: 'support',
        publicationId: pub.id,
      });
    }
    if (s.signalType === 'objection') {
      derived.push({
        learningType: 'objection_pattern',
        patternKey: 'objection',
        statement: s.description,
        objective: pub.objective,
        topic: pub.topic,
        format: pub.format,
        channel: pub.channel,
        audienceType: classifyAudience(s.audienceType),
        evidenceSummary: 'Qualitative objection signal.',
        uncertaintySummary: 'Objections may be idiosyncratic to one audience segment.',
        observationStrength: 0.45,
        attributionQuality: 0.5,
        polarity: 'support',
        publicationId: pub.id,
      });
    }
  }

  // Explicit contradiction candidates for distribution learnings of same scope.
  if (contradictsDistribution(latest, outOfNetworkPct) && pub.objective) {
    derived.push({
      learningType: 'distribution_pattern',
      patternKey: 'strong_discovery',
      statement: pub.topic
        ? `The "${pub.topic}" publication did not repeat strong out-of-network discovery.`
        : 'This publication did not repeat strong out-of-network discovery.',
      objective: pub.objective,
      topic: pub.topic,
      format: pub.format,
      channel: pub.channel,
      audienceType: null,
      evidenceSummary:
        outOfNetworkPct != null
          ? `${outOfNetworkPct}% out-of-network; impressions ${latest?.impressions ?? 'n/a'}`
          : `Impressions ${latest?.impressions ?? 'n/a'} (weak vs prior breakout threshold).`,
      uncertaintySummary: 'Weak distribution may reflect topic, timing, or format change.',
      observationStrength: 0.55,
      attributionQuality: 1,
      polarity: 'contradict',
      publicationId: pub.id,
    });
  }

  return derived.map((d) => ({
    ...d,
    fingerprint: fingerprintFor(d),
  }));
}

function createMemoryStore() {
  /** @type {Map<string, object>} */
  const learnings = new Map();
  return {
    kind: 'memory',
    async upsertLearning(row) {
      learnings.set(row.id, clone(row));
      return clone(row);
    },
    async getLearning(id, clientId) {
      const row = learnings.get(id);
      if (!row) return null;
      if (clientId != null && row.clientId !== clientId) return null;
      return clone(row);
    },
    async getByFingerprint(clientId, fingerprint) {
      for (const row of learnings.values()) {
        if (row.clientId === clientId && row.fingerprint === fingerprint) {
          return clone(row);
        }
      }
      return null;
    },
    async listLearnings(filter) {
      let rows = [...learnings.values()];
      if (filter.clientId != null) {
        rows = rows.filter((r) => r.clientId === filter.clientId);
      }
      if (filter.status) rows = rows.filter((r) => r.status === filter.status);
      if (filter.learningType) {
        rows = rows.filter((r) => r.learningType === filter.learningType);
      }
      if (filter.objective) {
        rows = rows.filter((r) => r.objective === filter.objective);
      }
      if (filter.topic) {
        const t = String(filter.topic).toLowerCase();
        rows = rows.filter(
          (r) => r.topic && String(r.topic).toLowerCase().includes(t)
        );
      }
      if (filter.audienceType) {
        rows = rows.filter((r) => r.audienceType === filter.audienceType);
      }
      if (filter.channel) rows = rows.filter((r) => r.channel === filter.channel);
      rows.sort(
        (a, b) =>
          b.confidence - a.confidence ||
          new Date(b.lastEvaluatedAt) - new Date(a.lastEvaluatedAt)
      );
      if (filter.limit != null) rows = rows.slice(0, filter.limit);
      return rows.map(clone);
    },
  };
}

function mapLearningRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    clientId: row.client_id,
    tenantId: row.tenant_id,
    fingerprint: row.fingerprint,
    learningType: row.learning_type,
    statement: row.statement,
    scope: row.scope || {},
    objective: row.objective,
    topic: row.topic,
    format: row.format,
    audienceType: row.audience_type,
    channel: row.channel,
    confidence: Number(row.confidence),
    observationConfidence: Number(row.observation_confidence),
    generalizationConfidence: Number(row.generalization_confidence),
    sampleSize: Number(row.sample_size),
    supportingPublicationIds: (row.supporting_publication_ids || []).map(String),
    contradictingPublicationIds: (row.contradicting_publication_ids || []).map(String),
    evidenceSummary: row.evidence_summary || '',
    uncertaintySummary: row.uncertainty_summary,
    status: row.status,
    firstObservedAt:
      row.first_observed_at instanceof Date
        ? row.first_observed_at.toISOString()
        : String(row.first_observed_at),
    lastEvaluatedAt:
      row.last_evaluated_at instanceof Date
        ? row.last_evaluated_at.toISOString()
        : String(row.last_evaluated_at),
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
    updatedAt:
      row.updated_at instanceof Date
        ? row.updated_at.toISOString()
        : String(row.updated_at),
  };
}

function createPostgresStore(pool) {
  const db = pool || require('../db');
  return {
    kind: 'postgres',
    async upsertLearning(row) {
      const result = await db.query(
        `INSERT INTO content_learnings (
          id, client_id, tenant_id, fingerprint, learning_type, statement, scope,
          objective, topic, format, audience_type, channel,
          confidence, observation_confidence, generalization_confidence, sample_size,
          supporting_publication_ids, contradicting_publication_ids,
          evidence_summary, uncertainty_summary, status,
          first_observed_at, last_evaluated_at, created_at, updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25
        )
        ON CONFLICT (client_id, fingerprint) DO UPDATE SET
          learning_type = EXCLUDED.learning_type,
          statement = EXCLUDED.statement,
          scope = EXCLUDED.scope,
          objective = EXCLUDED.objective,
          topic = EXCLUDED.topic,
          format = EXCLUDED.format,
          audience_type = EXCLUDED.audience_type,
          channel = EXCLUDED.channel,
          confidence = EXCLUDED.confidence,
          observation_confidence = EXCLUDED.observation_confidence,
          generalization_confidence = EXCLUDED.generalization_confidence,
          sample_size = EXCLUDED.sample_size,
          supporting_publication_ids = EXCLUDED.supporting_publication_ids,
          contradicting_publication_ids = EXCLUDED.contradicting_publication_ids,
          evidence_summary = EXCLUDED.evidence_summary,
          uncertainty_summary = EXCLUDED.uncertainty_summary,
          status = EXCLUDED.status,
          last_evaluated_at = EXCLUDED.last_evaluated_at,
          updated_at = EXCLUDED.updated_at
        RETURNING *`,
        [
          row.id,
          row.clientId,
          row.tenantId,
          row.fingerprint,
          row.learningType,
          row.statement,
          JSON.stringify(row.scope || {}),
          row.objective,
          row.topic,
          row.format,
          row.audienceType,
          row.channel,
          row.confidence,
          row.observationConfidence,
          row.generalizationConfidence,
          row.sampleSize,
          row.supportingPublicationIds,
          row.contradictingPublicationIds,
          row.evidenceSummary,
          row.uncertaintySummary,
          row.status,
          row.firstObservedAt,
          row.lastEvaluatedAt,
          row.createdAt,
          row.updatedAt,
        ]
      );
      return mapLearningRow(result.rows[0]);
    },
    async getLearning(id, clientId) {
      const result = await db.query(
        `SELECT * FROM content_learnings
         WHERE id = $1 AND ($2::int IS NULL OR client_id = $2)`,
        [id, clientId]
      );
      return mapLearningRow(result.rows[0]);
    },
    async getByFingerprint(clientId, fingerprint) {
      const result = await db.query(
        `SELECT * FROM content_learnings
         WHERE client_id = $1 AND fingerprint = $2`,
        [clientId, fingerprint]
      );
      return mapLearningRow(result.rows[0]);
    },
    async listLearnings(filter) {
      const clauses = ['client_id = $1'];
      const params = [filter.clientId];
      let i = 2;
      if (filter.status) {
        clauses.push(`status = $${i++}`);
        params.push(filter.status);
      }
      if (filter.learningType) {
        clauses.push(`learning_type = $${i++}`);
        params.push(filter.learningType);
      }
      if (filter.objective) {
        clauses.push(`objective = $${i++}`);
        params.push(filter.objective);
      }
      if (filter.topic) {
        clauses.push(`topic ILIKE $${i++}`);
        params.push(`%${filter.topic}%`);
      }
      if (filter.audienceType) {
        clauses.push(`audience_type = $${i++}`);
        params.push(filter.audienceType);
      }
      if (filter.channel) {
        clauses.push(`channel = $${i++}`);
        params.push(filter.channel);
      }
      let sql = `SELECT * FROM content_learnings WHERE ${clauses.join(' AND ')}
                 ORDER BY confidence DESC, last_evaluated_at DESC`;
      if (filter.limit != null) {
        sql += ` LIMIT $${i++}`;
        params.push(filter.limit);
      }
      const result = await db.query(sql, params);
      return result.rows.map(mapLearningRow);
    },
  };
}

function resolveLearningStore(opts = {}) {
  if (opts.learningStore) return opts.learningStore;
  if (opts.store && opts.store.upsertLearning) return opts.store;
  return createPostgresStore(opts.pool);
}

function resolveOutcomeOpts(opts = {}) {
  if (opts.outcomeStore) return { store: opts.outcomeStore, pool: opts.pool };
  if (opts.store && opts.store.insertPublication) return { store: opts.store, pool: opts.pool };
  return { pool: opts.pool };
}

function mergeLearning(existing, derived, now) {
  const supporting = new Set(existing?.supportingPublicationIds || []);
  const contradicting = new Set(existing?.contradictingPublicationIds || []);

  if (derived.polarity === 'contradict') {
    contradicting.add(derived.publicationId);
    supporting.delete(derived.publicationId);
  } else {
    supporting.add(derived.publicationId);
    // A later support from same pub removes contradiction from that pub.
    contradicting.delete(derived.publicationId);
  }

  const supportingIds = uniqueIds([...supporting]);
  const contradictingIds = uniqueIds([...contradicting]);
  const sampleSize = supportingIds.length;

  const conf = computeConfidence({
    observationStrength: derived.observationStrength,
    supportingCount: sampleSize,
    contradictingCount: contradictingIds.length,
    attributionQuality: derived.attributionQuality,
    daysSinceEvidence: 0,
    objectiveConsistent: true,
  });

  // Prefer durable supportive statement from first support; keep contradiction notes in evidence.
  let statement = derived.statement;
  if (existing && derived.polarity === 'contradict') {
    statement = existing.statement;
  } else if (existing && existing.statement && sampleSize > 1 && derived.polarity === 'support') {
    // Narrow/generalize carefully: keep first statement unless emerging multi-post.
    if (sampleSize >= CONFIG.emergingMinSample && existing.learningType === 'distribution_pattern') {
      statement =
        'Operator-centered / thesis-led posts in this scope have repeatedly produced strong out-of-network discovery.';
    } else {
      statement = existing.statement;
    }
  }

  const evidenceSummary = [
    existing?.evidenceSummary,
    derived.evidenceSummary,
    contradictingIds.length
      ? `Contradicting publications: ${contradictingIds.length}.`
      : null,
  ]
    .filter(Boolean)
    .join(' | ');

  const status = statusFromEvidence({
    supportingCount: sampleSize,
    contradictingCount: contradictingIds.length,
    generalizationConfidence: conf.generalizationConfidence,
    daysSinceEvidence: 0,
    now,
  });

  // Hard safeguard: never allow supported with a single publication.
  const safeStatus =
    sampleSize <= 1 && status === 'supported' ? 'signal' : status;

  const id = existing?.id || newId();
  const createdAt = existing?.createdAt || now;
  const firstObservedAt = existing?.firstObservedAt || now;

  return {
    id,
    clientId: derived.clientId,
    tenantId: derived.tenantId,
    fingerprint: derived.fingerprint,
    learningType: derived.learningType,
    statement,
    scope: {
      patternKey: derived.patternKey,
      objective: derived.objective,
      topic: normalizeTopic(derived.topic),
      format: derived.format,
      audienceType: derived.audienceType,
      channel: derived.channel,
    },
    objective: derived.objective,
    topic: derived.topic,
    format: derived.format,
    audienceType: derived.audienceType,
    channel: derived.channel,
    confidence: conf.confidence,
    observationConfidence: conf.observationConfidence,
    generalizationConfidence: conf.generalizationConfidence,
    sampleSize,
    supportingPublicationIds: supportingIds,
    contradictingPublicationIds: contradictingIds,
    evidenceSummary,
    uncertaintySummary:
      derived.uncertaintySummary ||
      existing?.uncertaintySummary ||
      'Further comparable publications needed before treating this as a durable pattern.',
    status: safeStatus,
    firstObservedAt,
    lastEvaluatedAt: now,
    createdAt,
    updatedAt: now,
    confidenceFactors: conf.factors,
  };
}

/**
 * Evaluate one publication: objective assessment + derive/upsert learnings.
 */
async function evaluateContentPublication(publicationId, opts = {}) {
  const clientId = asClientId(opts.clientId ?? opts.client_id ?? opts.tenantId);
  const outcomeOpts = resolveOutcomeOpts(opts);
  const learningStore = resolveLearningStore(opts);

  let full;
  try {
    full = await getPublicationOutcome(publicationId, {
      ...outcomeOpts,
      clientId,
    });
  } catch (err) {
    if (err instanceof ContentOutcomeError) {
      throw new ContentLearningError(err.code, err.message, err.status);
    }
    throw err;
  }

  const assessment = assessPublicationAgainstObjective(full);
  const derivedList = deriveContentSignalsFromOutcome(full, assessment);
  const now = nowIso();
  const upserted = [];

  for (const derived of derivedList) {
    const withTenant = {
      ...derived,
      clientId: full.publication.clientId,
      tenantId: full.publication.tenantId || tenantForClient(full.publication.clientId),
    };
    const existing = await learningStore.getByFingerprint(
      withTenant.clientId,
      withTenant.fingerprint
    );
    // Do not create brand-new learnings from contradiction-only observations.
    if (derived.polarity === 'contradict' && !existing) continue;
    const row = mergeLearning(existing, withTenant, now);
    const saved = await learningStore.upsertLearning(row);
    upserted.push({
      ...saved,
      polarity: derived.polarity,
      confidenceFactors: row.confidenceFactors,
    });
  }

  return {
    publicationId: full.publication.id,
    clientId: full.publication.clientId,
    tenantId: full.publication.tenantId,
    campaignId: full.publication.campaignId,
    assessment,
    derivedCount: derivedList.length,
    learnings: upserted,
    outcomeRef: {
      performanceSnapshots: full.performanceSnapshots.length,
      businessOutcomes: full.businessOutcomes.length,
      qualitativeSignals: full.qualitativeSignals.length,
    },
    guardrails: {
      universalContentScore: null,
      autonomousPublish: false,
      autonomousStrategyMutation: false,
      singlePostCannotBeSupportedPattern: true,
    },
    evaluatedAt: now,
  };
}

async function deriveContentSignals(input = {}, opts = {}) {
  const clientId = asClientId(input.tenantId ?? input.clientId ?? input.client_id);
  if (clientId == null) {
    throw new ContentLearningError('client_id_required', 'tenantId / client_id required');
  }
  const publicationIds = (input.publicationIds || input.publication_ids || []).map(String);
  const outcomeOpts = resolveOutcomeOpts(opts);
  const items = publicationIds.length
    ? await Promise.all(
        publicationIds.map((id) =>
          getPublicationOutcome(id, { ...outcomeOpts, clientId })
        )
      )
    : await listContentOutcomes({ clientId, limit: input.limit || 50 }, outcomeOpts);

  const all = [];
  for (const full of items) {
    const outcome = full.publication
      ? full
      : await getPublicationOutcome(full.publication?.id || full.id, {
          ...outcomeOpts,
          clientId,
        });
    const assessment = assessPublicationAgainstObjective(outcome);
    all.push(...deriveContentSignalsFromOutcome(outcome, assessment));
  }
  return all;
}

async function evaluateContentLearning(learningId, opts = {}) {
  const clientId = asClientId(opts.clientId ?? opts.client_id ?? opts.tenantId);
  const learningStore = resolveLearningStore(opts);
  const learning = await learningStore.getLearning(learningId, clientId);
  if (!learning) {
    throw new ContentLearningError('learning_not_found', 'learning not found', 404);
  }

  const outcomeOpts = resolveOutcomeOpts(opts);
  const pubIds = uniqueIds([
    ...learning.supportingPublicationIds,
    ...learning.contradictingPublicationIds,
  ]);

  let supporting = [];
  let contradicting = [];
  for (const pid of pubIds) {
    try {
      const full = await getPublicationOutcome(pid, {
        ...outcomeOpts,
        clientId: learning.clientId,
      });
      const assessment = assessPublicationAgainstObjective(full);
      const derived = deriveContentSignalsFromOutcome(full, assessment).filter(
        (d) => d.fingerprint === learning.fingerprint
      );
      const hit = derived[0];
      if (!hit) continue;
      if (hit.polarity === 'contradict') contradicting.push(pid);
      else supporting.push(pid);
    } catch {
      // publication may have been removed; skip
    }
  }

  supporting = uniqueIds(supporting);
  contradicting = uniqueIds(contradicting);
  const now = nowIso();
  const daysSince = daysBetween(learning.lastEvaluatedAt, now);
  const conf = computeConfidence({
    observationStrength: learning.observationConfidence || learning.confidence || 0.5,
    supportingCount: supporting.length,
    contradictingCount: contradicting.length,
    attributionQuality: 0.7,
    daysSinceEvidence: daysSince,
    objectiveConsistent: true,
  });
  let status = statusFromEvidence({
    supportingCount: supporting.length,
    contradictingCount: contradicting.length,
    generalizationConfidence: conf.generalizationConfidence,
    daysSinceEvidence: daysSince,
    now,
  });
  if (supporting.length <= 1 && status === 'supported') status = 'signal';

  const updated = {
    ...learning,
    supportingPublicationIds: supporting,
    contradictingPublicationIds: contradicting,
    sampleSize: supporting.length,
    confidence: conf.confidence,
    observationConfidence: conf.observationConfidence,
    generalizationConfidence: conf.generalizationConfidence,
    status,
    lastEvaluatedAt: now,
    updatedAt: now,
  };
  return learningStore.upsertLearning(updated);
}

async function listContentLearnings(filter = {}, opts = {}) {
  const clientId = asClientId(
    filter.clientId ?? filter.client_id ?? filter.tenantId ?? opts.clientId
  );
  if (clientId == null) {
    throw new ContentLearningError('client_id_required', 'client_id / tenantId required');
  }
  const learningStore = resolveLearningStore(opts);
  return learningStore.listLearnings({
    clientId,
    status: asText(filter.status),
    learningType: asText(filter.learningType ?? filter.learning_type),
    objective: asText(filter.objective),
    topic: asText(filter.topic),
    audienceType: asText(filter.audienceType ?? filter.audience_type ?? filter.audience),
    channel: asText(filter.channel),
    limit: filter.limit != null ? Number(filter.limit) : null,
  });
}

async function getContentLearning(id, opts = {}) {
  const clientId = asClientId(opts.clientId ?? opts.client_id ?? opts.tenantId);
  const learningStore = resolveLearningStore(opts);
  const row = await learningStore.getLearning(id, clientId);
  if (!row) {
    throw new ContentLearningError('learning_not_found', 'learning not found', 404);
  }
  return row;
}

/**
 * Retrieve relevant learnings for Paige planning context.
 */
async function getRelevantContentLearnings(context = {}, opts = {}) {
  const clientId = asClientId(
    context.tenantId ?? context.clientId ?? context.client_id
  );
  if (clientId == null) {
    throw new ContentLearningError('client_id_required', 'tenantId / client_id required');
  }
  const limit = Math.max(1, Math.min(Number(context.limit) || 8, 25));
  const rows = await listContentLearnings(
    {
      clientId,
      objective: context.objective,
      topic: context.topic,
      audienceType: context.audience ?? context.audienceType,
      channel: context.channel,
      limit: 50,
    },
    opts
  );

  const scored = rows
    .filter((r) => r.status !== 'stale')
    .map((r) => {
      let relevance = 0.4;
      if (context.objective && r.objective === context.objective) relevance += 0.25;
      if (context.channel && r.channel === context.channel) relevance += 0.1;
      if (context.topic && r.topic && normalizeTopic(r.topic) === normalizeTopic(context.topic)) {
        relevance += 0.15;
      } else if (
        context.topic &&
        r.topic &&
        normalizeTopic(r.topic)?.includes(normalizeTopic(context.topic) || '')
      ) {
        relevance += 0.08;
      }
      if (context.audience && r.audienceType === context.audience) relevance += 0.1;
      if (context.campaignId && r.scope?.campaignId === context.campaignId) relevance += 0.05;
      const recency = clamp01(
        1 - daysBetween(r.lastEvaluatedAt, nowIso()) / (CONFIG.staleDays * 1.2)
      );
      const score = round4(
        relevance * 0.45 + r.confidence * 0.35 + recency * 0.2
      );
      return { learning: r, score, relevance: round4(relevance) };
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);

  return scored.map((s) => ({
    ...s.learning,
    retrievalScore: s.score,
    relevance: s.relevance,
  }));
}

function buildExperiment(learnings, context) {
  const hasDistribution = learnings.some(
    (l) => l.learningType === 'distribution_pattern' && l.status !== 'contradicted'
  );
  const hasPartnership = learnings.some(
    (l) => l.learningType === 'partnership_signal' || l.learningType === 'audience_response'
  );

  const hypothesis = hasDistribution
    ? 'Operator-centered critiques of traditional software / AI decision-making generate discovery beyond the existing network.'
    : 'Continuing the current objective with a distinct argument will produce measurable, attributable signals.';

  const preserve = [
    'operator-centered framing',
    'strong declarative thesis',
    'AI / business-software subject',
  ];
  if (context.channel) preserve.push(`${context.channel} channel`);

  const vary = ['specific argument'];
  const nextArgument =
    context.nextArgument ||
    'AI systems should understand uncertainty before acting.';

  return {
    hypothesis,
    objective:
      context.objective ||
      learnings.find((l) => l.objective)?.objective ||
      'category_creation',
    preserve,
    vary,
    nextArgument,
    expected_signal: hasDistribution
      ? [
          'strong out-of-network distribution',
          hasPartnership
            ? 'qualified founder/operator and/or partner discussion'
            : 'qualified discussion',
        ]
      : ['clear objective-relative signal improvement'],
    failure_signal: [
      'distribution returns primarily to existing network',
      'audience becomes overwhelmingly one non-target class',
    ],
    supporting_learning_ids: learnings.map((l) => l.id),
  };
}

/**
 * Structured Paige recommendation — advisor only.
 */
async function generateContentRecommendation(context = {}, opts = {}) {
  const clientId = asClientId(
    context.tenantId ?? context.clientId ?? context.client_id
  );
  if (clientId == null) {
    throw new ContentLearningError('client_id_required', 'tenantId / client_id required');
  }

  const objective =
    asText(context.objective) ||
    'Build qualified attention and category understanding before the public Max reveal.';

  const learnings = await getRelevantContentLearnings(
    {
      tenantId: clientId,
      objective: context.objectiveFilter || context.learningObjective || null,
      topic: context.topic,
      audience: context.audience,
      channel: context.channel || 'linkedin',
      campaignId: context.campaignId ?? context.campaign_id,
      limit: context.limit || 8,
    },
    opts
  );

  const supportingLearningIds = learnings.map((l) => l.id);
  const supportingPublicationIds = uniqueIds(
    learnings.flatMap((l) => l.supportingPublicationIds || [])
  );

  const experiment = buildExperiment(learnings, {
    objective: typeof context.objective === 'string' && context.objective.includes(' ')
      ? context.learningObjective || 'category_creation'
      : context.objective || 'category_creation',
    channel: context.channel || 'linkedin',
    nextArgument: context.nextArgument,
  });

  const priorTopics = learnings
    .map((l) => normalizeTopic(l.topic))
    .filter(Boolean);
  const recommendedDirection =
    experiment.nextArgument &&
    !priorTopics.includes(normalizeTopic(experiment.nextArgument))
      ? experiment.nextArgument
      : 'Publish a distinct operator-centered argument about AI trust, confidence, or decision-making — do not recreate the prior post.';

  // Hard anti-clone: never recommend an identical prior topic as the direction.
  const cloned = priorTopics.includes(normalizeTopic(recommendedDirection));
  const direction = cloned
    ? 'Test a related but distinct operator-centered thesis (new argument; same framing family).'
    : recommendedDirection;

  const uncertainties = [
    'Whether breakout distribution performance is repeatable.',
    learnings.some((l) => l.learningType === 'partnership_signal')
      ? 'How much inbound partner/builder interest is directly attributable vs coincidental.'
      : 'Whether downstream business outcomes will accompany the next experiment.',
  ];
  if (!learnings.length) {
    uncertainties.unshift('No durable content learnings exist yet for this tenant.');
  }

  const avgGen =
    learnings.length === 0
      ? 0.2
      : learnings.reduce((s, l) => s + (l.generalizationConfidence || 0), 0) /
        learnings.length;

  const reasonParts = [];
  if (learnings.length) {
    reasonParts.push(
      `Based on ${learnings.length} relevant learning(s) from SPEC-092 outcomes.`
    );
    const dist = learnings.find((l) => l.learningType === 'distribution_pattern');
    if (dist) {
      reasonParts.push(
        `Distribution evidence is ${dist.status} (observation confidence ${dist.observationConfidence}, generalization ${dist.generalizationConfidence}).`
      );
    }
    reasonParts.push(
      'Recommend a controlled experiment that preserves framing while varying the argument — not a clone.'
    );
  } else {
    reasonParts.push(
      'No prior learnings; recommend a first instrumented experiment under the stated objective.'
    );
  }

  const alternatives = [
    {
      direction: 'Pause net-new thesis posts and deepen qualitative capture on the existing breakout.',
      reason: 'Useful if operator wants more attribution clarity before another public test.',
    },
    {
      direction: 'Shift objective temporarily to partnership_generation with explicit CTA for founders.',
      reason: 'Useful if business outcomes matter more than further category discovery this week.',
    },
  ];

  return {
    kind: 'content_recommendation',
    objective,
    recommended_direction: direction,
    reason: reasonParts.join(' '),
    supporting_learning_ids: supportingLearningIds,
    supporting_publication_ids: supportingPublicationIds,
    confidence: round4(Math.max(0.25, Math.min(0.75, avgGen + 0.15))),
    uncertainties,
    experiment,
    alternatives,
    learnings: learnings.map((l) => ({
      id: l.id,
      learningType: l.learningType,
      status: l.status,
      statement: l.statement,
      observationConfidence: l.observationConfidence,
      generalizationConfidence: l.generalizationConfidence,
      sampleSize: l.sampleSize,
    })),
    campaignId: asText(context.campaignId ?? context.campaign_id),
    operatorAuthority: true,
    autonomousPublish: false,
    autonomousStrategyMutation: false,
    generated_at: nowIso(),
  };
}

/**
 * Recompute learnings for a tenant from current SPEC-092 outcomes.
 */
async function recomputeContentLearnings(input = {}, opts = {}) {
  const clientId = asClientId(input.tenantId ?? input.clientId ?? input.client_id);
  if (clientId == null) {
    throw new ContentLearningError('client_id_required', 'tenantId / client_id required');
  }
  const outcomeOpts = resolveOutcomeOpts(opts);
  const items = await listContentOutcomes(
    { clientId, limit: input.limit || 100 },
    outcomeOpts
  );
  const results = [];
  for (const item of items) {
    const evaluated = await evaluateContentPublication(item.publication.id, {
      ...opts,
      clientId,
      outcomeStore: outcomeOpts.store,
    });
    results.push(evaluated);
  }
  const learnings = await listContentLearnings({ clientId }, opts);
  return {
    clientId,
    publicationsEvaluated: results.length,
    learningCount: learnings.length,
    learnings,
    evaluations: results,
    recomputedAt: nowIso(),
  };
}

/**
 * Convenience: shared memory harness for tests (outcomes + learnings).
 */
function createLinkedMemoryStores() {
  const outcomeStore = createOutcomeMemoryStore();
  const learningStore = createMemoryStore();
  return { outcomeStore, learningStore };
}

module.exports = {
  LEARNING_TYPES,
  LEARNING_STATUSES,
  AUDIENCE_CLASSES,
  ATTRIBUTION_WEIGHT,
  CONFIG,
  ContentLearningError,
  createMemoryStore,
  createPostgresStore,
  createLinkedMemoryStores,
  createOutcomeMemoryStore,
  createOutcomePostgresStore,
  assessPublicationAgainstObjective,
  deriveContentSignalsFromOutcome,
  deriveContentSignals,
  computeConfidence,
  statusFromEvidence,
  evaluateContentPublication,
  evaluateContentLearning,
  listContentLearnings,
  getContentLearning,
  getRelevantContentLearnings,
  generateContentRecommendation,
  recomputeContentLearnings,
  extractOutOfNetworkPct,
  classifyAudience,
};
