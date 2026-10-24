'use strict';

/**
 * Canonical cross-mission OutcomeLearning retrieval for SEC memoryContext.
 *
 * OutcomeLearning from completed missions → relevance filter → advisory priorLearning.
 * Does not mutate heuristics, configuration, or specialist contributions.
 */

const { asText, clone, round2 } = require('./types');
const { SPECIALISTS } = require('./types');
const { LEARNING_OBJECT_KINDS } = require('./OutcomeLearning');

const DEFAULT_LIMIT = 10;
const MIN_RELEVANCE_SCORE = 0.4;
const RECENCY_WINDOW_MS = 90 * 24 * 60 * 60 * 1000;

const SPECIALIST_ALLOWED_KINDS = Object.freeze({
  [SPECIALISTS.SCOUT]: Object.freeze([
    LEARNING_OBJECT_KINDS.HEURISTIC,
    LEARNING_OBJECT_KINDS.MARKET_UNDERSTANDING,
    LEARNING_OBJECT_KINDS.OPPORTUNITY_RULE,
  ]),
  [SPECIALISTS.MAX]: Object.freeze([
    LEARNING_OBJECT_KINDS.STRATEGY,
    LEARNING_OBJECT_KINDS.HEURISTIC,
    LEARNING_OBJECT_KINDS.MARKET_UNDERSTANDING,
    LEARNING_OBJECT_KINDS.OPPORTUNITY_RULE,
    LEARNING_OBJECT_KINDS.ORGANIZATIONAL,
  ]),
  [SPECIALISTS.PAIGE]: Object.freeze([
    LEARNING_OBJECT_KINDS.MESSAGING,
  ]),
  [SPECIALISTS.EMMETT]: Object.freeze([]),
});

const COMMUNICATION_KEYWORDS = /messaging|message|subject|hook|cta|copy|tone|communication|email copy|outreach copy/i;

function missionSegment(mission) {
  if (!mission) return '';
  const plan = mission.structuredMission || mission.missionPlanDraft || null;
  return asText(mission.targetSegment || plan?.market?.segment || plan?.market?.vertical).toLowerCase();
}

function missionGeography(mission) {
  if (!mission) return '';
  const plan = mission.structuredMission || mission.missionPlanDraft || null;
  const geo = plan?.geography || {};
  return asText(geo.primary || geo.region || geo.label || mission.geography).toLowerCase();
}

function normalizeSegment(value) {
  return asText(value).toLowerCase().replace(/[\s_-]+/g, '_');
}

function segmentsMatch(a, b) {
  const left = normalizeSegment(a);
  const right = normalizeSegment(b);
  if (!left || !right) return false;
  if (left === right) return true;
  return left.includes(right) || right.includes(left);
}

function geographiesMatch(a, b) {
  const left = asText(a).toLowerCase();
  const right = asText(b).toLowerCase();
  if (!left || !right) return false;
  if (left === right) return true;
  return left.includes(right) || right.includes(left);
}

function parseTimestamp(value) {
  if (!value) return null;
  const ms = Date.parse(String(value));
  return Number.isFinite(ms) ? ms : null;
}

function isRecent(at, nowMs = Date.now()) {
  const ts = parseTimestamp(at);
  if (ts == null) return false;
  return nowMs - ts <= RECENCY_WINDOW_MS;
}

function hasValidEvaluation(row) {
  const accuracy = asText(row.accuracy).toLowerCase();
  if (!accuracy) return false;
  return accuracy !== 'inconclusive';
}

function isCommunicationStrategy(row) {
  const text = [
    row.statement,
    row.primaryCause,
    row.secondaryCause,
    row.subject,
  ].filter(Boolean).join(' ');
  return COMMUNICATION_KEYWORDS.test(text);
}

function specialistAllowsKind(specialist, row) {
  const who = asText(specialist).toLowerCase();
  const kind = asText(row.kind).toLowerCase();
  const allowed = SPECIALIST_ALLOWED_KINDS[who];

  if (!allowed) return false;

  if (who === SPECIALISTS.EMMETT) {
    return allowed.includes(kind);
  }

  if (who === SPECIALISTS.PAIGE) {
    if (kind === LEARNING_OBJECT_KINDS.MESSAGING) return true;
    if (kind === LEARNING_OBJECT_KINDS.STRATEGY) return isCommunicationStrategy(row);
    return false;
  }

  if (who === SPECIALISTS.SCOUT) {
    if (kind === LEARNING_OBJECT_KINDS.MESSAGING) return false;
    return allowed.includes(kind);
  }

  return allowed.includes(kind);
}

function buildEvidenceRefs(row) {
  const evidence = [];
  if (row.evaluationId) {
    evidence.push({ kind: 'evaluation', id: row.evaluationId });
  }
  if (row.predictionId) {
    evidence.push({ kind: 'prediction', id: row.predictionId });
  }
  if (row.subjectId || row.subject) {
    evidence.push({
      kind: 'subject',
      id: row.subjectId || null,
      label: row.subject || null,
    });
  }
  if (row.accuracy) {
    evidence.push({ kind: 'accuracy', label: row.accuracy });
  }
  return evidence;
}

function scoreRelevance(row, ctx = {}) {
  const reasons = [];
  let score = 0.35;

  if (specialistAllowsKind(ctx.specialist, row)) {
    score += 0.25;
    reasons.push('specialist_relevant_kind');
  } else {
    return { score: 0, reasons: ['specialist_kind_excluded'] };
  }

  const sourceMission = ctx.sourceMission;
  const currentSegment = ctx.currentSegment;
  const currentGeography = ctx.currentGeography;

  if (sourceMission) {
    const sourceSegment = missionSegment(sourceMission);
    if (segmentsMatch(currentSegment, sourceSegment)) {
      score += 0.2;
      reasons.push('same_segment');
    }

    const sourceGeography = missionGeography(sourceMission);
    if (geographiesMatch(currentGeography, sourceGeography)) {
      score += 0.15;
      reasons.push('same_geography');
    }
  }

  if (isRecent(row.at, ctx.nowMs)) {
    score += 0.1;
    reasons.push('recent');
  }

  if (hasValidEvaluation(row)) {
    score += 0.05;
    reasons.push('valid_evaluation');
  }

  if (reasons.length === 1 && reasons[0] === 'specialist_relevant_kind') {
    score = Math.min(score, 0.45);
    reasons.push('limited_metadata');
  }

  return { score: round2(Math.min(1, score)), reasons };
}

function toPriorLearningItem(row, relevance) {
  return Object.freeze({
    id: row.id,
    kind: row.kind,
    sourceMissionId: row.missionId,
    evaluationId: row.evaluationId || null,
    predictionId: row.predictionId || null,
    statement: row.statement,
    direction: row.direction || null,
    relevance: Object.freeze({
      score: relevance.score,
      reasons: Object.freeze([...relevance.reasons]),
    }),
    evidence: Object.freeze(buildEvidenceRefs(row).map(clone)),
    autoApplied: false,
    at: row.at || null,
    subject: row.subject || null,
    accuracy: row.accuracy || null,
  });
}

/**
 * Retrieve advisory prior OutcomeLearning for SEC memoryContext.
 *
 * @param {object} input
 * @param {string} input.tenantId
 * @param {object} input.mission - current mission (excluded unless includeCurrentMission)
 * @param {string} input.specialist
 * @param {number} [input.limit]
 * @param {object} input.store - AMO store with listOutcomeLearnings + optional getMission
 * @param {boolean} [input.includeCurrentMission=false]
 * @returns {{ items: object[], warning?: string }}
 */
function retrieveRelevantOutcomeLearning(input = {}) {
  const tenantId = asText(input.tenantId || input.mission?.tenantId || input.mission?.clientId);
  const mission = input.mission;
  const specialist = asText(input.specialist).toLowerCase();
  const limit = Number.isFinite(Number(input.limit)) && Number(input.limit) > 0
    ? Math.floor(Number(input.limit))
    : DEFAULT_LIMIT;
  const store = input.store;
  const includeCurrentMission = input.includeCurrentMission === true;

  if (!tenantId) {
    return { items: [], warning: 'Prior learning retrieval skipped: tenantId missing.' };
  }
  if (!store || typeof store.listOutcomeLearnings !== 'function') {
    return { items: [], warning: 'Prior learning retrieval skipped: store unavailable.' };
  }
  if (!specialist) {
    return { items: [], warning: 'Prior learning retrieval skipped: specialist missing.' };
  }

  const currentMissionId = mission?.id || null;
  const currentSegment = missionSegment(mission);
  const currentGeography = missionGeography(mission);
  const nowMs = Date.now();

  let rows;
  try {
    rows = store.listOutcomeLearnings(tenantId);
  } catch (err) {
    return {
      items: [],
      warning: err.message || 'Prior learning retrieval failed.',
    };
  }

  const getMission = typeof store.getMission === 'function'
    ? (id) => store.getMission(id)
    : () => null;

  const scored = [];
  for (const row of rows || []) {
    if (!row || !row.id) continue;
    if (String(row.tenantId || '') !== String(tenantId)) continue;
    if (!includeCurrentMission && currentMissionId && row.missionId === currentMissionId) continue;
    if (!asText(row.statement)) continue;

    const sourceMission = row.missionId ? getMission(row.missionId) : null;
    const relevance = scoreRelevance(row, {
      specialist,
      sourceMission,
      currentSegment,
      currentGeography,
      nowMs,
    });

    if (relevance.score < MIN_RELEVANCE_SCORE) continue;

    scored.push({
      row,
      relevance,
    });
  }

  scored.sort((a, b) => {
    if (b.relevance.score !== a.relevance.score) {
      return b.relevance.score - a.relevance.score;
    }
    const aTs = parseTimestamp(a.row.at) || 0;
    const bTs = parseTimestamp(b.row.at) || 0;
    return bTs - aTs;
  });

  const items = scored
    .slice(0, limit)
    .map(({ row, relevance }) => toPriorLearningItem(row, relevance));

  return { items };
}

function buildMemoryContextWithPriorLearning(input = {}, mission, specialist) {
  const base = isPlainObject(input.memoryContext) ? input.memoryContext : {};
  const observations = Array.isArray(base.observations)
    ? clone(base.observations)
    : (Array.isArray(input.observations) ? clone(input.observations) : []);

  if (Array.isArray(base.priorLearning)) {
    return clone({
      ...base,
      observations,
      priorLearning: clone(base.priorLearning),
    });
  }

  const store = input.store;
  if (store && typeof store.listOutcomeLearnings === 'function') {
    try {
      const tenantId = mission?.tenantId || mission?.clientId;
      const result = retrieveRelevantOutcomeLearning({
        tenantId,
        mission,
        specialist,
        limit: input.priorLearningLimit,
        store,
        includeCurrentMission: input.includeCurrentMissionLearning === true,
      });
      const memoryContext = {
        observations,
        priorLearning: result.items,
      };
      if (result.warning) {
        memoryContext.priorLearningRetrievalWarning = result.warning;
      }
      return clone(memoryContext);
    } catch (err) {
      return clone({
        observations,
        priorLearning: [],
        priorLearningRetrievalWarning: err.message || 'Prior learning retrieval failed.',
      });
    }
  }

  return clone({
    observations,
    priorLearning: [],
  });
}

function isPlainObject(value) {
  return value != null && typeof value === 'object' && !Array.isArray(value);
}

module.exports = {
  DEFAULT_LIMIT,
  MIN_RELEVANCE_SCORE,
  SPECIALIST_ALLOWED_KINDS,
  retrieveRelevantOutcomeLearning,
  buildMemoryContextWithPriorLearning,
  missionSegment,
  specialistAllowsKind,
  toPriorLearningItem,
};
