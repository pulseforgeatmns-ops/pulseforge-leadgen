'use strict';

const {
  EVENT_TYPES,
  ENTITY_KINDS,
  SEVERITY,
  LIFECYCLE,
  DEFAULT_CONFIDENCE_THRESHOLD,
  lifecycleForEventType,
} = require('./LiveTypes');

/**
 * Diff two CommandDeckModels into IntelligenceEvent partials (SPEC-011).
 * Composer remains the source of truth — this only describes motion.
 */

/**
 * @param {object|null} previous
 * @param {object} next
 * @param {{ tenantId: string, confidenceThreshold?: number }} meta
 * @returns {object[]} event partials (no seq/id yet)
 */
function diffCommandDeck(previous, next, meta) {
  if (!next || !meta || !meta.tenantId) return [];
  const tenantId = String(meta.tenantId);
  const threshold =
    meta.confidenceThreshold != null
      ? Number(meta.confidenceThreshold)
      : DEFAULT_CONFIDENCE_THRESHOLD;
  const asOf =
    (next.meta && next.meta.generatedAt) ||
    (next.meta && next.meta.asOf) ||
    new Date().toISOString();
  /** @type {object[]} */
  const events = [];

  if (!previous) {
    events.push({
      type: EVENT_TYPES.DETECTED,
      entity: {
        kind: ENTITY_KINDS.DECK,
        id: `deck:${tenantId}`,
        label: 'Command Deck',
      },
      severity: SEVERITY.INFO,
      timestamp: asOf,
      summary: 'Command Deck assembled',
      tenantId,
      lifecycle: LIFECYCLE.DETECTED,
      material: false,
      relatedEvidence: [],
    });
    if (next.morningBrief) {
      events.push(
        briefingEntry(tenantId, asOf, morningSummary(next.morningBrief))
      );
    }
    return events;
  }

  // Highest Leverage Action replaced
  const prevHla = cardSubjectId(previous.highestLeverageAction);
  const nextHla = cardSubjectId(next.highestLeverageAction);
  if (prevHla && nextHla && prevHla !== nextHla) {
    events.push({
      type: EVENT_TYPES.HIGHEST_LEVERAGE_REPLACED,
      entity: {
        kind: ENTITY_KINDS.RECOMMENDATION,
        id: nextHla,
        label: cardTitle(next.highestLeverageAction) || nextHla,
      },
      severity: SEVERITY.HIGH,
      timestamp: asOf,
      summary: `Highest Leverage Action replaced · now ${
        cardTitle(next.highestLeverageAction) || nextHla
      }`,
      tenantId,
      lifecycle: LIFECYCLE.STRENGTHENED,
      material: true,
      relatedEvidence: [],
      payload: { previousId: prevHla, nextId: nextHla },
    });
  } else if (!prevHla && nextHla) {
    events.push({
      type: EVENT_TYPES.RECOMMENDATION_PROMOTED,
      entity: {
        kind: ENTITY_KINDS.RECOMMENDATION,
        id: nextHla,
        label: cardTitle(next.highestLeverageAction) || nextHla,
      },
      severity: SEVERITY.HIGH,
      timestamp: asOf,
      summary: `Highest Leverage Action set · ${
        cardTitle(next.highestLeverageAction) || nextHla
      }`,
      tenantId,
      lifecycle: LIFECYCLE.STRENGTHENED,
      material: true,
      relatedEvidence: [],
    });
  }

  // Watch alerts: new / severity promoted
  const prevWatches = indexById(previous.watchAlerts || []);
  for (const watch of next.watchAlerts || []) {
    const id = String(watch.id || '');
    if (!id) continue;
    const prior = prevWatches.get(id);
    if (!prior) {
      const promoted = severityRank(watch.severity || watch.priority) >= 3;
      events.push({
        type: promoted
          ? EVENT_TYPES.WATCH_ALERT_PROMOTED
          : EVENT_TYPES.WATCH_ALERT_APPEARED,
        entity: {
          kind: ENTITY_KINDS.WATCH,
          id,
          label: watch.title || watch.summary || id,
        },
        severity: mapWatchSeverity(watch.severity),
        timestamp: asOf,
        summary: promoted
          ? `Watch Alert promoted · ${watch.title || id}`
          : `Watch Alert appeared · ${watch.title || id}`,
        tenantId,
        lifecycle: promoted ? LIFECYCLE.STRENGTHENED : LIFECYCLE.DETECTED,
        material: promoted,
        relatedEvidence: [],
      });
    } else if (
      severityRank(watch.severity) > severityRank(prior.severity)
    ) {
      events.push({
        type: EVENT_TYPES.WATCH_ALERT_PROMOTED,
        entity: {
          kind: ENTITY_KINDS.WATCH,
          id,
          label: watch.title || watch.summary || id,
        },
        severity: SEVERITY.HIGH,
        timestamp: asOf,
        summary: `Watch Alert promoted · ${watch.title || id}`,
        tenantId,
        lifecycle: LIFECYCLE.STRENGTHENED,
        material: true,
        relatedEvidence: [],
      });
    }
  }

  // Priority queue confidence / policy / disappearance
  const prevQueue = indexById(previous.priorityQueue || []);
  const nextQueue = indexById(next.priorityQueue || []);

  for (const [id, item] of nextQueue) {
    const prior = prevQueue.get(id);
    const conf = num(item.confidence);
    const priorConf = prior ? num(prior.confidence) : null;

    if (
      conf != null &&
      priorConf != null &&
      priorConf < threshold &&
      conf >= threshold
    ) {
      events.push({
        type: EVENT_TYPES.CONFIDENCE_THRESHOLD_CROSSED,
        entity: {
          kind: ENTITY_KINDS.RECOMMENDATION,
          id,
          label: item.title || id,
        },
        severity: SEVERITY.HIGH,
        timestamp: asOf,
        summary: `Confidence crossed ${Math.round(threshold * 100)}% · ${
          item.title || id
        }`,
        tenantId,
        lifecycle: LIFECYCLE.STRENGTHENED,
        material: true,
        relatedEvidence: [],
        payload: { confidenceBefore: priorConf, confidenceAfter: conf },
      });
    } else if (
      conf != null &&
      priorConf != null &&
      conf > priorConf + 0.04
    ) {
      events.push({
        type: EVENT_TYPES.CONFIDENCE_INCREASED,
        entity: {
          kind: ENTITY_KINDS.RECOMMENDATION,
          id,
          label: item.title || id,
        },
        severity: SEVERITY.LOW,
        timestamp: asOf,
        summary: `Recommendation confidence increased · ${item.title || id}`,
        tenantId,
        lifecycle: LIFECYCLE.STRENGTHENED,
        material: false,
        relatedEvidence: [],
        payload: { confidenceBefore: priorConf, confidenceAfter: conf },
      });
    }

    const policyOutcome =
      (item.payload && item.payload.policyOutcome) ||
      (item.policy && item.policy.outcome) ||
      null;
    const priorPolicy =
      (prior && prior.payload && prior.payload.policyOutcome) ||
      (prior && prior.policy && prior.policy.outcome) ||
      null;
    if (
      policyOutcome &&
      /block/i.test(String(policyOutcome)) &&
      String(priorPolicy || '') !== String(policyOutcome)
    ) {
      events.push({
        type: EVENT_TYPES.RECOMMENDATION_BLOCKED,
        entity: {
          kind: ENTITY_KINDS.RECOMMENDATION,
          id,
          label: item.title || id,
        },
        severity: SEVERITY.CRITICAL,
        timestamp: asOf,
        summary: `Recommendation blocked · ${item.title || id}`,
        tenantId,
        lifecycle: LIFECYCLE.RESOLVED,
        material: true,
        relatedEvidence: [],
        payload: { policyOutcome },
      });
    }
  }

  for (const [id, prior] of prevQueue) {
    if (nextQueue.has(id)) continue;
    // Dropped from queue — opportunity expired (material)
    events.push({
      type: EVENT_TYPES.OPPORTUNITY_EXPIRED,
      entity: {
        kind: ENTITY_KINDS.RECOMMENDATION,
        id,
        label: prior.title || id,
      },
      severity: SEVERITY.MEDIUM,
      timestamp: asOf,
      summary: `Opportunity dropped from priority queue · ${prior.title || id}`,
      tenantId,
      lifecycle: LIFECYCLE.ARCHIVED,
      material: true,
      relatedEvidence: [],
    });
  }

  // Morning brief evolution (incremental narrative, not replacement)
  const prevHeadline =
    previous.morningBrief && previous.morningBrief.headline;
  const nextHeadline = next.morningBrief && next.morningBrief.headline;
  if (nextHeadline && nextHeadline !== prevHeadline) {
    events.push(
      briefingEntry(tenantId, asOf, String(nextHeadline), {
        previousHeadline: prevHeadline || null,
      })
    );
  }

  // Attach lifecycle hints consistently
  return events.map((ev) => ({
    ...ev,
    lifecycle:
      ev.lifecycle || lifecycleForEventType(ev.type, null) || LIFECYCLE.DETECTED,
  }));
}

function briefingEntry(tenantId, asOf, summary, payload = null) {
  return {
    type: EVENT_TYPES.BRIEFING_EVOLVED,
    entity: {
      kind: ENTITY_KINDS.BRIEFING,
      id: `briefing:${tenantId}`,
      label: 'Morning Brief',
    },
    severity: SEVERITY.INFO,
    timestamp: asOf,
    summary,
    tenantId,
    lifecycle: LIFECYCLE.STRENGTHENED,
    material: false,
    relatedEvidence: [],
    payload,
  };
}

function morningSummary(brief) {
  if (!brief) return 'Morning Brief ready';
  if (brief.headline) return String(brief.headline);
  if (brief.summary) return String(brief.summary);
  return 'Morning Brief ready';
}

function cardSubjectId(card) {
  if (!card) return null;
  if (card.payload && card.payload.recommendationId) {
    return String(card.payload.recommendationId);
  }
  if (card.recommendationId) return String(card.recommendationId);
  if (card.id) return String(card.id);
  return null;
}

function cardTitle(card) {
  if (!card) return '';
  return card.title || card.headline || card.summary || '';
}

function indexById(list) {
  /** @type {Map<string, object>} */
  const map = new Map();
  for (const item of list || []) {
    if (!item || item.id == null) continue;
    map.set(String(item.id), item);
  }
  return map;
}

function num(v) {
  if (v == null || !Number.isFinite(Number(v))) return null;
  return Number(v);
}

function severityRank(sev) {
  const s = String(sev || '').toLowerCase();
  if (s === 'critical' || s === '4') return 4;
  if (s === 'high' || s === '3') return 3;
  if (s === 'medium' || s === '2') return 2;
  if (s === 'low' || s === '1') return 1;
  return 0;
}

function mapWatchSeverity(sev) {
  const s = String(sev || '').toLowerCase();
  if (s === 'critical') return SEVERITY.CRITICAL;
  if (s === 'high') return SEVERITY.HIGH;
  if (s === 'medium') return SEVERITY.MEDIUM;
  if (s === 'low') return SEVERITY.LOW;
  return SEVERITY.INFO;
}

module.exports = {
  diffCommandDeck,
};
