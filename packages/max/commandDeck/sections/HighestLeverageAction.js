'use strict';

const {
  buildIntelligenceCard,
  recommendationActions,
  CARD_TYPES,
} = require('../cards/IntelligenceCard');

/**
 * Highest Leverage Action — top priority item + attached policy decision.
 * Composer does not re-score; it selects briefing.priorities[0].
 *
 * @param {object} input
 * @param {object|null} input.topPriority
 * @param {object|null} input.recommendation - full recommendation when available
 * @param {object|null} input.policyDecision
 * @param {string} input.briefingId
 * @param {string} input.generatedAt
 */
function buildHighestLeverageAction(input) {
  const top = input.topPriority;
  if (!top) {
    return { highestLeverageAction: null, card: null };
  }

  const rec = input.recommendation || null;
  const policy = summarizePolicy(input.policyDecision);

  const supportingSignals = pickSignals(
    rec && rec.supportingSignals,
    top.why || []
  );
  const contradictingSignals = pickSignals(
    rec && (rec.opposingSignals || rec.contradictingSignals),
    top.whyNot || []
  );

  const opportunity =
    top.score != null
      ? Number(top.score)
      : rec && rec.score != null
        ? Number(rec.score)
        : null;
  const confidence =
    top.confidence != null
      ? Number(top.confidence)
      : rec && rec.confidence != null
        ? Number(rec.confidence)
        : null;

  const companyName = top.companyName || (rec && rec.subject && rec.subject.name) || top.companyId;
  const actionLabel =
    top.recommendedAction ||
    (rec && rec.recommendedAction) ||
    'review';

  const highestLeverageAction = {
    recommendation: {
      id: top.id || (rec && rec.id) || null,
      companyId: top.companyId || null,
      companyName: companyName || null,
      type: top.type || (rec && rec.type) || null,
      recommendedAction: actionLabel,
      priority: top.priority || (rec && rec.priority) || null,
    },
    opportunity,
    confidence,
    trend: top.trend || 'insufficient',
    supportingSignals,
    contradictingSignals,
    policy,
  };

  const title = `${formatActionVerb(actionLabel)} ${companyName || 'opportunity'}`.trim();
  const summary = [
    opportunity != null ? `Opportunity ${opportunity}` : null,
    confidence != null ? `Confidence ${confidence}` : null,
    top.trend && top.trend !== 'insufficient' ? `Trend ${top.trend}` : null,
  ]
    .filter(Boolean)
    .join(' · ');

  const cardId = `card:highest_leverage:${top.id || top.companyId}`;
  const card = buildIntelligenceCard({
    id: cardId,
    type: CARD_TYPES.HIGHEST_LEVERAGE,
    priority: 900,
    title,
    summary,
    confidence,
    updatedAt: input.generatedAt,
    actions: recommendationActions({
      recommendationId: top.id || (rec && rec.id) || null,
      companyId: top.companyId || null,
      cardId,
      askContext: 'highest_leverage',
    }),
    sources: buildSources(top, rec, input.policyDecision, input.briefingId),
    reasoningId: top.id || (rec && rec.id) || null,
    policyId:
      (input.policyDecision &&
        input.policyDecision.audit &&
        input.policyDecision.audit.id) ||
      null,
    briefingId: input.briefingId,
    payload: highestLeverageAction,
  });

  return { highestLeverageAction, card };
}

function summarizePolicy(decision) {
  if (!decision) {
    return {
      allowed: null,
      requiresApproval: null,
      blocked: null,
      outcome: null,
      severity: null,
      reason: null,
      policyId: null,
    };
  }
  return {
    allowed: Boolean(decision.allowed),
    requiresApproval: Boolean(decision.requiresApproval),
    blocked: Boolean(decision.blocked),
    outcome: decision.outcome || null,
    severity: decision.severity || null,
    reason: decision.reason || null,
    policyId: (decision.audit && decision.audit.id) || null,
  };
}

function pickSignals(fromRec, fromWhy) {
  if (Array.isArray(fromRec) && fromRec.length > 0) {
    return fromRec.map((s) => ({
      kind: s.kind || 'signal',
      id: s.id || null,
      summary: s.summary || String(s),
    }));
  }
  return (fromWhy || []).map((w, i) => ({
    kind: 'reason',
    id: `why:${i}`,
    summary: String(w),
  }));
}

function buildSources(top, rec, decision, briefingId) {
  const sources = [
    { kind: 'briefing', id: briefingId, field: 'priorities' },
  ];
  if (top && top.id) {
    sources.push({ kind: 'recommendation', id: top.id });
  }
  if (rec && rec.evidence) {
    for (const ev of rec.evidence) {
      sources.push({ kind: 'evidence', id: String(ev) });
    }
  }
  if (decision && decision.audit && decision.audit.id) {
    sources.push({ kind: 'policy', id: decision.audit.id });
  }
  return sources;
}

function formatActionVerb(action) {
  const map = {
    follow_up_outreach: 'Follow up',
    pursue: 'Pursue',
    request_intro: 'Request intro',
    nurture: 'Nurture',
    call: 'Call',
    email: 'Email',
  };
  if (map[action]) return map[action];
  if (!action) return 'Review';
  return String(action)
    .replace(/_/g, ' ')
    .replace(/^\w/, (c) => c.toUpperCase());
}

module.exports = {
  buildHighestLeverageAction,
};
