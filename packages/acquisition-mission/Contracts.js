'use strict';

/**
 * SPEC-118 — capability contracts.
 * Scout discovers. Max decides. Paige communicates. Emmett protects and executes.
 */

const { SPECIALISTS, amoError, asText } = require('./types');

const FORBIDDEN = Object.freeze({
  [SPECIALISTS.SCOUT]: [
    'subject', 'subjects', 'body', 'cta', 'variant', 'variants',
    'messaging', 'copy', 'emailBody', 'email_body', 'hypothesis', 'hypotheses',
  ],
  [SPECIALISTS.MAX]: [
    'subject', 'subjects', 'body', 'cta', 'variant', 'variants',
    'messaging', 'copy', 'emailBody', 'email_body',
  ],
  [SPECIALISTS.PAIGE]: [
    'recipients', 'recipientIds', 'recipient_ids', 'sendTo', 'send_to',
    'whoReceives', 'who_receives', 'queue', 'sendList', 'send_list',
  ],
  [SPECIALISTS.PENNY]: [
    'campaignCreation', 'campaign_creation', 'campaignLaunch', 'campaign_launch',
    'campaignPause', 'campaign_pause', 'budgetMutation', 'budget_mutation',
    'bidMutation', 'bid_mutation', 'keywordMutation', 'keyword_mutation',
    'targetingMutation', 'targeting_mutation', 'creativeMutation', 'creative_mutation',
    'billingMutation', 'billing_mutation', 'externalSpend', 'external_spend',
    'authorizeSpend', 'authorize_spend', 'adCopy', 'ad_copy',
  ],
  [SPECIALISTS.EMMETT]: [
    'subject', 'subjects', 'body', 'cta', 'variant', 'variants',
    'messaging', 'copy', 'emailBody', 'email_body', 'hypothesis', 'hypotheses',
  ],
  [SPECIALISTS.VERA]: [
    'recipients', 'recipientIds', 'recipient_ids', 'sendTo', 'send_to',
    'messaging', 'copy', 'queue', 'sendList', 'send_list',
  ],
  [SPECIALISTS.REX]: [
    'recipients', 'recipientIds', 'sendTo', 'messaging', 'copy', 'queue',
  ],
});

const PRODUCES = Object.freeze({
  [SPECIALISTS.SCOUT]: [
    'companies', 'prospects', 'buyingSignals', 'buying_signals',
    'decisionMakers', 'decision_makers', 'confidence', 'evidence',
  ],
  [SPECIALISTS.MAX]: [
    'priorities', 'objectives', 'timing', 'recommendations',
    'constraints', 'delegation', 'structuredMission',
    'acquisitionApproach', 'approachDecision', 'selectedApproach', 'approach',
  ],
  [SPECIALISTS.PAIGE]: [
    'messaging', 'experiments', 'variants', 'subjects', 'subject',
    'cta', 'hypotheses', 'hypothesis', 'outreachSequence', 'outreach_sequence',
  ],
  [SPECIALISTS.PENNY]: [
    'paidAcquisitionRecommendation', 'paid_acquisition_recommendation',
    'viability', 'channelAssessments', 'channel_assessments',
    'recommendedTest', 'recommended_test', 'measurementRequirements',
    'measurement_requirements', 'budgetConstraints', 'budget_constraints',
    'stopConditions', 'stop_conditions', 'continueConditions',
    'continue_conditions', 'scaleConditions', 'scale_conditions',
    'evidence', 'confidence', 'unknowns', 'blockers',
  ],
  [SPECIALISTS.EMMETT]: [
    'capacity', 'queue', 'sendRecommendations', 'send_recommendations',
    'deliverability', 'reputation',
  ],
  [SPECIALISTS.VERA]: [
    'reviews', 'responses', 'draftResponses', 'draft_responses',
    'sentiment', 'reputation', 'confidence', 'evidence',
  ],
  [SPECIALISTS.REX]: [
    'report', 'summary', 'metrics', 'kpis', 'insights',
    'recommendations', 'performance',
  ],
});

const EMMETT_PAIGE_COPY_KEYS = new Set(['subject', 'body', 'cta']);

function isEmmettPaigeBoundCopyPath(path = []) {
  const paigeIdx = path.lastIndexOf('paige');
  if (paigeIdx < 1) return false;
  const parent = path[paigeIdx - 1];
  const grandparent = path[paigeIdx - 2];
  return parent === 'items' || (typeof parent === 'number' && grandparent === 'items');
}

function walkKeys(value, acc = [], path = [], specialist = null) {
  if (!value || typeof value !== 'object') return acc;
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i += 1) {
      walkKeys(value[i], acc, [...path, i], specialist);
    }
    return acc;
  }
  for (const [key, child] of Object.entries(value)) {
    const nextPath = [...path, key];
    const allowNestedPaigeCopy = specialist === SPECIALISTS.EMMETT
      && EMMETT_PAIGE_COPY_KEYS.has(key)
      && isEmmettPaigeBoundCopyPath(nextPath);
    if (!allowNestedPaigeCopy) {
      acc.push(key);
    }
    walkKeys(child, acc, nextPath, specialist);
  }
  return acc;
}

function assertContract(specialist, payload = {}) {
  const who = asText(specialist).toLowerCase();
  if (who === SPECIALISTS.OPERATOR) return { ok: true, specialist: who };

  const forbidden = FORBIDDEN[who];
  if (!forbidden) {
    throw amoError('amo_unknown_specialist', `Unknown capability: ${specialist}`);
  }

  const keys = new Set(walkKeys(payload, [], [], who));
  const violated = forbidden.filter((key) => keys.has(key));
  if (violated.length) {
    throw amoError(
      'amo_contract_violation',
      `${who} must not produce ${violated.join(', ')}.`
    );
  }

  const required = PRODUCES[who] || [];
  const produced = required.filter((key) => {
    const value = payload[key];
    if (value == null) return false;
    if (Array.isArray(value)) return value.length > 0;
    if (typeof value === 'object') return Object.keys(value).length > 0;
    return true;
  });
  if (!produced.length) {
    throw amoError(
      'amo_contract_empty',
      `${who} must produce at least one contracted output.`
    );
  }

  return { ok: true, specialist: who, produced };
}

function contractFor(specialist) {
  const who = asText(specialist).toLowerCase();
  return {
    specialist: who,
    produces: PRODUCES[who] || [],
    never: FORBIDDEN[who] || [],
  };
}

module.exports = {
  FORBIDDEN,
  PRODUCES,
  assertContract,
  contractFor,
};
