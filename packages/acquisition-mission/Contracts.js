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
  [SPECIALISTS.EMMETT]: [
    'subject', 'subjects', 'body', 'cta', 'variant', 'variants',
    'messaging', 'copy', 'emailBody', 'email_body', 'hypothesis', 'hypotheses',
  ],
});

const PRODUCES = Object.freeze({
  [SPECIALISTS.SCOUT]: [
    'companies', 'prospects', 'buyingSignals', 'buying_signals',
    'decisionMakers', 'decision_makers', 'confidence', 'evidence',
  ],
  [SPECIALISTS.MAX]: [
    'priorities', 'objectives', 'timing', 'recommendations',
    'constraints', 'delegation',
  ],
  [SPECIALISTS.PAIGE]: [
    'messaging', 'experiments', 'variants', 'subjects', 'subject',
    'cta', 'hypotheses', 'hypothesis',
  ],
  [SPECIALISTS.EMMETT]: [
    'capacity', 'queue', 'sendRecommendations', 'send_recommendations',
    'deliverability', 'reputation',
  ],
});

function walkKeys(value, acc = []) {
  if (!value || typeof value !== 'object') return acc;
  if (Array.isArray(value)) {
    for (const item of value) walkKeys(item, acc);
    return acc;
  }
  for (const [key, child] of Object.entries(value)) {
    acc.push(key);
    walkKeys(child, acc);
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

  const keys = new Set(walkKeys(payload));
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
