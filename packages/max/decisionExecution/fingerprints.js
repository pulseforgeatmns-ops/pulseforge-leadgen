'use strict';

const crypto = require('crypto');

function stableJson(value) {
  return JSON.stringify(value, Object.keys(value || {}).sort());
}

function decisionIdempotencyKey({
  clientId,
  triggerType,
  subjectType,
  subjectId,
  actionType,
  triggerState,
}) {
  const payload = stableJson({
    clientId,
    triggerType,
    subjectType,
    subjectId,
    actionType,
    triggerState: triggerState || {},
  });
  return crypto.createHash('sha256').update(payload).digest('hex').slice(0, 32);
}

function actionIntentKey({ clientId, decisionId, actionType, triggerState }) {
  const payload = stableJson({ clientId, decisionId, actionType, triggerState: triggerState || {} });
  return crypto.createHash('sha256').update(payload).digest('hex').slice(0, 32);
}

function newDecisionId() {
  return `mod-${crypto.randomUUID()}`;
}

function newIntentId() {
  return `moi-${crypto.randomUUID()}`;
}

module.exports = {
  decisionIdempotencyKey,
  actionIntentKey,
  newDecisionId,
  newIntentId,
};
