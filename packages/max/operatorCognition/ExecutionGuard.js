'use strict';

/**
 * SPEC-146 — Execution guard.
 * Only Execute and Edit may mutate mission state. Everything else is read-only.
 */

const { modeMutatesMission } = require('./ThinkingModes');

function mayMutateMission(conversationIntent) {
  if (!conversationIntent || typeof conversationIntent !== 'object') return false;
  if (conversationIntent.mutatesMission === true) return true;
  return modeMutatesMission(conversationIntent.intent);
}

function isReadOnlyCognition(conversationIntent) {
  return !mayMutateMission(conversationIntent);
}

function assertReadOnlyCognition(conversationIntent, context = 'mission') {
  if (mayMutateMission(conversationIntent)) {
    const err = new Error(`SPEC-146: ${context} mutation blocked for intent ${conversationIntent && conversationIntent.intent}`);
    err.code = 'COGNITION_READ_ONLY';
    err.conversationIntent = conversationIntent;
    throw err;
  }
}

module.exports = {
  mayMutateMission,
  isReadOnlyCognition,
  assertReadOnlyCognition,
};
