'use strict';

/**
 * ADR-071 — Session Directive Registry acceptance tests.
 *
 * Runtime guarantee: SESSION_CONFIGURATION classification implies extractable
 * session mutations (or reset) using the same registry vocabulary.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { MESSAGE_TYPES } = require('../MessageType');
const { classifyMessageType, isSessionConfigurationMessage } = require('../MessageTypeClassifier');
const { buildSessionState } = require('../SessionStateManager');
const {
  SESSION_DIRECTIVES,
  isInterpretableSessionConfiguration,
  extractDirectiveSignals,
  matchFieldDirectives,
  countSettingHits,
} = require('../SessionDirectiveRegistry');

const CONFIGURATION_FIXTURES = [
  "For the remainder of this conversation, don't execute anything.",
  "I'd like to evaluate how you operate.",
  'Operate according to your role.',
  'Treat Anchor Cleaning as a real production business.',
  "For the rest of this conversation: Don't execute anything.",
  [
    "I'd like to evaluate how you operate.",
    'Treat Anchor Cleaning as a real production business.',
    'Operate according to your role.',
  ].join('\n'),
  'Answer naturally. Autonomous execution.',
  "Going forward, don't execute anything.",
  'Explain your reasoning naturally.',
  "For today's session, evaluate your reasoning instead of executing tasks.",
  'Reset the session.',
  'Be concise. Keep it brief.',
];

describe('ADR-071 — Session Directive Registry', () => {
  it('registry defines field, scope, and reset directives', () => {
    const kinds = new Set(SESSION_DIRECTIVES.map((directive) => directive.kind));
    assert.ok(kinds.has('field'));
    assert.ok(kinds.has('scope'));
    assert.ok(kinds.has('reset'));
    assert.ok(SESSION_DIRECTIVES.every((directive) => directive.id && directive.aliases?.length));
  });

  it('runtime guarantee — classified configuration is always interpretable', () => {
    for (const text of CONFIGURATION_FIXTURES) {
      const classified = isSessionConfigurationMessage(text);
      if (!classified) continue;

      const signals = extractDirectiveSignals(text);
      assert.equal(
        isInterpretableSessionConfiguration(text, signals),
        true,
        `expected interpretable configuration for: ${text}`
      );
    }
  });

  it('runtime guarantee — interpretable configuration extracts writable fields or resets', () => {
    for (const text of CONFIGURATION_FIXTURES) {
      if (!isInterpretableSessionConfiguration(text)) continue;

      const signals = extractDirectiveSignals(text);
      const built = buildSessionState({ question: text });

      if (signals.reset) {
        assert.equal(built.reason, 'session_reset');
        assert.equal(built.changed, true);
        continue;
      }

      assert.ok(
        signals.executionPolicy != null ||
          signals.reasoningMode != null ||
          signals.conversationStyle != null ||
          signals.operatingMode != null ||
          signals.evaluationMode != null,
        `expected extracted fields for: ${text}`
      );
    }
  });

  it('classification and extraction share the same field-directive vocabulary', () => {
    const text = 'Operate according to your role. Answer naturally.';
    const hits = countSettingHits(text);
    const fields = matchFieldDirectives(text);

    assert.ok(hits >= 2);
    assert.equal(hits, fields.length);
    assert.ok(fields.every((directive) => directive.kind === 'field'));
  });

  it('new phrasing is registry data — be concise resolves through registry', () => {
    const text = 'For this session, be concise.';
    const signals = extractDirectiveSignals(text);
    assert.equal(signals.reasoningMode, 'concise');
    assert.equal(isSessionConfigurationMessage(text), true);
    assert.equal(buildSessionState({ question: text }).changed, true);
  });
});
