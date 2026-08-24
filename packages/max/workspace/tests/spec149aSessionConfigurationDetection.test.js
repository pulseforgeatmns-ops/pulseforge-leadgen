'use strict';

/**
 * SPEC-149A — Robust Session Configuration Detection acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { MESSAGE_TYPES } = require('../MessageType');
const {
  classifyMessageType,
  isSessionConfigurationMessage,
  countSessionSettingHits,
  SESSION_CONFIGURATION_THRESHOLD,
} = require('../MessageTypeClassifier');

describe('SPEC-149A — Robust Session Configuration Detection', () => {
  it('Test 1 — I\'d like to evaluate how you operate', () => {
    const text = "I'd like to evaluate how you operate.";
    assert.equal(isSessionConfigurationMessage(text), true);
    const result = classifyMessageType(text);
    assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
    assert.equal(result.mutatesSession, true);
  });

  it('Test 2 — Operate according to your role', () => {
    const text = 'Operate according to your role.';
    assert.equal(isSessionConfigurationMessage(text), true);
    const result = classifyMessageType(text);
    assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
  });

  it('Test 3 — Treat Anchor Cleaning as a real production business', () => {
    const text = 'Treat Anchor Cleaning as a real production business.';
    assert.equal(isSessionConfigurationMessage(text), true);
    const result = classifyMessageType(text);
    assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
  });

  it('Test 4 — scope marker with execution directive', () => {
    const text =
      "For the remainder of this conversation, don't execute anything.";
    assert.equal(isSessionConfigurationMessage(text), true);
    const result = classifyMessageType(text);
    assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
    assert.equal(result.mutatesSession, true);
  });

  it('Test 5 — combined natural-language configuration bundle', () => {
    const text = [
      "I'd like to evaluate how you operate.",
      'Treat Anchor Cleaning as a real production business.',
      'Operate according to your role.',
    ].join('\n');

    assert.equal(isSessionConfigurationMessage(text), true);
    const result = classifyMessageType(text);
    assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
    assert.ok(
      result.confidence > 0.9,
      `expected confidence > 0.90, got ${result.confidence}`
    );
    assert.ok(countSessionSettingHits(text) >= SESSION_CONFIGURATION_THRESHOLD);
  });

  it('Test 6 (regression) — mission creation must not classify as session configuration', () => {
    const text = 'Create a new acquisition mission for Anchor Cleaning.';
    assert.equal(isSessionConfigurationMessage(text), false);
    const result = classifyMessageType(text);
    assert.equal(result.type, MESSAGE_TYPES.MISSION_CREATION);
    assert.notEqual(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
  });

  it('Test 7 (regression) — explanatory question must not classify as session configuration', () => {
    const text = 'Why did you choose that market?';
    assert.equal(isSessionConfigurationMessage(text), false);
    const result = classifyMessageType(text);
    assert.equal(result.type, MESSAGE_TYPES.QUESTION);
    assert.notEqual(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
  });

  it('heuristic rescue — two independent configuration signals', () => {
    const text = 'Answer naturally. Autonomous execution.';
    assert.ok(countSessionSettingHits(text) >= SESSION_CONFIGURATION_THRESHOLD);
    assert.equal(isSessionConfigurationMessage(text), true);
  });

  it('heuristic rescue — one signal plus explicit scope marker', () => {
    const text = "Going forward, don't execute anything.";
    assert.equal(isSessionConfigurationMessage(text), true);
    const result = classifyMessageType(text);
    assert.equal(result.type, MESSAGE_TYPES.SESSION_CONFIGURATION);
  });
});
