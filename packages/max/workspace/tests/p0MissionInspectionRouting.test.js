'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { isSessionConfigurationMessage, classifyMessageType } = require('../MessageTypeClassifier');
const { looksLikeAcquisitionMissionQuestion } = require('../WorkspaceMissionInspection');
const { MESSAGE_TYPES } = require('../MessageType');

const inspections = [
  'Max, give me a read-only status report for Anchor Cleaning. Identify the current mission state, any execution blockers, and what evidence supports your conclusions. Do not execute anything.',
  'Show me the current mission status, read-only.',
  'Identify mission blockers. Do not execute anything.',
  'Give me a status report without executing anything.',
];

for (const question of inspections) {
  test('mission inspection is not session configuration: ' + question, () => {
    assert.equal(isSessionConfigurationMessage(question), false);
    assert.notEqual(classifyMessageType(question).type, MESSAGE_TYPES.SESSION_CONFIGURATION);
    assert.equal(looksLikeAcquisitionMissionQuestion(question), true);
  });
}

test('genuine persistent session setting remains configurable', () => {
  const question = "For the rest of this conversation: Don't execute anything.";
  assert.equal(isSessionConfigurationMessage(question), true);
  assert.equal(classifyMessageType(question).type, MESSAGE_TYPES.SESSION_CONFIGURATION);
});
