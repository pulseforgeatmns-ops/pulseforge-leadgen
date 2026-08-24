'use strict';

/**
 * Captures and structures benchmark conversation transcripts.
 */

const { randomUUID } = require('crypto');

/**
 * @param {object} options
 * @param {string} options.domainId
 * @param {string} options.conversationId
 * @returns {{ transcriptId: string, domainId: string, conversationId: string, turns: import('../types').ConversationTurn[], startedAt: string, metadata: object }}
 */
function createTranscript(options) {
  return {
    transcriptId: randomUUID(),
    domainId: options.domainId,
    conversationId: options.conversationId,
    turns: [],
    startedAt: new Date().toISOString(),
    metadata: options.metadata || {},
  };
}

/**
 * @param {ReturnType<typeof createTranscript>} transcript
 * @param {'operator'|'max'} role
 * @param {string} content
 * @param {object} [metadata]
 */
function appendTurn(transcript, role, content, metadata = {}) {
  const turn = {
    turnIndex: transcript.turns.length,
    role,
    content: String(content || ''),
    timestamp: new Date().toISOString(),
    metadata,
  };
  transcript.turns.push(turn);
  return turn;
}

function finalizeTranscript(transcript) {
  return {
    ...transcript,
    completedAt: new Date().toISOString(),
    turnCount: transcript.turns.length,
  };
}

/**
 * Extract Max responses from a transcript.
 * @param {import('../types').ConversationTurn[]} turns
 * @returns {import('../types').ConversationTurn[]}
 */
function getMaxTurns(turns) {
  return turns.filter(t => t.role === 'max');
}

/**
 * Get Max response at operator turn index (response follows operator prompt).
 * @param {import('../types').ConversationTurn[]} turns
 * @param {number} operatorTurnIndex - Index among operator-only turns (0-based)
 */
function getMaxResponseForOperatorTurn(turns, operatorTurnIndex) {
  let opCount = -1;
  for (let i = 0; i < turns.length; i++) {
    if (turns[i].role === 'operator') {
      opCount++;
      if (opCount === operatorTurnIndex) {
        const next = turns[i + 1];
        return next?.role === 'max' ? next : null;
      }
    }
  }
  return null;
}

/**
 * Serialize transcript for storage.
 */
function serializeTranscript(transcript) {
  return JSON.parse(JSON.stringify(transcript));
}

module.exports = {
  createTranscript,
  appendTurn,
  finalizeTranscript,
  getMaxTurns,
  getMaxResponseForOperatorTurn,
  serializeTranscript,
};
