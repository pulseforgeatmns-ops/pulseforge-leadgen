'use strict';

/**
 * Max workspace adapter for COG benchmark execution.
 */

const path = require('path');

/**
 * Create an ask function backed by Max WorkspaceEngine.
 * @param {object} [options]
 * @param {boolean} [options.disableLlm=true] - Deterministic mode for reproducible runs
 * @param {boolean} [options.missionsEnabled=false]
 * @param {object} [options.clientIntelligenceOpts]
 * @returns {Promise<import('./conversations/ConversationRunner').AskFn>}
 */
async function createMaxAskFn(options = {}) {
  const { createWorkspaceEngine } = require(path.join(
    __dirname, '..', 'max', 'workspace', 'WorkspaceEngine'
  ));

  const engine = createWorkspaceEngine({
    disableLlm: options.disableLlm !== false,
    missionsEnabled: options.missionsEnabled === true,
    clientIntelligenceOpts: options.clientIntelligenceOpts,
  });

  return async function askFn(input) {
    const result = await engine.ask({
      question: input.question,
      sessionId: input.sessionId,
      context: input.context,
    });

    return {
      prose: result.prose || result.answer || result.text || '',
      sessionId: result.sessionId,
      metadata: {
        intent: result.intent || result.operatorIntent || null,
        contract: result.conversationContract?.type || null,
        trace: result.trace || null,
      },
    };
  };
}

/**
 * Create a stub ask function for framework testing without Max runtime.
 * @param {Record<number, string>|function(string, number): string} responses
 */
function createStubAskFn(responses) {
  let sessionId = `stub-${Date.now()}`;
  let turnIndex = 0;

  return async function askFn(input) {
    let prose;
    if (typeof responses === 'function') {
      prose = responses(input.question, turnIndex);
    } else if (typeof responses === 'object') {
      prose = responses[turnIndex] || responses[String(turnIndex)] || 'Stub response.';
    } else {
      prose = 'Stub response.';
    }
    turnIndex++;
    return { prose, sessionId, metadata: { stub: true } };
  };
}

module.exports = {
  createMaxAskFn,
  createStubAskFn,
};
