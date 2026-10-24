'use strict';

const {
  createTranscript,
  appendTurn,
  finalizeTranscript,
} = require('./TranscriptCapture');
const { checkAllBehaviors, classifyFailuresFromBehaviors } = require('../behaviors/BehaviorChecker');
const { RUN_STATUS, REVIEW_STATUS } = require('../types');

/**
 * Executes a benchmark conversation turn-by-turn and captures the transcript.
 *
 * @typedef {object} AskFnInput
 * @property {string} question
 * @property {string} [sessionId]
 * @property {object} [context]
 *
 * @typedef {object} AskFnResult
 * @property {string} prose - Max response text
 * @property {string} [sessionId]
 * @property {object} [metadata]
 *
 * @typedef {(input: AskFnInput) => Promise<AskFnResult>} AskFn
 */

/**
 * @param {import('../types').CognitiveDomain} domain
 * @param {AskFn} askFn - Injected Max ask function
 * @param {object} [options]
 * @param {number} [options.turnDelayMs] - Delay between turns (simulation)
 * @returns {Promise<import('../types').DomainResult>}
 */
async function runBenchmarkConversation(domain, askFn, options = {}) {
  const started = Date.now();
  const conversation = domain.conversation;
  const transcript = createTranscript({
    domainId: domain.id,
    conversationId: conversation.id,
    metadata: { title: conversation.title },
  });

  let sessionId = null;
  const context = conversation.context || {};

  try {
    for (const turn of conversation.turns) {
      if (turn.role !== 'operator') continue;

      appendTurn(transcript, 'operator', turn.content);

      const result = await askFn({
        question: turn.content,
        sessionId,
        context,
      });

      sessionId = result.sessionId || sessionId;
      appendTurn(transcript, 'max', result.prose || result.content || '', result.metadata || {});

      if (options.turnDelayMs) {
        await sleep(options.turnDelayMs);
      }
    }

    finalizeTranscript(transcript);

    const behaviorResults = checkAllBehaviors(domain.expectedBehaviors, transcript.turns);
    const failures = classifyFailuresFromBehaviors(
      domain.expectedBehaviors,
      behaviorResults,
      transcript.turns
    );

    const needsReview = behaviorResults.some(r => r.requiresHumanReview)
      || failures.some(f => f.requiresHumanReview);

    return {
      domainId: domain.id,
      status: failures.length === 0 ? RUN_STATUS.COMPLETED : RUN_STATUS.NEEDS_REVIEW,
      conversationId: conversation.id,
      transcript: transcript.turns,
      failures,
      behaviorResults,
      score: null,
      reviewStatus: needsReview ? REVIEW_STATUS.PENDING : REVIEW_STATUS.NOT_REQUIRED,
      durationMs: Date.now() - started,
    };
  } catch (error) {
    return {
      domainId: domain.id,
      status: RUN_STATUS.FAILED,
      conversationId: conversation.id,
      transcript: transcript.turns,
      failures: [],
      behaviorResults: [],
      score: null,
      reviewStatus: REVIEW_STATUS.PENDING,
      error: error.message,
      durationMs: Date.now() - started,
    };
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = {
  runBenchmarkConversation,
};
