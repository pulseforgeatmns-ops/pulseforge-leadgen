'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  REFINEMENT_GUIDANCE_ACK_STRING,
  INTAKE_RESPONSE_BRANCH_SOURCES,
  buildIntakePathDebugSuffix,
  finalizeIntakePathPayload,
  createIntakePathTrace,
  intakePathDebugVisible,
} = require('../services/cieIntakePathTrace');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
} = require('../services/clientIntelligenceInterview');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store } };
}

describe('CIE intake path trace (production diagnostic)', () => {
  it('maps the repeated guidance string to known response branches', () => {
    const sources = Object.values(INTAKE_RESPONSE_BRANCH_SOURCES);
    const hits = sources.filter((s) => s.exactString === REFINEMENT_GUIDANCE_ACK_STRING);
    assert.ok(hits.length >= 2, 'expected interview + reasoning ack sources');
    assert.equal(
      INTAKE_RESPONSE_BRANCH_SOURCES.conversationalAck_refinement_feedback.fn,
      'conversationalAck'
    );
    assert.equal(
      INTAKE_RESPONSE_BRANCH_SOURCES.reasoningAck_refinement_feedback.file,
      'services/clientIntelligenceReasoning.js'
    );
  });

  it('appends visible debug suffix when CIE_INTAKE_PATH_VISIBLE is enabled', () => {
    const prev = process.env.CIE_INTAKE_PATH_VISIBLE;
    process.env.CIE_INTAKE_PATH_VISIBLE = '1';
    try {
      const trace = createIntakePathTrace({
        sessionId: 'sess-1',
        tenantId: 99,
        activeQuestionKeyBeforeClassify: 'success_metrics',
      });
      trace.finalIntent = 'refinement_feedback';
      trace.destinationSection = 'successMetrics';
      trace.responseBranch = 'direct_answer_skipped_as_guidance';
      trace.templateName = 'conversationalAck.refinement_feedback';
      const out = finalizeIntakePathPayload(trace, {
        message: `${REFINEMENT_GUIDANCE_ACK_STRING}\n\nHow will we know it's working?`,
      });
      assert.match(out.message, /debug_intake_path=client_intelligence_postInterviewMessage_v3/);
      assert.match(out.message, /active=success_metrics/);
      assert.ok(out.intakeTraceId);
      assert.ok(intakePathDebugVisible());
    } finally {
      if (prev === undefined) delete process.env.CIE_INTAKE_PATH_VISIBLE;
      else process.env.CIE_INTAKE_PATH_VISIBLE = prev;
    }
  });

  it('success-metrics direct answer reports advance branch (not guidance ack)', async () => {
    const prev = process.env.CIE_INTAKE_PATH_VISIBLE;
    process.env.CIE_INTAKE_PATH_VISIBLE = '1';
    try {
      const { opts, store } = withStore();
      const started = await startClientInterview({ clientId: 901 }, opts);
      for (const answer of [
        'Studio Substral — credible websites for local businesses.',
        'Website design and redesign.',
        'Local service businesses in southern NH.',
        'Commodity price shoppers.',
        'Greater Manchester NH.',
        'Responsiveness tips the decision.',
        'Grounded and direct.',
        'More booked discovery calls in 90 days.',
      ]) {
        await postInterviewMessage(started.interviewId, answer, opts);
      }
      const metricsAnswer =
        'Qualified prospects identified, positive replies, discovery calls booked, and revenue closed.';
      const turn = await postInterviewMessage(started.interviewId, metricsAnswer, opts);
      assert.doesNotMatch(turn.message || '', /guidance for how I write/i);
      assert.match(turn.message || '', /debug_intake_path=/);
      assert.equal(turn.intakePathDebug.branch, 'direct_answer_interview_complete');
      assert.equal(turn.intakePathDebug.guards.successMetricsHardGuardRan, false);
      const session = await store.getSession(started.interviewId);
      assert.ok(session.interview_state.answers.success_metrics);
    } finally {
      if (prev === undefined) delete process.env.CIE_INTAKE_PATH_VISIBLE;
      else process.env.CIE_INTAKE_PATH_VISIBLE = prev;
    }
  });

  it('does not expose client debug fields when CIE_INTAKE_PATH_VISIBLE is off', () => {
    const prevVisible = process.env.CIE_INTAKE_PATH_VISIBLE;
    const prevLog = process.env.CIE_INTAKE_PATH_LOG;
    delete process.env.CIE_INTAKE_PATH_VISIBLE;
    delete process.env.CIE_INTAKE_PATH_LOG;
    try {
      const trace = createIntakePathTrace({
        activeQuestionKeyBeforeClassify: 'identity',
      });
      trace.responseBranch = 'direct_answer_advance';
      const out = finalizeIntakePathPayload(trace, { message: 'Hello there.' });
      assert.equal(out.intakeTraceId, undefined);
      assert.equal(out.intakePathDebug, undefined);
      assert.equal(out.message, 'Hello there.');
    } finally {
      if (prevVisible === undefined) delete process.env.CIE_INTAKE_PATH_VISIBLE;
      else process.env.CIE_INTAKE_PATH_VISIBLE = prevVisible;
      if (prevLog === undefined) delete process.env.CIE_INTAKE_PATH_LOG;
      else process.env.CIE_INTAKE_PATH_LOG = prevLog;
    }
  });

  it('buildIntakePathDebugSuffix includes branch and trace id', () => {
    const trace = createIntakePathTrace({ activeQuestionKeyBeforeClassify: 'success_metrics' });
    trace.responseBranch = 'non_answer_refinement_feedback';
    trace.finalIntent = 'refinement_feedback';
    trace.destinationSection = 'successMetrics';
    trace.fieldMarkedComplete = false;
    trace.templateName = 'conversationalAck.refinement_feedback';
    const suffix = buildIntakePathDebugSuffix(trace);
    assert.match(suffix, /branch=non_answer_refinement_feedback/);
    assert.match(suffix, /template=conversationalAck.refinement_feedback/);
    assert.match(suffix, /trace=intake_/);
  });
});
