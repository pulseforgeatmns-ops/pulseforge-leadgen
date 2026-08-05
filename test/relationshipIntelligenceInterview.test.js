'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  INTERACTION_TYPES,
  INSIGHT_KINDS,
  RelationshipIntelligenceError,
  createMemoryStore,
  assertAllowedSql,
  assertInsightKind,
  startRelationshipInterview,
  answerRelationshipInterview,
  summarizeRelationshipInterview,
  commitRelationshipInterview,
  getInterview,
  listInteractions,
  getInteraction,
  extractInsightsFromNotes,
} = require('../services/relationshipIntelligenceInterview');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store } };
}

describe('relationshipIntelligenceInterview', () => {
  it('starts an interactive interview with the first question', async () => {
    const { opts } = withStore();
    const started = await startRelationshipInterview(
      { interactionType: 'discovery_call', companyId: 'co-1' },
      opts
    );
    assert.ok(started.interviewId);
    assert.equal(started.status, 'draft');
    assert.equal(started.mode, 'interactive');
    assert.equal(started.done, false);
    assert.equal(started.question.id, 'what_happened');
    assert.match(started.message, /What happened/);
  });

  it('answers prompts one at a time and reaches done', async () => {
    const { opts } = withStore();
    const started = await startRelationshipInterview(
      { type: 'walkthrough', companyId: 'co-2' },
      opts
    );

    let turn = await answerRelationshipInterview(
      started.interviewId,
      'Completed a site walkthrough with the office manager.',
      opts
    );
    assert.equal(turn.done, false);
    assert.equal(turn.question.id, 'who_involved');

    turn = await answerRelationshipInterview(
      started.interviewId,
      'Jordan (office manager) and Sam (owner).',
      opts
    );
    assert.equal(turn.question.id, 'cared_most');

    const answers = [
      'Cleanliness consistency and after-hours access.',
      'Price sensitivity and switching cost.',
      'Budget around 2k/month, timeline next quarter, decision maker is Sam.',
      'Send a written estimate by Friday.',
      'Schedule estimate review call.',
      'Prefer email before phone.',
    ];
    for (const a of answers) {
      turn = await answerRelationshipInterview(started.interviewId, a, opts);
    }
    assert.equal(turn.done, true);
    assert.equal(turn.question, null);
  });

  it('summarizes into structured insights with evidence shape', async () => {
    const { opts } = withStore();
    const started = await startRelationshipInterview(
      { type: 'meeting', companyId: 'co-3' },
      opts
    );
    await answerRelationshipInterview(
      started.interviewId,
      'Had a discovery meeting about commercial cleaning.',
      opts
    );
    await answerRelationshipInterview(started.interviewId, 'Alex owner.', opts);
    await answerRelationshipInterview(
      started.interviewId,
      'Reliable night crew coverage.',
      opts
    );
    await answerRelationshipInterview(started.interviewId, 'None yet.', opts);
    await answerRelationshipInterview(
      started.interviewId,
      'Budget not discussed; timeline ASAP; decision maker Alex.',
      opts
    );
    await answerRelationshipInterview(
      started.interviewId,
      'We promised a proposal draft.',
      opts
    );
    await answerRelationshipInterview(
      started.interviewId,
      'Send proposal Monday.',
      opts
    );
    await answerRelationshipInterview(
      started.interviewId,
      'Remember they dislike cold calls.',
      opts
    );

    const draft = await summarizeRelationshipInterview(started.interviewId, opts);
    assert.equal(draft.ok, true);
    assert.equal(draft.kind, 'relationship_intelligence_interview');
    assert.equal(draft.isEvidence, true);
    assert.equal(draft.status, 'draft');
    assert.equal(draft.interaction.interactionType, 'meeting');
    assert.equal(draft.interaction.companyId, 'co-3');
    assert.ok(draft.interaction.rawSummary);
    assert.ok(draft.interaction.confidence >= 0.4);
    assert.ok(draft.insights.length >= 3);
    assert.ok(draft.insights.every((i) => INSIGHT_KINDS.includes(i.kind)));
    assert.ok(draft.nextSteps.length >= 1);
  });

  it('refuses commit before summary exists', async () => {
    const { opts } = withStore();
    const started = await startRelationshipInterview(
      { type: 'cold_call', notes: null },
      opts
    );
    await assert.rejects(
      () => commitRelationshipInterview(started.interviewId, opts),
      (err) => {
        assert.ok(err instanceof RelationshipIntelligenceError);
        assert.equal(err.code, 'summary_required');
        return true;
      }
    );
  });

  it('commit marks interaction as committed and list is queryable', async () => {
    const { opts } = withStore();
    const started = await startRelationshipInterview(
      {
        type: 'follow_up',
        companyId: 'co-9',
        clientId: 10,
        notes:
          'Follow-up call. Owner cares about timeline next month. Budget was 1500. Next step: send estimate. We promised pricing by Thursday.',
      },
      opts
    );
    await summarizeRelationshipInterview(started.interviewId, opts);
    const committed = await commitRelationshipInterview(started.interviewId, opts);
    assert.equal(committed.status, 'committed');

    const listed = await listInteractions({ status: 'committed', clientId: 10 }, opts);
    assert.equal(listed.length, 1);
    assert.equal(listed[0].id, started.interviewId);

    const got = await getInteraction(started.interviewId, opts);
    assert.equal(got.status, 'committed');
    assert.ok(got.insights.length >= 1);
  });

  it('notes mode creates a draft summary', async () => {
    const { opts } = withStore();
    const started = await startRelationshipInterview(
      {
        type: 'discovery_call',
        companyId: 'co-notes',
        notes:
          'Discovery call with the owner. Main pain is missed night cleaning. Goal is consistent coverage. Objection was price. Next step is a walkthrough Friday. We promised to bring references.',
      },
      opts
    );
    assert.equal(started.mode, 'notes');
    assert.equal(started.done, true);

    const draft = await summarizeRelationshipInterview(started.interviewId, opts);
    assert.equal(draft.status, 'draft');
    assert.equal(draft.isEvidence, true);
    assert.ok(draft.insights.some((i) => i.kind === 'context'));
    assert.ok(draft.insights.some((i) => ['pain', 'goal', 'objection', 'next_step', 'commitment'].includes(i.kind)));
  });

  it('AS Cleaning comma-separated notes extract buying signals and next steps', () => {
    const AS_CLEANING_NOTES =
      'Aji is the owner of AS Cleaning Co. The company is less than 6 months old, focused on commercial cleaning clients, and currently doing about ,500 in monthly recurring revenue. Aji expressed interest after the discovery call, asked for more information, and received a personalized 2-page overview for AS Cleaning Co. Need to follow up to confirm interest, clarify target client type, budget/timeline, decision process, and whether they want help generating commercial cleaning leads.';

    const { insights } = extractInsightsFromNotes(AS_CLEANING_NOTES);
    const kinds = insights.map((i) => i.kind);
    const values = insights.map((i) => String(i.value || '').toLowerCase());

    assert.ok(kinds.includes('buying_signal'));
    assert.ok(values.some((v) => v.includes('expressed interest')));
    assert.ok(values.some((v) => v.includes('asked for more information') || v.includes('asked for more info')));
    assert.ok(kinds.includes('commitment'));
    assert.ok(values.some((v) => v.includes('2-page overview') || v.includes('personalized')));
    assert.ok(kinds.includes('next_step'));
    assert.ok(values.some((v) => v.includes('follow-up') || v.includes('follow up')));
    assert.ok(kinds.includes('open_question'));
    assert.ok(values.some((v) => v.includes('budget') || v.includes('timeline')));
    assert.ok(values.some((v) => v.includes('decision process') || v.includes('target client')));
    assert.ok(kinds.includes('decision_maker'));
    assert.ok(values.some((v) => v.includes('aji') && v.includes('owner')));
    assert.ok(kinds.includes('goal'));
    assert.ok(values.some((v) => v.includes('commercial cleaning')));
    assert.ok(kinds.includes('budget') || kinds.includes('context'));
    assert.ok(values.some((v) => v.includes('less than 6 months') || v.includes('monthly recurring')));
    // Raw notes preserved as context; extractor does not rewrite the source string.
    assert.ok(
      insights.some(
        (i) =>
          i.kind === 'context' &&
          String(i.value).includes('expressed interest') &&
          String(i.value).includes('AS Cleaning Co')
      )
    );
  });

  it('low-information notes produce caveats and open questions', async () => {
    const { opts } = withStore();
    const started = await startRelationshipInterview(
      { type: 'other', notes: 'Called. Ok.' },
      opts
    );
    const draft = await summarizeRelationshipInterview(started.interviewId, opts);
    assert.ok(draft.caveats.length >= 1);
    assert.ok(draft.insights.some((i) => i.kind === 'open_question'));
    assert.ok(draft.interaction.confidence <= 0.5);
  });

  it('validates insight kinds', () => {
    assert.equal(assertInsightKind('pain'), 'pain');
    assert.throws(
      () => assertInsightKind('not_a_real_kind'),
      (err) => err instanceof RelationshipIntelligenceError && err.code === 'invalid_insight_kind'
    );
  });

  it('rejects invalid interaction types on start', async () => {
    const { opts } = withStore();
    await assert.rejects(
      () => startRelationshipInterview({ type: 'telepathy' }, opts),
      (err) => err.code === 'invalid_interaction_type'
    );
  });

  it('does not touch CRM tables in memory store sql log', async () => {
    const { store, opts } = withStore();
    const started = await startRelationshipInterview(
      { type: 'demo', notes: 'Demo went well. Next step: proposal review. Budget discussed at 3k.' },
      opts
    );
    await summarizeRelationshipInterview(started.interviewId, opts);
    await commitRelationshipInterview(started.interviewId, opts);

    const tables = new Set(store.sqlLog.map((e) => e.table));
    assert.deepEqual(
      [...tables].sort(),
      ['relationship_interaction_insights', 'relationship_interactions']
    );
    for (const entry of store.sqlLog) {
      assert.ok(
        entry.table === 'relationship_interactions' ||
          entry.table === 'relationship_interaction_insights'
      );
    }
  });

  it('assertAllowedSql blocks CRM mutations', () => {
    assert.doesNotThrow(() =>
      assertAllowedSql('SELECT * FROM relationship_interactions WHERE id = $1')
    );
    assert.throws(
      () => assertAllowedSql('UPDATE prospects SET status = $1 WHERE id = $2'),
      (err) => err.code === 'crm_write_forbidden'
    );
    assert.throws(
      () => assertAllowedSql('UPDATE opportunities SET stage = $1 WHERE id = $2'),
      (err) => err.code === 'crm_write_forbidden'
    );
  });

  it('getInterview returns current question and state', async () => {
    const { opts } = withStore();
    const started = await startRelationshipInterview({ type: 'estimate' }, opts);
    const view = await getInterview(started.interviewId, opts);
    assert.equal(view.interviewId, started.interviewId);
    assert.equal(view.question.id, 'what_happened');
    assert.equal(view.summarized, false);
  });

  it('exposes expected interaction type enum', () => {
    assert.ok(INTERACTION_TYPES.includes('walkthrough'));
    assert.ok(INTERACTION_TYPES.includes('proposal_review'));
    assert.equal(INTERACTION_TYPES.length, 9);
  });
});
