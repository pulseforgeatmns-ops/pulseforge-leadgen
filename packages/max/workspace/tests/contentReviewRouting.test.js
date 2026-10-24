'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { isContentReviewRequest } = require('../ContentReviewIntent');
const { createWorkspaceEngine } = require('../WorkspaceEngine');

const reviewRequests = [
  'Show me the content waiting for my approval in the current workspace, explain the recommendations and help me review them.',
  'Open content review',
  'Review content',
  'Show me the pending posts',
  'Does Paige have pending posts for me to approve?',
  'What drafts are awaiting approval?',
];

test('content review intent recognizes navigation without treating execution as review', () => {
  reviewRequests.forEach(question => assert.equal(isContentReviewRequest(question), true, question));
  for (const question of [
    'Continue', 'Approve the mission', 'Approve all pending posts', 'Reject the draft',
    'Write content for Facebook', 'Generate new posts for review', 'Publish the pending posts',
    'Review content then publish it', 'Show me pending posts and schedule them',
    'Give me Penny’s current Google Ads report', 'Review the acquisition mission',
    'Review the outreach email drafts in this mission', 'Find saved previews',
  ]) assert.equal(isContentReviewRequest(question), false, question);
});

test('an active acquisition mission cannot intercept content review or execute work', async () => {
  const engine = createWorkspaceEngine({ disableLlm: true, loadOperatorContext: false });
  const opened = await engine.open({ tenantId: '10', page: 'command-deck', acquisitionMissionId: 'active-acquisition', missionId: 'mission-10', objective: 'Acquire recurring commercial cleaning clients.' }, { loadOperatorContext: false });
  const before = JSON.parse(JSON.stringify(engine.sessions.get(opened.sessionId).context));
  engine._amoRuntimeInput = () => assert.fail('content review must not hydrate or run an acquisition mission');
  for (const question of reviewRequests) {
    const result = await engine.ask({ sessionId: opened.sessionId, question });
    assert.equal(result.uiAction.type, 'open_content_review');
    assert.equal(result.uiAction.href, '/command-deck#content-review');
    assert.equal(result.workspaceOwnership.owner, 'content_review');
    assert.equal(result.metadata.readOnly, true);
    assert.equal(result.metadata.mutatesMissionState, false);
    assert.equal(result.mission, null);
    assert.match(result.prose, /Content review requested/);
    assert.doesNotMatch(result.prose, /Acquire recurring|commercial cleaning|Mission Resumed/);
    assert.deepEqual(result.context, before);
  }
});

test('a new scoped conversation can navigate directly to review', async () => {
  const engine = createWorkspaceEngine({ disableLlm: true, loadOperatorContext: false });
  const result = await engine.ask({ question: 'Review content', context: { tenantId: '11', page: 'command-deck' } });
  assert.equal(result.context.tenantId, '11');
  assert.equal(result.uiAction.type, 'open_content_review');
  assert.ok(result.sessionId);
});

test('account navigation needs trusted matching roster, never conversational roster or other tenant', async () => {
  const engine = createWorkspaceEngine({ disableLlm: true, loadOperatorContext: false });
  const input = {question:"Review Tony's accounts",context:{tenantId:'10',aos:[{id:24,name:'Tony'}]}};
  assert.equal((await engine.ask(input)).uiAction,null);
  const valid=await engine.ask({...input,reviewNavigationScope:{tenantId:10,aos:[{id:24,name:'Tony'}]}});
  assert.equal(valid.uiAction.aoId,24);assert.equal(valid.uiAction.tenantId,'10');
  assert.equal((await engine.ask({...input,reviewNavigationScope:{tenantId:11,aos:[{id:24,name:'Tony'}]}})).uiAction,null);
});
