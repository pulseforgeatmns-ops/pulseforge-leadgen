'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  selectVisitProbes,
  resolveNextActionOwner,
  isOwnerEscalation,
  isAoOwnedFollowUp,
  isWeakVisitNote,
  buildCompletionReply,
  resolveCompletionType,
  parseInterestLevel,
} = require('../utils/aoMessageTemplates');
const {
  initProbeState,
  advanceAfterBaseStep,
  processVisitNoteAnswer,
} = require('../utils/aoVisitFlow');

test('parseInterestLevel normalizes interest answers', () => {
  assert.equal(parseInterestLevel('high'), 'high');
  assert.equal(parseInterestLevel('Low interest'), 'low');
  assert.equal(parseInterestLevel('medium'), 'medium');
});

test('isOwnerEscalation detects Jake/admin phrasing', () => {
  assert.equal(isOwnerEscalation('Jake should call'), true);
  assert.equal(isOwnerEscalation('Pulseforge admin should call'), true);
  assert.equal(isOwnerEscalation('Needs Jake'), true);
  assert.equal(isOwnerEscalation('Someone from Anchor should call'), true);
  assert.equal(isOwnerEscalation('Follow up in 2 days'), false);
});

test('resolveNextActionOwner routes ownership correctly', () => {
  assert.equal(resolveNextActionOwner('Jake should call', {}), 'jake');
  assert.equal(resolveNextActionOwner('Book walkthrough', {}), 'walkthrough');
  assert.equal(resolveNextActionOwner('No follow-up needed', {}), 'none');
  assert.equal(resolveNextActionOwner('Mark as not a fit', {}), 'not_a_fit');
  assert.equal(resolveNextActionOwner('Call back tomorrow', {}), 'ao');
  assert.equal(resolveNextActionOwner('I should follow up', {}), 'ao');
  assert.equal(resolveNextActionOwner("I'll check back", {}), 'ao');
  assert.equal(resolveNextActionOwner('AO should follow up', {}), 'ao');
});

test('isWeakVisitNote detects action-only visit answers', () => {
  assert.equal(isWeakVisitNote('Jake should call'), true);
  assert.equal(isWeakVisitNote('Follow up later'), true);
  assert.equal(isWeakVisitNote('Send info'), true);
  assert.equal(isWeakVisitNote('They mentioned the current cleaner is inconsistent'), false);
});

test('processVisitNoteAnswer asks for clarification on weak answers', () => {
  const first = processVisitNoteAnswer({}, 'Jake should call');
  assert.equal(first.completed, false);
  assert.match(first.clarifyQuestion, /what did they actually say/i);

  const second = processVisitNoteAnswer(first.payload, 'They are unhappy with missed visits');
  assert.equal(second.completed, true);
  assert.match(second.payload.visit_note, /missed visits/i);
});

test('processVisitNoteAnswer does not re-clarify after one pass', () => {
  const clarified = processVisitNoteAnswer({ _visit_note_clarified: true }, 'Follow up');
  assert.equal(clarified.completed, true);
  assert.equal(clarified.payload.visit_note, 'Follow up');
});

test('selectVisitProbes picks gatekeeper probe after visit note', () => {
  const probes = selectVisitProbes({
    visit_note: 'Front desk only',
    contact_role: 'gatekeeper',
  }, { phase: 'after_visit_note', maxTotal: 1 });
  assert.equal(probes.length, 1);
  assert.match(probes[0].question, /handles cleaning decisions/i);
});

test('selectVisitProbes picks medium interest probe', () => {
  const probes = selectVisitProbes({
    visit_note: 'Interested but cautious',
    interest_level: 'medium',
    contact_role: 'decision_maker',
  }, { phase: 'after_interest_level', maxTotal: 2, existingKeys: [] });
  assert.ok(probes.some(p => /medium instead of high/i.test(p.question)));
});

test('selectVisitProbes caps at two total probes', () => {
  const first = selectVisitProbes({
    visit_note: 'Current cleaner is inconsistent and they want more info',
    contact_role: 'gatekeeper',
  }, { phase: 'after_visit_note', maxTotal: 2 });
  const second = selectVisitProbes({
    visit_note: 'Current cleaner is inconsistent and they want more info',
    interest_level: 'medium',
    contact_role: 'gatekeeper',
  }, { phase: 'after_interest_level', maxTotal: 2, existingKeys: first.map(p => p.key) });
  assert.equal(first.length + second.length, 2);
});

test('advanceAfterBaseStep enqueues probes without skipping base steps', () => {
  const payload = { visit_note: 'Gatekeeper only', contact_role: 'gatekeeper' };
  const { state, nextProbe } = advanceAfterBaseStep(payload, 'visit_note');
  assert.ok(nextProbe);
  assert.equal(state.in_probe_mode, true);
  assert.ok(state.probe_queue.length >= 1);
});

test('buildCompletionReply hides suggested message on Jake escalation', () => {
  const reply = buildCompletionReply({
    businessName: 'Acme Law',
    completionType: 'jake_escalation',
    suggestedMessage: 'Hi Sarah, Jake will call...',
  });
  assert.match(reply, /escalated it to Jake/i);
  assert.doesNotMatch(reply, /suggested follow-up/i);
  assert.doesNotMatch(reply, /Hi Sarah/);
});

test('buildCompletionReply shows suggested message for AO follow-up', () => {
  const reply = buildCompletionReply({
    businessName: 'Acme Law',
    completionType: 'ao_follow_up',
    suggestedMessage: 'Hi Sarah, thanks for your time.',
  });
  assert.match(reply, /suggested follow-up/i);
  assert.match(reply, /Hi Sarah/);
  assert.match(reply, /added it to your queue/i);
});

test('buildCompletionReply handles walkthrough escalation', () => {
  const reply = buildCompletionReply({
    businessName: 'Acme Law',
    completionType: 'walkthrough',
    preferredTiming: 'Tuesday mornings',
  });
  assert.match(reply, /walkthrough request to Jake/i);
  assert.match(reply, /Preferred timing: Tuesday mornings/);
  assert.doesNotMatch(reply, /suggested follow-up/i);
});

test('buildCompletionReply handles not a fit and no follow-up', () => {
  assert.match(buildCompletionReply({ businessName: 'X', completionType: 'not_a_fit' }), /not a fit/);
  assert.match(buildCompletionReply({ businessName: 'X', completionType: 'not_a_fit' }), /Log another visit or check your queue/);
  assert.match(buildCompletionReply({ businessName: 'X', completionType: 'no_follow_up' }), /No follow-up needed/);
  assert.match(buildCompletionReply({ businessName: 'X', completionType: 'no_follow_up' }), /Log another visit or check your queue/);
});

test('isAoOwnedFollowUp recognizes AO-owned phrasing', () => {
  assert.equal(isAoOwnedFollowUp('I should follow up'), true);
  assert.equal(isAoOwnedFollowUp("I'll stop back next week"), true);
  assert.equal(isAoOwnedFollowUp('Rep should follow up'), true);
  assert.equal(isAoOwnedFollowUp('Jake should call'), false);
});

test('resolveCompletionType maps owners to completion types', () => {
  assert.equal(resolveCompletionType({ nextActionOwner: 'jake', escalated: true }), 'jake_escalation');
  assert.equal(resolveCompletionType({ nextActionOwner: 'ao', escalated: false }), 'ao_follow_up');
  assert.equal(resolveCompletionType({ nextActionOwner: 'walkthrough', escalated: true }), 'walkthrough');
});

test('initProbeState returns stable defaults', () => {
  const state = initProbeState({});
  assert.deepEqual(state.probe_queue, []);
  assert.equal(state.probe_index, 0);
  assert.equal(state.in_probe_mode, false);
});
