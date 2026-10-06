'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');
const {
  interpretConversationalInput,
  formatUnderstandingPreview,
  EPISTEMIC_CATEGORY,
} = require('../packages/max/understanding');
const { TEMPORAL_ROLE } = require('../packages/max/understanding/temporal');

const PRODUCTION_SMOKE_TRANSCRIPT =
  "All right, I just left Exeter Phillips. Talked to Billy. Their cleaner is still missing some of the common areas, and he said he's going to talk to his boss. Well, actually, Dave isn't the decision maker. Lisa is the one that handles vendors. I didn't get her last name. He said that I should hear back by Friday. So if I don't, remind me to follow up on Monday.";

const NOW = new Date('2026-10-05T15:00:00.000Z');

function interpret(text) {
  return interpretConversationalInput({ text, now: NOW });
}

describe('MAX-VOICE-002 Voice / Understanding Gauntlet', () => {
  it('VR1 — exact production smoke transcript', () => {
    const { situationModel, preview } = interpret(PRODUCTION_SMOKE_TRANSCRIPT);
    assert.equal(situationModel.threads.length, 1);
    assert.match(situationModel.threads[0].accountName, /Exeter Phillips/i);
    assert.ok(situationModel.events.some(e => e.kind === 'in_person_visit'));
    const contactNames = situationModel.entities.filter(e => e.kind === 'contact').map(e => e.name);
    assert.ok(contactNames.some(n => /billy/i.test(n)));
    assert.ok(contactNames.some(n => /dave/i.test(n)));
    assert.ok(contactNames.some(n => /lisa/i.test(n)));
    assert.ok(situationModel.painPoints.some(p => p.current && /common areas/i.test(p.description)));
    const daveCorr = situationModel.corrections.find(c => c.kind === 'decision_maker_role' && c.contactName === 'Dave');
    assert.ok(daveCorr);
    const lisa = situationModel.decisionMakerSignals.find(s => /lisa/i.test(s.contactName));
    assert.ok(lisa);
    assert.equal(lisa.epistemic, EPISTEMIC_CATEGORY.REPORTED);
    const lisaEntity = situationModel.entities.find(e => e.kind === 'contact' && e.name === 'Lisa');
    assert.equal(lisaEntity?.lastNameKnown, false);
    const callback = situationModel.commitments.find(c => c.kind === 'callback');
    assert.ok(callback);
    assert.match(String(callback.windowPhrase), /friday/i);
    assert.equal(callback.sourceContactUnresolved, true);
    const followUp = situationModel.requestedActions.find(a => a.action === 'schedule_follow_up');
    assert.ok(followUp?.conditional);
    assert.match(String(followUp.temporal), /monday/i);
    assert.ok(!situationModel.corrections.some(c => c.kind === 'temporal' && /friday/i.test(String(c.priorValue)) && /monday/i.test(String(c.newValue))));
    assert.ok(!situationModel.corrections.some(c => c.kind === 'temporal' && String(c.priorValue).toLowerCase() === String(c.newValue).toLowerCase()));
    assert.match(preview, /Spoke with Billy/i);
    assert.match(preview, /missing common areas/i);
    assert.match(preview, /callback by Friday/i);
    assert.match(preview, /follow up Monday/i);
  });

  it('VR2 — same account re-mentioned yields one thread', () => {
    const msg =
      'I just left Exeter Phillips. Talked to Billy. Dave at Exeter Phillips said the cleaner is still missing common areas.';
    const { situationModel } = interpret(msg);
    assert.equal(situationModel.threads.length, 1);
    assert.match(situationModel.threads[0].accountName, /Exeter Phillips/i);
  });

  it('VR3 — two real accounts remain separate', () => {
    const msg = 'I left Exeter Phillips, then stopped at ABC Manufacturing.';
    const { situationModel } = interpret(msg);
    assert.equal(situationModel.threads.length, 2);
  });

  it('VR4 — simple contact intro captures Billy', () => {
    const { situationModel } = interpret('Talked to Billy at Exeter Phillips.');
    assert.ok(situationModel.entities.some(e => e.kind === 'contact' && e.name === 'Billy'));
  });

  it('VR5 — pain extraction for missed common areas', () => {
    const { situationModel } = interpret('Cleaner is still missing common areas at Exeter Phillips.');
    const pain = situationModel.painPoints.find(p => /common areas/i.test(p.description));
    assert.ok(pain);
    assert.equal(pain.current, true);
  });

  it('VR6 — Friday deadline and Monday conditional follow-up coexist', () => {
    const { situationModel } = interpret(
      'Dave at Exeter Phillips said I should hear back by Friday. If I do not hear back, remind me to follow up on Monday.'
    );
    assert.ok(situationModel.commitments.some(c => /friday/i.test(String(c.windowPhrase))));
    const followUp = situationModel.requestedActions.find(a => a.action === 'schedule_follow_up');
    assert.ok(followUp?.conditional);
    assert.match(String(followUp.temporal), /monday/i);
    assert.equal(situationModel.corrections.filter(c => c.kind === 'temporal').length, 0);
  });

  it('VR7 — real temporal correction language', () => {
    const { situationModel } = interpret('Lisa said Tuesday — actually, Thursday.');
    const temporal = situationModel.corrections.filter(c => c.kind === 'temporal');
    assert.equal(temporal.length, 1);
    assert.match(String(temporal[0].newValue), /thursday/i);
  });

  it('VR8 — no Monday → Monday self-correction', () => {
    const { situationModel } = interpret(PRODUCTION_SMOKE_TRANSCRIPT);
    assert.ok(!situationModel.corrections.some(c => {
      const p = String(c.priorValue || '').toLowerCase();
      const n = String(c.newValue || '').toLowerCase();
      return p === n && p.length > 0;
    }));
  });

  it('VR9 — duplicate temporal corrections suppressed', () => {
    const { situationModel } = interpret('Lisa said Tuesday — actually, sorry, Thursday.');
    const temporal = situationModel.corrections.filter(c => c.kind === 'temporal');
    assert.equal(temporal.length, 1);
  });

  it('VR10 — ambiguous speaker keeps callback without guessed attribution', () => {
    const { situationModel } = interpret(PRODUCTION_SMOKE_TRANSCRIPT);
    const callback = situationModel.commitments.find(c => c.kind === 'callback');
    assert.equal(callback.responsible, 'unresolved');
    assert.equal(callback.sourceContactUnresolved, true);
  });

  it('VR11 — known missing last name without fabrication', () => {
    const { situationModel } = interpret(PRODUCTION_SMOKE_TRANSCRIPT);
    const lisa = situationModel.entities.find(e => e.kind === 'contact' && e.name === 'Lisa');
    assert.equal(lisa?.lastNameKnown, false);
    assert.ok(!situationModel.entities.some(e => e.kind === 'contact' && /lisa\s+\w+/i.test(e.name) && !/^lisa$/i.test(e.name)));
  });

  it('VR12 — temporal roles distinguish deadline vs conditional follow-up', () => {
    const { situationModel } = interpret(PRODUCTION_SMOKE_TRANSCRIPT);
    const friday = situationModel.temporalReferences.find(t => /friday/i.test(t.phrase));
    const monday = situationModel.temporalReferences.find(t => /monday/i.test(t.phrase));
    assert.equal(friday?.role, TEMPORAL_ROLE.DEADLINE);
    assert.equal(monday?.role, TEMPORAL_ROLE.CONDITIONAL_FOLLOW_UP_TIME);
  });
});
