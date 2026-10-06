'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');
const {
  interpretConversationalInput,
  ConversationMemory,
  validateSituationModel,
  formatUnderstandingPreview,
  EPISTEMIC_CATEGORY,
} = require('../packages/max/understanding');
const {
  ingestOperationalUpdate,
  MemoryStateStore,
} = require('../packages/max/stateIngestion');

function seedStore(overrides = {}) {
  return new MemoryStateStore({
    clientId: 1,
    users: [{ id: 10, name: 'Tony' }],
    companies: overrides.companies || [
      { id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 },
      { id: 'co-abc', name: 'ABC Manufacturing', client_id: 1 },
    ],
    prospects: overrides.prospects || [
      {
        id: 'prospect-exeter',
        client_id: 1,
        company_id: 'co-exeter',
        company_name: 'Exeter Phillips',
        assigned_ao_id: 10,
        status: 'warm',
      },
    ],
    contacts: overrides.contacts || [
      { id: 'contact-dave', prospect_id: 'prospect-exeter', name: 'Dave' },
      { id: 'contact-mike', prospect_id: 'prospect-exeter', name: 'Mike' },
    ],
    ...overrides,
  });
}

describe('MAX Understanding Gauntlet', () => {
  it('A — multi-account update separates threads', () => {
    const msg =
      'Talked to Dave at Exeter Phillips again. Their cleaner is still missing common areas. He\'s going to talk to his boss and should call me this week. I also stopped at ABC Manufacturing but the woman at the desk said the facilities guy is out until Thursday.';
    const { situationModel } = interpretConversationalInput({ text: msg, now: new Date('2026-10-05T15:00:00.000Z') });
    assert.equal(situationModel.threads.length, 2);
    const exeter = situationModel.threads.find(t => /exeter/i.test(t.accountName || ''));
    const abc = situationModel.threads.find(t => /abc manufacturing/i.test(t.accountName || ''));
    assert.ok(exeter);
    assert.ok(abc);
    assert.ok(exeter.painPoints.some(p => /common areas/i.test(p.description)));
    assert.ok(abc.commitments.some(c => /thursday/i.test(c.windowPhrase || '')));
    assert.ok(!exeter.painPoints.some(p => /abc/i.test(p.description)));
  });

  it('B — pronoun continuation resolves Dave', () => {
    const memory = new ConversationMemory({ conversationId: 'conv-1' });
    interpretConversationalInput({
      text: 'Talked to Dave at Exeter Phillips.',
      memory,
      conversationId: 'conv-1',
    });
    const second = interpretConversationalInput({
      text: 'He\'s not actually the decision maker.',
      memory,
      conversationId: 'conv-1',
    });
    assert.ok(second.situationModel.corrections.some(c => c.kind === 'decision_maker_role' || c.targetClaim === 'decision_maker_role'));
    const preview = formatUnderstandingPreview(second.situationModel);
    assert.match(preview, /Role correction|decision/i);
  });

  it('C — mid-message correction preserves Thursday', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'Lisa said Tuesday — actually, sorry, Thursday.',
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    const corr = situationModel.corrections.find(c => c.kind === 'temporal');
    assert.ok(corr);
    assert.equal(corr.newValue.toLowerCase(), 'thursday');
    const canonical = situationModel.temporalReferences.find(t => t.canonical);
    assert.ok(canonical);
    assert.match(canonical.phrase, /thursday/i);
  });

  it('D — uncertain role stays uncertain', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'I think Mike might handle facilities.',
    });
    const dm = situationModel.decisionMakerSignals.find(s => /mike/i.test(s.contactName));
    assert.ok(dm);
    assert.equal(dm.epistemic, EPISTEMIC_CATEGORY.UNCERTAIN);
  });

  it('E — reported role attributes source Sarah', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'Sarah told me Lisa handles vendor decisions.',
    });
    const dm = situationModel.decisionMakerSignals.find(s => /lisa/i.test(s.contactName));
    assert.ok(dm);
    assert.equal(dm.epistemic, EPISTEMIC_CATEGORY.REPORTED);
    assert.equal(dm.reportedBy, 'Sarah');
  });

  it('F — mixed question + update', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'They\'re still having bathroom issues and Dave said Friday. Should I wait?',
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    assert.ok(situationModel.painPoints.some(p => /bathroom/i.test(p.description)));
    assert.ok(situationModel.commitments.length >= 1);
    assert.ok(situationModel.questions.length >= 1);
  });

  it('G — ambiguous pronoun triggers clarification', () => {
    const memory = new ConversationMemory({ conversationId: 'conv-2' });
    interpretConversationalInput({
      text: 'Talked to Dave at Exeter Phillips.',
      memory,
      conversationId: 'conv-2',
    });
    interpretConversationalInput({
      text: 'Also spoke with Mike at Exeter Phillips.',
      memory,
      conversationId: 'conv-2',
    });
    const third = interpretConversationalInput({
      text: 'He said call Thursday.',
      memory,
      conversationId: 'conv-2',
    });
    const validation = validateSituationModel(third.situationModel);
    assert.equal(validation.blockCommit, true);
    assert.ok(validation.narrowestClarification);
    assert.match(validation.narrowestClarification, /Dave|Mike/i);
  });

  it('H — rambling voice-style transcript', () => {
    const msg =
      'Okay so I just left Granite State Daycare, talked to Sarah up front, really nice, they do already have somebody cleaning but she said bathrooms have been kind of hit or miss, I think Lisa is the director or maybe she handles vendors, Sarah said come back Wednesday morning because she\'s usually there then.';
    const { situationModel } = interpretConversationalInput({
      text: msg,
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    assert.ok(situationModel.threads[0].accountName?.includes('Granite State Daycare'));
    assert.ok(situationModel.objections.some(o => /incumbent/i.test(o.kind)));
    assert.ok(situationModel.painPoints.some(p => /bathroom/i.test(p.description)));
    const lisa = situationModel.decisionMakerSignals.find(s => /lisa/i.test(s.contactName));
    assert.ok(lisa);
    assert.notEqual(lisa.epistemic, EPISTEMIC_CATEGORY.CONFIRMED);
  });

  it('I — negation does not create dissatisfaction pain', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'They are not unhappy with the cleaner.',
    });
    assert.equal(situationModel.painPoints.filter(p => p.current !== false).length, 0);
  });

  it('J — historical versus current pain', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'They used to have issues with bathrooms but said it\'s been fine lately.',
    });
    const pain = situationModel.painPoints.find(p => /bathroom/i.test(p.description));
    assert.ok(pain);
    assert.equal(pain.current, false);
    assert.equal(pain.historical, true);
  });

  it('K — multiple intents (update + advisory question)', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'Update Exeter with that and tell me who I should talk to next.',
    });
    assert.ok(situationModel.requestedActions.some(a => /advisory|apply_prior/i.test(a.action)));
  });

  it('L — correction of account identity', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'That wasn\'t Exeter Phillips, it was Exeter Packaging.',
    });
    const corr = situationModel.corrections.find(c => c.kind === 'account_identity');
    assert.ok(corr);
    assert.match(corr.newValue, /Exeter Packaging/i);
    assert.match(corr.priorValue, /Exeter Phillips/i);
  });

  it('ingestion blocks commit when material ambiguity detected', async () => {
    const store = seedStore();
    const memory = new ConversationMemory();
    await ingestOperationalUpdate({
      clientId: 1,
      text: 'Talked to Dave at Exeter Phillips.',
      store,
      memory,
    });
    await ingestOperationalUpdate({
      clientId: 1,
      text: 'Also spoke with Mike at Exeter Phillips.',
      store,
      memory,
    });
    const blocked = await ingestOperationalUpdate({
      clientId: 1,
      text: 'He said call Thursday.',
      store,
      memory,
    });
    assert.equal(blocked.commit_blocked, true);
    assert.ok(blocked.clarification_required);
  });

  it('multi-account ingestion creates distinct prospect updates', async () => {
    const store = seedStore();
    const msg =
      'Talked to Dave at Exeter Phillips again. Their cleaner is still missing common areas. I also stopped at ABC Manufacturing but the facilities guy is out until Thursday.';
    const result = await ingestOperationalUpdate({ clientId: 1, text: msg, store });
    assert.ok(result.prospects.length >= 1);
    assert.ok(result.situation_model);
    assert.ok(result.understanding_preview);
  });
});
