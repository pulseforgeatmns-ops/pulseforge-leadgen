'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');
const {
  interpretConversationalInput,
  ConversationMemory,
  validateSituationModel,
  formatUnderstandingPreview,
  deriveRecommendedNextActions,
  buildUnderstandingDiagnostics,
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

  it('M1 — typo-heavy input resolves Exeter Phillips with evidence', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'talkd to dave at exter philips he sed cleener still missin common areas',
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    assert.match(situationModel.threads[0].accountName, /Exeter Phillips/i);
    assert.ok(situationModel.evidence?.length || situationModel.threads[0].evidence?.length);
    assert.ok(situationModel.painPoints.some(p => /common areas/i.test(p.description)));
  });

  it('M2 — similar account names block shorthand Exeter reference', () => {
    const { validation } = interpretConversationalInput({
      text: 'Exeter said call Thursday.',
      contextAccounts: ['Exeter Phillips', 'Exeter Packaging'],
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    assert.equal(validation.blockCommit, true);
    assert.match(validation.narrowestClarification, /Exeter Phillips|Exeter Packaging/);
  });

  it('M3 — Granite shorthand resolves when context is unique', () => {
    const memory = new ConversationMemory();
    interpretConversationalInput({
      text: 'Granite State Daycare Center',
      memory,
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    const second = interpretConversationalInput({
      text: 'Granite said Lisa will be there tomorrow.',
      memory,
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    assert.match(second.situationModel.threads[0].accountName, /Granite State Daycare/i);
  });

  it('M4 — multiple temporal corrections keep Wednesday morning canonical', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'Sarah said Tuesday—actually Thursday. No, sorry, Wednesday morning.',
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    const canonical = situationModel.temporalReferences.find(t => t.canonical);
    assert.ok(canonical);
    assert.match(canonical.phrase, /wednesday morning/i);
    assert.ok(situationModel.corrections.length >= 2);
    assert.ok(situationModel.temporalReferences.some(t => t.superseded));
  });

  it('M5 — partial irrelevance keeps commentary out of CRM pain claims', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'Stopped at ABC Manufacturing. Nice lobby, traffic sucked getting there. Facilities guy is out until Thursday.',
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    assert.ok(situationModel.commitments.some(c => c.kind === 'availability'));
    assert.ok(situationModel.commentary.some(c => /lobby|traffic/i.test(c.text)));
    const claims = situationModel.threads[0].ingestionClaims || [];
    assert.equal(claims.filter(c => c.claim_type === 'PAIN_SIGNAL').length, 0);
  });

  it('M6 — negation traps for satisfaction and resolved bathroom pain', () => {
    const negated = interpretConversationalInput({ text: 'They\'re not unhappy with the cleaner.' });
    assert.equal(negated.situationModel.painPoints.filter(p => p.current !== false).length, 0);

    const resolved = interpretConversationalInput({
      text: 'They don\'t have any issue with bathrooms anymore.',
    });
    const pain = resolved.situationModel.painPoints.find(p => /bathroom/i.test(p.description));
    assert.ok(pain);
    assert.equal(pain.current, false);
  });

  it('M7 — hedged facilities role stays uncertain', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'I\'m pretty sure Mike handles facilities but I didn\'t confirm it.',
    });
    const dm = situationModel.decisionMakerSignals.find(s => /mike/i.test(s.contactName));
    assert.equal(dm.epistemic, EPISTEMIC_CATEGORY.UNCERTAIN);
    const durableDmClaims = (situationModel.threads[0].ingestionClaims || [])
      .filter(c => c.payload?.decision_maker);
    assert.equal(durableDmClaims.length, 0);
  });

  it('M8 — reported uncertainty keeps Lisa vendor role uncertain', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'Sarah thinks Lisa might be the person who handles vendors.',
    });
    const dm = situationModel.decisionMakerSignals.find(s => /lisa/i.test(s.contactName));
    assert.equal(dm.reportedBy, 'Sarah');
    assert.equal(dm.epistemic, EPISTEMIC_CATEGORY.UNCERTAIN);
  });

  it('M9 — mixed entity correction reassigns Mike at Exeter Packaging', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'Dave at Exeter Phillips said Friday. Actually, that was Mike at Exeter Packaging.',
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    const corr = situationModel.corrections.find(c => c.kind === 'entity_reassignment');
    assert.ok(corr);
    assert.match(corr.newValue, /Mike.*Exeter Packaging/i);
  });

  it('M10 — stale pronoun context blocks commit', () => {
    const memory = new ConversationMemory();
    interpretConversationalInput({ text: 'Talked to Dave at Exeter Phillips.', memory });
    interpretConversationalInput({ text: 'Stopped at ABC Manufacturing for a quick visit.', memory });
    interpretConversationalInput({ text: 'Also checked in at Granite State Daycare.', memory });
    const third = interpretConversationalInput({ text: 'He said call Thursday.', memory });
    assert.equal(third.validation.blockCommit, true);
  });

  it('M11 — compound update question and conditional follow-up', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'They still have bathroom issues. Dave said Friday. Should I wait? Also put a follow-up on Monday if I don\'t hear back.',
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    assert.ok(situationModel.painPoints.some(p => /bathroom/i.test(p.description)));
    assert.ok(situationModel.commitments.length >= 1);
    assert.ok(situationModel.questions.some(q => /should i wait/i.test(q.text)));
    const followUp = situationModel.requestedActions.find(a => a.action === 'schedule_follow_up');
    assert.ok(followUp);
    assert.equal(followUp.conditional, true);
  });

  it('M12 — contradiction reassesses decision-maker authority', () => {
    const first = interpretConversationalInput({ text: 'Lisa is the decision maker.' });
    const second = interpretConversationalInput({
      text: 'Sarah said Lisa actually doesn\'t handle vendors.',
      memory: first.memory,
    });
    assert.ok(second.situationModel.corrections.some(c => c.contactName === 'Lisa'));
  });

  it('M13 — inference does not become confirmed dissatisfaction fact', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'Sounds like they\'re probably unhappy.',
    });
    assert.equal(situationModel.painPoints.filter(p => p.current !== false).length, 0);
    const painClaims = (situationModel.threads[0].ingestionClaims || [])
      .filter(c => c.claim_type === 'PAIN_SIGNAL');
    assert.equal(painClaims.length, 0);
  });

  it('M14 — diagnostics derive from canonical situation model', () => {
    const { situationModel, diagnostics } = interpretConversationalInput({
      text: 'Talked to Dave at Exeter Phillips. Lisa said Tuesday — actually, sorry, Thursday.',
      now: new Date('2026-10-05T15:00:00.000Z'),
    });
    const derived = buildUnderstandingDiagnostics(situationModel);
    assert.deepEqual(diagnostics, derived);
    assert.equal(typeof derived.threadCount, 'number');
    assert.equal(typeof derived.blocked, 'boolean');
  });

  it('M15 — corrections supersede follow-up recommendations', () => {
    const first = interpretConversationalInput({ text: 'Dave is the decision maker.' });
    assert.ok(first.situationModel.recommendedNextActions.some(a => a.targetContact === 'Dave'));
    const second = interpretConversationalInput({
      text: 'Actually Dave isn\'t the decision maker. Lisa handles vendors.',
      memory: first.memory,
    });
    const actions = deriveRecommendedNextActions(second.situationModel);
    assert.ok(!actions.some(a => a.status === 'active' && a.targetContact === 'Dave'));
    assert.ok(actions.some(a => a.status === 'active' && a.targetContact === 'Lisa'));
  });
});
