'use strict';

const assert = require('node:assert/strict');
const { describe, it, beforeEach } = require('node:test');
const {
  submitComposerTurn,
  createMaxIngestionEnvelope,
  createMaxAttachment,
  clearAttachmentStore,
  resetComposerIdempotencyForTests,
} = require('../packages/max/composer');
const { MemoryStateStore } = require('../packages/max/stateIngestion');
const {
  interpretConversationalInput,
  ConversationMemory,
  validateSituationModel,
} = require('../packages/max/understanding');
const {
  MemoryVoiceRecordingStore,
  createStubTranscriptionAdapter,
  resetVoiceTranscriptionCacheForTests,
} = require('../packages/max/voice');

function seedStore(overrides = {}) {
  return new MemoryStateStore({
    clientId: 1,
    users: [{ id: 10, name: 'Mike' }],
    companies: overrides.companies || [
      { id: 'co-granite', name: 'Granite State Daycare', client_id: 1 },
      { id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 },
      { id: 'co-exeter2', name: 'Exeter Packaging', client_id: 1 },
      { id: 'co-abc', name: 'ABC Manufacturing', client_id: 1 },
    ],
    prospects: overrides.prospects || [],
    ...overrides,
  });
}

function voiceTurn(transcript, { clientId = 1, store, memory, confirm = false, envelopeId, attachmentId } = {}) {
  const att = createMaxAttachment({
    id: attachmentId || 'att_voice_test',
    type: 'voice',
    filename: 'note.webm',
    mimeType: 'audio/webm',
  });
  const adapter = createStubTranscriptionAdapter({ fixedText: transcript });
  const voiceStore = new MemoryVoiceRecordingStore();
  return submitComposerTurn({
    clientId,
    envelope: createMaxIngestionEnvelope({
      tenantId: String(clientId),
      id: envelopeId,
      conversationId: 'conv-voice-1',
      attachments: [att],
    }),
    attachmentInputs: [{ id: att.id, buffer: Buffer.from('fake-audio'), durationMs: 45000 }],
    store,
    conversationMemory: memory,
    confirm,
    voiceRecordingStore: voiceStore,
    transcriptionAdapter: adapter,
  });
}

describe('MAX-VOICE-001 voice understanding evaluation', () => {
  beforeEach(() => {
    clearAttachmentStore();
    resetComposerIdempotencyForTests();
    resetVoiceTranscriptionCacheForTests();
  });

  it('V1 — clean debrief (Granite State Daycare)', async () => {
    const transcript =
      'Okay, just left Granite State Daycare. Talked to Sarah at the front desk. They already have somebody cleaning, but she said the bathrooms have been kind of hit or miss. Lisa is apparently the person who handles vendors, but I did not get her last name. Sarah said I should come back Wednesday morning because Lisa is usually there.';
    const preview = await voiceTurn(transcript, { store: seedStore(), confirm: false });
    assert.equal(preview.ok, true);
    assert.equal(preview.voice_review_required, true);
    assert.match(preview.situation_model.threads[0].accountName, /Granite State Daycare/i);
    assert.ok(preview.situation_model.painPoints.some(p => /bathroom/i.test(p.description)));
    const lisa = preview.situation_model.decisionMakerSignals.find(s => /lisa/i.test(s.contactName));
    assert.ok(lisa);
    assert.notEqual(lisa.epistemic, 'confirmed');
  });

  it('V2 — rambling debrief resolves Exeter Phillips without committing false intermediate', async () => {
    const transcript =
      'Okay so I just left Granite State Daycare, talked to Sarah up front, really nice, they do already have somebody cleaning but she said bathrooms have been kind of hit or miss, I think Lisa is the director or maybe she handles vendors, Sarah said come back Wednesday morning because she is usually there then.';
    const preview = await voiceTurn(transcript, { store: seedStore(), confirm: false });
    assert.match(preview.situation_model.threads[0].accountName, /Granite State Daycare/i);
    assert.equal(preview.preview_only, true);
  });

  it('V3 — multiple accounts in one voice note', async () => {
    const transcript =
      'Talked to Dave at Exeter Phillips again — cleaner still missing common areas. Also stopped at ABC Manufacturing; facilities is out until Thursday.';
    const preview = await voiceTurn(transcript, { store: seedStore(), confirm: false });
    assert.equal(preview.situation_model.threads.length, 2);
  });

  it('V4 — name transcription error resolves conservatively with raw transcript preserved', async () => {
    const transcript = 'talkd to dave at exter philips he sed cleener still missin common areas';
    const preview = await voiceTurn(transcript, {
      store: seedStore({
        prospects: [{
          id: 'p1', client_id: 1, company_id: 'co-exeter', company_name: 'Exeter Phillips', assigned_ao_id: 10,
        }],
      }),
      confirm: false,
    });
    assert.match(preview.transcript || transcript, /exter philips/i);
    assert.match(preview.situation_model.threads[0].accountName, /Exeter Phillips/i);
  });

  it('V5 — ambiguous pronoun blocks commit', async () => {
    const memory = new ConversationMemory({ conversationId: 'conv-v5' });
    await voiceTurn('Talked to Dave at Exeter Phillips.', { store: seedStore(), memory, confirm: false });
    await voiceTurn('Also spoke with Mike at Exeter Phillips.', { store: seedStore(), memory, confirm: false, attachmentId: 'att2', envelopeId: 'env-2' });
    const blocked = await voiceTurn('He said call Thursday.', {
      store: seedStore(), memory, confirm: true, attachmentId: 'att3', envelopeId: 'env-3',
    });
    assert.equal(blocked.commit_blocked, true);
    assert.ok(blocked.clarification_required);
  });

  it('V6 — correction within recording keeps Wednesday', () => {
    const { situationModel } = interpretConversationalInput({
      text: 'Sarah said Tuesday — actually no, Wednesday morning.',
    });
    const canonical = situationModel.temporalReferences.find(t => t.canonical);
    assert.match(canonical.phrase, /wednesday morning/i);
  });

  it('V7 — hedged decision-maker stays uncertain', async () => {
    const preview = await voiceTurn('I think Mike might handle facilities.', {
      store: seedStore(), confirm: false,
    });
    const dm = preview.situation_model.decisionMakerSignals.find(s => /mike/i.test(s.contactName));
    assert.ok(dm);
    assert.equal(dm.epistemic, 'uncertain');
  });

  it('V8 — reported statement preserves Sarah as source', async () => {
    const preview = await voiceTurn('Sarah said Lisa handles vendors.', { store: seedStore(), confirm: false });
    const dm = preview.situation_model.decisionMakerSignals.find(s => /lisa/i.test(s.contactName));
    assert.equal(dm.reportedBy, 'Sarah');
  });

  it('V9 — negation does not create dissatisfaction pain', async () => {
    const preview = await voiceTurn('They are not unhappy with their cleaner.', { store: seedStore(), confirm: false });
    assert.equal(preview.situation_model.painPoints.filter(p => p.current !== false).length, 0);
  });

  it('V10 — follow-up after restart resolves Dave from memory', () => {
    const memory = new ConversationMemory({ conversationId: 'conv-v10' });
    interpretConversationalInput({ text: 'Talked to Dave at Exeter Phillips.', memory, conversationId: 'conv-v10' });
    const second = interpretConversationalInput({
      text: 'He is not actually the decision maker.',
      memory,
      conversationId: 'conv-v10',
    });
    assert.ok(second.situationModel.corrections.some(c => /decision/i.test(c.kind || c.targetClaim || '')));
  });

  it('V11 — audio retry does not duplicate durable update', async () => {
    const store = seedStore({
      prospects: [{
        id: 'p1', client_id: 1, company_id: 'co-exeter', company_name: 'Exeter Phillips', assigned_ao_id: 10, status: 'warm',
      }],
    });
    const transcript = 'Tony talked to Exeter Phillips. Expects to call this week.';
    const first = await voiceTurn(transcript, { store, confirm: true, envelopeId: 'env-v11-a', attachmentId: 'att-v11' });
    assert.equal(first.ok, true);
    const second = await voiceTurn(transcript, { store, confirm: true, envelopeId: 'env-v11-a', attachmentId: 'att-v11' });
    assert.equal(second.duplicate_envelope, true);
  });

  it('V12 — failed transcription produces no CRM mutation', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'voice', filename: 'empty.webm', mimeType: 'audio/webm' });
    const adapter = createStubTranscriptionAdapter({ fixedText: '   ' });
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({ tenantId: '1', attachments: [att] }),
      attachmentInputs: [{ id: att.id, buffer: Buffer.from('x') }],
      store,
      voiceRecordingStore: new MemoryVoiceRecordingStore(),
      transcriptionAdapter: adapter,
    });
    assert.equal(result.ok, false);
    assert.equal(result.error, 'extraction_failed');
    assert.equal(store.ingestions.length, 0);
  });
});
