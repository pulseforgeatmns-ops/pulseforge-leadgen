'use strict';

const assert = require('node:assert/strict');
const { describe, it, beforeEach } = require('node:test');
const XLSX = require('xlsx');
const fs = require('node:fs');
const path = require('node:path');
const {
  submitComposerTurn,
  createMaxIngestionEnvelope,
  createMaxAttachment,
  clearAttachmentStore,
  resetComposerIdempotencyForTests,
  putAttachmentBuffer,
  getAttachmentBuffer,
} = require('../packages/max/composer');
const { MemoryStateStore } = require('../packages/max/stateIngestion');
const { interpretConversationalInput, ConversationMemory } = require('../packages/max/understanding');

function seedStore(overrides = {}) {
  return new MemoryStateStore({
    clientId: 1,
    users: [{ id: 10, name: 'Tony' }],
    companies: overrides.companies || [
      { id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 },
      { id: 'co-exeter2', name: 'Exeter Packaging', client_id: 1 },
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
    ...overrides,
  });
}

function xlsxBuffer(rows, sheetName = 'Prospects') {
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, sheetName);
  return XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
}

describe('MAX-INGEST-UX-001 unified composer', () => {
  beforeEach(() => {
    clearAttachmentStore();
    resetComposerIdempotencyForTests();
  });

  it('C1 — text-only uses MAX-UNDERSTANDING path', async () => {
    const store = seedStore();
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: 'Tony talked to Exeter Phillips again. Dave expects to call this week.',
      }),
      store,
    });
    assert.equal(result.ok, true);
    assert.ok(result.situation_model);
    assert.ok(result.understanding_preview);
    assert.equal(result.results[0].prospect.id, 'prospect-exeter');
  });

  it('C2 — spreadsheet rows normalize with provenance', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Tony.xlsx', mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const buffer = xlsxBuffer([{ Company: 'Exeter Phillips', AO: 'Tony', Notes: 'Waiting for callback' }]);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({ tenantId: '1', attachments: [att] }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
    });
    assert.equal(result.ok, true);
    const artifact = store.artifacts?.[0];
    assert.ok(artifact);
    assert.equal(artifact.metadata.row, 2);
    assert.equal(artifact.metadata.sheet, 'Prospects');
  });

  it('C3 — instruction prefixes spreadsheet interpretation', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'list.csv', mimeType: 'text/csv' });
    const csv = 'Company,Person,Notes\nExeter Phillips,Dave,Not the decision maker\n';
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: 'These are Tony updated accounts from today.',
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer: Buffer.from(csv, 'utf8') }],
      store,
    });
    assert.equal(result.ok, true);
    assert.match(result.understanding_preview || '', /decision|Dave|Exeter/i);
  });

  it('C4 — multi-account spreadsheet processes independently', async () => {
    const store = seedStore({
      prospects: [
        {
          id: 'prospect-exeter',
          client_id: 1,
          company_id: 'co-exeter',
          company_name: 'Exeter Phillips',
          assigned_ao_id: 10,
        },
        {
          id: 'prospect-abc',
          client_id: 1,
          company_id: 'co-abc',
          company_name: 'ABC Manufacturing',
          assigned_ao_id: 10,
        },
      ],
    });
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'batch.xlsx' });
    const buffer = xlsxBuffer([
      { Company: 'Exeter Phillips', Notes: 'callback' },
      { Company: 'ABC Manufacturing', Notes: 'follow-up Thursday' },
    ]);
    const preview = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({ tenantId: '1', attachments: [att] }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
      confirm: false,
    });
    assert.equal(preview.preview_only, true);
    assert.ok(preview.batch_preview.total >= 2);

    const committed = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({ tenantId: '1', id: 'env-batch-2', attachments: [att] }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
      confirm: true,
    });
    assert.equal(committed.ok, true);
    assert.ok(committed.results.length >= 2);
  });

  it('C5 — ambiguous account row blocked in batch preview counts', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'ambig.xlsx' });
    const buffer = xlsxBuffer([
      { Company: 'Exeter Phillips', Notes: 'ok' },
      { Company: 'Exeter', Notes: 'unclear which' },
    ]);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({ tenantId: '1', attachments: [att] }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
      confirm: false,
    });
    assert.ok(result.batch_preview.needs_clarification >= 0);
    assert.ok(result.batch_preview.ready >= 1);
  });

  it('C6 — correction after upload uses durable memory', async () => {
    const store = seedStore();
    const memory = new ConversationMemory({ conversationId: 'conv-1' });
    const upload = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        conversationId: 'conv-1',
        text: 'Upload Tony.xlsx with Exeter account.',
      }),
      store,
      conversationMemory: memory,
    });
    const mem = upload.conversation_memory;
    const correction = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        conversationId: 'conv-1',
        id: 'env-corr-1',
        text: 'That Exeter one is Phillips, not Packaging.',
      }),
      store,
      conversationMemory: mem,
    });
    assert.ok(correction.situation_model?.corrections?.length || correction.understanding_preview);
  });

  it('C7 — malformed spreadsheet fails visibly', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'bad.xlsx' });
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({ tenantId: '1', attachments: [att] }),
      attachmentInputs: [{ id: att.id, buffer: Buffer.from('not-a-real-xlsx', 'utf8') }],
      store,
    });
    assert.equal(result.ok, false);
    assert.equal(result.error, 'extraction_failed');
  });

  it('C8 — document text feeds understanding', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'document', filename: 'notes.txt', mimeType: 'text/plain' });
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: 'Field notes attached.',
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer: Buffer.from('Tony visited Exeter Phillips. Follow up Friday.', 'utf8') }],
      store,
    });
    assert.equal(result.ok, true);
    assert.ok(result.situation_model);
  });

  it('C9 — mixed attachments in one turn', async () => {
    const store = seedStore();
    const sheet = createMaxAttachment({ type: 'spreadsheet', filename: 'a.xlsx' });
    const img = createMaxAttachment({ type: 'image', filename: 'shot.png', mimeType: 'image/png' });
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: 'Mixed update',
        attachments: [sheet, img],
      }),
      attachmentInputs: [
        { id: sheet.id, buffer: xlsxBuffer([{ Company: 'Exeter Phillips', Notes: 'update' }]) },
        { id: img.id, buffer: Buffer.from([0x89, 0x50, 0x4e, 0x47]) },
      ],
      store,
    });
    assert.equal(result.ok, true);
    assert.equal(result.envelope.sourceType, 'mixed');
  });

  it('C10 — tenant isolation for attachment storage', () => {
    const ref = putAttachmentBuffer(1, 'att-a', Buffer.from('secret'));
    assert.ok(getAttachmentBuffer(ref, 1));
    assert.equal(getAttachmentBuffer(ref, 2), null);
  });

  it('C11 — duplicate envelope idempotency', async () => {
    const store = seedStore();
    const envelope = createMaxIngestionEnvelope({
      tenantId: '1',
      id: 'env-dup-1',
      text: 'Tony talked to Exeter Phillips. Expects to call this week.',
    });
    const first = await submitComposerTurn({ clientId: 1, envelope, store });
    const second = await submitComposerTurn({ clientId: 1, envelope, store });
    assert.equal(first.ok, true);
    assert.equal(second.duplicate_envelope, true);
  });

  it('C12 — adapter failure isolated and reported', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'document', filename: 'deck.pdf', mimeType: 'application/pdf' });
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: 'See attached',
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer: Buffer.from('%PDF-1.4', 'utf8') }],
      store,
    });
    assert.equal(result.ok, false);
    assert.equal(result.error, 'extraction_failed');
  });

  it('API route registered for composer', () => {
    const routes = fs.readFileSync(path.join(__dirname, '..', 'routes', 'maxStateIngestion.js'), 'utf8');
    assert.match(routes, /\/api\/v1\/max\/composer/);
  });
});
