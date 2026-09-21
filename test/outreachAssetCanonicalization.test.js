'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  parseOutreachSourceText,
  tryParseOutreachSourceText,
} = require('../packages/acquisition-knowledge/parseOutreachSourceText');
const {
  canonicalizeOutreachAssetContent,
  auditOutreachAssetContent,
} = require('../packages/acquisition-knowledge/canonicalizeOutreachAssetContent');
const {
  normalizeKnowledgeObject,
  OBJECT_TYPES,
  LIFECYCLE_STATES,
  VALIDATION_STATES,
  EVIDENCE_TYPES,
  resolveOutreachAssetMessage,
  isStakeholderValidated,
} = require('../packages/acquisition-knowledge');

const KAYLEE_SOURCE_TEXT = `**5. Braiden & Kaylee Smith — KB Painting**

**Subject: Quick question about KB**

Hi Braiden and Kaylee,

I came across KB and noticed you've built a team while still remaining personally involved in both the field and operational sides of the business.

Do you find that too much of the company still depends on the two of you personally?

If KB continues expanding, what happens if those responsibilities continue growing with it?

I help small-business owners build teams that can carry more of the business without everything depending on the owners. Would you be open to a short conversation?

Fedir
---
`;

const KAYLEE_STATEMENT = `Hi Braiden and Kaylee,

I came across KB and noticed you've built a team while still remaining personally involved in both the field and operational sides of the business.

Do you find that too much of the company still depends on the two of you personally?

If KB continues expanding, what happens if those responsibilities continue growing with it?

I help small-business owners build teams that can carry more of the business without everything depending on the owners. Would you be open to a short conversation?

Fedir`;

function kayleeContentOnlySourceText() {
  return {
    version: 'Final revised first ten',
    category: 'Outreach Assets',
    sourceText: KAYLEE_SOURCE_TEXT,
    prospectCompany: 'KB Painting',
    prospectContact: 'Braiden & Kaylee Smith',
    approvedPackageSource: 'S09',
  };
}

function kayleeAsset(overrides = {}) {
  return {
    id: 'ak_babrun_outreach_final_05',
    object_type: 'outreach_asset',
    title: 'Final first-ten outreach 5: KB Painting',
    channel: 'email',
    lifecycle_state: 'STAKEHOLDER_VALIDATED',
    validation_state: 'STAKEHOLDER_VALIDATED',
    status: 'approved',
    version: 3,
    updated_at: '2026-09-12T08:15:00.000Z',
    content: kayleeContentOnlySourceText(),
    relationships: [
      { type: 'targets_prospect', target: { id: 'ak_babrun_prospect_p024' } },
    ],
    provenance: { importSource: 'babrun-ak.json' },
    evidence: [{
      id: 'evidence_1',
      type: EVIDENCE_TYPES.STAKEHOLDER,
      statement: 'Approved in S09 package.',
      source: { kind: 'stakeholder', ref: 'babrun-s09' },
      confidence: 1,
      observedAt: '2026-09-10T12:00:00.000Z',
    }],
    ...overrides,
  };
}

function operatorImportInput(contentOverrides = {}) {
  return {
    tenantId: '13',
    objectType: OBJECT_TYPES.OUTREACH_ASSET,
    title: 'Imported outreach asset',
    state: LIFECYCLE_STATES.STAKEHOLDER_VALIDATED,
    validationState: VALIDATION_STATES.STAKEHOLDER_VALIDATED,
    status: 'approved',
    channel: 'email',
    content: {
      ...kayleeContentOnlySourceText(),
      ...contentOverrides,
    },
    evidence: [{
      id: 'evidence_1',
      type: EVIDENCE_TYPES.STAKEHOLDER,
      statement: 'Approved package import.',
      source: { kind: 'stakeholder', ref: 'babrun-import' },
      confidence: 1,
      observedAt: '2026-09-10T12:00:00.000Z',
    }],
  };
}

describe('parseOutreachSourceText', () => {
  it('extracts Kaylee subject and body exactly', () => {
    const parsed = parseOutreachSourceText(KAYLEE_SOURCE_TEXT);
    assert.equal(parsed.subject, 'Quick question about KB');
    assert.equal(parsed.statement, KAYLEE_STATEMENT);
  });

  it('fails closed when subject line is missing', () => {
    assert.throws(
      () => parseOutreachSourceText('Hi there,\n\nNo subject here.\n---'),
      (err) => err.code === 'source_text_subject_missing'
    );
  });

  it('fails closed when multiple subject lines exist', () => {
    const ambiguous = `${KAYLEE_SOURCE_TEXT}\n**Subject: Second subject**\nBody`;
    assert.throws(
      () => parseOutreachSourceText(ambiguous),
      (err) => err.code === 'source_text_subject_ambiguous'
    );
  });
});

describe('canonicalizeOutreachAssetContent', () => {
  it('repairs Kaylee asset from sourceText without altering sourceText', () => {
    const before = kayleeContentOnlySourceText();
    const result = canonicalizeOutreachAssetContent(before);
    assert.equal(result.changed, true);
    assert.equal(result.content.sourceText, KAYLEE_SOURCE_TEXT);
    assert.equal(result.content.subject, 'Quick question about KB');
    assert.equal(result.content.statement, KAYLEE_STATEMENT);
    assert.equal(before.subject, undefined);
    assert.equal(before.statement, undefined);
  });

  it('is idempotent for already-structured assets', () => {
    const structured = {
      sourceText: KAYLEE_SOURCE_TEXT,
      subject: 'Quick question about KB',
      statement: KAYLEE_STATEMENT,
    };
    const first = canonicalizeOutreachAssetContent(structured);
    const second = canonicalizeOutreachAssetContent(first.content);
    assert.equal(first.skipped, true);
    assert.equal(first.changed, false);
    assert.equal(second.skipped, true);
    assert.equal(second.changed, false);
    assert.deepEqual(second.content, structured);
  });

  it('fails closed on partial structured copy', () => {
    assert.throws(
      () => canonicalizeOutreachAssetContent({ subject: 'Only subject', sourceText: KAYLEE_SOURCE_TEXT }),
      (err) => err.code === 'outreach_asset_copy_partial'
    );
  });

  it('audit marks Kaylee asset as needing repair before canonicalization', () => {
    const audit = auditOutreachAssetContent(kayleeContentOnlySourceText());
    assert.equal(audit.hasStructuredExecutableCopy, false);
    assert.equal(audit.hasSourceText, true);
    assert.equal(audit.sourceTextParseable, true);
    assert.equal(audit.needsRepair, true);
    assert.equal(audit.requiresManualReview, false);
  });
});

describe('resolveOutreachAssetMessage after canonicalization', () => {
  it('resolves Kaylee asset through structured canonical content', () => {
    const asset = kayleeAsset();
    const { content } = canonicalizeOutreachAssetContent(asset.content);
    const resolved = resolveOutreachAssetMessage({
      ...asset,
      content,
    }, { requireStakeholderValidated: true });

    assert.equal(resolved.subject, 'Quick question about KB');
    assert.equal(resolved.body, KAYLEE_STATEMENT);
    assert.equal(resolved.source, 'content');
    assert.equal(resolved.assetId, 'ak_babrun_outreach_final_05');
  });

  it('preserves stakeholder validation and prospect relationship metadata', () => {
    const asset = kayleeAsset();
    const { content } = canonicalizeOutreachAssetContent(asset.content);
    const normalized = { ...asset, content };
    assert.equal(isStakeholderValidated(normalized), true);
    assert.equal(normalized.relationships[0].target.id, 'ak_babrun_prospect_p024');
    assert.equal(normalized.lifecycle_state, 'STAKEHOLDER_VALIDATED');
    assert.equal(normalized.validation_state, 'STAKEHOLDER_VALIDATED');
  });
});

describe('normalizeKnowledgeObject importer enrichment', () => {
  it('creates structured subject/statement for finalized email assets at ingestion', () => {
    const normalized = normalizeKnowledgeObject(operatorImportInput(), { actorRole: 'operator' });
    assert.equal(normalized.content.subject, 'Quick question about KB');
    assert.equal(normalized.content.statement, KAYLEE_STATEMENT);
    assert.equal(normalized.content.sourceText, KAYLEE_SOURCE_TEXT);
  });

  it('leaves ambiguous sourceText unchanged on import', () => {
    const normalized = normalizeKnowledgeObject(operatorImportInput({
      sourceText: 'No explicit subject markup here.',
    }), { actorRole: 'operator' });
    assert.equal(normalized.content.subject, undefined);
    assert.equal(normalized.content.statement, undefined);
  });

  it('does not overwrite existing structured fields on import', () => {
    const normalized = normalizeKnowledgeObject(operatorImportInput({
      subject: 'Existing subject',
      statement: 'Existing body',
    }), { actorRole: 'operator' });
    assert.equal(normalized.content.subject, 'Existing subject');
    assert.equal(normalized.content.statement, 'Existing body');
  });
});

describe('tryParseOutreachSourceText', () => {
  it('returns ok:false for ambiguous input without throwing', () => {
    const result = tryParseOutreachSourceText('**Subject: One**\n\nA\n\n**Subject: Two**\n\nB');
    assert.equal(result.ok, false);
    assert.equal(result.code, 'source_text_subject_ambiguous');
  });
});
