'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  resolveOutreachAssetMessage,
  isStakeholderValidated,
} = require('../packages/acquisition-knowledge/resolveOutreachAssetMessage');

/** Babrun production outreach_asset shape (structure only — no live Kaylee copy). */
function babrunOutreachAsset(overrides = {}) {
  return {
    id: 'ak_babrun_outreach_final_05',
    object_type: 'outreach_asset',
    title: 'Final first-ten outreach 5: Example Painting Co',
    channel: 'email',
    lifecycle_state: 'STAKEHOLDER_VALIDATED',
    validation_state: 'STAKEHOLDER_VALIDATED',
    status: 'approved',
    version: 3,
    updated_at: '2026-09-12T08:15:00.000Z',
    content: {
      company: 'Example Painting Co',
      contact: 'Owner One & Owner Two',
      prospectCode: 'P024',
      role: 'Owners',
      subject: 'Quick question about EXAMPLE',
      statement: 'Hi Owner One and Owner Two,\n\nRegression fixture body — not production copy.',
      ...(overrides.content || {}),
    },
    relationships: [
      { type: 'targets_prospect', target: { id: 'ak_babrun_prospect_p024' } },
    ],
    provenance: { importSource: 'babrun-ak.json' },
    ...overrides,
  };
}

describe('resolveOutreachAssetMessage', () => {
  it('resolves Babrun subject + statement body without mutation', () => {
    const asset = babrunOutreachAsset();
    const resolved = resolveOutreachAssetMessage(asset, { requireStakeholderValidated: true });
    assert.equal(resolved.assetId, 'ak_babrun_outreach_final_05');
    assert.equal(resolved.subject, 'Quick question about EXAMPLE');
    assert.equal(resolved.body, asset.content.statement);
    assert.equal(resolved.channel, 'email');
    assert.equal(resolved.revision, '2026-09-12T08:15:00.000Z');
    assert.equal(resolved.version, 3);
    assert.equal(resolved.source, 'content');
  });

  it('resolves Paige-style variants', () => {
    const resolved = resolveOutreachAssetMessage({
      id: 'ak_variant_asset',
      status: 'approved',
      channel: 'email',
      content: {
        variants: [{
          label: 'Primary',
          subject: 'Variant subject',
          body: 'Variant body',
        }],
      },
    }, { requireStakeholderValidated: true });
    assert.equal(resolved.subject, 'Variant subject');
    assert.equal(resolved.body, 'Variant body');
    assert.equal(resolved.source, 'variants');
  });

  it('resolves nested messaging.copy blocks', () => {
    const resolved = resolveOutreachAssetMessage({
      id: 'ak_nested_asset',
      lifecycle_state: 'STAKEHOLDER_VALIDATED',
      content: {
        messaging: {
          email_subject: 'Nested subject',
          email_body: 'Nested body',
        },
      },
    }, { requireStakeholderValidated: true });
    assert.equal(resolved.subject, 'Nested subject');
    assert.equal(resolved.body, 'Nested body');
  });

  it('fails closed when executable copy is missing', () => {
    assert.throws(
      () => resolveOutreachAssetMessage({
        id: 'ak_missing_copy',
        status: 'approved',
        content: { company: 'No copy here' },
      }),
      (err) => err.code === 'outreach_asset_copy_missing'
    );
  });

  it('fails closed when stakeholder validation is required but absent', () => {
    assert.throws(
      () => resolveOutreachAssetMessage(babrunOutreachAsset({
        lifecycle_state: 'HYPOTHESIS',
        validation_state: 'UNVALIDATED',
        status: 'draft',
      }), { requireStakeholderValidated: true }),
      (err) => err.code === 'outreach_asset_not_validated'
    );
  });

  it('preserves stakeholder validation detection', () => {
    assert.equal(isStakeholderValidated(babrunOutreachAsset()), true);
    assert.equal(isStakeholderValidated(babrunOutreachAsset({
      lifecycle_state: 'HYPOTHESIS',
      validation_state: 'UNVALIDATED',
      status: 'draft',
    })), false);
  });

  it('freezes asset identity and revision for authorization snapshots', () => {
    const asset = babrunOutreachAsset();
    const resolved = resolveOutreachAssetMessage(asset);
    assert.equal(resolved.assetId, asset.id);
    assert.equal(resolved.revision, new Date(asset.updated_at).toISOString());
    assert.notEqual(resolved.body, 'mutated');
  });
});
