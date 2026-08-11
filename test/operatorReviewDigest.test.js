'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  DIGEST_SECTION_KEYS,
  buildOperatorReviewDigest,
  formatOperatorReviewDigestMessage,
  formatOperatorReviewEvidenceMessage,
  formatOperatorReviewArtifactMessage,
  splitDigestAndEvidence,
  EVIDENCE_COLLAPSED_NOTE,
  VIEW_EVIDENCE_LABEL,
} = require('../services/operatorReviewDigest');
const {
  renderOperatorReviewDigest,
  analyzeOperatorReviewHtml,
} = require('../public/shared/operator-review-digest');

describe('Operator Review Digest — shared pattern', () => {
  it('builds digest with required sections and collapsed evidence', () => {
    const digest = buildOperatorReviewDigest({
      kind: 'example_review',
      title: 'Example Review',
      recommendedDecision: 'Approve the proposed batch.',
      whyRecommended: ['Strong fit', 'Clear evidence'],
      included: ['Acme', 'Beta'],
      excluded: ['Held back: Gamma', 'Rejected: Delta'],
      keyWatchouts: ['Verify Gamma later'],
      nextStepAfterApproval: 'Outreach Strategy Preview',
      primaryActions: ['Approve', 'Request changes'],
      evidence: {
        records: [
          {
            companyName: 'Acme',
            location: 'Bedford NH',
            sourceUrl: 'https://acme.example',
            fitRationale: 'Primary-town PM',
            risks: 'Public-source only',
            confidence: 'high',
            reviewStatus: 'accepted',
          },
        ],
        auditNotes: ['review-only'],
      },
      disclaimer: 'Review only — no sends.',
    });

    assert.equal(digest.pattern, 'operator_review_digest');
    assert.equal(digest.evidenceCollapsedByDefault, true);
    assert.equal(digest.evidence.collapsedByDefault, true);
    assert.equal(digest.viewEvidenceLabel, VIEW_EVIDENCE_LABEL);
    assert.equal(digest.outreachCopyGenerated, false);
    assert.equal(digest.sendsMade, false);
    assert.equal(digest.crmWritesMade, false);
    DIGEST_SECTION_KEYS.forEach((key) => {
      assert.ok(key in digest, `missing digest key ${key}`);
    });
  });

  it('formats digest before evidence and keeps evidence optional', () => {
    const digest = buildOperatorReviewDigest({
      title: 'Example Review',
      recommendedDecision: 'Approve 2 cold prospects as Batch 1.',
      whyRecommended: ['Ready for Batch 1'],
      included: ['Acme', 'Beta'],
      excluded: ['Held back: Gamma'],
      keyWatchouts: ['No outreach yet'],
      nextStepAfterApproval: 'Outreach Strategy Preview',
      primaryActions: [
        { id: 'approve_batch_1', label: 'Approve Batch 1', style: 'primary' },
      ],
      evidence: {
        sections: [
          {
            title: 'Accepted',
            records: [
              {
                companyName: 'Acme',
                sourceUrl: 'https://acme.example',
                fitRationale: 'fit',
                risks: 'none',
                confidence: 'high',
              },
            ],
          },
        ],
      },
    });

    const defaultMsg = formatOperatorReviewArtifactMessage(digest);
    const split = splitDigestAndEvidence(defaultMsg);
    assert.match(split.digest, /## Recommended decision/);
    assert.ok(split.digest.includes(EVIDENCE_COLLAPSED_NOTE));
    assert.equal(split.evidence, '');
    assert.doesNotMatch(defaultMsg, /Source URL:/);

    const withEvidence = formatOperatorReviewArtifactMessage(digest, {
      includeEvidence: true,
    });
    assert.ok(
      withEvidence.indexOf('## Recommended decision') <
        withEvidence.indexOf('## View evidence')
    );
    assert.match(withEvidence, /Source URL:/);

    const evidenceOnly = formatOperatorReviewEvidenceMessage(digest.evidence);
    assert.match(evidenceOnly, /View evidence/);
    assert.match(evidenceOnly, /Acme/);
  });

  it('HTML renderer places actions before collapsed evidence drawer', () => {
    const digest = buildOperatorReviewDigest({
      title: 'Example Review',
      recommendedDecision: 'Approve now.',
      included: ['Acme'],
      excluded: ['Rejected: Delta'],
      nextStepAfterApproval: 'Next step',
      primaryActions: [
        { id: 'approve_batch_1', label: 'Approve Batch 1', style: 'primary' },
        { id: 'view_evidence', label: 'View evidence', style: 'secondary' },
      ],
      evidence: {
        records: [{ companyName: 'Acme', sourceUrl: 'https://acme.example' }],
      },
    });
    const html = renderOperatorReviewDigest(digest);
    const analysis = analyzeOperatorReviewHtml(html);
    assert.equal(analysis.digestBeforeEvidence, true);
    assert.equal(analysis.actionsBeforeEvidence, true);
    assert.equal(analysis.evidenceCollapsedByDefault, true);
    assert.match(html, /data-ord-action="approve_batch_1"/);
  });

  it('formatOperatorReviewDigestMessage includes all digest headings', () => {
    const digest = buildOperatorReviewDigest({
      title: 'T',
      recommendedDecision: 'Approve',
      whyRecommended: ['Because'],
      included: ['A'],
      excluded: ['B'],
      keyWatchouts: ['C'],
      nextStepAfterApproval: 'D',
      primaryActions: ['Approve'],
    });
    const msg = formatOperatorReviewDigestMessage(digest);
    assert.match(msg, /## Recommended decision/);
    assert.match(msg, /## Why this is recommended/);
    assert.match(msg, /## What is included/);
    assert.match(msg, /## What is excluded \/ held back/);
    assert.match(msg, /## Key watchouts/);
    assert.match(msg, /## Next step after approval/);
    assert.match(msg, /## Primary actions/);
  });
});
