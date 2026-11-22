'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeContact,
  previewReviewOnlyContacts,
  noteFor,
} = require('../services/reviewOnlyProspectImportService');

test('normalizes review-only prospect identity and safety state', () => {
  const contact = normalizeContact({
    name: 'David Heyburn',
    company: '321 Properties Group',
    phone: '502-338-0796',
    email: 'DAVID@example.com',
    status: 'Contact now',
    market: 'Kansas City, MO',
    linkedin: 'linkedin.com/in/davidheyburn',
  }, 0);
  assert.equal(contact.firstName, 'David');
  assert.equal(contact.lastName, 'Heyburn');
  assert.equal(contact.phone, '(502) 338-0796');
  assert.equal(contact.email, 'david@example.com');
  assert.equal(contact.reviewState, 'contact_now');
  assert.equal(contact.status, 'cold');
  assert.equal(contact.icpScore, 80);
  assert.equal(contact.linkedinUrl, 'https://linkedin.com/in/davidheyburn');
});

test('reject rows remain disqualified while retaining research evidence', () => {
  const contact = normalizeContact({
    name: 'Nicholas Reid',
    status: 'Reject',
    fit: 'Insufficient asset evidence',
  }, 0);
  assert.equal(contact.reviewState, 'reject');
  assert.equal(contact.status, 'disqualified');
  assert.match(noteFor(contact), /Automated outreach, calls, and campaign enrollment are not authorized/);
  assert.match(noteFor(contact), /Fit evidence: Insufficient asset evidence/);
});

test('rejects duplicates inside one import before any database write', () => {
  assert.throws(() => previewReviewOnlyContacts([
    { name: 'A One', email: 'same@example.com' },
    { name: 'B Two', email: 'SAME@example.com' },
  ]), /Duplicate contact/);
});

// P0-SCOUT-LEVI-001: structured evidenceDate / sourceType.
const { normalizeEvidenceDate, normalizeSourceType } = require('../services/reviewOnlyProspectImportService');

test('evidenceDate is a real, non-future calendar date distinct from import time', () => {
  const now = Date.UTC(2026, 10, 19, 12);
  assert.equal(normalizeEvidenceDate('2026-11-12', 0, now), '2026-11-12');
  assert.equal(normalizeEvidenceDate('2026-11-12T08:30:00Z', 0, now), '2026-11-12');
  assert.equal(normalizeEvidenceDate('', 0, now), null);
  assert.equal(normalizeEvidenceDate(undefined, 0, now), null);
  for (const bad of ['2026-02-30', '2026-13-01', 'last week', '11/12/2026', '2026-12-25', '1999-01-01']) {
    assert.throws(() => normalizeEvidenceDate(bad, 0, now), { code: 'evidence_date_invalid' }, bad);
  }
});

test('sourceType is a closed vocabulary', () => {
  assert.equal(normalizeSourceType('Public financing request', 0), 'public_financing_request');
  assert.equal(normalizeSourceType('property_record_signal', 0), 'property_record_signal');
  assert.equal(normalizeSourceType('', 0), null);
  assert.throws(() => normalizeSourceType('scraped_forum', 0), { code: 'source_type_invalid' });
});

test('evidence fields do not change notes, review state, score or safety defaults', () => {
  const base = { name: 'Example Person', company: 'Example LLC', reviewState: 'contact_now',
    fit: 'Executed purchase contract; requesting $40,000 for rehab.', sourceUrl: 'https://example.invalid/post/1' };
  const plain = normalizeContact(base, 0);
  const withEvidence = normalizeContact({ ...base, evidenceDate: '2026-11-10', sourceType: 'public_financing_request' }, 0);
  assert.equal(noteFor(withEvidence), noteFor(plain));
  assert.equal(withEvidence.reviewState, plain.reviewState);
  assert.equal(withEvidence.icpScore, plain.icpScore);
  assert.equal(withEvidence.status, plain.status);
  assert.equal(withEvidence.evidenceDate, '2026-11-10');
  assert.equal(withEvidence.sourceType, 'public_financing_request');
  assert.equal(plain.evidenceDate, null);
  assert.equal(plain.sourceType, null);
  assert.throws(() => previewReviewOnlyContacts([{ ...base, evidenceDate: 'yesterday' }]), { code: 'evidence_date_invalid' });
});
