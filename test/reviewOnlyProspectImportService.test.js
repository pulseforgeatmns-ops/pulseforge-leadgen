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
