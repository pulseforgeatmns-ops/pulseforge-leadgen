'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { composeAoFollowUp } = require('../utils/aoFollowupComposer');

function baseInput(overrides = {}) {
  return {
    tenantId: 10,
    accountId: 1,
    accountName: 'Example Account',
    assignedAoId: 101,
    assignedAoName: 'Tony',
    ...overrides,
  };
}

test('canonical AO signature when communication identity is present', () => {
  const out = composeAoFollowUp(baseInput({
    assignedAoName: 'Tony Jackson',
    aoCommunicationIdentity: {
      emailAddress: 'tony@goanchorcleaning.com',
      phoneNumber: '+1 978 505 1501',
      phoneDisplay: '(978) 505-1501',
    },
    contactName: 'Lori',
    aoNotes: 'Follow up on facility assessment interest.',
  }));
  assert.match(out.emailDraft, /Tony Jackson/);
  assert.match(out.emailDraft, /Acquisition Operator/);
  assert.match(out.emailDraft, /tony@goanchorcleaning\.com/);
  assert.match(out.emailDraft, /\(978\) 505-1501/);
});

test('NH Family Dentistry — internal staff cleaning', () => {
  const out = composeAoFollowUp(baseInput({
    accountName: 'New Hampshire Family Dentistry',
    contactName: 'Lori',
    aoNotes: 'Front desk receptionist is the person who cleans the office.',
  }));

  assert.equal(out.status, 'draft_ready');
  assert.equal(out.approvalPath, 'ao_can_send');
  assert.equal(out.doctrineChecks.humanAnchorVoice, true);
  assert.match(out.emailDraft, /Tony/);
  assert.match(out.emailDraft, /internally/i);
  assert.match(out.emailDraft, /facility assessment/i);
  assert.match(out.emailDraft, /We're not trying/i);
  assert.match(out.alternateShortNote, /Following up on the internal cleaning support we discussed/i);
  assert.doesNotMatch(out.emailDraft, /just checking in/i);
  assert.doesNotMatch(out.emailDraft, /would be happy to/i);
  assert.doesNotMatch(out.alternateShortNote, /open to/i);
});

test('TD Bank / Nash — Mike-facing property management pain signal', () => {
  const out = composeAoFollowUp(baseInput({
    accountName: 'TD Bank Concord',
    contactName: 'Mike',
    buildingManagementCompany: 'Nash Family Investment Properties',
    knownPainSignals: ['kitchen area missed', 'under tables not swept'],
    aoNotes: 'Kristy spoke with Tony and would speak with Mike. Employee unhappy with current cleaning contractor.',
  }));

  assert.match(out.emailDraft, /Mike/);
  assert.doesNotMatch(out.emailDraft, /Hi Kristy/i);
  assert.match(out.emailDraft, /Kristy may have mentioned/i);
  assert.match(out.emailDraft, /how cleaning or vendor issues usually get handled/i);
  assert.match(out.emailDraft, /I'm not assuming/i);
  assert.doesNotMatch(out.emailDraft, /would be happy/i);
  assert.equal(out.approvalPath, 'jake_review_recommended');
  assert.doesNotMatch(out.emailDraft, /terrible contractor/i);
});

test('Phillips Exeter — relationship-first tone', () => {
  const out = composeAoFollowUp(baseInput({
    accountName: 'Phillips Exeter Academy',
    contactName: 'Billy',
    aoNotes: 'William Gagnon is Tony former manager at UNH and Tony has a good relationship with him.',
  }));

  assert.match(out.emailDraft, /reconnect/i);
  assert.match(out.emailDraft, /Anchor Cleaning/i);
  assert.match(out.emailDraft, /we're not going to push/i);
  assert.doesNotMatch(out.emailDraft, /quote today/i);
  assert.doesNotMatch(out.emailDraft, /if it would be helpful/i);
  assert.match(out.emailDraft, /Jake/);
  assert.match(out.emailDraft, /Tony/);
  assert.equal(out.approvalPath, 'ao_can_send');
});

test('USPS Concord Post Office — procurement qualification', () => {
  const out = composeAoFollowUp(baseInput({
    accountName: 'Concord NH Post Office',
    contactName: 'Ron',
    accountType: 'federal/post office',
    aoNotes: 'USPS recently switched to in-house custodial services but is unhappy about it.',
  }));

  assert.match(out.emailDraft, /regional or supplier process/i);
  assert.match(out.emailDraft, /outside cleaning support/i);
  assert.match(out.emailDraft, /Following up on our conversation/i);
  assert.doesNotMatch(out.emailDraft, /I wanted to follow up/i);
  assert.doesNotMatch(out.emailDraft, /price/i);
  assert.doesNotMatch(out.emailDraft, /quote/i);
});

test('Wipfli / Trillium — vendor routing research required', () => {
  const out = composeAoFollowUp(baseInput({
    accountName: 'Wipfli',
    currentVendorOrProvider: 'Trillium',
    aoNotes: 'Told Tony to contact building management. Trillium listed as building management.',
  }));

  assert.equal(out.status, 'research_required');
  assert.equal(out.emailDraft, null);
  assert.ok(out.warnings.some(w => /Trillium/i.test(w)));
  assert.equal(out.approvalPath, 'jake_review_recommended');
});

test('SNHU / UNH ambiguity — needs clarification', () => {
  const out = composeAoFollowUp(baseInput({
    accountName: 'Southern New Hampshire University',
    contactEmail: 'Robert.Oelschlager@unh.edu',
    aoNotes: 'Facilities link points to UNH facilities page.',
  }));

  assert.equal(out.status, 'needs_clarification');
  assert.equal(out.approvalPath, 'do_not_send');
  assert.equal(out.emailDraft, null);
  assert.ok(out.warnings.some(w => /SNHU/i.test(w) && /UNH/i.test(w)));
});

test('Manchester Family Dentistry — do not contact', () => {
  const out = composeAoFollowUp(baseInput({
    accountName: 'Manchester Family Dentistry',
    aoNotes: 'Asked to be removed from the call list.',
  }));

  assert.equal(out.status, 'do_not_contact');
  assert.equal(out.approvalPath, 'do_not_send');
  assert.equal(out.emailDraft, null);
});
