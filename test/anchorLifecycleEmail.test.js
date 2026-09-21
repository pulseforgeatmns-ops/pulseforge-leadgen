const assert = require('assert');
const {
  PERSONALIZATION_STATUS,
  FACT_TYPES,
} = require('../utils/scoutPersonalizationEvidence');
const {
  buildAnchorLifecycleEmail,
  validateAnchorLifecycleCopy,
} = require('../utils/anchorLifecycleEmail');

const supportedEvidence = {
  personalization_status: PERSONALIZATION_STATUS.SUPPORTED,
  observed_fact: 'your firm lists multiple locations in Manchester and Bedford',
  source_url: 'https://example.com/locations',
  supporting_excerpt: 'Multiple locations in Manchester and Bedford.',
  fact_type: FACT_TYPES.MULTI_LOCATION,
  confidence: 0.8,
  checked_at: '2026-09-17T12:00:00.000Z',
};

const personalized = buildAnchorLifecycleEmail({
  prospect: { first_name: 'Sarah', company_name: 'Harbor Law Group' },
  company: { name: 'Harbor Law Group' },
  evidence: supportedEvidence,
});
assert.match(personalized.subject, /Cleaning for Harbor Law Group/i);
assert.match(personalized.body, /multiple locations/i);
assert.doesNotMatch(personalized.body, /I saw /i);
assert.match(personalized.body, /walk the space, agree on the areas and frequency/i);
assert.match(personalized.body, /Want me to send over what we'd need to price the office properly/i);
assert.equal(personalized.usedPersonalization, true);

const generic = buildAnchorLifecycleEmail({
  prospect: { first_name: 'Sarah', company_name: 'Harbor Law Group' },
  company: { name: 'Harbor Law Group' },
  evidence: { personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT },
});
assert.equal(generic.subject, 'Cleaning for Harbor Law Group');
assert.doesNotMatch(generic.body, /I saw /);
assert.match(generic.body, /vague on what actually gets done/i);
assert.equal(generic.usedPersonalization, false);

const houseGreeting = buildAnchorLifecycleEmail({
  prospect: { company_name: 'Harbor Law Group' },
  company: { name: 'Harbor Law Group' },
  evidence: { personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT },
});
assert.match(houseGreeting.body, /^Hi,\n\nI'm Jacob Maynard with Anchor Cleaning/m);

const badCopy = validateAnchorLifecycleCopy({
  subject: 'Cleaning',
  body: 'I know you need a cleaner for your office.',
});
assert.equal(badCopy.ok, false);
assert.ok(badCopy.violations.length > 0);

const insufficient = buildAnchorLifecycleEmail({
  prospect: { first_name: 'Sarah', company_name: 'Harbor Law Group' },
  company: { name: 'Harbor Law Group' },
  evidence: {
    personalization_status: PERSONALIZATION_STATUS.INSUFFICIENT_EVIDENCE,
    observed_fact: null,
  },
});
assert.equal(insufficient.usedPersonalization, false);
assert.doesNotMatch(insufficient.body, /I saw /);

console.log('anchorLifecycleEmail tests passed');
