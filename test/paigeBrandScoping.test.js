const assert = require('node:assert/strict');
const test = require('node:test');
const { _test } = require('../paigeAgent');

test('LinkedIn brand resolution is client-aware', () => {
  assert.equal(_test.getBrandForChannel('linkedin_page', 1), 'pulseforge');
  assert.equal(_test.getBrandForChannel('linkedin_personal', 1), 'jacob_personal');
  assert.equal(_test.getBrandForChannel('linkedin_page', 10), 'anchor');
  assert.equal(_test.getBrandForChannel('linkedin_personal', 10), null);
});

test('Anchor LinkedIn rules contain Anchor facts and no Pulseforge facts', () => {
  const rules = _test.buildLinkedInRules('anchor', 'punch');
  assert.match(rules, /Anchor Cleaning/i);
  assert.match(rules, /Manchester/i);
  assert.doesNotMatch(rules, /Pulseforge|Scout|Emmett|Riley|MSHI|Brad Hudson|Dustin Allison/i);
});

test('universal Paige rules enforce the new writing constraints', () => {
  const rules = _test.buildUniversalWritingRules('linkedin_page');
  assert.match(rules, /anaphoric negation/i);
  assert.match(rules, /screenshot line/i);
  assert.match(rules, /coffee shop/i);
  assert.match(rules, /What do you think/i);

  assert.deepEqual(_test.validateUniversalDraft('# A title — allowed\n\nBody with a hyphenated word.', 'blog'), []);
  assert.ok(_test.validateUniversalDraft('Body — with a dash.', 'facebook_page').length);
  assert.ok(_test.validateUniversalDraft("Here's the thing: this is canned.", 'linkedin_page').length);
  assert.ok(_test.validateUniversalDraft('Not reach. Not impressions. Results.', 'linkedin_page').length);
  assert.ok(_test.validateUniversalDraft('No handoffs. No guessing.', 'facebook_page').length);
  assert.ok(_test.validateUniversalDraft('More sends, more dials, more touchpoints.', 'linkedin_page').length);
  assert.ok(_test.validateUniversalDraft('Every send landed. Every call connected. Every metric lied.', 'linkedin_page').length);
  assert.ok(_test.validateUniversalDraft('The manager is re-explaining access, fielding complaints, and chasing the vendor.', 'linkedin_page').length);
});

test('dialogue is a sixth format with client-appropriate role labels', () => {
  assert.deepEqual(_test.LINKEDIN_FORMATS, ['punch', 'numbers', 'quote', 'stake', 'decision_log', 'dialogue']);
  assert.match(_test.buildLinkedInRules('pulseforge', 'dialogue'), /Owner:/);
  assert.match(_test.buildLinkedInRules('anchor', 'dialogue'), /Practice Manager:/);
  assert.doesNotMatch(_test.buildLinkedInRules('anchor', 'dialogue'), /Founder:/);
});

test('LinkedIn format rotation avoids seven-day repeats and downweights days 8-14', () => {
  const now = new Date('2026-07-15T12:00:00.000Z');
  assert.equal(_test.linkedInFormatWeight('2026-07-13T12:00:00.000Z', now), 0);
  assert.equal(_test.linkedInFormatWeight('2026-07-05T12:00:00.000Z', now), 0.2);
  assert.equal(_test.linkedInFormatWeight('2026-06-20T12:00:00.000Z', now), 1);

  const history = [
    { format: 'punch', last_used_at: '2026-07-13T12:00:00.000Z' },
    { format: 'numbers', last_used_at: '2026-07-05T12:00:00.000Z' },
  ];
  assert.notEqual(_test.chooseLinkedInFormat(history, now, () => 0), 'punch');
});

test('Mira grounding accepts only available content-safe context', () => {
  const block = _test.buildMiraGroundingBlock({
    available: true,
    client: { id: 10, name: 'Anchor Cleaning', city: 'Manchester', state: 'NH' },
    current_anchor: null,
    metrics: { sends_24h: 10, opens_24h: 7, replies_24h: 2, warm_signals_24h: 3 },
    recent_activity_summaries: ['10 emails sent over the past 24 hours in Manchester, NH'],
    client_health: { deliverability_status: 'healthy' },
  });
  assert.match(block, /10 emails sent/);
  assert.doesNotMatch(block, /"(?:prospect_name|email|phone|task_id|blocker_id|linkedin_url)"\s*:/i);
  assert.throws(
    () => _test.buildMiraGroundingBlock({ available: false }),
    /aborting generation rather than fabricating/i
  );
});

test('LinkedIn source anchors must include a client-scoped Mira fact', () => {
  assert.equal(_test.hasMiraSourceAnchor(['Canonical: six-week Cal run']), false);
  assert.equal(_test.hasMiraSourceAnchor(['Mira: 10 sends over the past 24 hours']), true);
});

test('Pulseforge claim guard rejects fabricated client anecdotes without blocking bare aggregate data', () => {
  assert.ok(
    _test.validatePulseforgeClaims(
      'A roofing contractor we work with in Southern NH came to us after losing seven estimates.',
      {},
      'facebook_page'
    ).includes('asserts an unnamed real client anecdote')
  );
  assert.ok(
    _test.validatePulseforgeClaims(
      'We recovered 18 leads for a client we work with.',
      {},
      'facebook_page'
    ).includes('asserts an unsupported specific client result')
  );
  assert.deepEqual(
    _test.validatePulseforgeClaims('Thirty leads came in and twelve got a follow-up.', {}, 'facebook_page'),
    []
  );
  assert.deepEqual(
    _test.validatePulseforgeClaims('Imagine a roofing contractor who sends quotes but never follows up.', {}, 'facebook_page'),
    []
  );
  assert.ok(
    _test.validatePulseforgeClaims(
      'Brad & Dustin at Mountain State validated the model in 28 days.',
      {},
      'blog'
    ).includes('names a real client or operator without a verified source')
  );
});

test('grounding guard rejects invented durations and clock times', () => {
  const context = {
    metrics: { sends_24h: 10 },
    recent_activity_summaries: ['10 emails sent over the past 24 hours in Manchester, NH'],
  };
  assert.deepEqual(
    _test.validateGroundedTimeClaims('Ten emails went out over the past 24 hours.', context, 'anchor'),
    []
  );
  assert.ok(_test.validateGroundedTimeClaims('The vendor went quiet for a week.', context, 'anchor').length);
  assert.ok(_test.validateGroundedTimeClaims('The call came at 7 a.m.', context, 'anchor').length);
  assert.ok(_test.validateGroundedTimeClaims('That took three weeks.', context, 'anchor').length);
  assert.ok(_test.validateGroundedTimeClaims('Sends have been quiet this week.', context, 'anchor').length);
  assert.ok(_test.validateGroundedTimeClaims('Mira: sends are quiet.', context, 'anchor').length);
});

test('public copy must visibly use a client-scoped Mira detail', () => {
  const context = {
    client: { name: 'Anchor Cleaning', city: 'Manchester', state: 'NH' },
    metrics: { sends_24h: 0, opens_24h: 1, replies_24h: 0, warm_signals_24h: 0 },
    client_health: { send_volume_status: 'dark', deliverability_status: 'healthy' },
  };
  assert.equal(_test.usesMiraGrounding('Serving Manchester law firms with a written scope.', context), true);
  assert.equal(_test.usesMiraGrounding('One open came through while send volume stayed dark.', context), true);
  assert.equal(_test.usesMiraGrounding('Ten sends went out.', { ...context, client: {} , client_health: {} }), false);
  assert.equal(_test.usesMiraGrounding('Professional offices deserve accountability.', context), false);
});
