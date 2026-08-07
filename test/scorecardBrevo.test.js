const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  syncScorecardContact,
  contactAttributes,
  configuredListIds,
} = require('../lib/scorecardBrevo');

const answers = {
  name: 'Alex Owner',
  business_name: 'Sparkle Clean Co',
  email: 'Alex@Example.com',
  mobile: '6035550100',
  marketing_consent: true,
};
const result = { category: 'call_recovery_gap', high_intent: true };

describe('scorecard Brevo handoff', () => {
  it('does not add contacts without explicit marketing consent', async () => {
    const response = await syncScorecardContact({ ...answers, marketing_consent: false }, result, {
      http: {
        put: async () => assert.fail('should not update in Brevo'),
        post: async () => assert.fail('should not create in Brevo'),
      },
    });
    assert.deepEqual(response, { synced: false, reason: 'no_marketing_consent' });
  });

  it('maps scorecard context to Brevo attributes', () => {
    assert.deepEqual(contactAttributes(answers, result), {
      FIRSTNAME: 'Alex Owner', BUSINESS_NAME: 'Sparkle Clean Co', PHONE: '6035550100',
      SCORECARD_RESULT: 'call_recovery_gap', SCORECARD_INTENT: 'high',
      SCORECARD_SOURCE: 'revenue_leak_scorecard',
    });
  });

  it('updates an existing opted-in contact', async () => {
    const priorKey = process.env.BREVO_API_KEY;
    const priorEnabled = process.env.SCORECARD_BREVO_SYNC_ENABLED;
    process.env.BREVO_API_KEY = 'test-key';
    process.env.SCORECARD_BREVO_SYNC_ENABLED = 'true';
    let put;
    try {
      const response = await syncScorecardContact(answers, result, {
        http: { put: async (...args) => { put = args; }, post: async () => assert.fail('should not create') },
      });
      assert.deepEqual(response, { synced: true, action: 'updated' });
      assert.match(put[0], /alex%40example\.com$/);
      assert.equal(put[1].attributes.SCORECARD_INTENT, 'high');
    } finally {
      if (priorKey === undefined) delete process.env.BREVO_API_KEY;
      else process.env.BREVO_API_KEY = priorKey;
      if (priorEnabled === undefined) delete process.env.SCORECARD_BREVO_SYNC_ENABLED;
      else process.env.SCORECARD_BREVO_SYNC_ENABLED = priorEnabled;
    }
  });

  it('creates a contact on list 10 with update enabled and mapped attributes', async () => {
    const priorKey = process.env.BREVO_API_KEY;
    const priorList = process.env.SCORECARD_BREVO_LIST_ID;
    const priorEnabled = process.env.SCORECARD_BREVO_SYNC_ENABLED;
    process.env.BREVO_API_KEY = 'test-key';
    process.env.SCORECARD_BREVO_LIST_ID = '10';
    process.env.SCORECARD_BREVO_SYNC_ENABLED = 'true';
    let post;
    try {
      assert.deepEqual(configuredListIds(), [10]);
      const response = await syncScorecardContact(answers, result, {
        http: {
          put: async () => { const err = new Error('not found'); err.response = { status: 404 }; throw err; },
          post: async (...args) => { post = args; },
        },
      });
      assert.deepEqual(response, { synced: true, action: 'created' });
      assert.deepEqual(post[1], {
        email: 'alex@example.com',
        attributes: {
          FIRSTNAME: 'Alex Owner',
          BUSINESS_NAME: 'Sparkle Clean Co',
          PHONE: '6035550100',
          SCORECARD_RESULT: 'call_recovery_gap',
          SCORECARD_INTENT: 'high',
          SCORECARD_SOURCE: 'revenue_leak_scorecard',
        },
        listIds: [10],
        updateEnabled: true,
      });
    } finally {
      if (priorKey === undefined) delete process.env.BREVO_API_KEY;
      else process.env.BREVO_API_KEY = priorKey;
      if (priorList === undefined) delete process.env.SCORECARD_BREVO_LIST_ID;
      else process.env.SCORECARD_BREVO_LIST_ID = priorList;
      if (priorEnabled === undefined) delete process.env.SCORECARD_BREVO_SYNC_ENABLED;
      else process.env.SCORECARD_BREVO_SYNC_ENABLED = priorEnabled;
    }
  });
});
