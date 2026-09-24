'use strict';

const assert = require('node:assert/strict');
const { describe, test, beforeEach, afterEach } = require('node:test');
const {
  validateAnchorSocialCopy,
  buildAnchorCopyDoctrineViolationError,
} = require('../utils/anchorCopyDoctrine');
const { assertSocialCopy } = require('../packages/capabilities/contentPublication/approvalBinding');
const {
  createSocialContentCapability,
  createInMemorySocialContentStore,
} = require('../packages/capabilities/contentGeneration');
const { fixture } = require('./helpers/paigeSocialFixture');

const COMPLIANT_BODY = 'Ten sends over the past 24 hours moved through Manchester outreach. A facilities assessment gives office managers a written scope before recurring service starts.';
const PASSING_SCORE = {
  specificity: 8,
  originality: 8,
  hook_strength: 8,
  total: 24,
  weak_dimension: 'none',
  reason: 'Specific, grounded, and direct.',
};

const ISOLATED_MODULE_PATHS = [
  require.resolve('../db'),
  require.resolve('@anthropic-ai/sdk'),
  require.resolve('../paigeAgent'),
];

function snapshotTestIsolation() {
  return {
    activeClientId: process.env.ACTIVE_CLIENT_ID,
    cache: Object.fromEntries(
      ISOLATED_MODULE_PATHS.map((modulePath) => [modulePath, require.cache[modulePath]])
    ),
  };
}

function restoreTestIsolation(snapshot) {
  if (snapshot.activeClientId === undefined) {
    delete process.env.ACTIVE_CLIENT_ID;
  } else {
    process.env.ACTIVE_CLIENT_ID = snapshot.activeClientId;
  }

  for (const modulePath of ISOLATED_MODULE_PATHS) {
    if (snapshot.cache[modulePath] === undefined) {
      delete require.cache[modulePath];
    } else {
      require.cache[modulePath] = snapshot.cache[modulePath];
    }
  }
}

function buildPaigeHarness({ draftSequence = [] } = {}) {
  process.env.ACTIVE_CLIENT_ID = '10';
  const writes = [];
  let generationCalls = 0;

  const dbPath = require.resolve('../db');
  require.cache[dbPath] = {
    id: dbPath,
    filename: dbPath,
    loaded: true,
    exports: {
      query: async (sql) => {
        const normalized = String(sql).trim();
        if (/^(?:INSERT|UPDATE|ALTER|DELETE|CREATE|DROP|DO)\b/i.test(normalized)) {
          writes.push(normalized);
          return { rows: [] };
        }
        if (/SELECT \* FROM clients WHERE id = \$1 AND active = true/i.test(sql)) {
          return { rows: [{
            id: 10,
            name: 'Anchor Cleaning',
            business_name: 'Anchor Cleaning',
            vertical: 'commercial_cleaning',
            city: 'Manchester',
            state: 'NH',
            enabled_agents: ['scout', 'paige'],
          }] };
        }
        if (/FROM clients\s+WHERE id = \$1 AND active = true/i.test(sql)) {
          return { rows: [{ id: 10, name: 'Anchor Cleaning', city: 'Manchester', state: 'NH' }] };
        }
        if (/AS send_count_24h/i.test(sql)) {
          return { rows: [{
            send_count_24h: 10,
            open_count_24h: 7,
            reply_count_24h: 2,
            bounce_count_24h: 0,
            warm_signal_count_24h: 3,
            send_daily_average_previous_7d: 8,
          }] };
        }
        if (/GROUP BY \(ran_at AT TIME ZONE/i.test(sql)) {
          return { rows: [{ activity_date: '2026-07-05', send_count: 10 }] };
        }
        if (/to_regclass\('public\.daily_anchors'\)/i.test(sql)) return { rows: [{ tbl: null }] };
        return { rows: [] };
      },
    },
  };

  const anthropicPath = require.resolve('@anthropic-ai/sdk');
  class FakeAnthropic {
    constructor() {
      this.messages = {
        create: async (request) => {
          const prompt = request.messages?.[0]?.content || '';
          if (/Score this social media post/i.test(prompt)) {
            return { content: [{ text: JSON.stringify(PASSING_SCORE) }] };
          }
          generationCalls += 1;
          const queue = draftSequence.length ? [...draftSequence] : [COMPLIANT_BODY];
          const body = queue[Math.min(generationCalls - 1, queue.length - 1)];
          return { content: [{ text: body }] };
        },
      };
    }
  }
  require.cache[anthropicPath] = {
    id: anthropicPath,
    filename: anthropicPath,
    loaded: true,
    exports: FakeAnthropic,
  };

  delete require.cache[require.resolve('../paigeAgent')];
  const paige = require('../paigeAgent');
  return { paige, writes, getGenerationCalls: () => generationCalls };
}

describe('Paige Anchor social doctrine reconciliation', () => {
  test('validateAnchorSocialCopy exposes structured patternId and match', () => {
    const check = validateAnchorSocialCopy('Would you be open to a quick call?');
    assert.equal(check.ok, false);
    assert.ok(check.violations.some((v) => v.patternId === 'open_to_call' && v.match));
  });

  test('assertSocialCopy attaches structured violation details', () => {
    try {
      assertSocialCopy({ clientId: 10, body: 'Book a walkthrough today.' });
      assert.fail('expected doctrine rejection');
    } catch (err) {
      assert.equal(err.code, 'anchor_copy_doctrine_violation');
      assert.ok(Array.isArray(err.violations));
      assert.ok(err.violations.some((v) => v.patternId === 'walkthrough'));
    }
  });

  test('approvalBinding still independently blocks invalid artifacts at preview', async () => {
    const f = await fixture();
    const a = await f.current();
    a.body = 'I wanted to reach out about a walkthrough.';
    await f.store.insertBatch([a]);
    try {
      await f.approval.preview({ ...f.scope, artifactId: a.id });
      assert.fail('expected preview rejection');
    } catch (err) {
      assert.match(err.message, /anchor_copy_doctrine_violation/);
      assert.equal(err.code, 'anchor_copy_doctrine_violation');
      assert.ok(err.violations.some((v) => v.patternId === 'wanted_to_reach_out' || v.patternId === 'walkthrough'));
    }
    assert.equal(f.counts().sends, 0);
  });

  test('no artifact is persisted when generation output still violates doctrine', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentCapability({
      socialContentStore: store,
      runGeneration: async () => ({
        success: true,
        outputs: [{
          company: 'Anchor Cleaning',
          channel: 'facebook_page',
          content: 'Would you be open to a quick call about cleaning?',
          content_type: 'educational',
          meta: {},
        }],
        drafts: [],
      }),
    });

    await assert.rejects(
      () => cap.execute({ tenantId: '10', clientId: 10, inputs: { dryRun: false } }),
      /anchor_copy_doctrine_violation/
    );
    assert.equal((await store.listByTenant('10', 10)).length, 0);
  });

  test('compliant generation output persists canonical artifacts', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentCapability({
      socialContentStore: store,
      runGeneration: async () => ({
        success: true,
        outputs: [{
          company: 'Anchor Cleaning',
          channel: 'facebook_page',
          content: COMPLIANT_BODY,
          content_type: 'educational',
          meta: {},
        }],
        drafts: [],
      }),
      mirrorPendingComment: async () => 'pending-anchor-1',
    });

    const result = await cap.execute({ tenantId: '10', clientId: 10, inputs: { dryRun: false } });
    assert.equal(result.status, 'completed');
    assert.equal((await store.listByTenant('10', 10)).length, 1);
  });

  describe('generation-time doctrine regeneration', () => {
    let isolationSnapshot;

    beforeEach(() => {
      isolationSnapshot = snapshotTestIsolation();
    });

    afterEach(() => {
      restoreTestIsolation(isolationSnapshot);
    });

    test('em dash draft is regenerated before returning content', async () => {
      const { paige, getGenerationCalls } = buildPaigeHarness({
        draftSequence: [
          'Ten sends over the past 24 hours — Manchester offices need a written scope before recurring service starts.',
          COMPLIANT_BODY,
        ],
      });

      const result = await paige.generateSocialContent({ client_id: 10, dryRun: true, channel: 'facebook_page' });
      assert.equal(result.success, true);
      assert.equal(result.outputs.length, 1);
      assert.doesNotMatch(result.outputs[0].content, /[—–]/);
      assert.ok(getGenerationCalls() >= 2);
    });

    test('generic closer draft is regenerated before returning content', async () => {
      const { paige, getGenerationCalls } = buildPaigeHarness({
        draftSequence: [
          'Ten sends over the past 24 hours in Manchester. Would you be open to a quick call about office cleaning scope?',
          COMPLIANT_BODY,
        ],
      });

      const result = await paige.generateSocialContent({ client_id: 10, dryRun: true, channel: 'facebook_page' });
      assert.equal(result.success, true);
      assert.equal(validateAnchorSocialCopy(result.outputs[0].content).ok, true);
      assert.ok(getGenerationCalls() >= 2);
    });

    test('walkthrough violation is regenerated before returning content', async () => {
      const { paige, getGenerationCalls } = buildPaigeHarness({
        draftSequence: [
          'Ten sends over the past 24 hours in Manchester. Book a walkthrough to review the office scope.',
          COMPLIANT_BODY,
        ],
      });

      const result = await paige.generateSocialContent({ client_id: 10, dryRun: true, channel: 'facebook_page' });
      assert.equal(result.success, true);
      assert.equal(validateAnchorSocialCopy(result.outputs[0].content).ok, true);
      assert.ok(getGenerationCalls() >= 2);
    });

    test('violations remain blocked after max retries with no content returned', async () => {
      const violating = 'I wanted to reach out — worth a quick conversation?';
      const { paige, getGenerationCalls } = buildPaigeHarness({
        draftSequence: Array(8).fill(violating),
      });

      const result = await paige.generateSocialContent({ client_id: 10, dryRun: true, channel: 'facebook_page' });
      assert.equal(result.success, false);
      assert.equal(result.outputs.length, 0);
      assert.ok(getGenerationCalls() >= 2);
    });
  });

  test('buildAnchorCopyDoctrineViolationError preserves safe violation details only', () => {
    const err = buildAnchorCopyDoctrineViolationError([
      { source: 'anchor_copy_doctrine', patternId: 'em_dash', match: '—' },
    ]);
    assert.equal(JSON.stringify(err.violations), '[{"source":"anchor_copy_doctrine","patternId":"em_dash","match":"—"}]');
    assert.doesNotMatch(JSON.stringify(err), /ANTHROPIC|prompt|secret/i);
  });
});
