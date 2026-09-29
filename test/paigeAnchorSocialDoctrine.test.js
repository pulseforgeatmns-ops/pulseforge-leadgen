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

const COMPLIANT_BODY = 'Ten sends over the past 24 hours moved through Manchester outreach. A facility assessment gives office managers a written scope before recurring service starts.';
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
  require.resolve('../utils/miraContext'),
  require.resolve('@anthropic-ai/sdk'),
  require.resolve('../paigeAgent'),
];

function writerTextBlock(text) {
  return { content: [{ type: 'text', text }] };
}

function assertCompliantAnchorFacebookDraft(content, { usesMiraGrounding }) {
  assert.equal(validateAnchorSocialCopy(content).ok, true);
  assert.doesNotMatch(content, /[—–]/);
  assert.doesNotMatch(content, /\bi wanted to reach out\b/i);
  assert.doesNotMatch(content, /\bworth a (?:quick )?conversation\b/i);
  assert.ok(
    usesMiraGrounding(content, DETERMINISTIC_CONTENT_SAFE_MIRA),
    'expected a concrete client-scoped Mira detail in public copy'
  );
  assert.match(content, /facility assessment/i);
}

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

function installDeterministicMiraContextMock() {
  const miraPath = require.resolve('../utils/miraContext');
  require.cache[miraPath] = {
    id: miraPath,
    filename: miraPath,
    loaded: true,
    exports: {
      buildMiraContext: async (clientId, options = {}) => ({
        ...DETERMINISTIC_CONTENT_SAFE_MIRA,
        channel: options.channel || DETERMINISTIC_CONTENT_SAFE_MIRA.channel,
        client: { ...DETERMINISTIC_CONTENT_SAFE_MIRA.client, id: Number(clientId) },
      }),
    },
  };
}

function buildPaigeHarness({ draftSequence = [], simulateMiraUnavailable = false } = {}) {
  process.env.ACTIVE_CLIENT_ID = '10';
  const writes = [];
  let generationCalls = 0;
  let providerCalls = 0;

  installDeterministicMiraContextMock();

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
        return { rows: [] };
      },
    },
  };

  const anthropicPath = require.resolve('@anthropic-ai/sdk');
  class FakeAnthropic {
    constructor() {
      this.messages = {
        create: async (request) => {
          providerCalls += 1;
          const prompt = request.messages?.[0]?.content || '';
          if (/Score this social media post/i.test(prompt)) {
            return writerTextBlock(JSON.stringify(PASSING_SCORE));
          }
          generationCalls += 1;
          const queue = draftSequence.length ? [...draftSequence] : [COMPLIANT_BODY];
          const body = queue[Math.min(generationCalls - 1, queue.length - 1)];
          return writerTextBlock(body);
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
  return {
    paige,
    writes,
    simulateMiraUnavailable,
    getGenerationCalls: () => generationCalls,
    getProviderCalls: () => providerCalls,
  };
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
      assert.ok(getGenerationCalls() >= 2);
      assertCompliantAnchorFacebookDraft(result.outputs[0].content, paige._test);
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
      assert.ok(getGenerationCalls() >= 2);
      assertCompliantAnchorFacebookDraft(result.outputs[0].content, paige._test);
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
      assert.ok(getGenerationCalls() >= 2);
      assertCompliantAnchorFacebookDraft(result.outputs[0].content, paige._test);
    });

    test('violations remain blocked after max retries with no content returned', async () => {
      const violating = 'I wanted to reach out — worth a quick conversation?';
      const { paige, getGenerationCalls, writes } = buildPaigeHarness({
        draftSequence: Array(8).fill(violating),
      });

      const result = await paige.generateSocialContent({ client_id: 10, dryRun: true, channel: 'facebook_page' });
      assert.equal(result.success, false);
      assert.equal(result.outputs.length, 0);
      assert.ok(getGenerationCalls() >= 2);
      assert.deepEqual(writes, []);
      assert.match(String(result.channels_failed?.[0] || ''), /facebook_page/);
    });

    test('happy-path facebook_page generation uses seeded Mira context without retries', async () => {
      const { paige, getGenerationCalls, writes } = buildPaigeHarness();

      const result = await paige.generateSocialContent({ client_id: 10, dryRun: true, channel: 'facebook_page' });
      assert.equal(result.success, true);
      assert.equal(result.outputs.length, 1);
      assert.equal(getGenerationCalls(), 1);
      assert.deepEqual(writes, []);
      assertCompliantAnchorFacebookDraft(result.outputs[0].content, paige._test);
    });

    test('Mira-unavailable aborts facebook_page generation without persisting artifacts', async () => {
      const { paige, getGenerationCalls, getProviderCalls, writes } = buildPaigeHarness();
      const loggedErrors = [];
      const originalConsoleError = console.error;
      console.error = (...args) => {
        loggedErrors.push(args.map(String).join(' '));
        originalConsoleError(...args);
      };

      try {
        const result = await paige.generateSocialContent({
          client_id: 10,
          dryRun: true,
          channel: 'facebook_page',
          simulateMiraUnavailable: true,
        });
        assert.equal(result.success, false);
        assert.equal(result.outputs.length, 0);
        assert.equal(getGenerationCalls(), 0);
        assert.equal(getProviderCalls(), 0);
        assert.deepEqual(writes, []);
        assert.match(String(result.channels_failed?.[0] || ''), /facebook_page/);
        assert.match(
          loggedErrors.join('\n'),
          /Mira content-safe context is unavailable|fabricating specifics/i
        );
      } finally {
        console.error = originalConsoleError;
      }
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
