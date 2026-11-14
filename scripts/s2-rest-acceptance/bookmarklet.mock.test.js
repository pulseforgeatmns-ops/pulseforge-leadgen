'use strict';
/*
 * Mock-server tests for the S2 bookmarklet. Executes the BUILT bookmarklet.txt
 * (exactly what goes on the tablet) against fake endpoints; no network.
 *   node --test scripts/s2-rest-acceptance/bookmarklet.mock.test.js
 */
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const MISSION = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169';
const STATUS = 'Plan approved (review only)';
const FIVE = ['provider_research', 'outreach', 'outreach_authorization', 'paid_media', 'content'];
const raw = fs.readFileSync(path.join(__dirname, 'bookmarklet.txt'), 'utf8').trim();
assert.ok(raw.startsWith('javascript:'));
const code = decodeURIComponent(raw.slice('javascript:'.length));

function mission(overrides = {}) {
  return { id: MISSION, tenantId: '25', stage: 'discover', status: STATUS, version: 3, pendingOperatorDecision: null, ...overrides };
}

async function run(opts = {}) {
  const o = {
    tenant: '25',
    constraints: { reviewOnly: true, prohibited: FIVE, revision: 0 },
    missions: [mission()],
    snapshot: (posts) => ({ spec: 'SPEC-118', tenantId: '25', mission: mission() }),
    execute: () => [403, { error: 'authorization_constraint_blocked', message: 'Mission authorization forbids x (review-only mission).' }],
    hash: () => 'h1',
    confirm: true,
    ...opts,
  };
  let posts = 0;
  const sent = [];
  let panel = null;
  const J = (status, body) => ({ status, redirected: false, text: async () => JSON.stringify(body) });
  global.location = { origin: o.origin || 'https://pulseforge-leadgen-production.up.railway.app' };
  global.document = { createElement: () => ({ style: {} }), body: { appendChild: (el) => { panel = el; } } };
  global.confirm = () => o.confirm;
  global.alert = () => {};
  Object.defineProperty(globalThis, 'navigator', { value: { clipboard: { writeText: async () => {} } }, configurable: true, writable: true });
  global.fetch = async (url, init = {}) => {
    if (o.html) return { status: 200, redirected: true, text: async () => '<html>login</html>' };
    if (url === '/api/v1/tenant/context') return J(200, { tenantId: o.tenant, active_client_id: Number(o.tenant) });
    if (url === '/api/v1/amo/missions') {
      const ms = typeof o.missions === 'function' ? o.missions(posts) : o.missions;
      return J(200, { kind: 'acquisition_missions', spec: 'SPEC-118', tenantId: o.tenant, missions: ms });
    }
    if (url.endsWith('/authorization')) {
      return J(200, { missionId: MISSION, tenantId: '25', constraintsHash: o.hash(posts), constraints: o.constraints });
    }
    if (url.endsWith('/execute')) {
      assert.strictEqual(init.method, 'POST');
      const body = JSON.parse(init.body);
      assert.deepStrictEqual(Object.keys(body).sort(), ['allowFixtureFallback', 'intent']);
      assert.strictEqual(body.allowFixtureFallback, false);
      sent.push(body.intent);
      const i = posts++;
      if (o.netErrAt === i) throw new Error('network');
      const [status, b] = o.execute(i, body.intent);
      return J(status, b);
    }
    return J(200, o.snapshot(posts));
  };
  await eval(code); // eslint-disable-line no-eval
  await new Promise((r) => setTimeout(r, 20));
  const log = JSON.parse(panel.textContent);
  return { posts, sent, log };
}

const INTENTS = ['EXECUTE_OUTBOUND', 'APPROVE_EXECUTION', 'GENERATE_VARIANTS', 'ASSESS_PAID_ACQUISITION', 'START_DISCOVERY', 'APPROVE_DISCOVERY'];

test('happy path: six 403s, in order, verdict ALL_SIX_BLOCKED_403', async () => {
  const r = await run();
  assert.strictEqual(r.log.verdict, 'ALL_SIX_BLOCKED_403');
  assert.deepStrictEqual(r.sent, INTENTS);
  assert.strictEqual(r.log.post.tenantMissionCount, 1);
});

const zeroPost = [
  ['revision missing', { constraints: { reviewOnly: true, prohibited: FIVE } }, 'authorization_revision_missing'],
  ['revision null', { constraints: { reviewOnly: true, prohibited: FIVE, revision: null } }, 'authorization_revision_mismatch'],
  ['revision "0" string', { constraints: { reviewOnly: true, prohibited: FIVE, revision: '0' } }, 'authorization_revision_mismatch'],
  ['revision 1', { constraints: { reviewOnly: true, prohibited: FIVE, revision: 1 } }, 'authorization_revision_mismatch'],
  ['not review-only', { constraints: { reviewOnly: false, prohibited: FIVE, revision: 0 } }, 'mission_not_review_only'],
  ['prohibited class missing', { constraints: { reviewOnly: true, prohibited: FIVE.slice(1), revision: 0 } }, 'prohibited_set_mismatch'],
  ['zero tenant missions', { missions: [] }, 'tenant_25_mission_count_not_1'],
  ['two tenant missions', { missions: [mission(), mission({ id: 'mission_other' })] }, 'tenant_25_mission_count_not_1'],
  ['only mission is a different one', { missions: [mission({ id: 'mission_other' })] }, 'tenant_25_only_mission_is_not_target'],
  ['listed status differs', { missions: [mission({ status: 'Awaiting discovery approval' })] }, 'listed_mission_status_mismatch'],
  ['snapshot status differs', { snapshot: () => ({ tenantId: '25', mission: mission({ status: 'Discovering' }) }) }, 'mission_status_mismatch'],
  ['snapshot without mission object', { snapshot: () => ({ tenantId: '25', id: MISSION, status: STATUS }) }, 'mission_snapshot_malformed'],
  ['pending decision', { snapshot: () => ({ tenantId: '25', mission: mission({ pendingOperatorDecision: { kind: 'discovery_approval' } }) }) }, 'pending_operator_decision_present'],
  ['session expired (login redirect)', { html: true }, 'read_non_json'],
  ['wrong active tenant', { tenant: '10' }, 'active_tenant_not_25'],
  ['wrong origin', { origin: 'https://evil.example' }, 'wrong_origin'],
  ['operator cancels at confirm', { confirm: false }, 'operator_cancelled_before_send'],
];
for (const [name, opts, reason] of zeroPost) {
  test(`halts before any POST: ${name}`, async () => {
    const r = await run(opts);
    assert.strictEqual(r.log.verdict, 'HALTED');
    assert.strictEqual(r.log.haltReason, reason);
    assert.strictEqual(r.posts, 0);
  });
}

test('unexpected 200 on 2nd intent halts; no retry; 4 intents unsent', async () => {
  const r = await run({ execute: (i) => (i === 1 ? [200, { spec: 'SPEC-171' }] : [403, { error: 'authorization_constraint_blocked' }]) });
  assert.strictEqual(r.log.haltReason, 'unexpected_response_APPROVE_EXECUTION_do_not_retry');
  assert.deepStrictEqual(r.sent, INTENTS.slice(0, 2));
});

test('403 with a different error code halts', async () => {
  const r = await run({ execute: (i) => (i === 0 ? [403, { error: 'cer_permission_denied' }] : [403, { error: 'authorization_constraint_blocked' }]) });
  assert.strictEqual(r.log.haltReason, 'unexpected_response_EXECUTE_OUTBOUND_do_not_retry');
  assert.strictEqual(r.posts, 1);
});

test('network error halts with outcome unknown; no retry', async () => {
  const r = await run({ netErrAt: 2 });
  assert.strictEqual(r.log.haltReason, 'network_error_outcome_unknown_GENERATE_VARIANTS_do_not_retry');
  assert.strictEqual(r.posts, 3);
});

test('restriction hash drift between POSTs halts', async () => {
  const r = await run({ hash: (posts) => (posts >= 2 ? 'h2' : 'h1') });
  assert.strictEqual(r.log.haltReason, 'constraints_hash_drift_before_GENERATE_VARIANTS');
  assert.strictEqual(r.posts, 2);
});

test('post-check: status changed after run halts', async () => {
  const r = await run({ snapshot: (posts) => ({ tenantId: '25', mission: mission(posts >= 6 ? { status: 'Discovering' } : {}) }) });
  assert.strictEqual(r.log.haltReason, 'mission_status_changed_after_run');
});

test('post-check: version changed after run halts', async () => {
  const r = await run({ snapshot: (posts) => ({ tenantId: '25', mission: mission(posts >= 6 ? { version: 4 } : {}) }) });
  assert.strictEqual(r.log.haltReason, 'mission_version_changed_after_run');
});

test('post-check: a second tenant-25 mission appeared halts', async () => {
  const r = await run({ missions: (posts) => (posts >= 6 ? [mission(), mission({ id: 'mission_new' })] : [mission()]) });
  assert.strictEqual(r.log.haltReason, 'tenant_25_mission_count_changed_after_run');
});
