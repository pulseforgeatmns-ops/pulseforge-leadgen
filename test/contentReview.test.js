'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { readContentReview, mediaFor, safeMediaUrl } = require('../services/contentReview');

function artifact(overrides = {}) {
  return { id: 'post-a', platform: 'facebook_page', label: 'Property care', body: 'A prepared team makes Monday easier.', approvalState: 'PENDING_APPROVAL', publishState: 'NOT_PUBLISHED', pendingCommentId: 7, createdAt: '2026-10-19T12:00:00Z', ...overrides };
}

test('review reads only the current tenant and deduplicates canonical mirror rows', async () => {
  let queries = 0;
  const result = await readContentReview({ clientId: 10,
    pool: { query: async (sql, values) => {
      queries++; assert.match(sql, /client_id = \$1 AND status = 'pending'/); assert.deepEqual(values, [10]);
      return { rows: [
        { id: 7, channel: 'facebook_page', comment: 'Duplicate mirror' },
        { id: 8, channel: 'linkedin', comment: 'Useful comment', author_name: 'A prospect', post_content: 'Original context', created_at: '2026-10-18T12:00:00Z' },
        { id: 9, channel: 'facebook_page', comment: 'Already approved canonical mirror' },
      ] };
    } },
    socialStore: { listByTenant: async (tenantId, clientId) => {
      assert.equal(tenantId, '10'); assert.equal(clientId, 10);
      return [artifact(), artifact({ id: 'decided', pendingCommentId: 9, approvalState: 'APPROVED' })];
    } }, now: new Date('2026-10-19T12:05:00Z'),
  });
  assert.equal(queries, 1); assert.equal(result.pendingCount, 2);
  assert.deepEqual(result.items.map(item => item.id), ['social:post-a', 'pending:8']);
  assert.equal(result.items[0].decisionMode, 'exact_preview');
  assert.equal(result.items[1].originalPost, 'Original context');
  assert.equal(result.generatedAt, '2026-10-19T12:05:00.000Z');
});

test('unbound drafts remain visible with approval blocked; publishing artifacts do not reappear', async () => {
  const result = await readContentReview({ clientId: 10,
    pool: { query: async () => ({ rows: [{ id: 4, channel: 'google_business', comment: 'Old social draft' }] }) },
    socialStore: { listByTenant: async () => ['PUBLISHING', 'VERIFYING', 'UNKNOWN', 'PUBLISHED'].map((publishState, i) => artifact({ id: String(i), pendingCommentId: null, publishState })) },
  });
  assert.equal(result.pendingCount, 1);
  assert.equal(result.items[0].canReview, false);
  assert.equal(result.items[0].decisionMode, 'binding_required');
  assert.match(result.items[0].blocker, /verified post version and destination/);
});

test('source failures are reported without hiding the remaining drafts or fabricating an empty queue', async () => {
  const pool = { query: async () => ({ rows: [{ id: 1, channel: 'facebook_page', comment: 'Still visible' }] }) };
  const unavailable = { listByTenant: async () => { throw new Error('offline'); } };
  const partial = await readContentReview({ clientId: 10, pool, socialStore: unavailable });
  assert.equal(partial.pendingCount, 1); assert.equal(partial.warnings.length, 1);
  assert.equal(partial.items[0].canReview, false);
  await assert.rejects(readContentReview({ clientId: 10, pool: { query: async () => { throw new Error('offline'); } }, socialStore: unavailable }), /unavailable/);
});

test('invalid tenant never reaches data sources; attachment URLs cannot execute script', async () => {
  for (const clientId of [null, 0, -1, 2.5, 'invalid']) await assert.rejects(readContentReview({ clientId, pool: { query: () => assert.fail('must not query') } }), /tenant_scope/);
  assert.equal(safeMediaUrl('javascript:alert(1)'), null);
  assert.equal(safeMediaUrl('data:text/html,test'), null);
  assert.equal(safeMediaUrl('//other.example/a.png'), null);
  assert.deepEqual(mediaFor({ mediaRefs: ['https://example.com/a.jpg', 'javascript:alert(1)', { url: 'https://example.com/a.jpg' }, { src: '/media/b.mp4', alt: 'Office walkthrough' }] }), [
    { url: 'https://example.com/a.jpg', alt: 'Draft attachment', type: 'image' },
    { url: '/media/b.mp4', alt: 'Office walkthrough', type: 'video' },
  ]);
});

test('review API preserves operator permissions and uses the authorized active workspace', async () => {
  const { createContentReviewRouter } = require('../routes/contentReview');
  let reads = 0;
  const router = createContentReviewRouter({ pool: { query: async (_sql, params) => { reads++; assert.deepEqual(params, [10]); return { rows: [] }; } }, socialStore: { listByTenant: async () => [] } });
  const request = (session, query = {}) => new Promise((resolve, reject) => {
    const req = { method: 'GET', url: '/api/content-review', originalUrl: '/api/content-review', path: '/api/content-review', query, session, get: () => 'application/json' };
    const res = { statusCode: 200, status(code) { this.statusCode = code; return this; }, set() { return this; }, json(body) { resolve({ status: this.statusCode, body }); } };
    router.handle(req, res, error => error ? reject(error) : reject(new Error('unhandled request')));
  });
  assert.equal((await request({})).status, 401);
  assert.equal((await request({ user: { role: 'viewer' }, active_client_id: 10 })).status, 403);
  assert.equal((await request({ user: { role: 'admin' } })).status, 400);
  assert.equal((await request({ user: { role: 'manager', client_id: 11 }, active_client_id: 10 })).status, 403);
  assert.equal((await request({ user: { role: 'admin' }, active_client_id: 10 }, { client_id: 11 })).status, 403);
  assert.equal(reads, 0);
  const ok = await request({ user: { id: 7, role: 'manager', client_id: 10 }, active_client_id: 10 });
  assert.equal(ok.status, 200); assert.equal(ok.body.clientId, 10); assert.equal(reads, 1);
});
