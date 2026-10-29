'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { makeHarness, loadAuth, login, response, source, startHttpHarness, fixturePassword } = require('./helpers/loginIdentityHarness');

for (const workflow of [undefined, '', 'IDENTITY-CHECK', 'identity-check ', ['identity-check'], { workflow: 'identity-check' }, 'https://evil.example/', '//evil.example/', 'identity-check\n']) {
  test(`workflow is an exact string allowlist: ${JSON.stringify(workflow)}`, async () => {
    const h = makeHarness(); const res = response();
    h.handlers['GET /login'][0]({ query: { workflow } }, res);
    assert.doesNotMatch(res.body, /name="workflow"/);
    const result = await login(h, { workflow, returnTo: 'https://evil.example/', next: '//evil.example/' });
    assert.equal(result.res.location, '/command-deck');
  });
}

test('selected form is explicit, same-origin, script-independent and anonymous', () => {
  const h = makeHarness(); const res = response();
  h.handlers['GET /login'][0]({ query: { workflow: 'identity-check', returnTo: 'https://evil.example/' } }, res);
  assert.match(res.body, /<form method="POST" action="\/login">/);
  assert.match(res.body, /<input type="hidden" name="workflow" value="identity-check">/);
  assert.match(res.body, /Identity check only/);
  assert.doesNotMatch(res.body, /evil\.example|fetch\(|XMLHttpRequest|window\.location/);
  assert.equal(res.headers['Cache-Control'], 'no-store');
  assert.equal(h.initCalls(), 0); assert.equal(h.queries.length, 0);
});

for (const [role, target] of [
  ['admin', '/command-deck'], ['manager', '/command-deck'], ['viewer', '/command-deck'],
  ['client', '/command-deck'], ['setter', '/setter'], ['closer', '/closer'], ['sales', '/sales'],
  ['ao', '/ao'], ['cleaner', '/portal/anchor'], ['facility_client', '/portal/anchor'],
]) {
  test(`${role}: opt-in changes only landing; default role routing stays unchanged`, async () => {
    const user = { role, client_id: 10 };
    const plain = await login(makeHarness({ user }));
    const h = makeHarness({ user });
    const selected = await login(h, { workflow: 'identity-check', client_id: 999, role: 'admin', returnTo: '//evil.example' });
    assert.equal(plain.res.location, target); assert.equal(selected.res.location, '/api/me');
    assert.deepEqual(JSON.parse(JSON.stringify(selected.req.session)), JSON.parse(JSON.stringify(plain.req.session)));
    assert.equal(selected.req.session.user.role, role); assert.equal(selected.req.session.active_client_id, 10);
    assert.equal(selected.res.headers['Cache-Control'], 'no-store');
    assert.deepEqual(h.queries.map(q => q.text.split(' ')[0]), ['SELECT', 'UPDATE']);
    assert.equal(h.queries[1].text, 'UPDATE users SET last_login_at = NOW() WHERE id = $1');
    assert.deepEqual(Array.from(h.queries[1].params), [73]);
  });
}

for (const [name, options, body, error] of [
  ['unknown account', { user: null }, {}, '1'],
  ['wrong password', {}, { password: 'not-the-fixture' }, '1'],
  ['inactive account', { user: { active: false } }, {}, '1'],
  ['unverified account', { user: { email_verified: false } }, {}, 'unverified'],
  ['missing client workspace', { user: { role: 'client', client_id: null } }, {}, 'no_workspace'],
]) {
  test(`${name}: auth guard remains intact and retry preserves selected workflow`, async () => {
    const h = makeHarness(options); const result = await login(h, { workflow: 'identity-check', ...body });
    assert.equal(result.res.location, `/login?error=${error}&workflow=identity-check`);
    assert.deepEqual(result.req.session, {});
    assert.equal(h.queries.some(q => q.text.startsWith('UPDATE')), false);
    const defaultResult = await login(makeHarness(options), body);
    assert.equal(defaultResult.res.location, `/login?error=${error}`);
  });
}

test('required password change still wins over the identity landing', async () => {
  const result = await login(makeHarness({ user: { password_change_required: true } }), { workflow: 'identity-check' });
  assert.equal(result.res.location, '/change-password');
  assert.equal(result.req.session.user.password_change_required, true);
  const html = source('public/change-password.html');
  assert.doesNotMatch(html, /<script[^>]+src=|DOMContentLoaded/);
  assert.match(html, /addEventListener\('submit'/);
});

test('identity workflow never authenticates a legacy no-user fallback; default remains unchanged', async () => {
  const options = { userCount: 0, user: null };
  const selected = await login(makeHarness(options), { workflow: 'identity-check', password: 'local-legacy-fixture' });
  assert.equal(selected.res.location, '/login?error=1&workflow=identity-check');
  assert.deepEqual(selected.req.session, {});
  const normal = await login(makeHarness(options), { password: 'local-legacy-fixture' });
  assert.equal(normal.res.location, '/command-deck'); assert.equal(normal.req.session.user.id, null);
});

test('admin without tenant remains tenant 1; workflow and query cannot select tenant 10', async () => {
  const result = await login(makeHarness(), { workflow: 'identity-check', client_id: 10, clientId: 10 });
  assert.equal(result.req.session.active_client_id, 1);
  assert.equal(result.req.session.user.client_id, null);
  assert.equal(result.res.location, '/api/me');
});

test('selected and ordinary sign-in rotate the session identically and drop pre-login state (fixation defence)', async () => {
  const current = { id: 'local-fixture-existing-session', marker: 'preserve', impersonation: { active: true, effectiveUser: { id: 5, role: 'ao' } } };
  const selectedSession = structuredClone(current), normalSession = structuredClone(current);
  const selected = await login(makeHarness(), { workflow: 'identity-check', _csrf: 'untrusted-fixture', role: 'manager' }, selectedSession);
  const normal = await login(makeHarness(), { _csrf: 'untrusted-fixture', role: 'manager' }, normalSession);
  // The identity check stays a landing difference only: the authenticated session is identical.
  assert.deepEqual(JSON.parse(JSON.stringify(selected.req.session)), JSON.parse(JSON.stringify(normal.req.session)));
  assert.equal(selectedSession.rotations, 1); assert.equal(normalSession.rotations, 1);
  assert.equal(selected.req.session.id, undefined, 'the pre-login session identity is not carried over');
  assert.equal(selected.req.session.marker, undefined);
  assert.equal(selected.req.session.impersonation, undefined, 'pre-login impersonation state is dropped');
  assert.equal(selected.req.session.user.id, 73); assert.equal(selected.req.session.user.role, 'admin');
  // Login still has no CSRF token check; untrusted body fields never choose identity or role.
});

test('cached initializer failure prevents account lookup and authentication', async () => {
  const h = makeHarness({ initFailure: true });
  await assert.rejects(login(h, { workflow: 'identity-check' }), /cached initializer failure/);
  assert.equal(h.queries.length, 0);
});

test('real auth initializer retains its cached failure without request-time retries', async () => {
  let count = 0; const auth = loadAuth({ async query() { count++; throw new Error('fixture schema unavailable'); } });
  const first = auth.initAuth(); await assert.rejects(first, /fixture schema unavailable/);
  assert.equal(auth.initAuth(), first); await assert.rejects(auth.initAuth()); assert.equal(count, 1);
});

test('real auth initializer retains cached success without later schema operations', async () => {
  let count = 0; const auth = loadAuth({ async query(sql) { count++; return { rows: /SELECT COUNT/.test(sql) ? [{ count: 1 }] : [] }; } });
  const first = auth.initAuth(); await first; const afterBoot = count;
  assert.equal(auth.initAuth(), first); await auth.initAuth(); assert.equal(count, afterBoot);
});

test('native HTTP form redirects once to authenticated, noncacheable JSON and no dashboard', async t => {
  const h = await startHttpHarness(t);
  const page = await fetch(`${h.origin}/login?workflow=identity-check`);
  assert.equal(page.headers.get('set-cookie'), null); assert.equal(h.savedSessions.length, 0);
  const posted = await fetch(`${h.origin}/login`, { method: 'POST', redirect: 'manual', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ email: 'identity@example.test', password: fixturePassword, workflow: 'identity-check' }) });
  assert.equal(posted.status, 302); assert.equal(posted.headers.get('location'), '/api/me');
  const cookie = posted.headers.get('set-cookie').split(';')[0];
  const landed = await fetch(`${h.origin}/api/me`, { headers: { Cookie: cookie } });
  assert.equal(landed.status, 200); assert.match(landed.headers.get('content-type'), /application\/json/);
  assert.equal(landed.headers.get('cache-control'), 'no-store');
  const payload = await landed.json();
  assert.equal(payload.user.id, 73); assert.equal(payload.user.role, 'admin'); assert.equal(payload.active_client_id, 1);
  assert.equal(payload.impersonation.active, false); assert.equal(payload.effective_user, null);
  assert.deepEqual(h.requests, ['GET /login', 'POST /login', 'GET /api/me']);
  assert.deepEqual(h.queries.map(q => q.text.split(' ')[0]), ['SELECT', 'UPDATE', 'SELECT']);
});

test('unauthenticated identity endpoint remains unauthorized', async t => {
  const h = await startHttpHarness(t);
  const result = await fetch(`${h.origin}/api/me`);
  assert.equal(result.status, 401); assert.deepEqual(await result.json(), { error: 'Unauthorized' });
  assert.equal(h.queries.length, 0); assert.equal(h.savedSessions.length, 0);
});
