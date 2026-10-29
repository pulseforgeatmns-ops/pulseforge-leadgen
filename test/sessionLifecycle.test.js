'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const remembered = require('../utils/sessionLifecycle');
const fresh = require('../utils/freshConfirmation');
const { makeHarness, login, source } = require('./helpers/loginIdentityHarness');

const DAY = 24 * 60 * 60 * 1000;

// Mirrors express-session: regenerate() drops every key and starts a new cookie.
function fakeSession(extra = {}) {
  const session = { cookie: { maxAge: DAY, originalMaxAge: DAY }, ...extra };
  Object.defineProperty(session, 'regenerated', { value: 0, writable: true });
  Object.defineProperty(session, 'regenerate', { configurable: true, value(cb) {
    for (const key of Object.keys(this)) delete this[key];
    this.cookie = { maxAge: DAY, originalMaxAge: DAY };
    this.regenerated += 1; cb();
  } });
  return session;
}
function guardWith(row, { error, now } = {}) {
  const queries = [];
  const pool = { async query(sql, params) { queries.push({ sql, params }); if (error) throw error; return { rows: row ? [row] : [] }; } };
  return { queries, guard: remembered.createSessionAccountGuard({ pool, now }) };
}
function run(guard, session) {
  const res = { headers: {}, set(k, v) { this.headers[k] = v; return this; }, status(c) { this.statusCode = c; return this; },
    json(b) { this.body = b; return this; } };
  return new Promise(resolve => {
    const req = { session };
    Promise.resolve(guard(req, res, error => resolve({ req, res, error, next: true }))).then(() => {
      if (res.statusCode) resolve({ req, res, next: false });
    });
  });
}
const account = (over = {}) => ({ id: 9, role: 'admin', active: true, email_verified: true, password_change_required: false,
  password_hash: '$2a$04$hash-one', ...over });
function sessionFor(user, { remember = true, now = Date.now() } = {}) {
  const session = fakeSession({ authenticated: true, user: { id: user.id, role: user.role, password_change_required: false } });
  remembered.applyLoginPersistence({ session, headers: {} }, user, remember, now);
  return session;
}
const rememberedFor = (user, now) => sessionFor(user, { now });

test('remember checkbox values are opt-in only', () => {
  for (const value of ['on', 'true', '1', 'yes', 'ON']) assert.equal(remembered.wantsRemember({ remember: value }), true);
  for (const value of [undefined, '', 'off', 'false', '0', {}]) assert.equal(remembered.wantsRemember({ remember: value }), false);
  assert.equal(remembered.wantsRemember(undefined), false);
});

test('remembered login sets a 30-day lifetime that survives express-session touch; unremembered keeps 24h', () => {
  const user = account();
  const session = fakeSession();
  remembered.applyLoginPersistence({ session, headers: {} }, user, true, 1000);
  assert.equal(session.cookie.originalMaxAge, 30 * DAY);
  assert.equal(session.remembered.since, 1000);
  assert.equal(session.remembered.expiresAt, 1000 + 30 * DAY);
  assert.match(session.credentialTag, /^[a-f0-9]{32}$/);
  assert.ok(!JSON.stringify(session).includes(user.password_hash), 'the password hash is never stored in the session');
  // A later login on the same session without the box falls back to the existing policy.
  remembered.applyLoginPersistence({ session, headers: {} }, user, false);
  assert.equal(session.cookie.originalMaxAge, DAY);
  assert.equal(session.remembered, undefined);
  assert.match(session.credentialTag, /^[a-f0-9]{32}$/, 'ordinary sessions are bound to the password too');
  // The env-password fallback has no account and is never remembered.
  const legacy = fakeSession();
  remembered.applyLoginPersistence({ session: legacy, headers: {} }, null, true);
  assert.equal(legacy.remembered, undefined);
  assert.equal(legacy.cookie.originalMaxAge, DAY);
});

test('cookie is marked Secure on HTTPS (direct or via the Railway proxy), not on local HTTP', () => {
  const https = fakeSession();
  remembered.applyLoginPersistence({ session: https, headers: { 'x-forwarded-proto': 'https' } }, account(), true);
  assert.equal(https.cookie.secure, true);
  const http = fakeSession();
  remembered.applyLoginPersistence({ session: http, headers: {} }, account(), true);
  assert.equal(http.cookie.secure, undefined);
  assert.equal(remembered.isSecureRequest({ secure: true, headers: {} }), true);
  assert.equal(remembered.isSecureRequest({ headers: { 'x-forwarded-proto': 'http, https' } }), false);
});

test('guard ignores signed-out sessions and the env-password fallback (no account row)', async () => {
  const { guard, queries } = guardWith(account());
  assert.equal((await run(guard, fakeSession())).next, true);
  assert.equal((await run(guard, fakeSession({ authenticated: true, user: { id: null, role: 'admin' } }))).next, true);
  assert.equal((await run(guard, undefined)).next, true);
  assert.equal(queries.length, 0);
});

test('guard checks ordinary sessions and sessions created before credential tags', async () => {
  const user = account({ id: 150 + Math.floor(Math.random() * 1e6) });
  const ordinary = sessionFor(user, { remember: false });
  const { guard, queries } = guardWith(user);
  assert.equal((await run(guard, ordinary)).next, true);
  assert.equal(ordinary.regenerated, 0);
  assert.equal(queries.length, 1, 'unremembered sessions are revalidated too');
  // A pre-deploy session (no credential tag) is still checked on active/role.
  const legacy = fakeSession({ authenticated: true, user: { id: user.id + 1, role: 'admin' } });
  assert.equal((await run(guardWith(user).guard, legacy)).next, true);
  assert.equal(legacy.regenerated, 0);
  const gone = fakeSession({ authenticated: true, user: { id: user.id + 2, role: 'admin' } });
  await run(guardWith({ ...user, active: false }).guard, gone);
  assert.equal(gone.regenerated, 1, 'pre-deploy sessions of a deactivated account end too');
});

test('guard keeps a valid remembered session and caches the account check briefly', async () => {
  const user = account({ id: 101 });
  let clock = 5000;
  const { guard, queries } = guardWith(user, { now: () => clock });
  const session = rememberedFor(user, clock);
  assert.equal((await run(guard, session)).next, true);
  assert.equal(session.regenerated, 0);
  await run(guard, session);
  assert.equal(queries.length, 1, 'second request inside the interval uses the cached result');
  clock += remembered.REVALIDATE_INTERVAL_MS + 1;
  await run(guard, session);
  assert.equal(queries.length, 2, 're-checked after the interval');
  remembered.forgetUser(101);
  await run(guard, session);
  assert.equal(queries.length, 3, 'forgetUser forces an immediate re-check');
});

for (const [name, change] of [
  ['deactivated account', { active: false }],
  ['deleted account', null],
  ['role change', { role: 'viewer' }],
  ['password change', { password_hash: '$2a$04$hash-two' }],
  ['forced password reset', { password_change_required: true }],
  ['unverified email', { email_verified: false }],
]) {
  for (const remember of [true, false]) test(`guard ends a${remember ? ' remembered' : 'n ordinary'} session on ${name}`, async () => {
    const id = 200 + Math.floor(Math.random() * 1e6);
    const user = account({ id });
    const session = sessionFor(user, { remember });
    const { guard } = guardWith(change === null ? null : { ...user, ...change });
    const result = await run(guard, session);
    assert.equal(result.next, true);
    assert.equal(session.regenerated, 1, 'session regenerated (old store row destroyed)');
    assert.equal(session.user, undefined, 'downstream handlers see a signed-out session');
  });
}

test('guard enforces the absolute 30-day cap even if the cookie was re-issued', async () => {
  const user = account({ id: 301 });
  const session = rememberedFor(user, 0);
  const { guard, queries } = guardWith(user, { now: () => 30 * DAY + 1 });
  await run(guard, session);
  assert.equal(session.regenerated, 1);
  assert.equal(queries.length, 0);
});

test('guard fails closed (503) when the account cannot be verified', async () => {
  const user = account({ id: 302 });
  const session = rememberedFor(user);
  const { guard } = guardWith(null, { error: new Error('db down') });
  const result = await run(guard, session);
  assert.equal(result.next, false);
  assert.equal(result.res.statusCode, 503);
  assert.deepEqual(result.res.body, { error: 'session_check_unavailable' });
});

test('sign-in rotates the session ID and drops pre-login data; a failed sign-in does not', async () => {
  const harness = makeHarness();
  const planted = fakeSession({ attackerMarker: 'planted', freshConfirmation: { userId: '73', purpose: 'scout_research_approval' } });
  const ok = await login(harness, {}, planted);
  assert.equal(ok.res.location, '/command-deck');
  assert.equal(planted.regenerated, 1, 'exactly one rotation on success');
  assert.equal(planted.attackerMarker, undefined);
  assert.equal(planted.freshConfirmation, undefined, 'a step-up never survives a new sign-in');
  assert.equal(planted.authenticated, true);
  const identity = fakeSession({ attackerMarker: 'planted' });
  assert.equal((await login(harness, { workflow: 'identity-check' }, identity)).res.location, '/api/me');
  assert.equal(identity.regenerated, 1, 'identity-check sign-in rotates too');
  const failed = fakeSession({ keep: 'me' });
  assert.equal((await login(harness, { password: 'wrong' }, failed)).res.location, '/login?error=1');
  assert.equal(failed.regenerated, 0); assert.equal(failed.keep, 'me'); assert.equal(failed.authenticated, undefined);
  const rotationFails = fakeSession();
  Object.defineProperty(rotationFails, 'regenerate', { value: cb => cb(new Error('store down')) });
  const outage = await login(harness, {}, rotationFails);
  assert.equal(outage.res.statusCode, 503, 'no sign-in on an unrotated session');
  assert.equal(outage.req.session, null);
});

test('login route applies Remember me only when selected and never for the identity check', async () => {
  const harness = makeHarness();
  const remember = await login(harness, { remember: 'on' }, fakeSession());
  assert.equal(remember.res.location, '/command-deck');
  assert.equal(remember.req.session.cookie.originalMaxAge, 30 * DAY);
  assert.ok(remember.req.session.remembered);
  const plain = await login(harness, {}, fakeSession());
  assert.equal(plain.req.session.cookie.originalMaxAge, DAY);
  assert.equal(plain.req.session.remembered, undefined);
  const identity = await login(harness, { remember: 'on', workflow: 'identity-check' }, fakeSession());
  assert.equal(identity.res.location, '/api/me');
  assert.equal(identity.req.session.remembered, undefined);
  const wrong = await login(harness, { remember: 'on', password: 'nope' }, fakeSession());
  assert.equal(wrong.res.location, '/login?error=1');
  assert.equal(wrong.req.session.remembered, undefined);
});

test('login form supports password-manager autofill and an explicit Remember me checkbox', async () => {
  const harness = makeHarness();
  const res = { set() { return this; }, send(body) { this.body = body; return this; } };
  harness.handlers['GET /login'][0]({ query: {} }, res);
  assert.match(res.body, /<form method="POST" action="\/login">/);
  assert.match(res.body, /type="email" name="email"[^>]*autocomplete="username"/);
  assert.match(res.body, /type="password" name="password"[^>]*autocomplete="current-password"/);
  assert.match(res.body, /<input id="login-remember" type="checkbox" name="remember" value="on">/);
  assert.doesNotMatch(res.body, /id="login-remember"[^>]*checked/, 'never pre-checked');
  assert.match(res.body, /name="viewport"/);
  const identity = { set() { return this; }, send(body) { this.body = body; return this; } };
  harness.handlers['GET /login'][0]({ query: { workflow: 'identity-check' } }, identity);
  assert.doesNotMatch(identity.body, /login-remember/);
  assert.doesNotMatch(res.body + source('public/scout-research-approval.js'), /\b(?:localStorage|sessionStorage|document\.cookie)\s*[.[=]/);
});

test('session cookie is HttpOnly, SameSite=Lax and Secure on HTTPS; logout destroys the store row', () => {
  const server = source('server.js');
  assert.match(server, /cookie: \{ maxAge: sessionLifecycle\.DEFAULT_MAX_AGE_MS, httpOnly: true, sameSite: 'lax', secure: 'auto' \}/);
  assert.match(server, /proxy: true,/);
  assert.match(server, /app\.use\(sessionLifecycle\.createSessionAccountGuard\(\{ pool \}\)\);/);
  assert.match(server, /req\.session\.destroy\(\(\) => done\(\)\)/);
  assert.match(server, /res\.clearCookie\('connect\.sid'/);
});

test('fresh confirmation lockout after repeated wrong passwords', () => {
  const user = `lock-${Math.random()}`;
  for (let i = 0; i < fresh.MAX_FAILURES - 1; i++) fresh.recordFailure(user, 1000);
  assert.equal(fresh.lockedUntil(user, 1000), 0);
  fresh.recordFailure(user, 1000);
  assert.ok(fresh.lockedUntil(user, 1000) > 1000);
  assert.equal(fresh.lockedUntil(user, 1000 + fresh.LOCKOUT_MS + 1), 0);
  fresh.recordFailure(user, 1000); fresh.clearFailures(user);
  assert.equal(fresh.lockedUntil(user, 1000), 0);
});

test('fresh confirmation marker is purpose-bound and short-lived', async () => {
  const m = fresh.marker(7, 'scout_research_approval', 1000);
  assert.deepEqual(m, { userId: '7', purpose: 'scout_research_approval', at: 1000, expiresAt: 1000 + fresh.VALID_MS });
  const db = { query: async () => { throw new Error('must not query'); } };
  assert.equal(await fresh.consume(db, { sessionId: 's', userId: 7, purpose: 'other' }), false);
  assert.equal(await fresh.consume(db, { sessionId: '', userId: 7, purpose: 'scout_research_approval' }), false);
});

test('revokeUserSessions deletes every session row of the account, optionally keeping one', async () => {
  const calls = [];
  const db = { async query(sql, params) { calls.push({ sql, params }); return { rowCount: 2 }; } };
  assert.equal(await remembered.revokeUserSessions(db, 42), 2);
  assert.match(calls[0].sql, /DELETE FROM "session" WHERE sess::jsonb #>> '\{user,id\}' = \$1/);
  assert.deepEqual(calls[0].params, ['42', null]);
  await remembered.revokeUserSessions(db, 42, { exceptSid: 'keep-me' });
  assert.deepEqual(calls[1].params, ['42', 'keep-me']);
});

test('every sign-in path rotates the session before authenticating it', () => {
  const server = source('server.js');
  const login = server.slice(server.indexOf("app.post('/login'"), server.indexOf("app.get('/logout'"));
  const rotations = login.match(/if \(!await rotateLoginSession\(\)\) return;\n\s+req\.session\.user = /g) || [];
  assert.equal(rotations.length, 2, 'account login and the env-password fallback both rotate first');
  const registration = source('routes/registration.js');
  assert.equal((registration.match(/rotateSession\(req\)[^\n]*\n(?:[^\n]*\n)?\s+(?:const session = )?establishRegisteredSession\(req\.session/g) || []).length, 2,
    'both email-verification sign-ins rotate first');
  // Deactivation, role/tenant change, reset and deletion revoke all sessions; self password change keeps only its own.
  const users = source('routes/users.js');
  assert.match(users, /updated\.active !== existing\.active \|\| updated\.role !== existing\.role[\s\S]{0,120}revokeUserSessions\(pool, updated\.id\)/);
  assert.equal((users.match(/revokeUserSessions\(pool, req\.params\.id\)/g) || []).length, 2);
  assert.match(source('routes/pilotOnboarding.js'), /revokeUserSessions\(pool, userId, \{ exceptSid: req\.sessionID \|\| null \}\)/);
});

test('the env-password fallback never matches an unset password', async () => {
  const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
  const server = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  const routes = server.slice(server.indexOf("app.get('/login'"), server.indexOf("app.get('/logout'"));
  const handlers = {};
  vm.runInNewContext(routes, { app: { get() {}, post(route, fn) { handlers[route] = fn; } }, pool: { query: async () => ({ rows: [] }) },
    bcrypt: {}, sessionLifecycle: remembered, process: { env: {} }, initAuth: async () => {}, getUserCount: async () => 0 });
  const session = fakeSession();
  const res = { set() { return this; }, status(c) { this.statusCode = c; return this; }, send() { return this; },
    redirect(l) { this.location = l; return this; } };
  await handlers['/login']({ body: { email: 'x@example.test' }, session }, res);
  assert.equal(res.location, '/login?error=1');
  assert.equal(session.authenticated, undefined);
  assert.equal(session.regenerated, 0);
});
