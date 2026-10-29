'use strict';
// Scout research approval page + Remember me on disposable PostgreSQL. Real
// login handlers, express-session/connect-pg-simple, the remembered-session
// guard, the page router and the unchanged tenant-outreach research endpoints.
// No discovery cycle runs, no provider is called and nothing is sent.
const { startApprovalHarness, ADMIN, AO, OTHER_ADMIN } = require('./helpers/scoutResearchApprovalHarness');
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');

const API = '/api/v1/tenant-outreach/anchor-program/';
const CONFIRM = '/api/v1/auth/fresh-confirmation';
const CONTEXT = '/api/v1/admin/scout-research-approval/context';
const PAGE = '/admin/scout-research-approval';
const DAY = 86400000;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const iso = ms => new Date(Math.floor((Date.now() + ms) / 1000) * 1000).toISOString();
const near = (actual, expected, slack = 120000) => Math.abs(actual - expected) < slack;

test('Scout research approval and Remember me (synthetic Tenant 10)', { timeout: 600000 }, async t => {
  const h = await startApprovalHarness(); t.after(() => h.close());
  const { http, pool } = h;
  const admin = h.adminCookie;
  const count = async sql => (await pool.query(sql)).rows[0].n;
  const confirm = (cookie, password = h.fixturePassword, extra = {}) =>
    http(CONFIRM, { method: 'POST', cookie, body: { purpose: 'scout_research_approval', password }, ...extra });
  const closedEnv = () => ({ maintenance: process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE, paused: process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED,
    outbound: process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED });
  const envBefore = closedEnv();
  const queueBefore = await count('SELECT count(*)::int n FROM scout_unenriched');

  await t.test('signed-out, AO and wrong-tenant admin cannot use approvals', async () => {
    const page = await http(PAGE);
    assert.equal(page.status, 302); assert.equal(page.headers.get('location'), '/login');
    assert.equal((await http(CONTEXT)).status, 401);
    assert.equal((await http(API + 'research-mission/preview', { method: 'POST', body: { approvalRef: 'x' } })).status, 401);

    const ao = await h.loginAs(AO);
    assert.equal(ao.response.headers.get('location'), '/ao');
    const aoPage = await http(PAGE, { cookie: ao.cookie });
    assert.equal(aoPage.status, 302); assert.equal(aoPage.headers.get('location'), '/ao');
    assert.equal((await http(CONTEXT, { cookie: ao.cookie })).status, 403);
    assert.equal((await confirm(ao.cookie)).status, 403);
    for (const op of ['research-mission/preview', 'research-only-grant/inspect', 'research-only-grant/revoke']) {
      const res = await http(API + op, { method: 'POST', cookie: ao.cookie, body: {} });
      assert.equal(res.status, 403, op); assert.equal(res.json.error, 'research_only_grant_forbidden');
    }

    const other = await h.loginAs(OTHER_ADMIN);
    assert.equal((await http(PAGE, { cookie: other.cookie })).status, 200, 'any admin can open the page');
    const ctx = (await http(CONTEXT, { cookie: other.cookie })).json;
    assert.equal(ctx.approvalReady, false); assert.equal(ctx.anchorSelected, false); assert.equal(ctx.activeClientId, 1);
    const preview = await http(API + 'research-mission/preview', { method: 'POST', cookie: other.cookie, body: { approvalRef: 'x' } });
    assert.equal(preview.status, 403, 'the existing backend check still decides');
  });

  await t.test('admin page is display-only, CSP-locked and leaks no session material', async () => {
    const page = await http(PAGE, { cookie: admin });
    assert.equal(page.status, 200);
    assert.match(page.headers.get('content-security-policy'), /script-src 'self'/);
    assert.equal(page.headers.get('cache-control'), 'no-store');
    assert.match(page.text, /Preview mission — no changes/);
    for (const asset of ['style.css', 'controller.js', 'tokens.css']) assert.equal((await http(`${PAGE}/assets/${asset}`, { cookie: admin })).status, 200);
    const ctx = await http(CONTEXT, { cookie: admin });
    assert.equal(ctx.json.approvalReady, true); assert.equal(ctx.json.anchorSelected, true);
    assert.doesNotMatch(ctx.text, /synthetic-research-admin-session|sid|sessionHash|password/i);
  });

  let mission;
  await t.test('mission preview shows segments, eight cities, hash and blockers; install needs a fresh password', async () => {
    const res = await http(API + 'research-mission/preview', { method: 'POST', cookie: admin, body: { approvalRef: 'synthetic-ui-mission-1' } });
    assert.equal(res.status, 200);
    mission = res.json;
    assert.equal(mission.status, 'previewed'); assert.deepEqual(mission.blockers, []);
    assert.deepEqual(mission.eligibleSubsegments, ['managed_facilities', 'restaurant_foh']);
    assert.equal(mission.cities.length, 8); assert.equal(mission.outboundEligible, false);
    assert.match(mission.missionHash, /^[a-f0-9]{64}$/);
    const body = { approvalRef: 'synthetic-ui-mission-1', mission: mission.mission, missionHash: mission.missionHash };

    const noStepUp = await http(API + 'research-mission/install', { method: 'POST', cookie: admin, body });
    assert.equal(noStepUp.status, 403); assert.equal(noStepUp.json.code, 'fresh_confirmation_required');
    assert.equal(noStepUp.json.writesMayHaveOccurred, false);
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_missions'), 0);

    assert.equal((await confirm(admin, h.fixturePassword, { headers: { origin: 'https://evil.example' } })).status, 403, 'cross-origin step-up');
    const wrong = await confirm(admin, 'not-the-password');
    assert.equal(wrong.status, 401); assert.equal(wrong.json.error, 'fresh_confirmation_rejected');
    // A confirmation recorded on another session of the same admin does not carry over.
    const otherSession = await h.loginAs(ADMIN);
    assert.equal((await confirm(otherSession.cookie)).status, 200);
    assert.equal((await http(API + 'research-mission/install', { method: 'POST', cookie: admin, body })).status, 403);

    const ok = await confirm(admin);
    assert.equal(ok.status, 200); assert.equal(ok.json.confirmed, true); assert.equal(ok.json.singleUse, true);
    assert.doesNotMatch(ok.text, new RegExp(h.fixturePassword));
    const installed = await http(API + 'research-mission/install', { method: 'POST', cookie: admin, body });
    assert.equal(installed.status, 200); assert.equal(installed.json.installed, true);
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_missions'), 1);
    const replay = await http(API + 'research-mission/install', { method: 'POST', cookie: admin, body });
    assert.equal(replay.status, 403, 'a confirmation is single-use');

    // An expired confirmation is refused.
    assert.equal((await confirm(admin)).status, 200);
    await pool.query(`UPDATE "session" SET sess=jsonb_set(sess::jsonb,'{freshConfirmation,expiresAt}','1')::json WHERE sid='synthetic-research-admin-session'`);
    assert.equal((await http(API + 'research-mission/install', { method: 'POST', cookie: admin, body })).status, 403);

    const again = (await http(API + 'research-mission/preview', { method: 'POST', cookie: admin, body: { approvalRef: 'synthetic-ui-mission-1' } })).json;
    assert.equal(again.status, 'already_installed'); assert.equal(again.installedMissionHash, mission.missionHash);
  });

  const grantBody = (ref, expiresIn = 3 * DAY) => ({ approvalRef: ref, expiresAt: iso(expiresIn),
    statement: 'Synthetic operator approval for research-only Scout discovery.', researchMissionId: mission.mission.id });
  let grantId;
  await t.test('grant preview shows limits and blockers; install needs a fresh password; inspect and revoke', async () => {
    process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'true';
    const blocked = (await http(API + 'research-only-grant/preview', { method: 'POST', cookie: admin, body: grantBody('synthetic-ui-grant-blocked') })).json;
    process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = envBefore.outbound;
    assert.equal(blocked.status, 'blocked'); assert.equal(blocked.installable, false);
    assert.ok(blocked.blockers.includes('research_only_closed_environment_required'));

    const preview = (await http(API + 'research-only-grant/preview', { method: 'POST', cookie: admin, body: grantBody('synthetic-ui-grant-1') })).json;
    assert.equal(preview.status, 'previewed'); assert.equal(preview.installable, true);
    assert.equal(preview.maxCycles, 8); assert.equal(preview.dailyGoogleUsdMicros, 5000000);
    assert.equal(preview.expectedRevision, 'b'.repeat(40));
    assert.equal(preview.manifest.researchMissionHash, mission.missionHash);
    const body = { manifest: preview.manifest, manifestHash: preview.manifestHash };
    assert.equal((await http(API + 'research-only-grant/install', { method: 'POST', cookie: admin, body })).status, 403);
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_only_grants'), 0);
    assert.equal((await confirm(admin)).status, 200);
    const installed = await http(API + 'research-only-grant/install', { method: 'POST', cookie: admin, body });
    assert.equal(installed.status, 200); assert.equal(installed.json.installed, true);
    grantId = installed.json.grantId;

    const inspect = (await http(API + 'research-only-grant/inspect', { method: 'POST', cookie: admin, body: {} })).json;
    assert.equal(inspect.status, 'installed'); assert.equal(inspect.cyclesConsumed, 0); assert.equal(inspect.reservedUsdMicros, 0);
    assert.equal(inspect.maxCycles, 8); assert.equal(inspect.grantId, grantId);

    const second = (await http(API + 'research-only-grant/preview', { method: 'POST', cookie: admin, body: grantBody('synthetic-ui-grant-2') })).json;
    assert.ok(second.blockers.includes('research_only_other_grant_open'));

    const badRevoke = await http(API + 'research-only-grant/revoke', { method: 'POST', cookie: admin, body: { grantId, approvalReference: 'bad ref!' } });
    assert.equal(badRevoke.status, 409); assert.equal(badRevoke.json.code, 'research_only_explicit_revocation_required');
    const revoked = await http(API + 'research-only-grant/revoke', { method: 'POST', cookie: admin,
      body: { grantId, approvalReference: 'synthetic-ui-revoke-1' } });
    assert.equal(revoked.status, 200, 'revocation never needs a password step-up'); assert.equal(revoked.json.revoked, true);
    assert.equal((await http(API + 'research-only-grant/inspect', { method: 'POST', cookie: admin, body: { grantId } })).json.status, 'revoked');
    assert.equal((await http(API + 'research-only-grant/inspect', { method: 'POST', cookie: admin, body: {} })).json.status, 'not_installed');
    const reinstall = await http(API + 'research-only-grant/install', { method: 'POST', cookie: admin, body });
    assert.equal(reinstall.status, 403, 'still needs a fresh confirmation before the server even considers it');
  });

  await t.test('an expired grant is reported as expired, blocks a new grant, and can be revoked', async () => {
    const preview = (await http(API + 'research-only-grant/preview', { method: 'POST', cookie: admin, body: grantBody('synthetic-ui-grant-short', 3000) })).json;
    assert.equal(preview.installable, true);
    assert.equal((await confirm(admin)).status, 200);
    const installed = await http(API + 'research-only-grant/install', { method: 'POST', cookie: admin,
      body: { manifest: preview.manifest, manifestHash: preview.manifestHash } });
    assert.equal(installed.status, 200);
    await sleep(Math.max(0, Date.parse(preview.expiresAt) - Date.now()) + 300);
    const inspect = (await http(API + 'research-only-grant/inspect', { method: 'POST', cookie: admin, body: {} })).json;
    assert.equal(inspect.status, 'expired');
    const next = (await http(API + 'research-only-grant/preview', { method: 'POST', cookie: admin, body: grantBody('synthetic-ui-grant-3') })).json;
    assert.ok(next.blockers.includes('research_only_other_grant_open'));
    const revoked = await http(API + 'research-only-grant/revoke', { method: 'POST', cookie: admin,
      body: { grantId: inspect.grantId, approvalReference: 'synthetic-ui-revoke-expired' } });
    assert.equal(revoked.status, 200);
  });

  await t.test('step-up locks after repeated wrong passwords', async () => {
    const other = await h.loginAs(OTHER_ADMIN);
    for (let i = 0; i < 5; i++) assert.equal((await confirm(other.cookie, 'wrong')).status, 401);
    const locked = await confirm(other.cookie);
    assert.equal(locked.status, 429); assert.equal(locked.json.error, 'fresh_confirmation_locked');
  });

  await t.test('Remember me: 30-day HttpOnly/SameSite cookie, Secure over HTTPS, kept across touches', async () => {
    const plain = await h.loginAs(ADMIN);
    assert.match(plain.raw, /HttpOnly/); assert.match(plain.raw, /SameSite=Lax/i); assert.doesNotMatch(plain.raw, /Secure/);
    assert.ok(near(Date.parse(/Expires=([^;]+)/.exec(plain.raw)[1]), Date.now() + DAY, 5000), 'unremembered keeps the 24h policy');

    const remember = await h.loginAs(ADMIN, { remember: true });
    assert.match(remember.raw, /HttpOnly/); assert.match(remember.raw, /SameSite=Lax/i);
    assert.ok(near(Date.parse(/Expires=([^;]+)/.exec(remember.raw)[1]), Date.now() + 30 * DAY, 5000));
    const sid = decodeURIComponent(remember.cookie.split('=')[1]).slice(2).split('.')[0];
    const row = async () => (await pool.query('SELECT sess, expire FROM "session" WHERE sid=$1', [sid])).rows[0];
    const stored = await row();
    assert.ok(stored.sess.remembered); assert.ok(near(+stored.expire, Date.now() + 30 * DAY));
    assert.doesNotMatch(JSON.stringify(stored.sess), /\$2[aby]\$/, 'no password hash in the session');
    // A fresh client holding only the persisted cookie (a restarted browser) is still signed in,
    // and express-session's touch does not shrink the lifetime back to 24h.
    assert.equal((await http(CONTEXT, { cookie: remember.cookie })).status, 200);
    assert.ok(near(+(await row()).expire, Date.now() + 30 * DAY));

    const https = await h.loginAs(ADMIN, { remember: true, headers: { 'x-forwarded-proto': 'https' } });
    assert.match(https.raw, /; Secure/);
  });

  const COOKIE = { originalMaxAge: DAY, expires: '2099-01-01T00:00:00.000Z', httpOnly: true, path: '/' };
  const fresh = require('../utils/freshConfirmation');
  const lifecycle = require('../utils/sessionLifecycle');
  const signedIn = async cookie => (await http('/api/me', { cookie })).status === 200;

  await t.test('sign-in rotates the session ID: a planted pre-login session never becomes authenticated', async () => {
    const planted = await h.seedSession('attacker-planted-session', { cookie: COOKIE, marker: 'attacker',
      freshConfirmation: fresh.marker(ADMIN.id, 'scout_research_approval') });
    const victim = await h.loginAs(ADMIN, { cookie: planted });
    assert.equal(victim.response.headers.get('location'), '/command-deck');
    assert.notEqual(h.sidOf(victim.cookie), 'attacker-planted-session', 'a new session ID is issued');
    assert.equal(await h.sessionRow(planted), null, 'the planted row is deleted');
    assert.equal(await signedIn(planted), false, 'the attacker cookie is not signed in');
    assert.equal(await signedIn(victim.cookie), true);
    const row = await h.sessionRow(victim.cookie);
    assert.equal(row.sess.marker, undefined); assert.equal(row.sess.freshConfirmation, undefined, 'no pre-login state carried over');

    // Signing in again while signed in also rotates and ends the previous ID.
    const again = await h.loginAs(ADMIN, { cookie: victim.cookie });
    assert.notEqual(h.sidOf(again.cookie), h.sidOf(victim.cookie));
    assert.equal(await signedIn(victim.cookie), false); assert.equal(await signedIn(again.cookie), true);

    // A failed sign-in neither authenticates nor rotates the presented session.
    const probe = await h.seedSession('attacker-probe-session', { cookie: COOKIE, marker: 'attacker' });
    const failed = await h.loginAs(ADMIN, { cookie: probe, password: 'wrong-password' });
    assert.equal(failed.response.headers.get('location'), '/login?error=1');
    assert.equal(failed.cookie, null, 'no new cookie on failure');
    assert.equal((await h.sessionRow(probe)).sess.authenticated, undefined);
    assert.equal(await signedIn(probe), false);
  });

  await t.test('native tenant selection still works on a rotated session and enables approvals', async () => {
    const other = await h.loginAs(OTHER_ADMIN);
    assert.equal((await http(CONTEXT, { cookie: other.cookie })).json.anchorSelected, false);
    const form = await http('/identity-check/tenant', { cookie: other.cookie });
    assert.equal(form.status, 200, form.text);
    const csrf = /name="csrf" value="([a-f0-9]{64})"/.exec(form.text)[1];
    const selected = await http('/identity-check/tenant', { method: 'POST', cookie: other.cookie, headers: { origin: h.origin },
      form: { workflow: /name="workflow" value="([^"]+)"/.exec(form.text)[1], client_id: '10', csrf, confirm: 'select-anchor' } });
    assert.equal(selected.status, 303, 'selection: ' + selected.text);
    const ctx = (await http(CONTEXT, { cookie: other.cookie })).json;
    assert.equal(ctx.anchorSelected, true); assert.equal(ctx.approvalReady, true);
    const preview = await http(API + 'research-mission/preview', { method: 'POST', cookie: other.cookie, body: { approvalRef: 'synthetic-ui-mission-1' } });
    assert.equal(preview.status, 200); assert.equal(preview.json.status, 'already_installed');
    // Signing in again starts a new session at the account's home tenant; the selection does not carry over.
    const again = await h.loginAs(OTHER_ADMIN, { cookie: other.cookie });
    assert.equal((await http(CONTEXT, { cookie: again.cookie })).json.anchorSelected, false);
  });

  await t.test('remembered and rotated sessions still need a fresh password; grants bind to the approving session', async () => {
    const remember = await h.loginAs(ADMIN, { remember: true });
    assert.equal((await http(CONTEXT, { cookie: remember.cookie })).json.rememberedSession, true);
    const preview = (await http(API + 'research-only-grant/preview', { method: 'POST', cookie: remember.cookie, body: grantBody('synthetic-ui-grant-remembered') })).json;
    assert.equal(preview.installable, true);
    const body = { manifest: preview.manifest, manifestHash: preview.manifestHash };
    const refused = await http(API + 'research-only-grant/install', { method: 'POST', cookie: remember.cookie, body });
    assert.equal(refused.status, 403); assert.equal(refused.json.code, 'fresh_confirmation_required');

    // A preview made before signing in again cannot be installed from the new session.
    const relogged = await h.loginAs(ADMIN, { remember: true, cookie: remember.cookie });
    assert.equal((await confirm(relogged.cookie)).status, 200);
    const stale = await http(API + 'research-only-grant/install', { method: 'POST', cookie: relogged.cookie, body });
    assert.equal(stale.status, 409); assert.equal(stale.json.code, 'research_only_operator_invalid');
    assert.equal(await count("SELECT count(*)::int n FROM scout_research_only_grants WHERE revoked_at IS NULL"), 0);

    // The rotated session previews, confirms and installs normally, then revokes.
    const current = (await http(API + 'research-only-grant/preview', { method: 'POST', cookie: relogged.cookie, body: grantBody('synthetic-ui-grant-rotated') })).json;
    assert.equal(current.installable, true);
    assert.equal((await confirm(relogged.cookie)).status, 200);
    const installed = await http(API + 'research-only-grant/install', { method: 'POST', cookie: relogged.cookie,
      body: { manifest: current.manifest, manifestHash: current.manifestHash } });
    assert.equal(installed.status, 200); assert.equal(installed.json.installed, true);
    const revoked = await http(API + 'research-only-grant/revoke', { method: 'POST', cookie: relogged.cookie,
      body: { grantId: installed.json.grantId, approvalReference: 'synthetic-ui-revoke-rotated' } });
    assert.equal(revoked.status, 200);
  });

  await t.test('password confirmation is single-use, expires at exactly two minutes and dies with logout or re-login', async () => {
    assert.equal(fresh.VALID_MS, 120000);
    const x = await h.loginAs(ADMIN);
    const sid = h.sidOf(x.cookie), at = Date.now();
    const plant = () => pool.query(`UPDATE "session" SET sess=jsonb_set(sess::jsonb,'{freshConfirmation}',$2::jsonb)::json WHERE sid=$1`,
      [sid, JSON.stringify(fresh.marker(ADMIN.id, 'scout_research_approval', at))]);
    const use = now => fresh.consume(pool, { sessionId: sid, userId: ADMIN.id, purpose: 'scout_research_approval', now });
    await plant();
    assert.equal(await use(at + 119999), true, 'valid just inside two minutes');
    assert.equal(await use(at + 1000), false, 'single-use');
    await plant();
    assert.equal(await use(at + 120000), false, 'expired at two minutes');
    await plant();
    assert.equal(await fresh.consume(pool, { sessionId: sid, userId: OTHER_ADMIN.id, purpose: 'scout_research_approval', now: at }), false, 'bound to the user');
    assert.equal(await fresh.consume(pool, { sessionId: 'another-session', userId: ADMIN.id, purpose: 'scout_research_approval', now: at }), false, 'bound to the session');

    // Logout: the marker goes with the session row; the old cookie cannot use it.
    assert.equal((await confirm(x.cookie)).status, 200);
    await http('/logout', { cookie: x.cookie });
    assert.equal(await h.sessionRow(x.cookie), null);
    const body = { approvalRef: 'synthetic-ui-mission-1', mission: mission.mission, missionHash: mission.missionHash };
    assert.equal((await http(API + 'research-mission/install', { method: 'POST', cookie: x.cookie, body })).status, 401);
    const y = await h.loginAs(ADMIN);
    const afterLogout = await http(API + 'research-mission/install', { method: 'POST', cookie: y.cookie, body });
    assert.equal(afterLogout.status, 403); assert.equal(afterLogout.json.code, 'fresh_confirmation_required');

    // Re-login on the same browser: the confirmation does not follow the rotated session.
    assert.equal((await confirm(y.cookie)).status, 200);
    const z = await h.loginAs(ADMIN, { cookie: y.cookie });
    assert.equal((await http(API + 'research-mission/install', { method: 'POST', cookie: z.cookie, body })).json.code, 'fresh_confirmation_required');
  });

  await t.test('admin deactivation, role change, tenant change, password reset and deletion end every session', async () => {
    await pool.query(`INSERT INTO users(id,client_id,role,active,name,email,password_hash,email_verified,password_change_required)
      SELECT '74',10,'manager',true,'Synthetic Target','target@example.test',password_hash,true,false FROM users WHERE id=$1`, [ADMIN.id]);
    const TARGET = { id: '74', email: 'target@example.test' };
    const manager = (await h.loginAs(h.users.OTHER_ADMIN)).cookie;
    const patch = body => http('/api/users/74', { method: 'PATCH', cookie: manager, body });
    const both = async () => [(await h.loginAs(TARGET, { remember: true })).cookie, (await h.loginAs(TARGET)).cookie];
    const alive = async cookies => Promise.all(cookies.map(signedIn));

    let sessions = await both();
    assert.deepEqual(await alive(sessions), [true, true]);
    assert.equal((await patch({ name: 'Renamed Target' })).status, 200);
    assert.deepEqual(await alive(sessions), [true, true], 'a name edit is not an authority change');

    assert.equal((await patch({ active: false })).status, 200);
    assert.deepEqual(await alive(sessions), [false, false], 'deactivation ends remembered AND ordinary sessions');
    for (const cookie of sessions) assert.equal(await h.sessionRow(cookie), null);
    assert.equal((await h.loginAs(TARGET)).response.headers.get('location'), '/login?error=1');
    assert.equal((await patch({ active: true })).status, 200);

    sessions = await both();
    assert.equal((await patch({ role: 'viewer' })).status, 200);
    assert.deepEqual(await alive(sessions), [false, false], 'role change');
    assert.equal((await patch({ role: 'manager' })).status, 200);

    sessions = await both();
    await pool.query("INSERT INTO clients(id,active) VALUES(11,true) ON CONFLICT (id) DO NOTHING");
    const moved = await patch({ client_id: 11 });
    assert.equal(moved.status, 200, moved.text);
    assert.deepEqual(await alive(sessions), [false, false], 'tenant change');
    assert.equal((await patch({ client_id: 10 })).status, 200);

    // A stale save that recreated a deleted row (a request in flight at the time) is still refused.
    sessions = await both();
    const stale = await h.sessionRow(sessions[1]);
    assert.equal((await patch({ active: false })).status, 200);
    await pool.query('INSERT INTO "session"(sid,sess,expire) VALUES($1,$2,$3)', [h.sidOf(sessions[1]), stale.sess, stale.expire]);
    assert.equal(await signedIn(sessions[1]), false, 'the session guard re-checks the account');
    assert.equal((await patch({ active: true })).status, 200);

    sessions = await both();
    assert.equal((await http('/api/users/74/reset-password', { method: 'POST', cookie: manager, body: { password: 'synthetic-reset-pass' } })).status, 200);
    assert.deepEqual(await alive(sessions), [false, false], 'password reset');
    assert.equal((await h.loginAs(TARGET)).response.headers.get('location'), '/login?error=1', 'old password refused');
    assert.equal((await h.loginAs(TARGET, { password: 'synthetic-reset-pass' })).response.headers.get('location'), '/change-password');
    await pool.query("UPDATE users SET password_hash=(SELECT password_hash FROM users WHERE id=$1), password_change_required=false WHERE id='74'", [ADMIN.id]);

    // Self-service password change keeps the changing session and ends the others.
    sessions = await both();
    const changed = await http('/api/me/password', { method: 'POST', cookie: sessions[0], body: { password: 'synthetic-self-pass' } });
    assert.equal(changed.status, 200, changed.text);
    assert.deepEqual(await alive(sessions), [true, false], 'other sessions end; the changing session stays');
    await pool.query("UPDATE users SET password_hash=(SELECT password_hash FROM users WHERE id=$1) WHERE id='74'", [ADMIN.id]);

    sessions = await both();
    assert.equal((await http('/api/users/74', { method: 'DELETE', cookie: manager })).status, 200);
    assert.deepEqual(await alive(sessions), [false, false], 'deletion');
  });

  await t.test('logout, a change outside the app, the absolute cap and store revocation end sessions', async () => {
    const a = await h.loginAs(ADMIN, { remember: true });
    const out = await http('/logout', { cookie: a.cookie });
    assert.equal(out.status, 302); assert.match(out.setCookie.join(), /connect\.sid=;/);
    assert.equal(await h.sessionRow(a.cookie), null);
    assert.equal(await signedIn(a.cookie), false, 'old persistent cookie is dead');

    // Changed directly in the database (another instance, a script): the guard
    // catches it once its 30s cache lapses — forgetUser stands in for the lapse.
    for (const remember of [true, false]) {
      const b = await h.loginAs(ADMIN, { remember });
      await pool.query('UPDATE users SET active=false WHERE id=$1', [ADMIN.id]);
      lifecycle.forgetUser(ADMIN.id);
      assert.equal(await signedIn(b.cookie), false, `direct deactivation (${remember ? 'remembered' : 'ordinary'})`);
      assert.equal(await h.sessionRow(b.cookie), null);
      await pool.query('UPDATE users SET active=true WHERE id=$1', [ADMIN.id]);

      const c = await h.loginAs(ADMIN, { remember });
      await pool.query("UPDATE users SET password_hash=password_hash||'x' WHERE id=$1", [ADMIN.id]);
      lifecycle.forgetUser(ADMIN.id);
      assert.equal(await signedIn(c.cookie), false, `direct password change (${remember ? 'remembered' : 'ordinary'})`);
      await pool.query("UPDATE users SET password_hash=left(password_hash,length(password_hash)-1) WHERE id=$1", [ADMIN.id]);
    }

    const d = await h.loginAs(ADMIN, { remember: true });
    await pool.query(`UPDATE "session" SET sess=jsonb_set(sess::jsonb,'{remembered,expiresAt}',$2::jsonb)::json WHERE sid=$1`,
      [h.sidOf(d.cookie), String(Date.now() - 1)]);
    assert.equal(await signedIn(d.cookie), false, 'absolute 30-day cap');

    const e = await h.loginAs(ADMIN);
    await pool.query('DELETE FROM "session" WHERE sid=$1', [h.sidOf(e.cookie)]);
    assert.equal(await signedIn(e.cookie), false, 'store-row revocation still works');
  });

  await t.test('closed recovery state, outbound restriction and enrichment queue are untouched', async () => {
    assert.deepEqual(closedEnv(), envBefore);
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_only_cycles'), 0, 'no research cycle ran');
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_google_spend'), 0, 'no Google spend recorded');
    assert.equal(await count('SELECT count(*)::int n FROM scout_unenriched'), queueBefore);
    assert.equal(deny.attempts(), 0, 'no external transport');
  });
});
