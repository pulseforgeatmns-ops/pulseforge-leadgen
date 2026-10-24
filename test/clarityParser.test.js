'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { parseClarityDigest, ClarityParseError } = require('../packages/max/clarity/parser');

// Synthetic example reproducing the aggregate issue facts; not a real mailbox artifact.
const TEXT = `Microsoft Clarity weekly digest
October 11 - 17, 2026
Sessions: 232 +180%
Pages per session: 1.22 +11.7%
Scroll depth: 8.65% -68%
Session duration: 51.5 seconds -34.1%
Dead clicks: 3.88% -59.8%
Quick backs: 0.43% -64.2%
Rage clicks: 0%
JS errors: 0%`;
function message(text = TEXT, mimeType = 'text/plain') {
  return { id: 'message_1', threadId: 'thread_1', internalDate: String(Date.parse('2026-10-18T10:00:00Z')), payload: { mimeType, headers: [{ name: 'Subject', value: 'Microsoft Clarity weekly digest' }, { name: 'From', value: 'Microsoft Clarity <fixture@microsoft.com>' }], body: { data: Buffer.from(text).toString('base64url') } } };
}
const expectedMetrics = { sessions: 232, pages_per_session: 1.22, scroll_depth: 8.65, session_duration: 51.5, dead_clicks: 3.88, rage_clicks: 0, quick_backs: 0.43, js_errors: 0 };
const expectedDeltas = { sessions: 180, pages_per_session: 11.7, scroll_depth: -68, session_duration: -34.1, dead_clicks: -59.8, rage_clicks: null, quick_backs: -64.2, js_errors: null };

test('plain Gmail weekly digest preserves issue metrics, dates, deltas and source provenance', () => {
  const parsed = parseClarityDigest(message(), { expectedSender: 'fixture@microsoft.com' });
  assert.deepEqual(parsed.metrics, expectedMetrics);
  assert.deepEqual(parsed.deltas_pct, expectedDeltas);
  assert.equal(parsed.window_start, '2026-10-11');
  assert.equal(parsed.window_end, '2026-10-17');
  assert.equal(parsed.provenance.gmail_message_id, 'message_1');
  assert.match(parsed.provenance.content_sha256, /^[a-f0-9]{64}$/);
  assert.equal(parsed.provenance.source_type, 'gmail_plain');
  assert.ok(!JSON.stringify(parsed).includes('Microsoft Clarity weekly digest'));
});

test('HTML nested MIME digest, entities and arrow deltas normalize without script text', () => {
  const html = `<html><head><style>.sessions{display:none}</style></head><body><h1>Clarity weekly digest</h1><p>Oct 11 &ndash; Oct 17, 2026</p><table>${Object.entries(expectedMetrics).map(([key, value]) => `<tr><td>${({ sessions: 'Sessions', pages_per_session: 'Pages per session', scroll_depth: 'Scroll depth', session_duration: 'Session duration', dead_clicks: 'Dead clicks', rage_clicks: 'Rage clicks', quick_backs: 'Quick backs', js_errors: 'Javascript errors' })[key]}</td><td>${value}${['sessions', 'pages_per_session'].includes(key) ? '' : key === 'session_duration' ? '&nbsp;sec' : '%'}</td><td>${expectedDeltas[key] === null ? '' : expectedDeltas[key] < 0 ? `↓ ${-expectedDeltas[key]}%` : `↑ ${expectedDeltas[key]}%`}</td></tr>`).join('')}</table><script>Sessions 999</script></body></html>`;
  const msg = message();
  msg.payload.mimeType = 'multipart/mixed';
  delete msg.payload.body;
  msg.payload.parts = [{ mimeType: 'multipart/alternative', parts: [{ mimeType: 'text/html', body: { data: Buffer.from(html).toString('base64url') } }] }];
  const parsed = parseClarityDigest(msg);
  assert.deepEqual(parsed.metrics, expectedMetrics);
  assert.deepEqual(parsed.deltas_pct, expectedDeltas);
  assert.equal(parsed.provenance.source_type, 'gmail_html');
});

test('alternative MIME parts are not double-counted and attachments are ignored', () => {
  const msg = message();
  msg.payload = { ...msg.payload, mimeType: 'multipart/alternative', body: undefined, parts: [{ mimeType: 'text/plain', body: { data: Buffer.from(TEXT).toString('base64url') } }, { mimeType: 'text/html', body: { data: Buffer.from('<p>Sessions 999</p>').toString('base64url') } }, { mimeType: 'text/plain', filename: 'extra.txt', body: { data: Buffer.from('Sessions 999').toString('base64url') } }] };
  assert.deepEqual(parseClarityDigest(msg).metrics, expectedMetrics);
});

test('value-before-label digest cards and duration mm:ss supported', () => {
  const body = 'Clarity weekly report\nOct 11 - 17, 2026\n232\nSessions\n+180%\n1.22\nPages per session\n+11.7%\n00:51.5\nSession duration\n-34.1%';
  const parsed = parseClarityDigest(message(body));
  assert.equal(parsed.metrics.sessions, 232);
  assert.equal(parsed.metrics.pages_per_session, 1.22);
  assert.equal(parsed.metrics.session_duration, 51.5);
  assert.equal(parsed.deltas_pct.sessions, 180);
});

test('missing/null metrics never turn into zero; signed zero deltas survive', () => {
  const parsed = parseClarityDigest(message('Clarity weekly report\n2026-10-11 to 2026-10-17\nSessions: 0 +0%\nPages per session: 0\nScroll depth: N/A\nSession duration: --\nDead clicks: 0%'));
  assert.equal(parsed.metrics.sessions, 0);
  assert.equal(parsed.deltas_pct.sessions, 0);
  assert.equal(parsed.metrics.dead_clicks, 0);
  assert.equal(parsed.metrics.scroll_depth, null);
  assert.equal(parsed.metrics.session_duration, null);
  assert.equal(parsed.metrics.js_errors, null);
});

test('value-before-label cards preserve explicit missing values', () => {
  const parsed = parseClarityDigest(message('Clarity weekly report\nOct 11 - 17, 2026\nN/A\nSessions\n1.22\nPages per session\n--\nScroll depth\n0%\nRage clicks'));
  assert.equal(parsed.metrics.sessions, null);
  assert.equal(parsed.metrics.pages_per_session, 1.22);
  assert.equal(parsed.metrics.scroll_depth, null);
  assert.equal(parsed.metrics.rage_clicks, 0);
});

test('missing reporting year uses Gmail receipt, including year-crossing window', () => {
  const msg = message('Clarity weekly report\nDec 28 - Jan 3\nSessions 20\nPages per session 1.5');
  msg.internalDate = String(Date.parse('2027-01-04T00:00:00Z'));
  const parsed = parseClarityDigest(msg);
  assert.equal(parsed.window_start, '2026-12-28');
  assert.equal(parsed.window_end, '2027-01-03');
});

for (const [label, body, code] of [
  ['malformed numeric metric', TEXT.replace('Sessions: 232', 'Sessions: two'), 'metric_invalid_sessions'],
  ['invalid percentage', TEXT.replace('8.65%', '108.65%'), 'metric_invalid_scroll_depth'],
  ['percentage masquerading as duration', TEXT.replace('51.5 seconds', '51.5%'), 'metric_invalid_session_duration'],
  ['percentage masquerading as count', TEXT.replace('Sessions: 232', 'Sessions: 232%'), 'metric_invalid_sessions'],
  ['invalid seconds component', TEXT.replace('51.5 seconds', '00:75'), 'metric_invalid_session_duration'],
  ['unrecognized duration unit', TEXT.replace('51.5 seconds', '1 hour'), 'metric_invalid_session_duration'],
  ['non-weekly range', TEXT.replace('11 - 17', '11 - 19'), 'reporting_window_invalid'],
  ['invalid date', TEXT.replace('October 11 - 17', 'February 29 - March 7'), 'reporting_window_invalid'],
  ['duplicate ambiguous metric', `${TEXT}\nSessions: 1`, 'metric_duplicate_sessions'],
  ['missing window', TEXT.replace('October 11 - 17, 2026', ''), 'reporting_window_missing'],
  ['too little evidence', 'Clarity weekly report\nOct 11 - 17, 2026\nSessions: 0', 'digest_metrics_missing'],
]) test(`rejects ${label} with safe provenance`, () => {
  assert.throws(() => parseClarityDigest(message(body)), (error) => error instanceof ClarityParseError && error.code === code && error.provenance.gmail_message_id === 'message_1' && !error.message.includes(body));
});

test('non-digest and wrong sender rejected, never logged or treated as empty metrics', () => {
  const ordinary = message('Lunch tomorrow?');
  ordinary.payload.headers[0].value = 'Lunch';
  assert.throws(() => parseClarityDigest(ordinary), { code: 'not_clarity_digest' });
  assert.throws(() => parseClarityDigest(message(), { expectedSender: 'someone@microsoft.com' }), { code: 'digest_sender_mismatch' });
});

test('malformed MIME encoding and resource bounds reject safely', () => {
  const msg = message();
  msg.payload.body.data = '!invalid base64!';
  assert.throws(() => parseClarityDigest(msg), { code: 'invalid_body_encoding' });
  assert.throws(() => parseClarityDigest(message('x'.repeat(600000))), { code: 'body_limit_exceeded' });
});

test('footer discounts or distant percentages never become a weekly delta', () => {
  for (const ending of ['Scroll depth: 8.65%\nSave -50% on this offer', 'Scroll depth: 8.65%\nView dashboard\n-50%', '8.65%\nScroll depth\nSave -50% on this offer']) {
    const beforeLayout = ending.startsWith('8.65');
    const prefix = beforeLayout ? '232\nSessions\n+180%\n' : 'Sessions: 232 +180%\n';
    const parsed = parseClarityDigest(message(`Clarity weekly digest\nOct 11 - 17, 2026\n${prefix}${ending}`));
    assert.equal(parsed.metrics.scroll_depth, 8.65);
    assert.equal(parsed.deltas_pct.scroll_depth, null);
  }
});

test('immediate change lines preserve deltas but reject unrelated suffixes', () => {
  const parsed = parseClarityDigest(message('Clarity weekly digest\nOct 11 - 17, 2026\nSessions: 232\n+180% vs last week\nScroll depth: 8.65%\n-68%'));
  assert.equal(parsed.deltas_pct.sessions, 180);
  assert.equal(parsed.deltas_pct.scroll_depth, -68);
  assert.throws(() => parseClarityDigest(message(TEXT.replace('51.5 seconds', '51.5 bananas'))), { code: 'metric_invalid_session_duration' });
});

test('project identity requires one exact official project link, not mailbox/sender alone', () => {
  const plain = message(`${TEXT}\nhttps://clarity.microsoft.com/projects/view/anchorfixture976/dashboard`);
  const parsed = parseClarityDigest(plain, { expectedProjectId: 'anchorfixture976' });
  assert.equal(parsed.project_id, 'anchorfixture976');
  assert.equal(parsed.provenance.project_id, 'anchorfixture976');
  assert.throws(() => parseClarityDigest(plain, { expectedProjectId: 'otherfixture976' }), { code: 'digest_project_mismatch' });
  assert.throws(() => parseClarityDigest(message(TEXT), { expectedProjectId: 'anchorfixture976' }), { code: 'digest_project_mismatch' });
  const conflict = message(`${TEXT}\nhttps://clarity.microsoft.com/projects/view/anchorfixture976/dashboard\nhttps://clarity.microsoft.com/projects/view/otherfixture976/dashboard`);
  assert.throws(() => parseClarityDigest(conflict, { expectedProjectId: 'anchorfixture976' }), { code: 'digest_project_mismatch' });
  for (const forged of ['https://clarity.microsoft.com.evil.example/projects/view/anchorfixture976/dashboard', 'https://evil.example/?url=https://clarity.microsoft.com/projects/view/anchorfixture976/dashboard', 'https://clarity.microsoft.com@evil.example/projects/view/anchorfixture976/dashboard']) {
    assert.throws(() => parseClarityDigest(message(`${TEXT}\n${forged}`), { expectedProjectId: 'anchorfixture976' }), { code: 'digest_project_mismatch' });
  }
});

test('project link in HTML href is retained even when the visible text is only View dashboard', () => {
  const html = `<p>${TEXT.replace(/\n/g, '<br>')}</p><a href="https://clarity.microsoft.com/projects/view/anchorfixture976/dashboard?Date=Last%207%20days&amp;source=digest">View dashboard</a>`;
  const parsed = parseClarityDigest(message(html, 'text/html'), { expectedProjectId: 'anchorfixture976' });
  assert.equal(parsed.project_id, 'anchorfixture976');
  assert.equal(parsed.metrics.sessions, 232);
});

test('authenticated source requires first Gmail DKIM and DMARC result, never a forged later pass', () => {
  const good = 'mx.google.com; dkim=pass header.i=@microsoft.com; dmarc=pass header.from=microsoft.com';
  const parsedMessage = message();
  parsedMessage.payload.headers.unshift({ name: 'Authentication-Results', value: good });
  assert.equal(parseClarityDigest(parsedMessage, { requireAuthentication: true }).metrics.sessions, 232);
  assert.throws(() => parseClarityDigest(message(), { requireAuthentication: true }), { code: 'digest_authentication_missing' });
  for (const bad of [
    good.replace('mx.google.com', 'attacker.example'),
    good.replace('dkim=pass', 'dkim=fail'),
    good.replace('dmarc=pass', 'dmarc=fail'),
    good.replace('header.i=@microsoft.com', 'header.i=@microsoft.com.evil.example'),
    good.replace('header.from=microsoft.com', 'header.from=evil.example'),
  ]) {
    const forged = message();
    forged.payload.headers.unshift({ name: 'Authentication-Results', value: bad }, { name: 'Authentication-Results', value: good });
    assert.throws(() => parseClarityDigest(forged, { requireAuthentication: true }), (error) => ['digest_authentication_failed', 'digest_authentication_missing'].includes(error.code));
  }
});
