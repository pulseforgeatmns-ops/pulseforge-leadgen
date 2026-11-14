/*
 * P0 S2 — six prohibited REST intents against the existing review-only mission.
 *
 * Runs in the browser, same origin, on the operator's EXISTING signed-in
 * session (cookie sent by the browser; this script never reads, prints or
 * stores it). Build the Android bookmarklet with:
 *   node scripts/s2-rest-acceptance/buildBookmarklet.js
 *
 * Fail-closed rules (stronger than the runbook, which only logs):
 *   - Every precondition failure THROWS and nothing is sent.
 *   - Requests are strictly sequential, each sent at most once. No retry on any
 *     outcome, including network errors (outcome unknown = halt).
 *   - Any response other than HTTP 403 + error "authorization_constraint_blocked"
 *     halts immediately; remaining intents are not sent.
 *   - The restriction hash is re-read before every POST; any drift halts.
 *   - allowFixtureFallback is sent explicitly false. No payload, no metadata.
 *   - An explicit confirm() is required before the first POST.
 *
 * Comments in this file use block syntax only so it collapses to one line.
 */
(async () => {
  const EXPECT = {
    tenantId: '25',
    missionId: 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169',
    revision: 0,
    prohibited: ['content', 'outreach', 'outreach_authorization', 'paid_media', 'provider_research'],
    blockedError: 'authorization_constraint_blocked',
  };
  const INTENTS = [
    'EXECUTE_OUTBOUND',
    'APPROVE_EXECUTION',
    'GENERATE_VARIANTS',
    'ASSESS_PAID_ACQUISITION',
    'START_DISCOVERY',
    'APPROVE_DISCOVERY',
  ];
  const BASE = '/api/v1/amo/missions/' + encodeURIComponent(EXPECT.missionId);
  const log = { startedAt: new Date().toISOString(), origin: location.origin, pre: {}, results: [], post: {}, verdict: null };

  const out = document.createElement('pre');
  out.style.cssText = 'position:fixed;inset:0;z-index:2147483647;margin:0;padding:12px;overflow:auto;background:#111;color:#eee;font:12px/1.4 monospace;white-space:pre-wrap';
  document.body.appendChild(out);
  const render = () => { out.textContent = JSON.stringify(log, null, 2); };
  const halt = (reason, detail) => {
    log.verdict = 'HALTED';
    log.haltReason = reason;
    if (detail !== undefined) log.haltDetail = detail;
    render();
    throw new Error('S2 HALT: ' + reason);
  };

  const readJson = async (path) => {
    let res;
    try {
      res = await fetch(path, { method: 'GET', credentials: 'same-origin', cache: 'no-store', headers: { Accept: 'application/json' } });
    } catch (e) {
      return halt('read_network_error', { path, message: String(e && e.message) });
    }
    const text = await res.text();
    let body = null;
    try { body = JSON.parse(text); } catch (e) { return halt('read_non_json', { path, status: res.status, redirected: res.redirected, head: text.slice(0, 200) }); }
    if (res.status !== 200) return halt('read_not_200', { path, status: res.status, body });
    return body;
  };

  const sameSet = (a, b) => {
    const x = [...new Set(a || [])].sort();
    const y = [...new Set(b || [])].sort();
    return x.length === y.length && x.every((v, i) => v === y[i]);
  };

  const readAuthorization = async () => {
    const a = await readJson(BASE + '/authorization');
    const c = a && a.constraints;
    if (!a || String(a.missionId) !== EXPECT.missionId) halt('authorization_wrong_mission', a);
    if (String(a.tenantId) !== EXPECT.tenantId) halt('authorization_wrong_tenant', { tenantId: a.tenantId });
    if (!c || c.reviewOnly !== true) halt('mission_not_review_only', c);
    if (!sameSet(c.prohibited, EXPECT.prohibited)) halt('prohibited_set_mismatch', { prohibited: c.prohibited });
    if (Number(c.revision || 0) !== EXPECT.revision) halt('authorization_revision_mismatch', { revision: c.revision });
    if (typeof a.constraintsHash !== 'string' || !a.constraintsHash) halt('constraints_hash_missing', a);
    return a;
  };

  if (!/^https:\/\/pulseforge-leadgen-production\.up\.railway\.app$/.test(location.origin)) {
    halt('wrong_origin', { origin: location.origin });
  }

  /* 1. Tenant context: the active tenant on THIS session must be 25. */
  const ctx = await readJson('/api/v1/tenant/context');
  const activeTenant = String(ctx.active_client_id != null ? ctx.active_client_id : ctx.tenantId);
  log.pre.activeTenant = activeTenant;
  if (activeTenant !== EXPECT.tenantId) halt('active_tenant_not_25', { activeTenant });

  /* 2. Persisted restrictions, independently re-read. */
  const auth = await readAuthorization();
  log.pre.authorization = { constraintsHash: auth.constraintsHash, constraints: auth.constraints };

  /* 3. Mission state: exists in tenant 25, no pending operator decision. */
  const snap = await readJson(BASE);
  const m = (snap && snap.mission) || snap || {};
  const pending = m.pendingOperatorDecision != null ? m.pendingOperatorDecision : snap.pendingOperatorDecision;
  log.pre.mission = {
    id: m.id, tenantId: snap.tenantId, stage: m.stage, status: m.status, version: m.version,
    pendingOperatorDecision: pending == null ? null : pending,
  };
  if (String(snap.tenantId) !== EXPECT.tenantId) halt('mission_wrong_tenant', { tenantId: snap.tenantId });
  if (m.id && m.id !== EXPECT.missionId) halt('mission_id_mismatch', { id: m.id });
  if (pending) halt('pending_operator_decision_present', pending);
  if (m.syntheticEvidence) halt('mission_has_synthetic_evidence', m.syntheticEvidence);
  render();

  if (!confirm('S2: preconditions passed for ' + EXPECT.missionId + ' (tenant 25, review-only, revision 0, no pending decision).\n\nSend the six prohibited REST intents now? Each must return 403 ' + EXPECT.blockedError + '. Any other response halts.')) {
    halt('operator_cancelled_before_send');
  }

  /* 4. Six intents, sequential, once each, halt on anything unexpected. */
  for (const intent of INTENTS) {
    const before = await readAuthorization();
    if (before.constraintsHash !== auth.constraintsHash) halt('constraints_hash_drift_before_' + intent, { was: auth.constraintsHash, now: before.constraintsHash });

    const sentAt = new Date().toISOString();
    let res;
    try {
      res = await fetch(BASE + '/execute', {
        method: 'POST',
        credentials: 'same-origin',
        cache: 'no-store',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ intent, allowFixtureFallback: false }),
      });
    } catch (e) {
      log.results.push({ intent, sentAt, networkError: String(e && e.message) });
      halt('network_error_outcome_unknown_' + intent + '_do_not_retry');
    }
    const text = await res.text();
    let body = null;
    try { body = JSON.parse(text); } catch (e) { body = { nonJson: text.slice(0, 300) }; }
    const row = { intent, sentAt, httpStatus: res.status, redirected: res.redirected, body };
    log.results.push(row);
    render();
    if (res.status !== 403 || !body || body.error !== EXPECT.blockedError) {
      halt('unexpected_response_' + intent + '_do_not_retry', row);
    }
  }

  /* 5. Post-checks: restrictions unchanged, still no pending decision. */
  const after = await readAuthorization();
  const snap2 = await readJson(BASE);
  const m2 = (snap2 && snap2.mission) || snap2 || {};
  const pending2 = m2.pendingOperatorDecision != null ? m2.pendingOperatorDecision : snap2.pendingOperatorDecision;
  log.post = {
    constraintsHash: after.constraintsHash,
    hashUnchanged: after.constraintsHash === auth.constraintsHash,
    stage: m2.stage, status: m2.status, version: m2.version,
    versionUnchanged: m2.version === m.version,
    pendingOperatorDecision: pending2 == null ? null : pending2,
  };
  if (!log.post.hashUnchanged) halt('constraints_hash_changed_after_run', log.post);
  if (pending2) halt('pending_decision_appeared_after_run', pending2);
  if (m.version !== undefined && !log.post.versionUnchanged) halt('mission_version_changed_after_run', log.post);

  log.finishedAt = new Date().toISOString();
  log.verdict = 'ALL_SIX_BLOCKED_403';
  render();
  try { await navigator.clipboard.writeText(JSON.stringify(log, null, 2)); log.copiedToClipboard = true; } catch (e) { log.copiedToClipboard = false; }
  render();
})().catch((e) => { try { alert(String(e && e.message)); } catch (_) { /* no-op */ } });
