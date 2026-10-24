'use strict';

// PF-CLARITY-001. A render-only, tenant-bound review; no implementation endpoint.
(function () {
  const el = id => document.getElementById(id);
  const content = el('clarityContent');
  const dialog = el('clarityConfirm');
  let epoch = 0;
  let controller = null;
  let current = null;
  let busy = false;
  let blocked = false;
  let selectedDecision = null;
  // Reuse the same key when the outcome of a decision could not be established.
  // Never persist recommendations or tenant evidence in browser storage.
  const attempts = new Map();
  const metrics = [
    ['sessions', 'Sessions', ''], ['pages_per_session', 'Pages per session', ''],
    ['scroll_depth', 'Scroll depth', '%'], ['session_duration', 'Session duration', ' seconds'],
    ['dead_clicks', 'Dead clicks', '%'], ['rage_clicks', 'Rage clicks', '%'],
    ['quick_backs', 'Quick backs', '%'], ['js_errors', 'JavaScript errors', '%'],
  ];

  function node(tag, text, className) {
    const result = document.createElement(tag);
    if (text != null) result.textContent = String(text);
    if (className) result.className = className;
    return result;
  }

  function status(text, tone = '') {
    el('clarityStatus').textContent = text;
    el('clarityStatus').dataset.tone = tone;
  }

  function scope() {
    const query = new URLSearchParams(location.search);
    const id = query.get('id');
    const clientId = query.get('client_id');
    if (query.getAll('id').length !== 1 || query.getAll('client_id').length !== 1
      || !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(id || '')
      || !/^[1-9]\d*$/.test(clientId || '') || !Number.isSafeInteger(Number(clientId))) return null;
    return { id, clientId };
  }

  function bound(recommendation, target) {
    return recommendation && String(recommendation.id).toLowerCase() === target.id.toLowerCase()
      && String(recommendation.client_id) === target.clientId;
  }

  function attemptKey() {
    const rec = current?.recommendation;
    return rec ? `${rec.client_id}:${rec.id}:${rec.digest}` : '';
  }

  function updateActions() {
    const rec = current?.recommendation;
    const allowed = rec?.status === 'pending' && current?.permissions?.can_decide === true
      && typeof current.csrf_token === 'string' && !!current.csrf_token && typeof rec.digest === 'string' && !!rec.digest;
    el('clarityActions').hidden = !allowed;
    const uncertain = attempts.get(attemptKey());
    el('clarityApprove').disabled = busy || blocked || !allowed || !!(uncertain && uncertain.decision !== 'approve');
    el('clarityReject').disabled = busy || blocked || !allowed || !!(uncertain && uncertain.decision !== 'reject');
    el('clarityReload').disabled = busy;
    el('clarityConfirmSubmit').disabled = busy;
    el('clarityCancel').disabled = busy;
  }

  function card(title) {
    const section = node('section', null, 'clarity-card');
    section.appendChild(node('h2', title));
    content.appendChild(section);
    return section;
  }

  function render(data) {
    current = data;
    const rec = data.recommendation;
    const report = data.report || {};
    content.replaceChildren();
    content.hidden = false;
    el('clarityTitle').textContent = rec.title || 'Website recommendation';
    const overview = card('Recommendation');
    const meta = node('div', null, 'clarity-meta');
    meta.append(node('span', `${rec.confidence || 'Unspecified'} confidence`, 'clarity-badge'),
      node('span', rec.status || 'Unknown review status', 'clarity-badge'));
    if (report.synthetic) meta.appendChild(node('span', 'Synthetic test data', 'clarity-badge synthetic'));
    overview.append(meta, node('p', rec.summary || 'No summary provided.'));
    if (rec.expected_outcome) overview.appendChild(node('p', `Expected outcome (hypothesis): ${rec.expected_outcome}`));
    if (rec.limitation) overview.appendChild(node('p', rec.limitation));
    const evidence = card('Observed evidence');
    const list = node('ul', null, 'clarity-evidence');
    for (const item of Array.isArray(rec.evidence) ? rec.evidence : []) {
      const line = node('li');
      line.append(node('strong', item.label || 'Observation'), node('span', item.value == null ? 'Unavailable' : item.value));
      list.appendChild(line);
    }
    evidence.appendChild(list.children.length ? list : node('p', 'No structured evidence supplied.'));
    const validation = card('Proposed validation');
    const steps = node('ol', null, 'clarity-evidence');
    for (const step of Array.isArray(rec.validation_steps) ? rec.validation_steps : []) steps.appendChild(node('li', step));
    validation.appendChild(steps.children.length ? steps : node('p', 'No validation steps supplied.'));
    validation.appendChild(node('p', 'Validation is proposed, not performed. Review approval does not authorize a test or website change.'));
    const snapshot = card('Reported website metrics');
    snapshot.appendChild(node('p', [report.window_start, report.window_end].filter(Boolean).join(' to ') || 'Reporting interval unavailable.'));
    const grid = node('dl', null, 'clarity-grid');
    for (const [key, label, unit] of metrics) {
      const value = report.metrics?.[key];
      const cell = node('div', null, 'clarity-metric');
      cell.append(node('dt', label), node('dd', typeof value === 'number' && Number.isFinite(value) ? `${value}${unit}` : 'Unavailable'));
      const delta = report.deltas_pct?.[key];
      cell.appendChild(node('small', typeof delta === 'number' && Number.isFinite(delta)
        ? `Reported change: ${delta > 0 ? '+' : ''}${delta}%` : 'Change unavailable'));
      grid.appendChild(cell);
    }
    snapshot.appendChild(grid);
    const provenance = card('Source provenance');
    const fields = node('dl', null, 'clarity-provenance');
    for (const [key, label] of [
      ['source_type', 'Source'], ['received_at', 'Received'], ['project_id', 'Clarity project ID'], ['gmail_message_id', 'Gmail message ID'],
      ['gmail_thread_id', 'Gmail thread ID'], ['content_sha256', 'Content SHA-256'], ['parser_version', 'Parser version'],
    ]) if (report.provenance?.[key] != null) fields.append(node('dt', label), node('dd', report.provenance[key]));
    provenance.appendChild(fields.children.length ? fields : node('p', 'No source provenance supplied.'));
    if (rec.status === 'approved' || rec.status === 'rejected') {
      status(`Recommendation ${rec.status}. No website changes have been authorized.`, 'success');
    } else if (data.permissions?.can_decide !== true) {
      status('Read-only review. An authorized administrator or manager must record the decision.');
    } else if (!data.csrf_token || !rec.digest) {
      status('Review decisions are unavailable in this session. Reload the latest evidence.', 'error');
    } else if (attempts.has(attemptKey())) {
      status('The prior decision outcome was not confirmed. You can retry that same decision safely, or reload to check its status.');
    } else status('Review the evidence and proposed validation before recording a decision.');
    updateActions();
  }

  function invalidate() {
    epoch += 1;
    controller?.abort();
    controller = null;
    busy = false;
    blocked = false;
    current = null;
    selectedDecision = null;
    dialog.close();
    content.hidden = true;
    content.replaceChildren();
    el('clarityTitle').textContent = 'Loading recommendation…';
    updateActions();
  }

  function failure(code) {
    if (code === 401) return 'Your session has expired. Sign in again to review this recommendation.';
    if (code === 403) return 'You do not have permission to review this recommendation in the current workspace.';
    if (code === 404) return 'This recommendation is not available in the selected workspace.';
    if (code === 409) return 'This recommendation changed or was already reviewed. Reload the latest evidence before continuing.';
    return 'The review service is unavailable. Reload to try again.';
  }

  async function load() {
    invalidate();
    const token = epoch;
    const target = scope();
    if (!target) {
      el('clarityTitle').textContent = 'Review link unavailable';
      status('Open a recommendation from the daily brief to use a valid workspace-bound review link.', 'error');
      return;
    }
    status('Loading the latest evidence…');
    busy = true;
    updateActions();
    controller = new AbortController();
    try {
      const response = await fetch(`/api/v1/clarity/recommendations/${encodeURIComponent(target.id)}?client_id=${encodeURIComponent(target.clientId)}`, {
        credentials: 'same-origin', cache: 'no-store', signal: controller.signal, headers: { Accept: 'application/json' },
      });
      if (!response.ok) throw Object.assign(new Error(), { status: response.status });
      const data = await response.json();
      if (token !== epoch) return;
      if (!bound(data.recommendation, target)) throw new Error('scope_mismatch');
      busy = false;
      render(data);
    } catch (error) {
      if (token !== epoch || error.name === 'AbortError') return;
      el('clarityTitle').textContent = 'Recommendation unavailable';
      status(failure(error.status), 'error');
    } finally {
      if (token === epoch) { busy = false; updateActions(); }
    }
  }

  function confirm(decision) {
    if (busy || blocked || !current || el(decision === 'approve' ? 'clarityApprove' : 'clarityReject').disabled) return;
    selectedDecision = decision;
    el('clarityConfirmTitle').textContent = decision === 'approve' ? 'Approve this recommendation?' : 'Reject this recommendation?';
    el('clarityConfirmCopy').textContent = decision === 'approve'
      ? 'This records your review of the exact evidence shown. It does not authorize implementation, testing, publication, or deployment. Your website will not change.'
      : 'This records a rejection of this exact recommendation. Your website will not change.';
    dialog.showModal();
    el('clarityCancel').focus();
  }

  async function decide() {
    if (busy || blocked || !selectedDecision || !current || !dialog.open) return;
    const target = scope();
    if (!target || !bound(current.recommendation, target)) { invalidate(); return; }
    const token = epoch;
    const decision = selectedDecision;
    const key = attemptKey();
    let attempt = attempts.get(key);
    if (attempt && attempt.decision !== decision) return;
    if (!attempt) { attempt = { decision, idempotency_key: crypto.randomUUID() }; attempts.set(key, attempt); }
    const body = { decision, expected_digest: current.recommendation.digest, idempotency_key: attempt.idempotency_key, client_id: Number(target.clientId) };
    const csrf = current.csrf_token;
    busy = true;
    updateActions();
    status('Recording your recommendation review…');
    try {
      const response = await fetch(`/api/v1/clarity/recommendations/${encodeURIComponent(target.id)}/decision`, {
        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf, Accept: 'application/json' },
        body: JSON.stringify(body),
      });
      if (!response.ok) throw Object.assign(new Error(), { status: response.status });
      const data = await response.json();
      if (token !== epoch) return;
      if (!bound(data.recommendation, target)
        || data.recommendation.status !== (decision === 'approve' ? 'approved' : 'rejected')
        || data.decision?.website_changes_authorized !== false) throw new Error('unconfirmed_decision');
      attempts.delete(key);
      busy = false;
      dialog.close();
      selectedDecision = null;
      render({ ...current, recommendation: data.recommendation });
    } catch (error) {
      if (token !== epoch) return;
      blocked = true;
      dialog.close();
      selectedDecision = null;
      status(error.status ? failure(error.status)
        : 'The decision outcome could not be confirmed. Reload to check its status before trying again.', 'error');
    } finally {
      if (token === epoch) { busy = false; updateActions(); }
    }
  }

  el('clarityApprove').addEventListener('click', () => confirm('approve'));
  el('clarityReject').addEventListener('click', () => confirm('reject'));
  el('clarityConfirmSubmit').addEventListener('click', decide);
  el('clarityReload').addEventListener('click', load);
  el('clarityCancel').addEventListener('click', () => { if (!busy) { selectedDecision = null; dialog.close(); } });
  dialog.addEventListener('cancel', event => { if (busy) event.preventDefault(); else selectedDecision = null; });
  window.addEventListener('popstate', load);
  window.addEventListener('pagehide', invalidate);
  window.addEventListener('pageshow', event => { if (event.persisted) load(); });
  document.addEventListener('pulseforge:tenant-changed', () => {
    invalidate();
    el('clarityTitle').textContent = 'Workspace changed';
    status('Return to the daily brief and open a recommendation for the current workspace.');
    el('clarityReload').disabled = true;
  });
  load();
})();
