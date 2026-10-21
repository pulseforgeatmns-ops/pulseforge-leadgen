'use strict';
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else {
    root.PulseforgeReviewNavigation = api.create({
      tenant: () => root.PulseforgeTenantCoordinator?.getActiveClientId?.() ?? root.PulseforgeShell?.activeClientId,
      scope: async () => {
        const res = await fetch('/api/v1/max/spreadsheet/scope', { credentials: 'same-origin' });
        const data = await res.json();
        if (!res.ok || !data.tenant_id || !data.actor_id) throw new Error('Authenticated review scope unavailable.');
        return String(data.tenant_id);
      },
      request: async body => {
        const res = await fetch('/api/v1/max/workspace/review-navigation', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const data = await res.json(); if (!res.ok) throw new Error(data.message || 'Review navigation unavailable.'); return data;
      },
      beforeRender: () => document.dispatchEvent(new CustomEvent('pulseforge:review-will-render')),
      render: action => action.type === 'open_content_review' ? root.PulseforgeContentReview?.open({ tenantId: action.tenantId }) : root.PulseforgeAccountReview?.open({ aoId: action.aoId, aoName: action.aoName, tenantId: action.tenantId }),
      notify: text => {
        let status = document.getElementById('reviewNavigationStatus');
        if (!status) { status = document.createElement('p'); status.id = 'reviewNavigationStatus'; status.setAttribute('role', 'status'); document.body.append(status); }
        status.textContent = text;
      },
      rendered: action => document.dispatchEvent(new CustomEvent('pulseforge:review-rendered', { detail: { type: action.type, tenantId: action.tenantId, status: 'surface_rendered' } })),
    });
    for (const event of ['pulseforge:tenant-changing', 'pulseforge:tenant-changed', 'pulseforge:review-dismissed']) document.addEventListener(event, () => root.PulseforgeReviewNavigation.invalidate());
    window.addEventListener('popstate', () => root.PulseforgeReviewNavigation.invalidate());
    document.addEventListener('keydown', event => { if (event.key === 'Escape') root.PulseforgeReviewNavigation.invalidate(); });
    window.addEventListener('hashchange', () => { if (location.hash !== '#content-review') root.PulseforgeReviewNavigation.invalidate(); });
  }
}(typeof globalThis === 'undefined' ? this : globalThis, function () {
  function create({ tenant, scope, request, render, beforeRender = () => {}, notify = () => {}, rendered = () => {} }) {
    let generation = 0, authenticatedTenant = '';
    const tenantId = () => String(tenant() || authenticatedTenant || '');
    async function ready(token) {
      if (!token.tenantId && scope) {
        const verified = await scope();
        if (token.generation !== generation) return false;
        authenticatedTenant = String(verified || ''); token.tenantId = tenantId();
      }
      return token.generation === generation;
    }
    function begin() { return { generation: ++generation, tenantId: tenantId() }; }
    function current(token) { return token && token.generation === generation && token.tenantId && token.tenantId === tenantId(); }
    function consume(action, token) {
      if (!current(token) || !action || String(action.tenantId) !== token.tenantId || !['open_content_review', 'open_account_review'].includes(action.type)) return false;
      if (action.type === 'open_account_review' && !action.aoId) return false;
      beforeRender(action);
      if (!current(token)) return false;
      const didRender = render(action);
      if (didRender !== true) { notify('The review surface could not be opened. Nothing is confirmed.'); return false; }
      rendered(action); return true;
    }
    async function open(reviewNavigation, question = '') {
      const token = begin(); notify('Checking review scope…');
      try {
        if ((!token.tenantId && !await ready(token)) || !token.tenantId) { if (token.generation === generation) notify('Authenticated review scope is not ready. Please retry.'); return false; }
        const result = await request({ question, reviewNavigation, tenantId: token.tenantId });
        if (!current(token)) return false;
        if (!result.uiAction) { notify(result.prose || 'No review was opened.'); return false; }
        const shown = consume(result.uiAction, token);
        if (shown) notify('Review surface opened. Data availability is shown in the review.');
        return shown;
      } catch (_) { if (token.generation === generation) notify('Review navigation is unavailable. Please try again; no action is confirmed.'); return false; }
    }
    async function routeText(question) {
      const token = begin();
      try {
        if (!token.tenantId && !await ready(token)) return true;
        const result = await request({ question, ...(token.tenantId ? { tenantId: token.tenantId } : {}) });
        if (token.generation !== generation) return true;
        if (result.handled === false) return false;
        if (!current(token)) { notify('Authenticated review scope is not ready. Please retry.'); return true; }
        if (result.uiAction) consume(result.uiAction, token);
        else notify(result.prose || 'No review was opened.');
        return true;
      } catch (_) { if (token.generation === generation) notify('Review scope could not be checked. Retry before continuing.'); return 'blocked'; }
    }
    return { begin, current, consume, open, routeText, invalidate: () => { generation++; authenticatedTenant = ''; } };
  }
  return { create };
}));
