'use strict';

/**
 * Canonical browser-side active tenant state for operator switchers.
 * All `[data-pf-tenant-select]` controls bind here; one switch updates every selector.
 */
(function () {
  const SELECTOR = '[data-pf-tenant-select]';

  const state = {
    clients: [],
    activeClientId: null,
    tenantName: null,
    featureMap: {},
    switching: false,
    boundSelects: new WeakSet(),
    reloadHandlers: new Set(),
  };

  function escapeHtml(text) {
    return String(text)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fillSelectOptions(select) {
    if (!select || !state.clients.length) return;
    const current = String(state.activeClientId ?? '');
    select.innerHTML = state.clients.map((c) => {
      const id = String(c.id);
      const selected = id === current ? ' selected' : '';
      return `<option value="${escapeHtml(id)}"${selected}>${escapeHtml(c.name)}</option>`;
    }).join('');
  }

  function syncAllSelects() {
    document.querySelectorAll(SELECTOR).forEach((el) => {
      if (state.activeClientId != null && el.value !== String(state.activeClientId)) {
        el.value = String(state.activeClientId);
      }
      el.disabled = state.switching;
    });
    const tenantEl = document.querySelector('.pf-nav-tenant');
    if (tenantEl && state.tenantName) tenantEl.textContent = state.tenantName;
  }

  function hydrateFromClientsApi(data) {
    if (!data) return;
    state.clients = data.clients || [];
    state.activeClientId = data.active_client_id != null ? Number(data.active_client_id) : null;
    state.featureMap = Object.fromEntries(
      state.clients.map((c) => [String(c.id), c.setter_pipeline_v2_enabled === true])
    );
    const active = state.clients.find((c) => Number(c.id) === Number(state.activeClientId));
    state.tenantName = active ? active.name : state.tenantName;
    document.querySelectorAll(SELECTOR).forEach(fillSelectOptions);
    syncAllSelects();
  }

  async function postSwitch(clientId) {
    const response = await fetch('/api/clients/active', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: clientId }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.message || payload.error || 'Tenant switch failed');
    }
    return response.json();
  }

  function dispatchTenantChanged(detail) {
    document.dispatchEvent(new CustomEvent('pulseforge:tenant-changed', { detail }));
  }

  async function switchActiveTenant(clientId, { source = 'coordinator' } = {}) {
    const nextId = Number(clientId);
    const previousId = state.activeClientId;
    if (!Number.isFinite(nextId) || nextId === previousId) {
      return null;
    }
    if (state.switching) return null;

    state.switching = true;
    syncAllSelects();
    try {
      const payload = await postSwitch(nextId);
      state.activeClientId = nextId;
      const active = state.clients.find((c) => Number(c.id) === nextId);
      state.tenantName = active ? active.name : state.tenantName;
      if (payload?.features && payload.features.client_id != null) {
        state.featureMap[String(payload.features.client_id)] =
          payload.features.setter_pipeline_v2_enabled === true;
      }
      syncAllSelects();
      const detail = {
        active_client_id: nextId,
        tenantName: state.tenantName,
        features: payload?.features || null,
        workspaceStatus: payload?.status || null,
        source,
      };
      dispatchTenantChanged(detail);
      for (const handler of state.reloadHandlers) {
        try {
          await handler(detail);
        } catch (err) {
          console.error('[tenantCoordinator] reload handler failed:', err);
        }
      }
      return payload;
    } catch (err) {
      state.activeClientId = previousId;
      syncAllSelects();
      throw err;
    } finally {
      state.switching = false;
      syncAllSelects();
    }
  }

  function bindTenantSelect(select) {
    if (!select || state.boundSelects.has(select)) return;
    select.setAttribute('data-pf-tenant-select', '1');
    state.boundSelects.add(select);
    fillSelectOptions(select);
    select.addEventListener('change', async () => {
      const previous = String(state.activeClientId ?? '');
      const next = select.value;
      if (next === previous) return;
      try {
        await switchActiveTenant(next, { source: select.id || 'tenant-select' });
      } catch (err) {
        console.error('[tenantCoordinator] switch failed:', err);
        select.value = previous;
        window.alert(err.message || 'Could not switch workspace');
      }
    });
  }

  function registerReloadHandler(fn) {
    if (typeof fn === 'function') state.reloadHandlers.add(fn);
  }

  window.PulseforgeTenantCoordinator = {
    hydrateFromClientsApi,
    switchActiveTenant,
    bindTenantSelect,
    registerReloadHandler,
    syncAllSelects,
    getActiveClientId: () => state.activeClientId,
    getTenantName: () => state.tenantName,
    getClients: () => state.clients.slice(),
    getFeatureMap: () => ({ ...state.featureMap }),
  };
})();
