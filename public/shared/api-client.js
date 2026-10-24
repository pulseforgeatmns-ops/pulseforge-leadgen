'use strict';

// Pulseforge shared API client (Phase A2). One fetch wrapper for both shells:
// same-origin credentials, JSON handling, 401 → login redirect, and typed
// helpers for the canonical workspace endpoints.

(function () {
  async function request(path, options = {}) {
    const response = await fetch(path, {
      credentials: 'same-origin',
      headers: {
        Accept: 'application/json',
        ...(options.body ? { 'Content-Type': 'application/json' } : {}),
        ...(options.headers || {}),
      },
      ...options,
      body: options.body && typeof options.body !== 'string'
        ? JSON.stringify(options.body)
        : options.body,
    });
    if (response.status === 401) {
      window.location.href = '/login';
      throw new Error('Session expired');
    }
    let payload = null;
    try { payload = await response.json(); } catch (_err) { /* non-JSON */ }
    if (!response.ok) {
      const error = new Error(payload?.error || `Request failed (${response.status})`);
      error.status = response.status;
      error.payload = payload;
      throw error;
    }
    return payload;
  }

  function idempotencyKey() {
    if (window.crypto?.randomUUID) return window.crypto.randomUUID();
    return `pf-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  }

  const PulseforgeApi = {
    request,
    idempotencyKey,
    getWorkspace: prospectId => request(`/api/prospects/${encodeURIComponent(prospectId)}/workspace`),
    getCallPreparation: prospectId => request(`/api/prospects/${encodeURIComponent(prospectId)}/call-preparation`),
    addNote: (prospectId, text, noteType = 'operator') => request(`/api/prospects/${encodeURIComponent(prospectId)}/notes`, {
      method: 'POST',
      body: { text, note_type: noteType },
    }),
    transitionLifecycle: (prospectId, body) => request(`/api/prospects/${encodeURIComponent(prospectId)}/lifecycle`, {
      method: 'POST',
      body: { idempotency_key: idempotencyKey(), ...body },
    }),
    logCallDisposition: (prospectId, body) => request(`/setter/api/leads/${encodeURIComponent(prospectId)}/call-disposition`, {
      method: 'POST',
      body: { idempotency_key: idempotencyKey(), ...body },
    }),
    scheduleCallback: (prospectId, callbackAt) => request(`/setter/api/leads/${encodeURIComponent(prospectId)}/callback`, {
      method: 'PATCH',
      body: { callback_at: callbackAt },
    }),
  };

  window.PulseforgeApi = PulseforgeApi;
})();
