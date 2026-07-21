'use strict';

// Pulseforge shared phone module (Phase A2).
// - One normalization/display/tel: path for every surface (mirror of
//   utils/phone.js on the server).
// - Dial handoff controller: persists the active call workspace to
//   sessionStorage before opening tel:, and restores it on visibilitychange /
//   pageshow / load so mobile OS handoff never loses workspace state.
//   Pulseforge deliberately does NOT try to force which native calling app
//   handles tel: — that is the device's choice. We control state continuity.

(function () {
  const ACTIVE_CALL_KEY = 'pulseforge.activeCall';
  const ACTIVE_CALL_TTL_MS = 4 * 60 * 60 * 1000; // stale after 4 hours

  function digitsOnly(value) {
    return String(value || '').replace(/\D+/g, '');
  }

  function normalizePhone(raw) {
    const trimmed = String(raw || '').trim();
    if (!trimmed) return null;
    let digits = digitsOnly(trimmed);
    if (digits.length === 11 && digits.startsWith('1')) digits = digits.slice(1);
    if (digits.length !== 10) return null;
    if (/^[01]/.test(digits) || /^[01]/.test(digits.slice(3, 6))) return null;
    return `+1${digits}`;
  }

  function formatPhoneDisplay(raw) {
    const normalized = normalizePhone(raw);
    if (normalized) {
      const d = normalized.slice(2);
      return `(${d.slice(0, 3)}) ${d.slice(3, 6)}-${d.slice(6)}`;
    }
    const trimmed = String(raw || '').trim();
    return trimmed || null;
  }

  function telHref(raw) {
    const normalized = normalizePhone(raw);
    if (normalized) return `tel:${normalized}`;
    const digits = digitsOnly(raw);
    return digits ? `tel:${digits}` : null;
  }

  async function copyPhone(raw) {
    const display = formatPhoneDisplay(raw) || String(raw || '');
    try {
      await navigator.clipboard.writeText(display);
      window.PulseforgeA11y?.announce(`Phone number ${display} copied`);
      return true;
    } catch (_err) {
      window.PulseforgeA11y?.announce('Copy failed — select the number manually', { assertive: true });
      return false;
    }
  }

  // ── Dial handoff controller ───────────────────────────────────────────
  function readActiveCall() {
    try {
      const raw = sessionStorage.getItem(ACTIVE_CALL_KEY);
      if (!raw) return null;
      const state = JSON.parse(raw);
      if (!state?.prospectId) return null;
      if (Date.now() - new Date(state.startedAt || 0).getTime() > ACTIVE_CALL_TTL_MS) {
        sessionStorage.removeItem(ACTIVE_CALL_KEY);
        return null;
      }
      return state;
    } catch (_err) {
      return null;
    }
  }

  function clearActiveCall() {
    try { sessionStorage.removeItem(ACTIVE_CALL_KEY); } catch (_err) { /* private mode */ }
    removeBanner();
  }

  /** Persist call state and open the OS dialer via a normalized tel: link. */
  function beginDialHandoff({ prospectId, phone, companyName, clientId, workspaceRoute }) {
    const href = telHref(phone);
    if (!href) {
      window.PulseforgeA11y?.announce('No dialable phone number for this prospect', { assertive: true });
      return false;
    }
    try {
      sessionStorage.setItem(ACTIVE_CALL_KEY, JSON.stringify({
        prospectId,
        phone: String(phone || ''),
        companyName: companyName || null,
        clientId: clientId || null,
        workspaceRoute: workspaceRoute || window.location.pathname,
        startedAt: new Date().toISOString(),
      }));
    } catch (_err) { /* sessionStorage unavailable — dial anyway */ }
    window.location.href = href;
    return true;
  }

  function removeBanner() {
    document.getElementById('pf-call-banner')?.remove();
  }

  function showRestoreBanner(state) {
    if (document.getElementById('pf-call-banner')) return;
    const banner = document.createElement('div');
    banner.id = 'pf-call-banner';
    banner.className = 'pf-call-banner';
    banner.setAttribute('role', 'region');
    banner.setAttribute('aria-label', 'Back from call — log outcome');
    const name = state.companyName || 'Prospect';
    banner.innerHTML = `
      <div class="pf-call-banner-dot" aria-hidden="true"></div>
      <div class="pf-call-banner-text">
        <strong>Back from call with ${escapeHtml(name)} — log outcome</strong>
        <span>${state.phone ? `${escapeHtml(formatPhoneDisplay(state.phone) || state.phone)} · ` : ''}uses the device phone dialer (tel:), never FaceTime</span>
      </div>
      <button type="button" class="pf-primary" data-pf-call-action="log">Log outcome</button>
      <button type="button" data-pf-call-action="resume">Resume workspace</button>
      <button type="button" data-pf-call-action="dismiss" aria-label="Dismiss call reminder">Dismiss</button>
    `;
    banner.addEventListener('click', event => {
      const action = event.target.closest('[data-pf-call-action]')?.dataset.pfCallAction;
      if (!action) return;
      if (action === 'dismiss') {
        clearActiveCall();
        return;
      }
      removeBanner();
      const detail = { ...state, intent: action === 'log' ? 'log_outcome' : 'resume' };
      // Pages subscribe to this event; the shared workspace also listens.
      document.dispatchEvent(new CustomEvent('pulseforge:restore-call', { detail }));
    });
    document.body.appendChild(banner);
    window.PulseforgeA11y?.announce(`Back from call with ${name} — log outcome`);
  }

  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  let restoreScheduled = false;
  function restoreActiveCall() {
    if (document.visibilityState === 'hidden') return;
    if (restoreScheduled) return;
    restoreScheduled = true;
    window.setTimeout(() => {
      restoreScheduled = false;
      const state = readActiveCall();
      if (state) showRestoreBanner(state);
    }, 250);
  }

  document.addEventListener('visibilitychange', restoreActiveCall);
  window.addEventListener('pageshow', restoreActiveCall);
  if (document.readyState !== 'loading') restoreActiveCall();
  else document.addEventListener('DOMContentLoaded', restoreActiveCall);

  window.PulseforgePhone = {
    ACTIVE_CALL_KEY,
    beginDialHandoff,
    clearActiveCall,
    copyPhone,
    formatPhoneDisplay,
    normalizePhone,
    readActiveCall,
    restoreActiveCall,
    telHref,
  };
})();
