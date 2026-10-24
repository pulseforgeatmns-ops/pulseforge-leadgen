(function (root) {
  'use strict';

  const BANNER_ID = 'pfAoImpersonationBanner';

  async function fetchImpersonationState() {
    const res = await fetch('/api/v1/admin/impersonation', { credentials: 'same-origin' });
    if (res.status === 401 || res.status === 403) return { active: false };
    if (!res.ok) return { active: false };
    const data = await res.json();
    return data.impersonation || { active: false };
  }

  async function exitImpersonation() {
    const res = await fetch('/api/v1/admin/impersonation/stop', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || 'Could not exit impersonation');
    }
    window.location.reload();
  }

  function renderBanner(state) {
    if (!state?.active) {
      document.getElementById(BANNER_ID)?.remove();
      return null;
    }
    const effectiveName = state.effective_user?.name || 'AO';
    const authName = state.authenticated_user?.name || 'Admin';
    let banner = document.getElementById(BANNER_ID);
    if (!banner) {
      banner = document.createElement('div');
      banner.id = BANNER_ID;
      banner.className = 'pf-impersonation-banner';
      banner.setAttribute('role', 'status');
      banner.setAttribute('aria-live', 'polite');
      document.body.prepend(banner);
    }
    banner.innerHTML = `
      <span><strong>Acting as ${escapeHtml(effectiveName)}</strong></span>
      <span>Signed in as ${escapeHtml(authName)}</span>
      <button type="button" data-exit-impersonation>Exit impersonation</button>
    `;
    banner.querySelector('[data-exit-impersonation]')?.addEventListener('click', () => {
      exitImpersonation().catch(err => {
        window.alert(err.message || 'Exit failed');
      });
    });
    return banner;
  }

  function escapeHtml(value) {
    return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function ensureBannerStyles() {
    if (document.querySelector('[data-pf-impersonation-css="1"]')) return;
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = '/shared/aoImpersonationBanner.css';
    link.dataset.pfImpersonationCss = '1';
    document.head.appendChild(link);
  }

  async function mountAoImpersonationBanner() {
    try {
      ensureBannerStyles();
      const state = await fetchImpersonationState();
      return renderBanner(state);
    } catch (err) {
      console.warn('[aoImpersonationBanner]', err.message);
      return null;
    }
  }

  const AoImpersonationBanner = {
    mountAoImpersonationBanner,
    fetchImpersonationState,
    exitImpersonation,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = AoImpersonationBanner;
  } else {
    root.AoImpersonationBanner = AoImpersonationBanner;
  }
}(typeof globalThis !== 'undefined' ? globalThis : this));
