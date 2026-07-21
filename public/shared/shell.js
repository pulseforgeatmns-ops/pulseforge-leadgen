'use strict';

// Pulseforge unified shell (Phase B).
// One shared navigation, tenant context, identity, theme control, and logout
// for every HTML entry point. Route compatibility is preserved: /dashboard,
// /setter, and /closer all still resolve — they render the same shell.
//
// Role visibility mirrors (never weakens) server authorization: hiding a link
// is presentation only; every route keeps its own requireAuth/requireRole.

(function () {
  const THEME_KEY = 'pulseforge-theme';

  // Primary navigation. `roles` controls visibility; hrefs may vary per role
  // so each role lands on the surface it is authorized to see.
  const NAV_ITEMS = [
    {
      id: 'home', label: 'Home',
      roles: ['admin', 'manager', 'viewer', 'client', 'setter', 'sales', 'closer'],
      href: { default: '/dashboard', setter: '/setter#view=home', sales: '/setter#view=home', closer: '/closer' },
    },
    {
      id: 'pipeline', label: 'Pipeline',
      roles: ['admin', 'manager', 'viewer', 'client', 'setter', 'sales'],
      href: { default: '/dashboard#pf-tab=pipeline', setter: '/setter#view=pipeline', sales: '/setter#view=pipeline' },
    },
    {
      id: 'calls', label: 'Calls',
      roles: ['admin', 'manager', 'setter', 'sales'],
      href: { default: '/setter#view=home' },
    },
    {
      id: 'customers', label: 'Customers',
      roles: ['admin', 'manager', 'closer', 'sales'],
      href: { default: '/closer' },
    },
    {
      id: 'revenue', label: 'Revenue',
      roles: ['admin', 'manager'],
      href: { default: '/dashboard#pf-tab=pipeline' },
    },
    {
      id: 'campaigns', label: 'Campaigns',
      roles: ['admin', 'manager'],
      href: { default: '/dashboard#pf-tab=approvals' },
    },
    {
      id: 'analytics', label: 'Analytics',
      roles: ['admin', 'manager', 'viewer', 'client'],
      href: { default: '/dashboard#pf-tab=analytics' },
    },
    {
      id: 'operations', label: 'Operations',
      roles: ['admin', 'manager'],
      href: { default: '/dashboard#pf-tab=agents' },
    },
    {
      id: 'settings', label: 'Settings',
      roles: ['admin'],
      href: { default: '/dashboard#pf-tab=users' },
    },
  ];

  function hrefFor(item, role) {
    return (role && item.href[role]) || item.href.default;
  }

  function currentSurface() {
    const path = window.location.pathname;
    if (path.startsWith('/setter')) return 'calls';
    if (path.startsWith('/closer')) return 'customers';
    if (path.startsWith('/dashboard')) {
      const match = /pf-tab=([a-z_-]+)/.exec(window.location.hash || '');
      const tab = match ? match[1] : null;
      if (tab === 'pipeline') return 'pipeline';
      if (tab === 'analytics') return 'analytics';
      if (tab === 'approvals') return 'campaigns';
      if (['agents', 'actions', 'activity'].includes(tab)) return 'operations';
      if (tab === 'users') return 'settings';
      return 'home';
    }
    return null;
  }

  // ── Theme ───────────────────────────────────────────────────────────
  // 'warm' (Phase B default) or 'dark' (legacy command palette).
  // Legacy stored values migrate: 'light' → warm, 'dark' → dark.
  function readTheme() {
    const stored = localStorage.getItem(THEME_KEY);
    if (stored === 'dark') return 'dark';
    return 'warm';
  }

  function applyTheme(theme) {
    document.body.classList.toggle('theme-dark', theme === 'dark');
    // Legacy page styles key off light-mode for input/table contrast.
    document.body.classList.toggle('light-mode', theme !== 'dark');
    const btn = document.getElementById('pfThemeToggle');
    if (btn) {
      btn.textContent = theme === 'dark' ? '☀' : '☾';
      btn.setAttribute('aria-label', theme === 'dark' ? 'Switch to warm theme' : 'Switch to dark theme');
      btn.title = btn.getAttribute('aria-label');
    }
    document.dispatchEvent(new CustomEvent('pulseforge:theme-changed', { detail: { theme } }));
  }

  function toggleTheme() {
    const next = document.body.classList.contains('theme-dark') ? 'warm' : 'dark';
    localStorage.setItem(THEME_KEY, next);
    applyTheme(next);
  }

  async function fetchContext() {
    try {
      const response = await fetch('/api/me', { credentials: 'same-origin' });
      if (!response.ok) return null;
      return await response.json();
    } catch (_err) {
      return null;
    }
  }

  async function fetchTenantName(context) {
    const role = context?.user?.role;
    if (!['admin', 'manager'].includes(role)) return null;
    try {
      const response = await fetch('/api/clients', { credentials: 'same-origin' });
      if (!response.ok) return null;
      const data = await response.json();
      const active = (data.clients || []).find(c => Number(c.id) === Number(data.active_client_id));
      return active ? active.name : null;
    } catch (_err) {
      return null;
    }
  }

  function buildNav(context, tenantName) {
    const role = context?.user?.role || null;
    const surface = currentSurface();
    const nav = document.createElement('nav');
    nav.className = 'pf-shell-nav';
    nav.setAttribute('aria-label', 'Pulseforge primary navigation');

    const brand = document.createElement('a');
    brand.className = 'pf-nav-brand';
    brand.href = ['setter', 'sales'].includes(role) ? '/setter' : (role === 'closer' ? '/closer' : '/dashboard');
    brand.textContent = 'PULSEFORGE';
    nav.appendChild(brand);

    const links = document.createElement('div');
    links.className = 'pf-nav-links';
    for (const item of NAV_ITEMS) {
      if (role && !item.roles.includes(role)) continue;
      const link = document.createElement('a');
      link.className = 'pf-nav-link';
      link.href = hrefFor(item, role);
      link.textContent = item.label;
      link.dataset.pfNav = item.id;
      if (surface === item.id) link.setAttribute('aria-current', 'page');
      links.appendChild(link);
    }
    nav.appendChild(links);

    const group = document.createElement('div');
    group.className = 'pf-nav-group';
    if (tenantName) {
      const tenant = document.createElement('span');
      tenant.className = 'pf-nav-tenant';
      tenant.textContent = tenantName;
      tenant.title = 'Active client';
      group.appendChild(tenant);
    }
    if (context?.user?.name) {
      const who = document.createElement('span');
      who.className = 'pf-nav-who';
      who.textContent = context.user.name;
      who.title = context.user.role ? `Signed in · ${context.user.role}` : 'Signed in';
      group.appendChild(who);
    }
    const themeBtn = document.createElement('button');
    themeBtn.type = 'button';
    themeBtn.id = 'pfThemeToggle';
    themeBtn.className = 'pf-nav-theme';
    themeBtn.addEventListener('click', toggleTheme);
    group.appendChild(themeBtn);

    const logout = document.createElement('a');
    logout.className = 'pf-nav-logout';
    logout.href = '/logout';
    logout.textContent = 'Log out';
    group.appendChild(logout);

    nav.appendChild(group);
    return nav;
  }

  function refreshCurrent() {
    const surface = currentSurface();
    document.querySelectorAll('.pf-shell-nav .pf-nav-link').forEach(link => {
      if (link.dataset.pfNav === surface) link.setAttribute('aria-current', 'page');
      else link.removeAttribute('aria-current');
    });
  }

  // Deep-link support: /dashboard#pf-tab=pipeline activates the matching
  // legacy sidebar tab once the page's own script has rendered it.
  function activateHashTab() {
    const match = /pf-tab=([a-z_-]+)/.exec(window.location.hash || '');
    if (!match) return;
    const tab = match[1];
    let attempts = 0;
    const timer = window.setInterval(() => {
      attempts += 1;
      const target = document.querySelector(`.sidebar [data-tab="${tab}"], .nav-tabs [data-tab="${tab}"]`);
      if (target) {
        target.click();
        window.clearInterval(timer);
      } else if (attempts > 40) {
        window.clearInterval(timer);
      }
    }, 150);
  }

  async function init() {
    applyTheme(readTheme());
    const context = await fetchContext();
    const tenantName = await fetchTenantName(context);
    const nav = buildNav(context, tenantName);
    document.body.prepend(nav);
    applyTheme(readTheme());
    activateHashTab();
    window.addEventListener('hashchange', () => { activateHashTab(); refreshCurrent(); });
    window.PulseforgeShell = { context, tenantName, toggleTheme, applyTheme, readTheme };
    document.dispatchEvent(new CustomEvent('pulseforge:shell-ready', { detail: { context, tenantName } }));
  }

  if (document.readyState !== 'loading') init();
  else document.addEventListener('DOMContentLoaded', init);
})();
