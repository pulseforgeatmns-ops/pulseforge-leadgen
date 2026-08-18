'use strict';

// Pulseforge unified shell (Phase B).
// One shared navigation, tenant context, identity, theme control, and logout
// for every HTML entry point. Route compatibility is preserved: /dashboard,
// /setter, and /closer all still resolve — they render the same shell.
//
// Role visibility mirrors (never weakens) server authorization: hiding a link
// is presentation only; every route keeps its own requireAuth/requireRole.
//
// SPEC-099 — for client-role users, identity shows the business workspace name
// (from /api/me.client), and nav labels use MAX / MY BUSINESS.

(function () {
  const THEME_KEY = 'pulseforge-theme';

  // Primary navigation. `roles` controls visibility; hrefs may vary per role
  // so each role lands on the surface it is authorized to see.
  // `clientLabel` overrides the label for client-role users only (presentation).
  const NAV_ITEMS = [
    {
      id: 'home', label: 'Home',
      roles: ['admin', 'manager', 'viewer', 'client', 'setter', 'sales', 'closer'],
      href: { default: '/dashboard', setter: '/setter#view=home', sales: '/setter#view=home', closer: '/closer' },
    },
    {
      id: 'command-deck', label: 'Command Deck', clientLabel: 'Max',
      roles: ['admin', 'manager', 'viewer', 'client'],
      href: { default: '/command-deck' },
    },
    {
      id: 'client-intel', label: 'Client Intel', clientLabel: 'My Business',
      roles: ['admin', 'manager', 'client'],
      href: { default: '/client-intel' },
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
      href: { default: '/command-deck#operations' },
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

  function labelFor(item, role) {
    if (role === 'client' && item.clientLabel) return item.clientLabel;
    return item.label;
  }

  function currentSurface() {
    const path = window.location.pathname;
    if (path.startsWith('/command-deck')) {
      if ((window.location.hash || '') === '#operations') return 'operations';
      return 'command-deck';
    }
    if (path.startsWith('/max-briefing')) return 'command-deck';
    if (path.startsWith('/client-intel')) return 'client-intel';
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

  async function fetchTenantContext(context) {
    const role = context?.user?.role;
    // Client identity comes from /api/me.client (authoritative). Do not call
    // /api/clients — clients cannot list tenants, and must not influence display
    // via URL/query client id.
    if (role === 'client') {
      return {
        tenantName:
          context?.client?.display_name || context?.client?.name || null,
        activeClientId: context?.active_client_id ?? null,
        clients: [],
        canSwitch: false,
      };
    }
    if (!['admin', 'manager'].includes(role)) {
      return {
        tenantName: context?.client?.display_name || context?.client?.name || null,
        activeClientId: context?.active_client_id ?? null,
        clients: [],
        canSwitch: false,
      };
    }
    try {
      const response = await fetch('/api/clients', { credentials: 'same-origin' });
      if (!response.ok) {
        return {
          tenantName: null,
          activeClientId: context?.active_client_id ?? null,
          clients: [],
          canSwitch: false,
        };
      }
      const data = await response.json();
      const activeId = Number(data.active_client_id);
      const active = (data.clients || []).find(c => Number(c.id) === activeId);
      return {
        tenantName: active ? active.name : null,
        activeClientId: activeId,
        clients: data.clients || [],
        canSwitch: true,
      };
    } catch (_err) {
      return {
        tenantName: null,
        activeClientId: context?.active_client_id ?? null,
        clients: [],
        canSwitch: false,
      };
    }
  }

  async function switchActiveTenant(clientId) {
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

  function updateTenantDisplay(tenantName, activeClientId) {
    const tenantEl = document.querySelector('.pf-nav-tenant');
    const selectEl = document.getElementById('pfTenantSelect');
    if (tenantEl && tenantName) tenantEl.textContent = tenantName;
    if (selectEl && activeClientId != null) {
      selectEl.value = String(activeClientId);
    }
  }

  async function handleTenantSwitch(clientId, context, tenantState) {
    await switchActiveTenant(clientId);
    const nextId = Number(clientId);
    const active = (tenantState.clients || []).find(c => Number(c.id) === nextId);
    tenantState.activeClientId = nextId;
    tenantState.tenantName = active ? active.name : tenantState.tenantName;
    if (context) context.active_client_id = nextId;
    updateTenantDisplay(tenantState.tenantName, nextId);
    window.PulseforgeShell = {
      ...window.PulseforgeShell,
      context,
      tenantName: tenantState.tenantName,
      activeClientId: nextId,
    };
    document.dispatchEvent(new CustomEvent('pulseforge:tenant-changed', {
      detail: {
        active_client_id: nextId,
        tenantName: tenantState.tenantName,
      },
    }));
  }

  function buildNav(context, tenantState) {
    const role = context?.user?.role || null;
    const tenantName = tenantState?.tenantName || null;
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
      link.textContent = labelFor(item, role);
      link.dataset.pfNav = item.id;
      if (surface === item.id) link.setAttribute('aria-current', 'page');
      links.appendChild(link);
    }
    nav.appendChild(links);

    const group = document.createElement('div');
    group.className = 'pf-nav-group';

    // SPEC-099 — client users: upper-right identity is the business workspace.
    // Admin/manager: keep gold tenant pill + personal who.
    if (role === 'client' && tenantName) {
      const who = document.createElement('span');
      who.className = 'pf-nav-who pf-nav-workspace';
      who.textContent = tenantName;
      who.title = 'Your business workspace';
      who.dataset.pfWorkspace = '1';
      group.appendChild(who);
    } else {
      if (tenantState?.canSwitch && (tenantState.clients || []).length > 0) {
        const tenantWrap = document.createElement('div');
        tenantWrap.className = 'pf-nav-tenant-wrap';

        const tenant = document.createElement('span');
        tenant.className = 'pf-nav-tenant';
        tenant.textContent = tenantName || 'Select client';
        tenant.title = 'Active business workspace';
        tenantWrap.appendChild(tenant);

        const select = document.createElement('select');
        select.id = 'pfTenantSelect';
        select.className = 'pf-nav-tenant-select';
        select.setAttribute('aria-label', 'Active business workspace');
        for (const client of tenantState.clients) {
          const option = document.createElement('option');
          option.value = String(client.id);
          option.textContent = client.name;
          if (Number(client.id) === Number(tenantState.activeClientId)) {
            option.selected = true;
          }
          select.appendChild(option);
        }
        select.addEventListener('change', async () => {
          const previous = String(tenantState.activeClientId ?? '');
          const next = select.value;
          if (next === previous) return;
          select.disabled = true;
          try {
            await handleTenantSwitch(next, context, tenantState);
          } catch (err) {
            console.error('[shell] tenant switch failed:', err);
            select.value = previous;
            window.alert(err.message || 'Could not switch workspace');
          } finally {
            select.disabled = false;
          }
        });
        tenantWrap.appendChild(select);

        const createLink = document.createElement('a');
        createLink.className = 'pf-nav-link pf-nav-create-client';
        createLink.href = '/admin/clients';
        createLink.textContent = 'New client';
        createLink.title = 'Create and provision a tenant workspace';
        tenantWrap.appendChild(createLink);

        group.appendChild(tenantWrap);
      } else if (tenantName) {
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
    const tenantState = await fetchTenantContext(context);
    const nav = buildNav(context, tenantState);
    document.body.prepend(nav);
    applyTheme(readTheme());
    activateHashTab();
    window.addEventListener('hashchange', () => { activateHashTab(); refreshCurrent(); });
    window.PulseforgeShell = {
      context,
      tenantName: tenantState.tenantName,
      activeClientId: tenantState.activeClientId,
      toggleTheme,
      applyTheme,
      readTheme,
    };
    document.dispatchEvent(new CustomEvent('pulseforge:shell-ready', {
      detail: {
        context,
        tenantName: tenantState.tenantName,
        activeClientId: tenantState.activeClientId,
      },
    }));
  }

  if (document.readyState !== 'loading') init();
  else document.addEventListener('DOMContentLoaded', init);
})();
