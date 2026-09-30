(function (root) {
  'use strict';

  const MANAGER_ROLES = new Set(['admin', 'manager']);

  const AO_NAV_ITEMS = Object.freeze([
    { id: 'field', label: 'Field Mode', href: '/ao/field' },
    { id: 'accounts', label: 'Open CRM', href: '/ao/crm' },
    { id: 'manager', label: 'Manager View', href: '/ao/crm/manager', managerOnly: true },
  ]);

  function currentAoSurface(pathname) {
    const path = String(pathname || '').replace(/\/+$/, '') || '/';
    if (path.startsWith('/ao/crm/manager')) return 'manager';
    if (path.startsWith('/ao/crm')) return 'accounts';
    if (path.startsWith('/ao/field')) return 'field';
    if (path.startsWith('/ao/command-center')) return 'command-center';
    return null;
  }

  function aoNavItemsForRole(role) {
    return AO_NAV_ITEMS.filter(item => {
      if (item.managerOnly) return MANAGER_ROLES.has(role);
      return true;
    });
  }

  async function fetchAoNavContext() {
    const res = await fetch('/api/me', { credentials: 'same-origin' });
    if (!res.ok) throw new Error('Could not load session');
    return res.json();
  }

  function buildAoShellNav(context, pathname) {
    const role = context?.user?.role || null;
    const surface = currentAoSurface(pathname);
    const nav = document.createElement('nav');
    nav.className = 'pf-shell-nav';
    nav.setAttribute('aria-label', 'AO navigation');
    nav.dataset.aoShellNav = '1';

    const brand = document.createElement('span');
    brand.className = 'pf-nav-brand';
    brand.textContent = 'AO';
    brand.setAttribute('aria-hidden', 'true');
    nav.appendChild(brand);

    const links = document.createElement('div');
    links.className = 'pf-nav-links';
    for (const item of aoNavItemsForRole(role)) {
      const link = document.createElement('a');
      link.className = 'pf-nav-link';
      link.href = item.href;
      link.textContent = item.label;
      link.dataset.aoNav = item.id;
      if (surface === item.id) link.setAttribute('aria-current', 'page');
      links.appendChild(link);
    }
    nav.appendChild(links);

    const group = document.createElement('div');
    group.className = 'pf-nav-group';

    if (context?.user?.name) {
      const who = document.createElement('span');
      who.className = 'pf-nav-who';
      who.textContent = context.user.name;
      who.title = role ? `Signed in · ${role}` : 'Signed in';
      group.appendChild(who);
    }

    const logout = document.createElement('a');
    logout.className = 'pf-nav-logout';
    logout.href = '/logout';
    logout.textContent = 'Log out';
    group.appendChild(logout);

    nav.appendChild(group);
    return nav;
  }

  async function mountAoShellNav() {
    if (document.querySelector('[data-ao-shell-nav="1"]')) return null;
    let context = { user: {} };
    try {
      context = await fetchAoNavContext();
    } catch (err) {
      console.warn('[aoShellNav] session lookup failed:', err.message);
    }
    const nav = buildAoShellNav(context, window.location.pathname);
    document.body.prepend(nav);
    document.dispatchEvent(new CustomEvent('ao:shell-nav-ready', { detail: { context } }));
    return nav;
  }

  const AoShellNav = {
    AO_NAV_ITEMS,
    MANAGER_ROLES,
    currentAoSurface,
    aoNavItemsForRole,
    buildAoShellNav,
    mountAoShellNav,
    fetchAoNavContext,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = AoShellNav;
  } else {
    root.AoShellNav = AoShellNav;
    if (document.readyState !== 'loading') mountAoShellNav();
    else document.addEventListener('DOMContentLoaded', mountAoShellNav);
  }
}(typeof globalThis !== 'undefined' ? globalThis : this));
