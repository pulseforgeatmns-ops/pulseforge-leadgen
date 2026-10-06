'use strict';

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.MaxAoBriefingTenantApi = factory();
  }
})(typeof globalThis !== 'undefined' ? globalThis : this, function maxAoBriefingTenantApiFactory() {
  const PAGE_DEFAULT_CLIENT_ID = 10;

  const AO_FLAG_API_PREFIXES = [
    '/api/v1/max/ao-flags',
    '/api/v1/max/ao-flag-notifications',
  ];

  function resolveMaxBriefingClientId({
    urlClientId,
    selectValue,
    clients,
    pageDefault = PAGE_DEFAULT_CLIENT_ID,
  } = {}) {
    const list = Array.isArray(clients) ? clients : [];
    const clientIds = new Set(list.map((c) => Number(c.id)).filter((n) => Number.isFinite(n) && n > 0));

    const pick = (raw) => {
      if (raw == null || raw === '') return null;
      const n = Number(raw);
      if (!Number.isFinite(n) || n <= 0) return null;
      if (clientIds.size > 0 && !clientIds.has(n)) return null;
      return n;
    };

    const fromUrl = pick(urlClientId);
    if (fromUrl != null) return fromUrl;

    const fromSelect = pick(selectValue);
    if (fromSelect != null) return fromSelect;

    const fromDefault = pick(pageDefault);
    if (fromDefault != null) return fromDefault;

    if (list.length > 0) {
      const first = Number(list[0].id);
      if (Number.isFinite(first) && first > 0) return first;
    }

    return pageDefault;
  }

  function withMaxBriefingClientId(path, clientId) {
    const id = Number(clientId);
    if (!Number.isFinite(id) || id <= 0) {
      throw new Error('client_id required for tenant-scoped Max AO briefing request');
    }
    const base = String(path || '');
    const hashIdx = base.indexOf('#');
    const hash = hashIdx >= 0 ? base.slice(hashIdx) : '';
    const pathAndQuery = hashIdx >= 0 ? base.slice(0, hashIdx) : base;
    const qIdx = pathAndQuery.indexOf('?');
    const pathname = qIdx >= 0 ? pathAndQuery.slice(0, qIdx) : pathAndQuery;
    const search = qIdx >= 0 ? pathAndQuery.slice(qIdx + 1) : '';
    const params = new URLSearchParams(search);
    params.set('client_id', String(id));
    const qs = params.toString();
    return `${pathname}${qs ? `?${qs}` : ''}${hash}`;
  }

  function pathRequiresBriefingClientId(path) {
    const base = String(path || '').split('#')[0];
    return AO_FLAG_API_PREFIXES.some(
      (prefix) => base === prefix || base.startsWith(`${prefix}/`) || base.startsWith(`${prefix}?`)
    );
  }

  return {
    PAGE_DEFAULT_CLIENT_ID,
    resolveMaxBriefingClientId,
    withMaxBriefingClientId,
    pathRequiresBriefingClientId,
    AO_FLAG_API_PREFIXES,
  };
});
