'use strict';

// Pulseforge consolidated activity panel (Phase B §7).
// One contextual activity component replacing the page-specific "Command
// Feed" (dashboard) and "Scout Feed" (calls). Pages register event sources
// that return normalized events; the panel owns filtering, rendering, and
// refresh. Filters: Prospect, Operator, Agents, System, Errors.
//
// Normalized event shape:
//   { id, kind: 'prospect'|'operator'|'agent'|'system'|'error',
//     icon, strong, text, occurredAt, prospectId? }

(function () {
  const FILTERS = [
    { id: 'prospect', label: 'Prospect' },
    { id: 'operator', label: 'Operator' },
    { id: 'agent', label: 'Agents' },
    { id: 'system', label: 'System' },
    { id: 'error', label: 'Errors' },
  ];

  function esc(value) {
    return String(value ?? '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  function fmtTime(value) {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    const sameDay = date.toDateString() === new Date().toDateString();
    return sameDay
      ? date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      : date.toLocaleDateString([], { month: 'short', day: 'numeric' });
  }

  function create({ container, title = 'Activity', sources = [], defaultFilters = ['prospect', 'operator'], refreshMs = 60000, onOpenProspect = null }) {
    const state = {
      events: [],
      active: new Set(defaultFilters),
      timer: null,
      degraded: false,
    };

    container.classList.add('pf-activity-panel');
    container.innerHTML = `
      <div class="pf-activity-head">
        <div class="pf-activity-title">${esc(title)}</div>
        <div class="pf-activity-sub" data-pf-activity="sub">Loading…</div>
      </div>
      <div class="pf-activity-filters" role="group" aria-label="Activity filters">
        ${FILTERS.map(filter => `
          <button type="button" data-pf-filter="${filter.id}" aria-pressed="${state.active.has(filter.id) ? 'true' : 'false'}">${filter.label}</button>
        `).join('')}
      </div>
      <div class="pf-activity-feed" data-pf-activity="feed" aria-label="Activity feed">
        <div class="pf-activity-empty">Loading…</div>
      </div>
    `;

    const feedEl = container.querySelector('[data-pf-activity="feed"]');
    const subEl = container.querySelector('[data-pf-activity="sub"]');

    function visibleEvents() {
      return state.events
        .filter(event => state.active.has(event.kind))
        .sort((a, b) => new Date(b.occurredAt || 0) - new Date(a.occurredAt || 0))
        .slice(0, 120);
    }

    function renderFeed() {
      const events = visibleEvents();
      // A failed activity source is non-blocking: surface a subtle inline
      // status here instead of any global error badge.
      subEl.textContent = state.degraded
        ? 'Activity temporarily unavailable — retrying'
        : `${events.length} events · auto-refreshing`;
      if (!events.length) {
        feedEl.innerHTML = state.degraded
          ? '<div class="pf-activity-empty">Activity temporarily unavailable — retrying automatically</div>'
          : '<div class="pf-activity-empty">No matching activity</div>';
        return;
      }
      feedEl.innerHTML = events.map(event => `
        <div class="pf-activity-item pf-kind-${esc(event.kind)}" ${event.prospectId ? `data-pf-prospect="${esc(event.prospectId)}" role="button" tabindex="0"` : ''}>
          <div class="pf-activity-icon" aria-hidden="true">${esc(event.icon || event.kind.slice(0, 1).toUpperCase())}</div>
          <div class="pf-activity-text">${event.strong ? `<strong>${esc(event.strong)}</strong> ` : ''}${esc(event.text || '')}
            <div class="pf-activity-time">${fmtTime(event.occurredAt)}</div>
          </div>
        </div>
      `).join('');
    }

    async function refresh() {
      const settled = await Promise.allSettled(sources.map(source => source()));
      const events = [];
      let failures = 0;
      for (const result of settled) {
        if (result.status === 'fulfilled' && Array.isArray(result.value)) events.push(...result.value);
        else if (result.status === 'rejected') failures += 1;
      }
      state.events = events;
      state.degraded = failures > 0;
      renderFeed();
    }

    container.addEventListener('click', event => {
      const filterBtn = event.target.closest('[data-pf-filter]');
      if (filterBtn) {
        const id = filterBtn.dataset.pfFilter;
        if (state.active.has(id)) state.active.delete(id);
        else state.active.add(id);
        filterBtn.setAttribute('aria-pressed', state.active.has(id) ? 'true' : 'false');
        renderFeed();
        return;
      }
      const item = event.target.closest('[data-pf-prospect]');
      if (item && typeof onOpenProspect === 'function') onOpenProspect(item.dataset.pfProspect);
    });
    container.addEventListener('keydown', event => {
      if (event.key !== 'Enter' && event.key !== ' ') return;
      const item = event.target.closest('[data-pf-prospect]');
      if (item && typeof onOpenProspect === 'function') {
        event.preventDefault();
        onOpenProspect(item.dataset.pfProspect);
      }
    });

    refresh();
    if (refreshMs > 0) state.timer = window.setInterval(refresh, refreshMs);

    return {
      refresh,
      destroy() {
        if (state.timer) window.clearInterval(state.timer);
        container.innerHTML = '';
      },
    };
  }

  window.PulseforgeActivityPanel = { create, FILTERS };
})();
