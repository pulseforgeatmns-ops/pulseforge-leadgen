'use strict';

/**
 * SPEC-097 Living Command Deck — spatial domain renderer.
 * Render-only consumer of model.spatialOverview from CommandDeckModel.
 */

(function () {
  const PRIORITY_LABELS = Object.freeze({
    monitored: 'Monitored',
    normal: 'Normal',
    elevated: 'Elevated',
    urgent: 'Urgent',
  });

  const SLOT_POSITIONS = Object.freeze({
    top: { tx: 0, ty: -1 },
    left: { tx: -1, ty: 0 },
    right: { tx: 1, ty: 0 },
    bottom: { tx: 0, ty: 1 },
  });

  /** @type {object|null} */
  let activeOverview = null;
  /** @type {string|null} */
  let activeDomainId = null;
  /** @type {boolean} */
  let listView = false;

  const handlers = {
    onDiscussMax: null,
    onOpenMission: null,
    onReviewRecommendation: null,
    onAskMax: null,
    escapeHtml: null,
    prefersReducedMotion: null,
  };

  const els = {};

  function bindElements() {
    els.deck = document.getElementById('cdSpatialDeck');
    els.orbit = document.getElementById('cdSpatialOrbit');
    els.maxHeadline = document.getElementById('cdSpatialMaxHeadline');
    els.maxSubline = document.getElementById('cdSpatialMaxSubline');
    els.list = document.getElementById('cdSpatialList');
    els.viewToggle = document.getElementById('cdViewToggle');
    els.unseenBanner = document.getElementById('cdUnseenBanner');
    els.drawer = document.getElementById('cdDomainDrawer');
    els.drawerTitle = document.getElementById('cdDomainDrawerTitle');
    els.drawerEyebrow = document.getElementById('cdDomainDrawerEyebrow');
    els.drawerBody = document.getElementById('cdDomainDrawerBody');
    els.drawerDiscuss = document.getElementById('cdDomainDiscussMax');
    els.explain = document.getElementById('cdPriorityExplain');
    els.explainBody = document.getElementById('cdPriorityExplainBody');
    els.recentSelect = document.getElementById('cdRecentConversations');
    els.askInput = document.getElementById('cdAskInput');
  }

  function esc(value) {
    return handlers.escapeHtml
      ? handlers.escapeHtml(value)
      : String(value == null ? '' : value);
  }

  function reducedMotion() {
    return handlers.prefersReducedMotion ? handlers.prefersReducedMotion() : false;
  }

  function orbitRadius() {
    if (window.matchMedia('(max-width: 640px)').matches) return 0;
    return 148;
  }

  /**
   * @param {object} overview - spatialOverview from CommandDeckModel
   */
  function render(overview) {
    if (!els.deck) bindElements();
    if (!overview || !els.deck) return false;

    activeOverview = overview;
    els.deck.hidden = false;

    renderMaxAnchor(overview.maxAnchor);
    renderDomains(overview.domains || []);
    renderListFallback(overview.listFallback || overview.domains || []);
    renderUnseenBanner(overview.unseenChanges || []);
    updateAskPlaceholder(overview.maxAnchor);
    renderRecentConversations();

    if (window.matchMedia('(max-width: 640px)').matches) {
      setListView(true, { silent: true });
    }

    return true;
  }

  function renderMaxAnchor(anchor) {
    if (!anchor) return;
    if (els.maxHeadline) {
      els.maxHeadline.textContent = anchor.headline || 'Max';
    }
    if (els.maxSubline) {
      els.maxSubline.textContent = anchor.subline || '';
      els.maxSubline.hidden = !anchor.subline;
    }
  }

  function updateAskPlaceholder(anchor) {
    if (els.askInput && anchor && anchor.askPlaceholder) {
      els.askInput.placeholder = anchor.askPlaceholder;
    }
  }

  function renderDomains(domains) {
    if (!els.orbit) return;

    const existing = els.orbit.querySelectorAll('.cd-spatial-node');
    existing.forEach((n) => n.remove());

    const radius = orbitRadius();
    for (const domain of domains) {
      const node = document.createElement('button');
      node.type = 'button';
      node.className = buildNodeClasses(domain);
      node.dataset.domainId = domain.id;
      node.setAttribute('aria-label', `${domain.label}, ${PRIORITY_LABELS[domain.priority] || domain.priority}`);

      const pos = domain.position || {};
      const slot = SLOT_POSITIONS[pos.slot] || SLOT_POSITIONS.top;
      const dist = Number(pos.distance) || 0.68;
      const offset = radius * dist;
      const x = slot.tx * offset;
      const y = slot.ty * offset;

      node.style.setProperty('--cd-node-x', `${x}px`);
      node.style.setProperty('--cd-node-y', `${y}px`);
      node.style.setProperty('--cd-node-scale', scaleForPriority(domain.priority));

      node.innerHTML = `
        <span class="cd-spatial-node-label">${esc(domain.label)}</span>
        <span class="cd-spatial-node-summary">${esc(domain.summary && domain.summary.compressed)}</span>
        ${domain.transition ? `<span class="cd-spatial-node-badge">↑ ${esc(PRIORITY_LABELS[domain.transition.newState] || domain.transition.newState).toUpperCase()} BY MAX</span>` : ''}
        ${domain.intelligence && domain.intelligence.active ? '<span class="cd-spatial-node-glow" aria-hidden="true"></span>' : ''}
      `;

      if (domain.transition && !reducedMotion()) {
        node.classList.add('cd-spatial-node-animating');
        window.setTimeout(() => node.classList.remove('cd-spatial-node-animating'), 4200);
      }

      node.addEventListener('click', () => openDomainDrawer(domain.id));
      els.orbit.appendChild(node);
    }
  }

  function scaleForPriority(priority) {
    const map = { urgent: 1.08, elevated: 1.04, normal: 1, monitored: 0.94 };
    return map[priority] || 1;
  }

  function buildNodeClasses(domain) {
    const classes = ['cd-spatial-node', `cd-priority-${domain.priority || 'normal'}`];
    if (domain.intelligence && domain.intelligence.active) {
      classes.push('cd-intelligence-active');
    }
    if (domain.priority === 'monitored') classes.push('cd-spatial-node-muted');
    return classes.join(' ');
  }

  function renderListFallback(items) {
    if (!els.list) return;
    els.list.innerHTML = items
      .map((item) => {
        const priority = PRIORITY_LABELS[item.priority] || item.priority;
        return `
          <button type="button" class="cd-spatial-list-item cd-priority-${esc(item.priority)}" data-domain-id="${esc(item.id)}">
            <span class="cd-spatial-list-label">${esc(item.label)}</span>
            <span class="cd-spatial-list-priority">${esc(priority)}</span>
            <span class="cd-spatial-list-summary">${esc(item.summary)}</span>
            ${item.intelligence ? '<span class="cd-spatial-list-intel">New intelligence</span>' : ''}
          </button>`;
      })
      .join('');

    els.list.querySelectorAll('[data-domain-id]').forEach((btn) => {
      btn.addEventListener('click', () => openDomainDrawer(btn.dataset.domainId));
    });
  }

  function renderUnseenBanner(changes) {
    if (!els.unseenBanner) return;
    if (!changes.length) {
      els.unseenBanner.hidden = true;
      els.unseenBanner.innerHTML = '';
      return;
    }
    els.unseenBanner.hidden = false;
    els.unseenBanner.innerHTML = `
      <p class="cd-unseen-label">Since your last visit</p>
      <ul class="cd-unseen-list">
        ${changes
          .map(
            (c) =>
              `<li><strong>${esc(c.label)}</strong> ${c.direction === 'up' ? '↑' : '↓'} ${esc(c.summary)}</li>`
          )
          .join('')}
      </ul>`;
  }

  function findDomain(domainId) {
    if (!activeOverview) return null;
    return (activeOverview.domains || []).find((d) => d.id === domainId) || null;
  }

  function openDomainDrawer(domainId) {
    const domain = findDomain(domainId);
    if (!domain || !els.drawer) return;
    activeDomainId = domainId;

    if (els.drawerTitle) els.drawerTitle.textContent = domain.label;
    if (els.drawerEyebrow) {
      els.drawerEyebrow.textContent = PRIORITY_LABELS[domain.priority] || domain.priority;
    }
    if (els.drawerBody) els.drawerBody.innerHTML = renderDrawerBody(domain);

    els.drawer.hidden = false;
    document.body.classList.add('cd-domain-open');

    bindDrawerActions(domain);
  }

  function closeDomainDrawer() {
    if (!els.drawer) return;
    els.drawer.hidden = true;
    activeDomainId = null;
    document.body.classList.remove('cd-domain-open');
  }

  function bindDrawerActions(domain) {
    if (!els.drawerBody) return;

    els.drawerBody.querySelectorAll('[data-cd-explain]').forEach((btn) => {
      btn.addEventListener('click', () => openPriorityExplain(domain));
    });

    els.drawerBody.querySelectorAll('[data-cd-mission-id]').forEach((btn) => {
      btn.addEventListener('click', () => {
        if (handlers.onOpenMission) {
          handlers.onOpenMission(btn.dataset.cdMissionId);
        }
        closeDomainDrawer();
      });
    });

    els.drawerBody.querySelectorAll('[data-cd-rec-id]').forEach((btn) => {
      btn.addEventListener('click', () => {
        if (handlers.onReviewRecommendation) {
          handlers.onReviewRecommendation(btn.dataset.cdRecId);
        }
        closeDomainDrawer();
      });
    });

    if (els.drawerDiscuss) {
      els.drawerDiscuss.onclick = () => {
        if (handlers.onDiscussMax) {
          handlers.onDiscussMax({ domainId: domain.id, domain });
        }
        closeDomainDrawer();
      };
    }
  }

  function renderDrawerBody(domain) {
    const drawer = domain.drawer || {};
    const parts = [];

    if (domain.summary && domain.summary.lines.length) {
      parts.push(
        `<ul class="cd-domain-summary">${domain.summary.lines
          .map((l) => `<li>${esc(l)}</li>`)
          .join('')}</ul>`
      );
    }

    if (domain.transition) {
      parts.push(`
        <button type="button" class="cd-domain-explain-link" data-cd-explain>
          Elevated by Max — why this moved
        </button>`);
    }

    if (domain.id === 'campaigns' && drawer.groups) {
      parts.push(renderCampaignGroups(drawer.groups));
    } else if (domain.id === 'content' && drawer.recommendations) {
      parts.push(renderContentItems(drawer.recommendations));
    } else if (domain.id === 'acquisition') {
      parts.push(renderAcquisitionItems(drawer));
    } else if (domain.id === 'clients' && drawer.objectives) {
      parts.push(renderClientItems(drawer.objectives));
    }

    return parts.join('') || '<p class="cd-domain-empty">No additional detail right now.</p>';
  }

  function renderCampaignGroups(groups) {
    const sections = [
      ['needsAttention', 'Needs attention'],
      ['active', 'Active'],
      ['recent', 'Recent'],
      ['archived', 'Archived / Historical'],
    ];
    return sections
      .map(([key, title]) => {
        const items = groups[key] || [];
        if (!items.length) return '';
        return `
          <section class="cd-domain-group">
            <h3 class="cd-domain-group-title">${esc(title)}</h3>
            <ul class="cd-domain-items">
              ${items
                .map(
                  (m) => `
                <li>
                  <button type="button" class="cd-domain-item-btn" data-cd-mission-id="${esc(m.id)}">
                    <span class="cd-domain-item-title">${esc(m.title || 'Mission')}</span>
                    <span class="cd-domain-item-meta">${esc(m.statusLabel || m.status || '')}</span>
                  </button>
                </li>`
                )
                .join('')}
            </ul>
          </section>`;
      })
      .join('');
  }

  function renderContentItems(recs) {
    if (!recs.length) return '';
    return `
      <section class="cd-domain-group">
        <h3 class="cd-domain-group-title">Recommendations</h3>
        <ul class="cd-domain-items">
          ${recs
            .map(
              (r) => `
            <li>
              <button type="button" class="cd-domain-item-btn" data-cd-rec-id="${esc(r.id)}">
                <span class="cd-domain-item-title">${esc(r.title)}</span>
                <span class="cd-domain-item-meta">${esc(r.status)}${r.channel ? ` · ${esc(r.channel)}` : ''}</span>
              </button>
            </li>`
            )
            .join('')}
        </ul>
      </section>`;
  }

  function renderAcquisitionItems(drawer) {
    const parts = [];
    if (drawer.aoIntelligence) {
      parts.push('<p class="cd-domain-ao">AO Intelligence active</p>');
    }
    const priorities = drawer.priorityItems || [];
    if (priorities.length) {
      parts.push(`
        <section class="cd-domain-group">
          <h3 class="cd-domain-group-title">Priorities</h3>
          <ul class="cd-domain-items">${priorities
            .map((p) => `<li><span class="cd-domain-item-title">${esc(p.title)}</span></li>`)
            .join('')}</ul>
        </section>`);
    }
    const alerts = drawer.watchAlerts || [];
    if (alerts.length) {
      parts.push(`
        <section class="cd-domain-group">
          <h3 class="cd-domain-group-title">Market signals</h3>
          <ul class="cd-domain-items">${alerts
            .map((a) => `<li><span class="cd-domain-item-title">${esc(a.title)}</span></li>`)
            .join('')}</ul>
        </section>`);
    }
    return parts.join('');
  }

  function renderClientItems(objectives) {
    if (!objectives.length) return '';
    return `
      <section class="cd-domain-group">
        <h3 class="cd-domain-group-title">Client objectives</h3>
        <ul class="cd-domain-items">
          ${objectives
            .map(
              (o) => `
            <li>
              <span class="cd-domain-item-title">${esc(o.title)}</span>
              <span class="cd-domain-item-meta">${esc(o.status)}</span>
            </li>`
            )
            .join('')}
        </ul>
      </section>`;
  }

  function openPriorityExplain(domain) {
    if (!els.explain || !els.explainBody || !domain.transition) return;
    const t = domain.transition;
    els.explainBody.innerHTML = `
      <div class="cd-explain-block">
        <p class="cd-explain-kicker">Changed</p>
        <p>${esc(PRIORITY_LABELS[t.previousState] || t.previousState)} → ${esc(PRIORITY_LABELS[t.newState] || t.newState)}</p>
      </div>
      <div class="cd-explain-block">
        <p class="cd-explain-kicker">Why Max elevated it</p>
        <p>${esc(t.reason)}</p>
      </div>
      ${
        t.evidenceRefs && t.evidenceRefs.length
          ? `<div class="cd-explain-block"><p class="cd-explain-kicker">Evidence</p><ul>${t.evidenceRefs
              .map((e) => `<li>${esc(e.label || e.kind)}</li>`)
              .join('')}</ul></div>`
          : ''
      }
      <p class="cd-explain-time">Changed ${esc(formatTime(t.changedAt))}</p>`;
    els.explain.hidden = false;
  }

  function closePriorityExplain() {
    if (els.explain) els.explain.hidden = true;
  }

  function formatTime(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return new Intl.DateTimeFormat(undefined, {
      hour: 'numeric',
      minute: '2-digit',
    }).format(d);
  }

  function setListView(enabled, options = {}) {
    listView = Boolean(enabled);
    if (els.deck) els.deck.classList.toggle('cd-spatial-list-mode', listView);
    if (els.list) els.list.hidden = !listView;
    if (els.orbit && els.orbit.parentElement) {
      els.orbit.parentElement.hidden = listView;
    }
    if (els.viewToggle) {
      els.viewToggle.textContent = listView ? 'View spatially' : 'View as list';
      els.viewToggle.setAttribute('aria-pressed', listView ? 'true' : 'false');
    }
    if (!options.silent) {
      try {
        localStorage.setItem('pulseforge.commandDeck.viewMode', listView ? 'list' : 'spatial');
      } catch (_) {
        /* ignore */
      }
    }
  }

  function restoreViewPreference() {
    try {
      const pref = localStorage.getItem('pulseforge.commandDeck.viewMode');
      if (pref === 'list') setListView(true, { silent: true });
    } catch (_) {
      /* ignore */
    }
  }

  function renderRecentConversations() {
    if (!els.recentSelect) return;
    let entries = [];
    try {
      entries = JSON.parse(localStorage.getItem('pulseforge.max.recentConversations') || '[]');
    } catch (_) {
      entries = [];
    }
    if (!entries.length) {
      els.recentSelect.hidden = true;
      return;
    }
    els.recentSelect.hidden = false;
    els.recentSelect.innerHTML = `<option value="">Recent ▾</option>${entries
      .slice(0, 8)
      .map(
        (e) =>
          `<option value="${esc(e.id || e.title)}">${esc(e.title || 'Conversation')} · ${esc(e.when || '')}</option>`
      )
      .join('')}`;
  }

  function recordConversation(title) {
    if (!title) return;
    let entries = [];
    try {
      entries = JSON.parse(localStorage.getItem('pulseforge.max.recentConversations') || '[]');
    } catch (_) {
      entries = [];
    }
    entries.unshift({
      id: String(Date.now()),
      title: String(title).slice(0, 80),
      when: 'Today',
    });
    entries = entries.slice(0, 12);
    try {
      localStorage.setItem('pulseforge.max.recentConversations', JSON.stringify(entries));
    } catch (_) {
      /* ignore */
    }
    renderRecentConversations();
  }

  function hide() {
    if (els.deck) els.deck.hidden = true;
  }

  function clear() {
    hide();
    activeOverview = null;
    activeDomainId = null;
    if (els.orbit) {
      els.orbit.querySelectorAll('.cd-spatial-node').forEach((n) => n.remove());
    }
    if (els.list) els.list.innerHTML = '';
    if (els.unseenBanner) {
      els.unseenBanner.hidden = true;
      els.unseenBanner.innerHTML = '';
    }
  }

  function init(options = {}) {
    bindElements();
    handlers.onDiscussMax = options.onDiscussMax || null;
    handlers.onOpenMission = options.onOpenMission || null;
    handlers.onReviewRecommendation = options.onReviewRecommendation || null;
    handlers.onAskMax = options.onAskMax || null;
    handlers.escapeHtml = options.escapeHtml || null;
    handlers.prefersReducedMotion = options.prefersReducedMotion || null;

    restoreViewPreference();

    if (els.viewToggle) {
      els.viewToggle.addEventListener('click', () => setListView(!listView));
    }

    document.querySelectorAll('[data-cd-domain-close]').forEach((node) => {
      node.addEventListener('click', closeDomainDrawer);
    });
    document.querySelectorAll('[data-cd-explain-close]').forEach((node) => {
      node.addEventListener('click', closePriorityExplain);
    });

    if (els.recentSelect) {
      els.recentSelect.addEventListener('change', () => {
        const val = els.recentSelect.value;
        if (val && handlers.onAskMax) handlers.onAskMax({ prompt: null, conversationId: val });
        els.recentSelect.value = '';
      });
    }

    document.addEventListener('keydown', (event) => {
      if (event.key !== 'Escape') return;
      if (els.explain && !els.explain.hidden) {
        closePriorityExplain();
        return;
      }
      if (els.drawer && !els.drawer.hidden) closeDomainDrawer();
    });
  }

  window.SpatialDeck = {
    init,
    render,
    hide,
    clear,
    openDomainDrawer,
    closeDomainDrawer,
    recordConversation,
    setListView,
    findDomain,
    getActiveDomainId: () => activeDomainId,
  };
})();
