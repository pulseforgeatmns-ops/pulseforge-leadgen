'use strict';

/**
 * SPEC-097 / SPEC-097A / SPEC-097B Living Command Deck — spatial intelligence field renderer.
 * Render-only consumer of model.spatialOverview from CommandDeckModel.
 * Motion communicates intelligence state — it does not decorate the interface.
 * SPEC-097B: presentation-only spatial composition (orbit, halo, protected zone).
 */

(function () {
  const PRIORITY_LABELS = Object.freeze({
    monitored: 'Monitored',
    normal: 'Normal',
    elevated: 'Elevated',
    urgent: 'Urgent',
  });

  const PRIORITY_RANK = Object.freeze({
    monitored: 0,
    normal: 1,
    elevated: 2,
    urgent: 3,
  });

  const SLOT_POSITIONS = Object.freeze({
    top: { tx: 0, ty: -1 },
    left: { tx: -1, ty: 0 },
    right: { tx: 1, ty: 0 },
    bottom: { tx: 0, ty: 1 },
  });

  const ELEVATION_MS = 900;
  const SETTLE_MS = 1100;
  const SIGNAL_MS = 8000;
  const HALO_EXTENT_PX = 26;
  const PROTECTED_GAP_PX = 18;
  const NODE_ESTIMATE = Object.freeze({ w: 132, h: 72 });

  /** Desktop center-to-center band targets (SPEC-097B). Scaled to viewport. */
  const BAND_DESKTOP = Object.freeze({
    monitored: 280,
    normal: 235,
    elevated: 178,
    urgent: 142,
  });

  /** @type {object|null} */
  let activeOverview = null;
  /** @type {string|null} */
  let activeDomainId = null;
  /** @type {boolean} */
  let listView = false;
  /** @type {Map<string, HTMLElement>} */
  let nodeByDomain = new Map();
  /** @type {number|null} */
  let signalTimer = null;
  /** @type {number} */
  let signalCursor = 0;
  /** @type {Set<string>} */
  let signaledTransitions = new Set();
  /** @type {string|null} */
  let lastMaxSignature = null;

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
    els.canvas = document.getElementById('cdSpatialCanvas');
    els.connections = document.getElementById('cdIntelConnections');
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
    const layout = computeFieldLayout();
    return layout && layout.bands ? layout.bands.normal : BAND_DESKTOP.normal;
  }

  function scaleForPriority(priority) {
    const map = { urgent: 1.08, elevated: 1.04, normal: 1, monitored: 0.9 };
    return map[priority] || 1;
  }

  function readCssLength(name, fallback) {
    const root = getComputedStyle(document.documentElement);
    const val = root.getPropertyValue(name).trim();
    if (val.endsWith('px')) {
      const n = Number.parseFloat(val);
      if (Number.isFinite(n)) return n;
    }
    return fallback;
  }

  /**
   * Viewport-aware priority bands with a protected Max zone.
   * Invariant: monitored > normal > elevated > urgent > protected halo + gap.
   */
  function computeFieldLayout() {
    const orbit = els.orbit ? els.orbit.getBoundingClientRect() : { width: 960, height: 700 };
    const maxEl = document.getElementById('cdSpatialMax');
    const maxRect = maxEl
      ? maxEl.getBoundingClientRect()
      : { width: 148, height: 178 };

    const halfW = Math.max(orbit.width / 2, 1);
    const halfH = Math.max(orbit.height / 2, 1);
    const protectedRx = maxRect.width / 2 + HALO_EXTENT_PX;
    const protectedRy = maxRect.height / 2 + HALO_EXTENT_PX;
    const gap = readCssLength('--cd-protected-gap', PROTECTED_GAP_PX);

    const desktop = {
      monitored: readCssLength('--cd-band-monitored', BAND_DESKTOP.monitored),
      normal: readCssLength('--cd-band-normal', BAND_DESKTOP.normal),
      elevated: readCssLength('--cd-band-elevated', BAND_DESKTOP.elevated),
      urgent: readCssLength('--cd-band-urgent', BAND_DESKTOP.urgent),
    };

    const nodeClear = Math.max(NODE_ESTIMATE.w, NODE_ESTIMATE.h) / 2 + 10;
    const usable = Math.min(halfW, halfH) - nodeClear;
    const scale = Math.min(1, Math.max(0.58, usable / desktop.monitored));

    const minUrgent = Math.max(
      protectedRx + NODE_ESTIMATE.w / 2 + gap,
      protectedRy + NODE_ESTIMATE.h / 2 + gap
    );

    const bands = {
      urgent: Math.max(desktop.urgent * scale, minUrgent),
      elevated: desktop.elevated * scale,
      normal: desktop.normal * scale,
      monitored: desktop.monitored * scale,
    };

    const bandGap = Math.max(24 * scale, 18);
    bands.elevated = Math.max(bands.elevated, bands.urgent + bandGap);
    bands.normal = Math.max(bands.normal, bands.elevated + bandGap * 1.15);
    bands.monitored = Math.max(bands.monitored, bands.normal + bandGap);

    const maxFit = Math.min(halfW, halfH) - nodeClear;
    if (bands.monitored > maxFit && maxFit > bands.urgent + bandGap * 3) {
      const extra = bands.monitored - bands.urgent;
      const fitExtra = maxFit - bands.urgent;
      const s = fitExtra / extra;
      bands.elevated = bands.urgent + (bands.elevated - bands.urgent) * s;
      bands.normal = bands.urgent + (bands.normal - bands.urgent) * s;
      bands.monitored = bands.urgent + (bands.monitored - bands.urgent) * s;
    }

    return {
      halfW,
      halfH,
      protectedRx,
      protectedRy,
      gap,
      bands,
      scale,
      maxRect,
      orbit,
    };
  }

  function offsetForDomain(domain, layout, nodeRect) {
    const pos = domain.position || {};
    const slot = SLOT_POSITIONS[pos.slot] || SLOT_POSITIONS.top;
    const priority = domain.priority || 'normal';
    let dist = layout.bands[priority] || layout.bands.normal;

    const nw = (nodeRect && nodeRect.width) || NODE_ESTIMATE.w;
    const nh = (nodeRect && nodeRect.height) || NODE_ESTIMATE.h;
    const prot =
      slot.tx !== 0
        ? layout.protectedRx + nw / 2 + layout.gap
        : layout.protectedRy + nh / 2 + layout.gap;
    dist = Math.max(dist, prot);

    const maxDist =
      slot.tx !== 0 ? layout.halfW - nw / 2 - 8 : layout.halfH - nh / 2 - 8;
    if (maxDist > prot) dist = Math.min(dist, maxDist);

    return { x: slot.tx * dist, y: slot.ty * dist, dist };
  }

  /**
   * Max communicates aggregate intelligence state — never domain activity counts.
   */
  function synthesizeMaxCopy(overview) {
    const domains = (overview && overview.domains) || [];
    const areaCount = domains.length;
    const attention = domains.filter(
      (d) => d.priority === 'urgent' || d.priority === 'elevated'
    );
    const n = attention.length;

    let headline;
    let subline;

    if (n === 0) {
      headline = 'Briefing current';
      subline = areaCount
        ? `Watching ${areaCount} area${areaCount === 1 ? '' : 's'}`
        : '';
    } else {
      headline = areaCount
        ? `Watching ${areaCount} area${areaCount === 1 ? '' : 's'}`
        : 'Briefing current';
      subline =
        n === 1 ? '1 area needs your attention' : `${n} areas need your attention`;
    }

    return { headline, subline };
  }

  function nodeSummaryText(domain) {
    const lines = (domain.summary && domain.summary.lines) || [];
    const compressed = (domain.summary && domain.summary.compressed) || '';
    const compact = compressed
      .split(' · ')
      .filter((part) => part && !/historical|contained/i.test(part))
      .slice(0, 2)
      .join(' · ');

    if (domain.priority === 'normal' || domain.priority === 'monitored') {
      return (lines[0] || compact || compressed).trim();
    }
    return (compact || lines[0] || compressed).trim();
  }

  function ellipseEdge(cx, cy, rx, ry, tx, ty) {
    const dx = tx - cx;
    const dy = ty - cy;
    const len = Math.hypot(dx, dy) || 1;
    const nx = dx / len;
    const ny = dy / len;
    const denom = Math.sqrt((nx * nx) / (rx * rx) + (ny * ny) / (ry * ry)) || 1;
    const t = 1 / denom;
    return { x: cx + nx * t, y: cy + ny * t };
  }

  function rectEdge(cx, cy, halfW, halfH, fromX, fromY) {
    const dx = cx - fromX;
    const dy = cy - fromY;
    if (dx === 0 && dy === 0) return { x: cx, y: cy };
    const px = dx === 0 ? Infinity : halfW / Math.abs(dx);
    const py = dy === 0 ? Infinity : halfH / Math.abs(dy);
    const t = Math.min(px, py);
    return { x: cx - dx * t, y: cy - dy * t };
  }

  function isElevationTransition(transition) {
    if (!transition) return false;
    const prev = PRIORITY_RANK[transition.previousState] ?? 1;
    const next = PRIORITY_RANK[transition.newState] ?? 1;
    return next > prev;
  }

  function isDeElevationTransition(transition) {
    if (!transition) return false;
    const prev = PRIORITY_RANK[transition.previousState] ?? 1;
    const next = PRIORITY_RANK[transition.newState] ?? 1;
    return next < prev;
  }

  /**
   * @param {object} overview - spatialOverview from CommandDeckModel
   */
  function render(overview) {
    if (!els.deck) bindElements();
    if (!overview || !els.deck) return false;

    activeOverview = overview;
    els.deck.hidden = false;

    renderMaxAnchor(overview);
    renderDomains(overview.domains || []);
    renderConnections(overview.domains || []);
    requestAnimationFrame(() => {
      if (activeOverview === overview && !listView) {
        renderConnections(overview.domains || []);
      }
    });
    renderListFallback(overview.listFallback || overview.domains || []);
    renderUnseenBanner(overview.unseenChanges || []);
    updateAskPlaceholder(overview.maxAnchor, activeDomainId);
    renderRecentConversations();
    syncViewVisibility();
    syncSignalLoop(overview.domains || []);
    maybePulseMax(overview);

    if (window.matchMedia('(max-width: 640px)').matches) {
      setListView(true, { silent: true });
    }

    return true;
  }

  function renderMaxAnchor(overview) {
    const copy = synthesizeMaxCopy(overview);
    if (els.maxHeadline) {
      els.maxHeadline.textContent = copy.headline || 'Max';
    }
    if (els.maxSubline) {
      els.maxSubline.textContent = copy.subline || '';
      els.maxSubline.hidden = !copy.subline;
    }
  }

  function updateAskPlaceholder(anchor, domainId) {
    if (!els.askInput) return;
    const domain = domainId ? findDomain(domainId) : null;
    if (domain) {
      els.askInput.placeholder = `Ask Max about ${domain.label}…`;
      return;
    }
    if (anchor && anchor.askPlaceholder) {
      els.askInput.placeholder = anchor.askPlaceholder;
      return;
    }
    els.askInput.placeholder = 'Ask Max…';
  }

  function buildNodeAriaLabel(domain) {
    const parts = [
      domain.label,
      PRIORITY_LABELS[domain.priority] || domain.priority,
      domain.summary && domain.summary.compressed,
    ].filter(Boolean);
    if (domain.transition) {
      parts.push(`Priority changed to ${PRIORITY_LABELS[domain.transition.newState] || domain.transition.newState}`);
    }
    if (domain.intelligence && domain.intelligence.active) {
      parts.push('New intelligence');
    }
    return parts.join('. ');
  }

  function renderDomains(domains) {
    if (!els.orbit) return;

    const layout = computeFieldLayout();
    const seen = new Set();

    for (const domain of domains) {
      seen.add(domain.id);
      let node = nodeByDomain.get(domain.id);
      const isNew = !node;
      if (!node) {
        node = document.createElement('button');
        node.type = 'button';
        node.dataset.domainId = domain.id;
        node.addEventListener('click', () => openDomainDrawer(domain.id));
        els.orbit.appendChild(node);
        nodeByDomain.set(domain.id, node);
      }

      node.className = buildNodeClasses(domain) + (isNew ? ' cd-spatial-node-placing' : '');
      node.setAttribute('aria-label', buildNodeAriaLabel(domain));

      const measured = isNew ? null : node.getBoundingClientRect();
      const offset = offsetForDomain(domain, layout, measured);
      node.style.setProperty('--cd-node-x', `${offset.x}px`);
      node.style.setProperty('--cd-node-y', `${offset.y}px`);
      node.style.setProperty('--cd-node-scale', String(scaleForPriority(domain.priority)));

      const priorityLabel = PRIORITY_LABELS[domain.priority] || domain.priority;
      const showPriorityLabel =
        domain.priority === 'urgent' ||
        domain.priority === 'elevated' ||
        domain.priority === 'monitored';

      node.innerHTML = `
        <span class="cd-spatial-node-label">${esc(domain.label)}</span>
        ${showPriorityLabel ? `<span class="cd-spatial-node-priority">${esc(priorityLabel)}</span>` : ''}
        <span class="cd-spatial-node-summary">${esc(nodeSummaryText(domain))}</span>
        ${domain.transition && isElevationTransition(domain.transition)
          ? `<span class="cd-spatial-node-badge">↑ ${esc(PRIORITY_LABELS[domain.transition.newState] || domain.transition.newState).toUpperCase()} BY MAX</span>`
          : ''}
        ${domain.intelligence && domain.intelligence.active ? '<span class="cd-spatial-node-glow" aria-hidden="true"></span>' : ''}
      `;

      applyTransitionAnimation(node, domain);
      if (isNew) {
        requestAnimationFrame(() => node.classList.remove('cd-spatial-node-placing'));
      }
    }

    nodeByDomain.forEach((node, id) => {
      if (seen.has(id)) return;
      node.remove();
      nodeByDomain.delete(id);
    });

    // Second pass: enforce protected zone with measured node geometry.
    const refined = computeFieldLayout();
    for (const domain of domains) {
      const node = nodeByDomain.get(domain.id);
      if (!node) continue;
      const offset = offsetForDomain(domain, refined, node.getBoundingClientRect());
      node.style.setProperty('--cd-node-x', `${offset.x}px`);
      node.style.setProperty('--cd-node-y', `${offset.y}px`);
    }
  }

  function applyTransitionAnimation(node, domain) {
    if (reducedMotion() || !domain.transition) return;
    const sig = `${domain.transition.previousState}:${domain.transition.newState}:${domain.transition.changedAt || ''}`;
    if (node.dataset.transitionSig === sig) return;
    node.dataset.transitionSig = sig;

    if (isElevationTransition(domain.transition)) {
      node.classList.add('cd-spatial-node-elevating');
      window.setTimeout(() => node.classList.remove('cd-spatial-node-elevating'), ELEVATION_MS + 200);
    } else if (isDeElevationTransition(domain.transition)) {
      node.classList.add('cd-spatial-node-settling');
      window.setTimeout(() => node.classList.remove('cd-spatial-node-settling'), SETTLE_MS + 200);
    }
  }

  function buildNodeClasses(domain) {
    const classes = ['cd-spatial-node', `cd-priority-${domain.priority || 'normal'}`];
    if (domain.intelligence && domain.intelligence.active) {
      classes.push('cd-intelligence-active');
    }
    if (domain.priority === 'monitored') classes.push('cd-spatial-node-muted');
    if (activeDomainId === domain.id) classes.push('cd-spatial-node-focused');
    return classes.join(' ');
  }

  function renderConnections(domains) {
    if (!els.connections || !els.orbit) return;

    const svg = els.connections;
    svg.innerHTML = '';

    const canvasRect = els.canvas
      ? els.canvas.getBoundingClientRect()
      : els.orbit.getBoundingClientRect();
    const orbitRect = els.orbit.getBoundingClientRect();
    if (!orbitRect.width) return;

    svg.setAttribute('viewBox', `0 0 ${orbitRect.width} ${orbitRect.height}`);
    svg.setAttribute('width', String(orbitRect.width));
    svg.setAttribute('height', String(orbitRect.height));
    svg.style.left = `${orbitRect.left - canvasRect.left}px`;
    svg.style.top = `${orbitRect.top - canvasRect.top}px`;
    svg.style.width = `${orbitRect.width}px`;
    svg.style.height = `${orbitRect.height}px`;

    const maxEl = document.getElementById('cdSpatialMax');
    const maxRect = maxEl ? maxEl.getBoundingClientRect() : orbitRect;
    const cx = maxRect.left + maxRect.width / 2 - orbitRect.left;
    const cy = maxRect.top + maxRect.height / 2 - orbitRect.top;
    const haloRx = maxRect.width / 2 + HALO_EXTENT_PX;
    const haloRy = maxRect.height / 2 + HALO_EXTENT_PX;

    for (const domain of domains) {
      const node = nodeByDomain.get(domain.id);
      if (!node) continue;
      const nr = node.getBoundingClientRect();
      const nx = nr.left + nr.width / 2 - orbitRect.left;
      const ny = nr.top + nr.height / 2 - orbitRect.top;

      const start = ellipseEdge(cx, cy, haloRx, haloRy, nx, ny);
      const end = rectEdge(nx, ny, nr.width / 2, nr.height / 2, cx, cy);

      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      const d = `M ${start.x.toFixed(1)} ${start.y.toFixed(1)} L ${end.x.toFixed(1)} ${end.y.toFixed(1)}`;
      path.setAttribute('d', d);
      path.setAttribute('id', `cd-conn-${domain.id}`);
      path.classList.add('cd-intel-conn');

      if (domain.priority === 'urgent') path.classList.add('cd-intel-conn-urgent');
      else if (domain.priority === 'elevated') path.classList.add('cd-intel-conn-elevated');

      const isActive =
        domain.priority === 'urgent' ||
        (domain.transition && isElevationTransition(domain.transition)) ||
        (domain.intelligence && domain.intelligence.active) ||
        activeDomainId === domain.id;

      if (isActive) path.classList.add('cd-intel-conn-active');

      svg.appendChild(path);

      if (domain.transition && isElevationTransition(domain.transition) && !reducedMotion()) {
        const key = `${domain.id}:${domain.transition.previousState}:${domain.transition.newState}:${domain.transition.changedAt || ''}`;
        if (!signaledTransitions.has(key)) {
          signaledTransitions.add(key);
          spawnSignalOnPath(path, d);
        }
      }
    }
  }

  function spawnSignalOnPath(path, d) {
    if (!path || !path.parentNode || reducedMotion()) return;
    const signal = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    signal.setAttribute('r', '2.6');
    signal.classList.add('cd-intel-conn-signal');
    signal.style.offsetPath = `path('${d}')`;
    signal.style.offsetRotate = '0deg';
    path.parentNode.appendChild(signal);
    requestAnimationFrame(() => signal.classList.add('cd-intel-signal-travel'));
    window.setTimeout(() => signal.remove(), ELEVATION_MS + 80);
  }

  function travelSignal(domainId) {
    if (!els.connections || reducedMotion()) return;
    const path = els.connections.querySelector(`#cd-conn-${domainId}`);
    if (!path) return;
    path.classList.add('cd-intel-conn-active');
    spawnSignalOnPath(path, path.getAttribute('d'));
    window.setTimeout(() => {
      if (activeDomainId !== domainId) {
        const domain = findDomain(domainId);
        if (!domain || domain.priority !== 'urgent') {
          path.classList.remove('cd-intel-conn-active');
        }
      }
    }, ELEVATION_MS + 400);
  }

  function pulseMaxHalo() {
    const max = document.getElementById('cdSpatialMax');
    if (!max || reducedMotion()) return;
    max.classList.remove('cd-max-signaling');
    void max.offsetWidth;
    max.classList.add('cd-max-signaling');
    window.setTimeout(() => max.classList.remove('cd-max-signaling'), 1800);
  }

  function intelligenceSignature(overview) {
    const domains = (overview && overview.domains) || [];
    return domains
      .map((d) => {
        const t = d.transition
          ? `${d.transition.previousState}:${d.transition.newState}:${d.transition.changedAt || ''}`
          : '';
        const intel = d.intelligence && d.intelligence.active ? '1' : '0';
        return `${d.id}:${d.priority}:${intel}:${t}`;
      })
      .join('|');
  }

  function maybePulseMax(overview) {
    const sig = intelligenceSignature(overview);
    if (lastMaxSignature && sig !== lastMaxSignature) {
      const elevatedNow = (overview.domains || []).some(
        (d) =>
          (d.transition && isElevationTransition(d.transition)) ||
          (d.intelligence && d.intelligence.active)
      );
      if (elevatedNow) pulseMaxHalo();
    }
    lastMaxSignature = sig;
  }

  function stopSignalLoop() {
    if (signalTimer) {
      window.clearInterval(signalTimer);
      signalTimer = null;
    }
  }

  function syncSignalLoop(domains) {
    stopSignalLoop();
    if (reducedMotion() || listView) return;
    const urgentIds = (domains || [])
      .filter((d) => d.priority === 'urgent')
      .map((d) => d.id);
    if (!urgentIds.length) return;

    signalTimer = window.setInterval(() => {
      if (document.hidden || reducedMotion() || listView) return;
      const id = urgentIds[signalCursor % urgentIds.length];
      signalCursor += 1;
      travelSignal(id);
    }, SIGNAL_MS);
  }

  function triggerConnectionSignal(domainId) {
    travelSignal(domainId);
  }

  function renderListFallback(items) {
    if (!els.list) return;
    els.list.innerHTML = items
      .map((item) => {
        const priority = PRIORITY_LABELS[item.priority] || item.priority;
        const changed = item.transition
          ? ` · Changed to ${PRIORITY_LABELS[item.transition.newState] || item.transition.newState}`
          : '';
        return `
          <button type="button" class="cd-spatial-list-item cd-priority-${esc(item.priority)}" data-domain-id="${esc(item.id)}" aria-label="${esc(item.label)}, ${esc(priority)}${changed}">
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

  function setDomainFocus(domainId) {
    nodeByDomain.forEach((node, id) => {
      node.classList.toggle('cd-spatial-node-focused', id === domainId);
    });
    if (activeOverview) renderConnections(activeOverview.domains || []);
  }

  function openDomainDrawer(domainId) {
    const domain = findDomain(domainId);
    if (!domain || !els.drawer) return;
    activeDomainId = domainId;
    setDomainFocus(domainId);
    updateAskPlaceholder(activeOverview && activeOverview.maxAnchor, domainId);

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
    setDomainFocus(null);
    updateAskPlaceholder(activeOverview && activeOverview.maxAnchor, null);
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

  function syncViewVisibility() {
    if (!els.deck) return;
    if (els.list) els.list.hidden = !listView;
    if (els.canvas) els.canvas.hidden = listView;
  }

  function setListView(enabled, options = {}) {
    listView = Boolean(enabled);
    if (els.deck) els.deck.classList.toggle('cd-spatial-list-mode', listView);
    syncViewVisibility();
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
    if (!listView && activeOverview) {
      renderConnections(activeOverview.domains || []);
      syncSignalLoop(activeOverview.domains || []);
    } else {
      stopSignalLoop();
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

  function setupIntelFieldPause() {
    const pause = () => document.body.classList.add('cd-intel-paused');
    const resume = () => document.body.classList.remove('cd-intel-paused');

    document.addEventListener('visibilitychange', () => {
      if (document.hidden) {
        pause();
        stopSignalLoop();
      } else {
        resume();
        if (activeOverview && !listView) syncSignalLoop(activeOverview.domains || []);
      }
    });

    if (reducedMotion()) pause();
  }

  function setupResizeHandler() {
    let resizeTimer = null;
    window.addEventListener('resize', () => {
      if (resizeTimer) window.clearTimeout(resizeTimer);
      resizeTimer = window.setTimeout(() => {
        if (activeOverview && !listView) {
          renderDomains(activeOverview.domains || []);
          renderConnections(activeOverview.domains || []);
        }
      }, 120);
    });
  }

  function hide() {
    if (els.deck) els.deck.hidden = true;
  }

  function clear() {
    hide();
    stopSignalLoop();
    activeOverview = null;
    activeDomainId = null;
    lastMaxSignature = null;
    signaledTransitions = new Set();
    nodeByDomain = new Map();
    if (els.orbit) {
      els.orbit.querySelectorAll('.cd-spatial-node').forEach((n) => n.remove());
    }
    if (els.connections) els.connections.innerHTML = '';
    if (els.list) els.list.innerHTML = '';
    if (els.unseenBanner) {
      els.unseenBanner.hidden = true;
      els.unseenBanner.innerHTML = '';
    }
    document.body.classList.remove('cd-domain-open', 'cd-intel-paused');
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
    setupIntelFieldPause();
    setupResizeHandler();

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
